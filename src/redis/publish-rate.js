/**
 * Cluster-wide publish-rate aggregator for svelte-adapter-uws.
 *
 * Each instance broadcasts its own per-topic slice (sampled from
 * `platform.pressure.topPublishers`) on a Redis pub/sub channel; every
 * instance maintains a sliding-window view of all instances' slices and
 * merges them into a cluster-wide top-N. No leader election - each
 * instance is its own aggregator. Storage cost is `O(instances * topN)`
 * per instance, bounded and small.
 *
 * Why this exists: `platform.pressure.topPublishers` is per-instance.
 * In an N-instance cluster, a topic that's hot across every instance
 * looks the same locally as one that's only hot here - but it warrants
 * a different response. Cluster-wide pressure is genuine load; local-only
 * is a hot-shard signal. The cluster `topPublisher` admission rule
 * (`shared/admission.js`) reads the merged view to shed at the cluster
 * layer; the prometheus `wireClusterPublishRateMetrics` wirer exposes
 * the merged view as gauges alongside the per-instance ones.
 *
 * Wire envelope on `{channel}`:
 *   `{instanceId, ts, slice: [{topic, messagesPerSec, bytesPerSec}, ...]}`
 *
 * Receivers merge into a per-instanceId map; entries older than
 * `staleAfter` are aged out on each merge.
 *
 * @module svelte-adapter-uws-extensions/redis/publish-rate
 */

import { assert } from '../shared/assert.js';
import { randomBytes, wallEpoch, setIntervalTimer, clearIntervalTimer } from '../shared/runtime.js';
import { MAX_AGGREGATOR_REMOTE_INSTANCES } from '../shared/caps.js';

/**
 * @typedef {Object} ClusterSubject
 * @property {string} topic
 * @property {number} count - Local subscriber count for this topic on the contributing instance.
 */

/**
 * @typedef {Object} PublishRateAggregatorOptions
 * @property {string} [channel='uws:pressure:rates'] - Redis pub/sub channel for slice broadcasts.
 * @property {number} [publishInterval=5000] - How often this instance broadcasts its slice (ms).
 * @property {number} [staleAfter=12000] - Drop a remote instance's slice from the merge if no fresher one arrives within this window (ms). Should be at least 2x `publishInterval` to tolerate a missed beat.
 * @property {number} [topN=20] - Cap on the per-instance slice and on the merged result. Bounds storage cost.
 * @property {() => ClusterSubject[]} [subjects] - Optional contributor for cluster-wide subscriber counts. Called fresh on every broadcast tick to gather this instance's per-topic subscriber list. Sorted descending by count and capped at `topN`. When wired, the broadcast envelope grows a `subs` field; `subscribersOf(topic)` returns the merged sum.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

/**
 * @typedef {Object} ClusterTopicRate
 * @property {string} topic
 * @property {number} messagesPerSec - Sum across contributing instances.
 * @property {number} bytesPerSec - Sum across contributing instances.
 * @property {number} contributingInstances - How many instances had this topic in their last broadcast slice.
 */

const DEFAULT_CHANNEL = 'uws:pressure:rates';
const DEFAULT_PUBLISH_INTERVAL_MS = 5000;
const DEFAULT_STALE_AFTER_MS = 12000;
const DEFAULT_TOP_N = 20;
const DEFAULT_TOPIC_RATE_PER_SEC = 5000;

export function createPublishRateAggregator(client, options = {}) {
	if (!client || !client.redis) {
		throw new Error('publish-rate: client (from createRedisClient) is required');
	}
	const channel = options.channel || DEFAULT_CHANNEL;
	const publishInterval = options.publishInterval ?? DEFAULT_PUBLISH_INTERVAL_MS;
	if (typeof publishInterval !== 'number' || !Number.isFinite(publishInterval) || publishInterval < 1) {
		throw new Error('publish-rate: publishInterval must be a positive number (ms)');
	}
	const staleAfter = options.staleAfter ?? DEFAULT_STALE_AFTER_MS;
	if (typeof staleAfter !== 'number' || !Number.isFinite(staleAfter) || staleAfter < 1) {
		throw new Error('publish-rate: staleAfter must be a positive number (ms)');
	}
	const topN = options.topN ?? DEFAULT_TOP_N;
	if (!Number.isInteger(topN) || topN < 1) {
		throw new Error('publish-rate: topN must be a positive integer');
	}
	// Cluster-wide per-topic messages/sec threshold for onPublishRate crossings.
	// Mirrors the adapter's pressure.topicPublishRatePerSec (default 5000); the
	// difference is that this rate is the cluster-wide SUM across instances. Set
	// false to disable crossing callbacks.
	const topicRateThreshold = options.topicPublishRatePerSec === false
		? null
		: (options.topicPublishRatePerSec ?? DEFAULT_TOPIC_RATE_PER_SEC);
	if (topicRateThreshold !== null &&
		(typeof topicRateThreshold !== 'number' || !Number.isFinite(topicRateThreshold) || topicRateThreshold < 0)) {
		throw new Error('publish-rate: topicPublishRatePerSec must be a non-negative number or false');
	}
	const subjects = options.subjects;
	if (subjects !== undefined && typeof subjects !== 'function') {
		throw new Error('publish-rate: subjects must be a function returning Array<{topic, count}>');
	}
	const breaker = options.breaker;

	const m = options.metrics;
	const mBroadcasts = m?.counter('cluster_publish_rate_broadcasts_total', 'Slices broadcast by this instance');
	const mReceived = m?.counter('cluster_publish_rate_received_total', 'Slices received from sibling instances');
	const mParseErrors = m?.counter('cluster_publish_rate_parse_errors_total', 'Malformed slice envelopes dropped on receive');
	const mInstanceCount = m?.gauge('cluster_publish_rate_instance_count', 'Number of sibling instances contributing slices (excluding self) at scrape time');

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;

	/**
	 * Per-remote-instance slice cache. Local slice is read fresh from
	 * platform on each merge so it's always current.
	 * @type {Map<string, { ts: number, slice: Array<{topic: string, messagesPerSec: number, bytesPerSec: number}> }>}
	 */
	const remoteSlices = new Map();

	/**
	 * Per-remote-instance subscriber-count cache. Same staleness rules as
	 * `remoteSlices`. Local count is read fresh from `subjects()` on each
	 * `subscribersOf(topic)` query so it's always current.
	 * @type {Map<string, { ts: number, subs: Map<string, number> }>}
	 */
	const remoteSubs = new Map();

	let activePlatform = null;
	let subscriber = null;
	let publishTimer = null;
	let activated = false;
	let remoteInstancesWarnFired = false;

	/**
	 * onPublishRate subscribers. Each holds a callback and the set of topics it
	 * has already seen at or above the configured `topicPublishRatePerSec`, so a
	 * topic fires once on the rising edge and re-arms only after it drops below.
	 * @type {Array<{ callback: (events: ClusterTopicRate[]) => void, over: Set<string> }>}
	 */
	const rateSubscribers = [];

	if (mInstanceCount) {
		mInstanceCount.collect(() => {
			pruneStale();
			mInstanceCount.set(remoteSlices.size);
		});
	}

	function pruneStale() {
		// Exact wall clock (not the 1Hz-cached `now`): the slice `ts` is stamped on
		// another instance and compared here, so it must be wall-clock (monotonic is
		// per-process and meaningless across instances) AND precise enough to honor a
		// sub-second staleAfter - the cached clock quantizes the delta to ~1s.
		const nowTs = wallEpoch();
		for (const [id, entry] of remoteSlices) {
			if (nowTs - entry.ts > staleAfter) remoteSlices.delete(id);
		}
		for (const [id, entry] of remoteSubs) {
			if (nowTs - entry.ts > staleAfter) remoteSubs.delete(id);
		}
	}

	function snapshotLocalSlice() {
		const top = activePlatform?.pressure?.topPublishers;
		if (!Array.isArray(top)) return [];
		return top.length > topN ? top.slice(0, topN) : top.slice();
	}

	function snapshotLocalSubjects() {
		if (!subjects) return null;
		let raw;
		try { raw = subjects(); } catch { return []; }
		if (!Array.isArray(raw)) return [];
		// Dedupe by topic (sum counts) and cap at topN by descending count.
		const merged = new Map();
		for (const s of raw) {
			if (!s || typeof s.topic !== 'string') continue;
			const c = Number(s.count) || 0;
			merged.set(s.topic, (merged.get(s.topic) || 0) + c);
		}
		const arr = [];
		for (const [topic, count] of merged) arr.push({ topic, count });
		arr.sort((a, b) => b.count - a.count);
		return arr.length > topN ? arr.slice(0, topN) : arr;
	}

	async function broadcastSlice() {
		if (!activated) return;
		const slice = snapshotLocalSlice();
		const subs = snapshotLocalSubjects();
		const payload = subs === null
			? { instanceId, ts: wallEpoch(), slice }
			: { instanceId, ts: wallEpoch(), slice, subs };
		const envelope = JSON.stringify(payload);
		try {
			await redis.publish(channel, envelope);
			breaker?.success();
			mBroadcasts?.inc();
		} catch (err) {
			breaker?.failure(err);
			// best-effort: a missed broadcast just means our slice is invisible
			// to the cluster for one tick; the next one will land.
		}
	}

	async function activate(platform) {
		if (activated) {
			activePlatform = platform || activePlatform;
			return;
		}
		activated = true;
		activePlatform = platform;

		subscriber = client.duplicate({ enableReadyCheck: false });
		subscriber.on('error', (err) => {
			console.error('[publish-rate] subscriber error:', err.message);
		});
		subscriber.on('message', (ch, raw) => {
			if (ch !== channel) return;
			let env;
			try { env = JSON.parse(raw); } catch { mParseErrors?.inc(); return; }
			if (!env || typeof env !== 'object') { mParseErrors?.inc(); return; }
			if (env.instanceId === instanceId) return; // echo suppression
			if (typeof env.instanceId !== 'string' || !Array.isArray(env.slice)) {
				mParseErrors?.inc();
				return;
			}
			assert(
				env.instanceId !== instanceId,
				'publish-rate.echo-suppression',
				{ instanceId: env.instanceId }
			);
			const ts = typeof env.ts === 'number' ? env.ts : wallEpoch();
			remoteSlices.set(env.instanceId, { ts, slice: env.slice });
			if (remoteSlices.size >= MAX_AGGREGATOR_REMOTE_INSTANCES && !remoteInstancesWarnFired) {
				remoteInstancesWarnFired = true;
				console.warn(
					'[publish-rate] aggregator is tracking ' + remoteSlices.size +
					' sibling instances. Cluster sizes past this threshold typically ' +
					'indicate a deployment misconfig or stale-instance leak; staleAfter ' +
					'pruning normally caps remote slice retention.\n' +
					'  See: https://svti.me/publish-rate-aggregator'
				);
			}
			if (Array.isArray(env.subs)) {
				const subsMap = new Map();
				for (const s of env.subs) {
					if (!s || typeof s.topic !== 'string') continue;
					const c = Number(s.count) || 0;
					subsMap.set(s.topic, (subsMap.get(s.topic) || 0) + c);
				}
				remoteSubs.set(env.instanceId, { ts, subs: subsMap });
			}
			mReceived?.inc();
		});
		await subscriber.subscribe(channel);

		// First broadcast on the next tick (so subscribers in this same
		// process can't race against an in-flight subscribe). Each tick also
		// evaluates threshold crossings over the freshly-merged view.
		publishTimer = setIntervalTimer(tick, publishInterval);
		if (publishTimer.unref) publishTimer.unref();
	}

	async function deactivate() {
		activated = false;
		if (publishTimer) {
			clearIntervalTimer(publishTimer);
			publishTimer = null;
		}
		if (subscriber) {
			const sub = subscriber;
			subscriber = null;
			try { await sub.quit(); } catch { try { sub.disconnect(); } catch { /* ignore */ } }
		}
		remoteSlices.clear();
		remoteSubs.clear();
		// Reset each subscriber's over-set: after the merged view is torn down,
		// a re-activate should treat a still-hot topic as a fresh crossing
		// rather than suppress it against a stale pre-deactivate over-set.
		for (const sub of rateSubscribers) sub.over = new Set();
		activePlatform = null;
	}

	/**
	 * Merge the local slice (read fresh from platform.pressure.topPublishers)
	 * with the cached remote slices (stale dropped) into the full cluster-wide
	 * list, sorted descending by messagesPerSec. UNCAPPED: the top-N cap is
	 * applied by computeTopPublishers for the display / storage-bounded
	 * consumers; crossing detection needs the full set so a topic that stays
	 * above the threshold but is pushed out of the top-N display by hotter
	 * topics is not spuriously re-armed and re-fired.
	 */
	function computeMerged() {
		pruneStale();

		/** @type {Map<string, { topic: string, messagesPerSec: number, bytesPerSec: number, instances: Set<string> }>} */
		const merged = new Map();

		function add(slice, fromInstance) {
			if (!Array.isArray(slice)) return;
			for (const t of slice) {
				if (!t || typeof t.topic !== 'string') continue;
				const cur = merged.get(t.topic);
				if (cur) {
					cur.messagesPerSec += Number(t.messagesPerSec) || 0;
					cur.bytesPerSec += Number(t.bytesPerSec) || 0;
					cur.instances.add(fromInstance);
				} else {
					merged.set(t.topic, {
						topic: t.topic,
						messagesPerSec: Number(t.messagesPerSec) || 0,
						bytesPerSec: Number(t.bytesPerSec) || 0,
						instances: new Set([fromInstance])
					});
				}
			}
		}

		add(snapshotLocalSlice(), instanceId);
		for (const [id, entry] of remoteSlices) add(entry.slice, id);

		/** @type {ClusterTopicRate[]} */
		const result = [];
		for (const v of merged.values()) {
			result.push({
				topic: v.topic,
				messagesPerSec: v.messagesPerSec,
				bytesPerSec: v.bytesPerSec,
				contributingInstances: v.instances.size
			});
		}
		result.sort((a, b) => b.messagesPerSec - a.messagesPerSec);
		return result;
	}

	// Top-N-capped view for the display / storage-bounded consumers (the
	// topPublishers getter and rateOf). Crossing detection uses computeMerged
	// (uncapped) instead, so the edge-trigger contract holds beyond the top-N.
	function computeTopPublishers() {
		const result = computeMerged();
		return result.length > topN ? result.slice(0, topN) : result;
	}

	/**
	 * Lookup helper for the admission rule and other per-topic queries.
	 * Returns the cluster-wide messagesPerSec for a topic, summed across
	 * contributing instances, or 0 if the topic is not in the merged
	 * top-N. Pure memory lookup; no Redis traffic on the hot path.
	 */
	function rateOf(topic) {
		const merged = computeTopPublishers();
		for (const t of merged) if (t.topic === topic) return t.messagesPerSec;
		return 0;
	}

	/**
	 * Cluster-wide subscriber count for a topic, summed across this
	 * instance's live local count (read fresh from `subjects()` on every
	 * call) and all non-stale remote contributions. Returns 0 when no
	 * `subjects` callback was wired and no remote instance has reported
	 * the topic. Pure memory lookup; no Redis traffic on the hot path.
	 *
	 * Eventually-consistent within `publishInterval`; staleness bound by
	 * `staleAfter`. Callers needing exact counts should track a Redis SET
	 * cluster-wide and `SCARD` it instead.
	 */
	function subscribersOf(topic) {
		pruneStale();
		let total = 0;
		const local = snapshotLocalSubjects();
		if (local) {
			for (const s of local) if (s.topic === topic) total += s.count;
		}
		for (const entry of remoteSubs.values()) {
			const c = entry.subs.get(topic);
			if (c) total += c;
		}
		return total;
	}

	/**
	 * Register a callback that fires when one or more topics cross the configured
	 * cluster-wide `topicPublishRatePerSec` threshold. Mirrors the adapter's
	 * `platform.onPublishRate(cb)` (per-instance) at the cluster layer. Evaluated
	 * once per broadcast tick over the merged view, edge-triggered: a topic fires
	 * the tick it rises to or above the threshold and does not fire again until
	 * it drops below and rises once more. The callback receives the array of
	 * `ClusterTopicRate` entries that crossed on this tick; a throwing callback is
	 * contained (logged, never breaks the timer or a sibling). Returns an
	 * unsubscribe function. No-op (never fires) when `topicPublishRatePerSec` is
	 * `false`.
	 *
	 * @param {(events: ClusterTopicRate[]) => void} callback
	 * @returns {() => void}
	 */
	function onPublishRate(callback) {
		if (typeof callback !== 'function') {
			throw new Error('publish-rate: onPublishRate callback must be a function');
		}
		const sub = { callback, over: new Set() };
		rateSubscribers.push(sub);
		return function unsubscribe() {
			const i = rateSubscribers.indexOf(sub);
			if (i !== -1) rateSubscribers.splice(i, 1);
		};
	}

	// Edge-triggered crossing detection over the merged view. Runs once per
	// broadcast tick (never on the per-message receive path), no-op when no
	// subscriber is registered or the threshold is disabled.
	function evaluateCrossings() {
		if (rateSubscribers.length === 0 || topicRateThreshold === null) return;
		// UNCAPPED merged view (see computeMerged): a topic above threshold but
		// outside the top-N display must still be tracked, or the edge-trigger
		// would re-fire it when top-N churn pushes it out and back in.
		const overNow = computeMerged().filter((t) => t.messagesPerSec >= topicRateThreshold);
		const overTopics = new Set(overNow.map((t) => t.topic));
		// Snapshot subscribers so a callback that (un)subscribes mid-loop cannot
		// perturb this tick. A subscriber's over-set is updated BEFORE its callback
		// runs, so a throwing callback does not re-fire the same crossing next tick.
		for (const sub of rateSubscribers.slice()) {
			const crossings = overNow.filter((t) => !sub.over.has(t.topic));
			sub.over = overTopics;
			if (crossings.length > 0) {
				try {
					sub.callback(crossings);
				} catch (err) {
					console.error('[publish-rate] onPublishRate callback threw:', err?.message ?? err);
				}
			}
		}
	}

	// One scheduler tick: broadcast this instance's slice, then evaluate
	// threshold crossings over the freshly-merged view.
	async function tick() {
		await broadcastSlice();
		evaluateCrossings();
	}

	return {
		instanceId,
		activate,
		deactivate,
		get topPublishers() { return computeTopPublishers(); },
		rateOf,
		subscribersOf,
		onPublishRate,
		/** Force an immediate broadcast tick. Useful in tests; bypasses the timer. */
		_broadcastNow() { return broadcastSlice(); },
		/** Force an immediate crossing evaluation over the current merged view. Tests only. */
		_evaluateCrossingsNow() { return evaluateCrossings(); }
	};
}
