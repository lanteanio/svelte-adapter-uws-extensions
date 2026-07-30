/**
 * Cluster coordinator for topic broadcast-with-reply (`svelte-realtime`
 * `live.push({ topic })` / `live.notify({ topic })`).
 *
 * The adapter's `platform.requestTopic(topic, ...)` fans a request out to every
 * LOCAL subscriber socket of a topic and aggregates the per-subscriber replies.
 * In a cluster a topic's subscribers are spread across instances, so this module
 * fans the request out to every instance, has each run its OWN local
 * `requestTopic`, and aggregates all instances' replies back at the origin.
 *
 * The model, and why it differs from the smooth coordinator and the connection
 * registry:
 *
 *   - **Fan-in, not single-owner.** The smooth sync reaches exactly one owner
 *     and correlates exactly one reply; a userId request routes to exactly one
 *     owning instance. A topic broadcast is scatter-gather: EVERY instance is a
 *     potential responder, and the origin must aggregate ALL of their replies,
 *     not pick one. So the request rides a single shared channel that every
 *     instance subscribes to, and each instance answers with its own local
 *     aggregate (an empty array when it has no subscribers).
 *   - **Correct early-completion via an instance-presence set.** The origin does
 *     not know how many instances exist, so a naive collector would have to wait
 *     out the whole `timeoutMs` on every call - a multi-second floor on a
 *     primitive that usually resolves in milliseconds. Instead each coordinator
 *     records itself in a heartbeated Redis sorted set (`{prefix}{...}:instances`,
 *     score = wall clock). The origin ALWAYS publishes the request, and reads the
 *     live instance set only to decide WHEN to stop waiting: with known peers it
 *     finishes as soon as every one has answered (or at `timeoutMs`); with no
 *     known peers it still waits a short grace window for a peer whose presence
 *     write has not yet landed, rather than concluding instantly that it is alone.
 *     The set is a completion HINT, never a publish gate or a reply filter: every
 *     instance receives the broadcast over the shared channel and any reply that
 *     arrives before completion is collected, so a just-joined instance is not
 *     dropped. The only residual is a peer that is BOTH absent from the presence
 *     set AND slower to answer than the wait window - bounded to the sub-second
 *     gap between a fresh instance's start and its first presence write, and
 *     self-healing on the next call. A generous TTL (heartbeat x 3) keeps a
 *     GC-paused-but-live instance in the set.
 *   - **Single-slot presence on Redis Cluster.** The presence key carries a
 *     `{...}` hash tag so its ZADD / ZRANGEBYSCORE land on one slot on a Cluster
 *     client (the pub/sub channel needs no tag - classic PUBLISH/SUBSCRIBE
 *     broadcasts cluster-wide).
 *
 * The replies carried back are the opaque per-subscriber outcomes the adapter
 * already produced (`{ ok, reply } | { ok: false, error }`), so nothing here
 * authorizes or re-checks them - the same trusted-peer model the other relays
 * use. Defense against a foreign publisher on a shared Redis is the bus
 * validator's pre-parse size cap plus the app-key-prefixed channel name.
 *
 * @module svelte-adapter-uws-extensions/redis/topic-broadcast
 */

import { randomBytes, now as cachedNow, setTimer, clearTimer, setIntervalTimer, clearIntervalTimer } from '../shared/runtime.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { MAX_REGISTRY_PENDING_REQUESTS } from '../shared/caps.js';

/** Relay message kinds (the `k` field on every envelope). */
const KIND_REQUEST = 'treq';
const KIND_REPLY = 'trep';

const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_HEARTBEAT_MS = 10000;
/** An instance is considered live within `heartbeat * PRESENCE_TTL_MULT`. */
const PRESENCE_TTL_MULT = 3;
/**
 * When the presence set shows no peers, the origin still publishes and waits
 * this brief window (capped by the call's timeoutMs) for a peer whose presence
 * write has not yet landed, instead of concluding instantly that it is alone.
 * Short so a genuinely single-instance deployment stays fast.
 */
const NO_KNOWN_PEER_GRACE_MS = 50;

/** Coerce a positive-number option, else the default. */
function positive(v, fallback) {
	return typeof v === 'number' && Number.isFinite(v) && v > 0 ? v : fallback;
}

/**
 * @typedef {Object} TopicBroadcastOptions
 * @property {string} [keyPrefix=''] - Prefix prepended to the coordinator's channel and presence key (stacks with the client's own `keyPrefix`). Lets two logical coordinators on one ioredis client stay separate.
 * @property {number} [requestTimeoutMs=5000] - Default whole-fan-out budget for `broadcast(...)` when the call omits `timeoutMs`. The hard ceiling: the call resolves no later than this even if a recorded instance never answers.
 * @property {number} [heartbeat=10000] - Presence refresh interval in ms. Each tick re-stamps this instance's score and evicts instances stale beyond the TTL.
 * @property {number} [presenceTtlMs] - How long (ms) an instance counts as live after its last heartbeat. Default `heartbeat * 3` so a GC pause inside one missed beat does not drop a live instance from the early-completion set.
 * @property {number} [maxEnvelopeBytes=1048576] - Reject inbound relay envelopes larger than this BEFORE JSON.parse. Defends against a bus-side DoS on shared-Redis deployments.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional circuit breaker; when open, outbound publishes and presence writes are skipped.
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics] - Optional Prometheus metrics registry.
 * @property {(err: unknown) => void} [onError] - Observe a relay-subscriber failure (the subscriber tears down so a later broadcast re-subscribes).
 */

/**
 * Create the topic-broadcast cluster coordinator. Attach it to the platform the
 * way the other Redis plugins are wired (`platform.topicBroadcast =
 * createTopicBroadcast(client)`); `bus.wrap` forwards it, and the realtime
 * `live.push({ topic })` layer detects `platform.topicBroadcast` and routes the
 * fan-out through it. Without it, `live.push({ topic })` runs single-instance
 * (correct only when every subscriber of a topic lands on one instance).
 *
 * @param {import('./index.js').RedisClient} client
 * @param {TopicBroadcastOptions} [options]
 *
 * @example
 * ```js
 * // hooks.ws.js (init)
 * import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';
 * import { createTopicBroadcast } from 'svelte-adapter-uws-extensions/redis/topic-broadcast';
 *
 * const redis = createRedisClient({ url: process.env.REDIS_URL });
 * export function init({ platform }) {
 *   platform.topicBroadcast = createTopicBroadcast(redis);
 *   // realtime detects platform.topicBroadcast and wires its onRequest handler.
 * }
 * ```
 */
export function createTopicBroadcast(client, options = {}) {
	const redis = client.redis;
	const instanceId = randomBytes(8).toString('hex');
	// Stacks on the client's own keyPrefix (mirrors the connection registry) so
	// two logical coordinators sharing one ioredis client can keep separate
	// channels + presence sets.
	const keyPrefix = options.keyPrefix == null ? '' : String(options.keyPrefix);
	const channel = client.key(keyPrefix + 'topic-broadcast:events');
	// Hash-tagged so ZADD / ZRANGEBYSCORE stay on one slot on a Cluster client.
	const presenceKey = client.key(keyPrefix + '{topic-broadcast}:instances');
	const heartbeatMs = positive(options.heartbeat, DEFAULT_HEARTBEAT_MS);
	const presenceTtlMs = positive(options.presenceTtlMs, heartbeatMs * PRESENCE_TTL_MULT);
	const defaultTimeoutMs = positive(options.requestTimeoutMs, DEFAULT_TIMEOUT_MS);
	// `allowSystemTopics` is deliberate here, unlike the data-plane buses.
	// requestTopic is a SERVER-initiated fan-out and the framework's own
	// coordination topics are `__`-prefixed (`__signal:{userId}` and
	// friends), so denying them at the boundary would not stop an attacker -
	// it would break the feature, and silently: the origin blocks for the
	// full timeout while every peer drops a request it was supposed to
	// serve. Shape, length and control-byte validation still apply, and
	// those are what protect the adapter's publish path.
	const validator = createBusValidator({ label: 'topic-broadcast', maxBytes: options.maxEnvelopeBytes, allowSystemTopics: true });
	const b = options.breaker;

	const m = options.metrics;
	const mBroadcasts = m?.counter('topic_broadcasts_total', 'Topic broadcasts by completion', ['result']);
	const mReplies = m?.counter('topic_broadcast_replies_total', 'Per-instance reply envelopes collected');
	const mServed = m?.counter('topic_broadcast_served_total', 'Inbound topic requests served locally');

	/**
	 * Local-serve handler registered by the realtime layer:
	 * `(topic, event, data, { timeoutMs }) => Promise<Array<{ ok, reply } | { ok: false, error }>>`.
	 * Runs this instance's `platform.requestTopic` over its own subscriber set.
	 * @type {((topic: string, event: string, data: any, opts: { timeoutMs: number }) => Promise<any[]>) | null}
	 */
	let handler = null;
	/** @type {any} */
	let subscriber = null;
	/** @type {Promise<any> | null} */
	let subscriberReady = null;
	/** @type {any} */
	let heartbeatTimer = null;
	let destroyed = false;

	/**
	 * In-flight outbound broadcasts: ref -> { collected, remaining, finish }.
	 * @type {Map<string, { collected: any[], remaining: Set<string>, finish: () => void }>}
	 */
	const pending = new Map();

	function publish(obj) {
		if (destroyed) return;
		if (b) { try { b.guard(); } catch { return; } }
		const msg = JSON.stringify(obj);
		if (subscriberReady) subscriberReady.then(() => redis.publish(channel, msg).then(() => b?.success()).catch((err) => b?.failure(err)));
		else redis.publish(channel, msg).then(() => b?.success()).catch((err) => b?.failure(err));
	}

	/** Stamp this instance's presence and evict instances stale beyond the TTL. */
	async function refreshPresence() {
		if (destroyed) return;
		if (b) { try { b.guard(); } catch { return; } }
		try {
			await redis.zadd(presenceKey, cachedNow(), instanceId);
			// Inclusive cutoff (1ms of TTL slack is immaterial against a multi-second window).
			await redis.zremrangebyscore(presenceKey, '-inf', cachedNow() - presenceTtlMs);
			b?.success();
		} catch (err) {
			b?.failure(err);
			// Best-effort: a missed stamp self-corrects on the next tick; a live
			// instance only drops out after a full TTL of silence.
		}
	}

	/**
	 * Read the live instance ids (those stamped within the TTL window). On a
	 * Redis error returns `[]`, which makes the origin fall back to the
	 * `timeoutMs` ceiling for completion rather than finishing prematurely.
	 * @returns {Promise<string[]>}
	 */
	async function liveInstances() {
		const cutoff = cachedNow() - presenceTtlMs;
		try {
			const members = await redis.zrangebyscore(presenceKey, cutoff, '+inf');
			b?.success();
			return Array.isArray(members) ? members : [];
		} catch (err) {
			b?.failure(err);
			return [];
		}
	}

	function ensureSubscriber() {
		if (subscriber || destroyed) return;
		const sub = client.duplicate({ enableReadyCheck: false });
		subscriber = sub;
		sub.on('message', (ch, message) => {
			if (ch !== channel) return;
			if (!validator.acceptRaw(message)) return; // size cap before parse
			let parsed;
			try { parsed = JSON.parse(message); } catch { return; }
			if (!parsed || typeof parsed !== 'object') return;
			if (parsed.i === instanceId) return; // echo suppression
			if (parsed.k === KIND_REQUEST) handleInboundRequest(parsed);
			else if (parsed.k === KIND_REPLY) handleInboundReply(parsed);
		});
		subscriberReady = sub.subscribe(channel).catch((err) => {
			// Failed subscribe: tear down so the next broadcast re-subscribes fresh.
			try { sub.quit().catch(() => sub.disconnect()); } catch { /* already gone */ }
			if (subscriber === sub) { subscriber = null; subscriberReady = null; }
			if (typeof options.onError === 'function') { try { options.onError(err); } catch { /* host handler */ } }
		});
	}

	/** Another instance asked us to serve a topic request over our local subscribers. */
	async function handleInboundRequest(env) {
		const { ref, t: topic, e: event, d: data, i: origin, ms } = env;
		if (typeof ref !== 'string' || typeof event !== 'string' || typeof origin !== 'string') return;
		// Same predicate the sender applies, so a malformed topic cannot
		// reach the adapter's publish path (where a control byte or a
		// backslash throws inside esc(topic)) from a foreign publisher.
		if (!validator.acceptEnvelope(topic, event)) return;
		let replies = [];
		if (handler) {
			try {
				const r = await handler(topic, event, data, { timeoutMs: positive(ms, defaultTimeoutMs) });
				if (Array.isArray(r)) replies = r;
			} catch {
				// A failed local serve contributes nothing; it never breaks the
				// broadcast (partial-success is the contract end to end).
				replies = [];
			}
		}
		mServed?.inc();
		// `to: origin` targets the reply at the asking instance.
		publish({ i: instanceId, k: KIND_REPLY, ref, to: origin, r: replies });
	}

	/** A remote instance answered one of our broadcasts. */
	function handleInboundReply(env) {
		const { ref, to, r: replies, i: from } = env;
		if (typeof ref !== 'string') return;
		if (to !== instanceId) return; // targeted: only the origin collects
		const slot = pending.get(ref);
		if (!slot) return; // already settled (timed out) or unknown ref
		if (Array.isArray(replies)) {
			for (const item of replies) slot.collected.push(item);
		}
		mReplies?.inc();
		if (typeof from === 'string') slot.remaining.delete(from);
		// Early-complete only when we had a known peer set to drain. In sole-peer
		// grace mode the grace timer owns completion (remaining started empty, so a
		// size check would finish on the first straggler and drop later ones).
		if (!slot.solePeer && slot.remaining.size === 0) slot.finish();
	}

	return {
		/** This coordinator's relay identity (stable for its life). */
		instanceId,

		/**
		 * Register the local-serve handler and bring up the relay subscriber +
		 * presence heartbeat. The realtime layer calls this once with a handler
		 * that runs `platform.requestTopic` over this instance's subscribers.
		 * @param {(topic: string, event: string, data: any, opts: { timeoutMs: number }) => Promise<any[]>} fn
		 */
		onRequest(fn) {
			handler = typeof fn === 'function' ? fn : null;
			ensureSubscriber();
			if (!heartbeatTimer && !destroyed) {
				refreshPresence();
				heartbeatTimer = setIntervalTimer(refreshPresence, heartbeatMs);
				if (heartbeatTimer.unref) heartbeatTimer.unref();
			}
		},

		/**
		 * Broadcast a request to every subscriber of `topic` across the cluster
		 * and resolve with the FLAT list of per-subscriber outcomes
		 * (`Array<{ ok, reply } | { ok: false, error }>`) - this instance's own
		 * subscribers first, then every other live instance's. Resolves as soon
		 * as every live instance has answered, or at `timeoutMs`, whichever comes
		 * first; a subscriber/instance that does not answer in time simply does
		 * not contribute (partial-success).
		 *
		 * @param {string} topic @param {string} event @param {any} [data]
		 * @param {{ timeoutMs?: number }} [opts]
		 * @returns {Promise<any[]>}
		 */
		async broadcast(topic, event, data, opts = {}) {
			// Validate on the way OUT with the same predicate the receivers
			// apply on the way in. Checking only one end is what makes a
			// rejected topic look like a slow network: every peer drops the
			// request, `remaining` never drains, and the caller waits out the
			// whole timeout for a local-only result. Fail fast instead.
			if (!validator.acceptEnvelope(topic, event)) {
				throw new Error(
					`topic-broadcast: invalid topic or event (topic must be 1-256 chars with no control bytes, ` +
					`'"' or '\\'; event must be 1-256 chars) - peers would drop this request on receipt`
				);
			}
			const timeoutMs = positive(opts && opts.timeoutMs, defaultTimeoutMs);
			ensureSubscriber();

			// Serve this instance's own subscribers concurrently with the fan-out.
			const localPromise = handler
				? Promise.resolve().then(() => handler(topic, event, data, { timeoutMs })).then((r) => (Array.isArray(r) ? r : [])).catch(() => [])
				: Promise.resolve([]);

			const live = await liveInstances();
			const remaining = new Set(live.filter((id) => id !== instanceId));

			// Per-instance cap on in-flight broadcasts (mirrors the registry's
			// pending-request cap): a leaking caller hits the cap before the heap
			// fills with collectors. Degrade to a local-only result rather than
			// throwing into the caller's broadcast.
			if (pending.size >= MAX_REGISTRY_PENDING_REQUESTS) {
				mBroadcasts?.inc({ result: 'overflow' });
				return await localPromise;
			}

			const ref = randomBytes(12).toString('hex');
			const collected = [];
			let finished = false;
			let resolveDone;
			const done = new Promise((resolve) => { resolveDone = resolve; });

			// Always publish - presence decides WHEN to stop, never WHETHER to ask,
			// so a peer whose presence write has not landed still receives the
			// broadcast and can answer. With known peers, wait for all of them up to
			// the budget; with none, wait a short grace window for a just-joined peer
			// rather than concluding instantly that we are alone.
			const solePeer = remaining.size === 0;
			const waitMs = solePeer ? Math.min(timeoutMs, NO_KNOWN_PEER_GRACE_MS) : timeoutMs;
			const timer = setTimer(() => finish('timeout'), waitMs);
			if (timer.unref) timer.unref();

			function finish(result) {
				if (finished) return;
				finished = true;
				clearTimer(timer);
				pending.delete(ref);
				mBroadcasts?.inc({ result: result || 'complete' });
				resolveDone();
			}

			// In sole-peer (grace) mode completion is the grace timer alone: with no
			// known peer count to drain, an early straggler reply must not finish the
			// collector, so a second straggler within the window is still gathered.
			pending.set(ref, { collected, remaining, solePeer, finish: () => finish('complete') });
			publish({ i: instanceId, k: KIND_REQUEST, ref, t: topic, e: event, d: data, ms: timeoutMs });

			await done;
			const localReplies = await localPromise;
			return localReplies.concat(collected);
		},

		/** Tear down the subscriber + heartbeat and drop this instance's presence. Idempotent. */
		async destroy() {
			if (destroyed) return;
			destroyed = true;
			for (const slot of pending.values()) slot.finish();
			pending.clear();
			if (heartbeatTimer) {
				clearIntervalTimer(heartbeatTimer);
				heartbeatTimer = null;
			}
			try { await redis.zrem(presenceKey, instanceId); } catch { /* best-effort: TTL evicts us anyway */ }
			handler = null;
			if (subscriber) {
				const s = subscriber;
				subscriber = null;
				subscriberReady = null;
				try { await s.quit().catch(() => s.disconnect()); } catch { /* already gone */ }
			}
		}
	};
}
