/**
 * Sharded Redis pub/sub bus for svelte-adapter-uws.
 *
 * SPUBLISH / SSUBSCRIBE variant of `createPubSubBus`. In Redis Cluster
 * deployments, sharded pub/sub keeps messages on the shard that owns
 * the channel rather than fanning out across every node, which reduces
 * cluster bus bandwidth proportionally to the number of subscribers
 * actually interested in each topic.
 *
 * Differs from `createPubSubBus` in three ways:
 *
 * - Per-topic channels (`channelPrefix + shardKey(topic)`), not one
 *   shared channel for everything. SSUBSCRIBE has no wildcard, so
 *   every subscriber must SSUBSCRIBE to each channel it cares about.
 * - Dynamic subscription: `follow(topic)` and `unfollow(topic)`
 *   maintain a refcount; `SSUBSCRIBE` on first follower per channel,
 *   `SUNSUBSCRIBE` on the last one out. Wire via `bus.hooks` for
 *   automatic management against WebSocket subscribe/unsubscribe.
 * - Requires Redis 7+. `activate()` runs `INFO server` and throws on
 *   older servers; users on Redis 6 should keep using `createPubSubBus`.
 *
 * @module svelte-adapter-uws-extensions/redis/sharded-pubsub
 */

import { randomBytes } from 'node:crypto';
import { parseRedisVersion } from '../shared/redis-version.js';
import { assert } from '../shared/assert.js';
import {
	MAX_SHARDED_BUS_TOPICS,
	MAX_SHARDED_BUS_BATCH_CHANNELS_PER_TICK
} from '../shared/caps.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { execMultiSlot, isCluster, keySlot } from '../shared/cluster.js';

// A node.ssubscribe() / sunsubscribe() aimed at a master that does not own the
// channel's slot never resolves - the command hangs instead of returning a MOVED
// redirect. So every shard (un)subscribe is bounded by this timeout; a timeout
// means the resolved owner was stale, and the call is retried on the true owner
// after a topology refresh.
const SHARD_SUBSCRIBE_TIMEOUT_MS = 5000;

/**
 * @typedef {Object} ShardedBusOptions
 * @property {string} [channelPrefix='uws:sharded:'] - Prefix for sharded pub/sub channels.
 * @property {(topic: string) => string} [shardKey] - Map a topic to a shard label. The channel name is `channelPrefix + shardKey(topic)`. Default: identity (one channel per topic).
 * @property {{ subscribersOf(topic: string): number }} [subscribersAggregator] - Optional aggregator (typically from `redis/publish-rate`) wired with this bus's `localSubjects` as its `subjects` source. When present, `bus.subscribers(topic)` returns the cluster-wide count. When absent, `bus.subscribers(topic)` returns the local count only.
 */

/**
 * @typedef {Object} ShardedBus
 * @property {(platform: import('svelte-adapter-uws').Platform) => import('svelte-adapter-uws').Platform} wrap
 * @property {(platform: import('svelte-adapter-uws').Platform) => Promise<void>} activate
 * @property {() => Promise<void>} deactivate
 * @property {(topic: string) => Promise<void>} follow
 * @property {(topic: string) => Promise<void>} unfollow
 * @property {{ subscribe: Function, unsubscribe: Function, close: Function }} hooks
 */

/**
 * Create a sharded Redis pub/sub bus.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {ShardedBusOptions} [options]
 * @returns {ShardedBus}
 */
export function createShardedBus(client, options = {}) {
	const channelPrefix = options.channelPrefix ?? 'uws:sharded:';
	if (typeof channelPrefix !== 'string') {
		throw new Error('sharded bus: channelPrefix must be a string');
	}
	if (options.shardKey !== undefined && typeof options.shardKey !== 'function') {
		throw new Error('sharded bus: shardKey must be a function');
	}
	const shardKey = options.shardKey || ((topic) => topic);
	const subscribersAggregator = options.subscribersAggregator;
	if (subscribersAggregator !== undefined && (
		!subscribersAggregator || typeof subscribersAggregator.subscribersOf !== 'function'
	)) {
		throw new Error('sharded bus: subscribersAggregator must expose subscribersOf(topic)');
	}
	const instanceId = randomBytes(8).toString('hex');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mRelayed = m?.counter('sharded_pubsub_messages_relayed_total', 'Messages SPUBLISHed', ['topic']);
	const mReceived = m?.counter('sharded_pubsub_messages_received_total', 'Messages received via SSUBSCRIBE', ['topic']);
	const mEchoSuppressed = m?.counter('sharded_pubsub_echo_suppressed_total', 'Messages dropped by echo suppression');
	const mParseErrors = m?.counter('sharded_pubsub_parse_errors_total', 'Malformed envelopes dropped on receive');
	const mFollows = m?.counter('sharded_pubsub_ssubscribes_total', 'SSUBSCRIBE calls');
	const mUnfollows = m?.counter('sharded_pubsub_sunsubscribes_total', 'SUNSUBSCRIBE calls');
	const mResubscribes = m?.counter('sharded_pubsub_resubscribes_total', 'Channels re-subscribed on a new owner after a cluster reshard');

	function channelFor(topic) {
		return channelPrefix + shardKey(topic);
	}

	const validator = createBusValidator({
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: options.allowSystemTopics === true,
		allowedSystemTopics: []
	});

	// Sharded subscriber connections. Standalone: one duplicate. Cluster: one
	// subscriber connection per master, with each channel ssubscribed on the
	// master owning its slot. ioredis `Cluster.ssubscribe()` does not resolve
	// against a cluster (the call hangs), but a direct `node.ssubscribe()` on the
	// owning master subscribes and delivers `smessage`. The owner is resolved
	// natMap-agnostically by node id (CLUSTER SLOTS owner id -> CLUSTER MYID).
	/** @type {import('ioredis').Redis | null} */
	let subStandalone = null;
	/** @type {Map<string, import('ioredis').Redis>} nodeId -> subscriber connection */
	const subByNodeId = new Map();
	/** @type {Array<{start: number, end: number, nodeId: string}> | null} */
	let slotRanges = null;
	let subReady = false;
	let active = false;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;

	/** @type {Map<string, number>} topic -> follow refcount */
	const followCounts = new Map();
	/** @type {Map<string, number>} channel -> # of topics resolving to it */
	const channelRefcounts = new Map();
	/** @type {Set<string>} */
	const subscribedChannels = new Set();
	/** @type {Map<string, string>} channel -> node id it is currently ssubscribed on (cluster) */
	const channelNode = new Map();
	/** @type {ReturnType<typeof setTimeout> | null} debounce for the reshard reconcile sweep */
	let reconcileTimer = null;
	// Set when a subscriber error signals a possible reshard, so the next
	// subscribe refreshes the slot map before resolving an owner (rather than
	// blind-subscribing on a stale - and therefore hanging - old owner).
	let topologyDirty = false;
	/** @type {Promise<void> | null} in-flight topology refresh, shared by concurrent callers */
	let topologyRefresh = null;
	/** @type {WeakMap<any, Set<string>>} ws -> set of followed topics */
	const wsFollows = new WeakMap();

	// One-shot warn flag for the per-tick batch cap.
	let batchChannelsWarnFired = false;

	// Per-channel tick batch: coalesce SPUBLISHes for the same channel
	// within one event-loop iteration into a single pipelined call. Each
	// entry tracks the underlying topic list so the relayed-messages
	// counter stays accurate when batch envelopes carry many messages.
	//
	// Why `setTimeout(0)` and not `queueMicrotask`: uWS dispatches each WS
	// message as its own JS task, and N-API drains microtasks at the C++/JS
	// boundary between tasks. A microtask-deferred flush fires BEFORE the
	// next socket's handler runs, so cross-socket coalescing is impossible
	// at the microtask level. `setTimeout(0)` lands in libuv's timers
	// phase, which fires only after the poll phase has dispatched every
	// ready socket message in the current iteration. Same structural
	// choice the 0.5.7 cursor always-tick rewrite locked in.
	/** @type {Map<string, Array<{msg: string, topics: string[]}>>} */
	let channelBatches = new Map();
	/** @type {ReturnType<typeof setTimeout> | null} */
	let relayTimer = null;

	function scheduleRelay(channel, msg, topics) {
		let arr = channelBatches.get(channel);
		if (!arr) {
			arr = [];
			channelBatches.set(channel, arr);
		}
		arr.push({ msg, topics });
		if (channelBatches.size >= MAX_SHARDED_BUS_BATCH_CHANNELS_PER_TICK && !batchChannelsWarnFired) {
			batchChannelsWarnFired = true;
			console.warn(
				'[sharded-bus] tick batch reached ' + channelBatches.size +
				' distinct channels in one iteration. The batch is drained every tick, ' +
				'so reaching this size means a publisher emitted a million distinct ' +
				'channels in one synchronous burst - likely a topic-cardinality leak.\n' +
				'  See: https://svti.me/sharded-bus-burst'
			);
		}
		if (relayTimer === null) {
			relayTimer = setTimeout(flushRelay, 0);
			if (relayTimer.unref) relayTimer.unref();
		}
	}

	function flushRelay() {
		const batches = channelBatches;
		channelBatches = new Map();
		if (relayTimer !== null) {
			clearTimeout(relayTimer);
			relayTimer = null;
		}
		if (b) {
			try { b.guard(); } catch { return; }
		}
		// Each sharded channel is owned by the shard that holds its slot, so a
		// SPUBLISH must land on that node. A single pipeline would be delivered
		// to one node and silently drop the channels owned elsewhere, so route
		// each SPUBLISH per owning-node on a cluster (one pipeline on standalone).
		const commands = [];
		const counts = []; // for metric inc per channel
		for (const [ch, entries] of batches) {
			for (const { msg } of entries) {
				commands.push(['spublish', ch, msg]);
			}
			counts.push({ entries });
		}
		execMultiSlot(client.redis, commands).then(() => {
			for (const { entries } of counts) {
				for (const { topics } of entries) {
					for (let i = 0; i < topics.length; i++) {
						mRelayed?.inc({ topic: mt(topics[i]) });
					}
				}
			}
			b?.success();
		}).catch((err) => { b?.failure(err); });
	}

	function handleSMessage(ch, message) {
			if (!validator.acceptRaw(message)) {
				mParseErrors?.inc();
				return;
			}
			try {
				const parsed = JSON.parse(message);
				assert(
					typeof parsed === 'object' && parsed !== null && typeof parsed.instanceId === 'string',
					'sharded-bus.envelope.shape',
					{ ch }
				);
				// One echo check per envelope; batched envelopes carry
				// one instanceId for the whole batch.
				if (parsed.instanceId === instanceId) {
					mEchoSuppressed?.inc();
					return;
				}
				if (!activePlatform) return;
				if (Array.isArray(parsed.batch)) {
					const local = [];
					for (let i = 0; i < parsed.batch.length; i++) {
						const m = parsed.batch[i];
						if (!m || !validator.acceptEnvelope(m.topic, m.event)) {
							mParseErrors?.inc();
							continue;
						}
						mReceived?.inc({ topic: mt(m.topic) });
						local.push({
							topic: m.topic,
							event: m.event,
							data: m.data,
							options: { relay: false }
						});
					}
					if (local.length === 0) return;
					activePlatform.publishBatched(local);
				} else {
					if (!validator.acceptEnvelope(parsed.topic, parsed.event)) {
						mParseErrors?.inc();
						return;
					}
					mReceived?.inc({ topic: mt(parsed.topic) });
					activePlatform.publish(parsed.topic, parsed.event, parsed.data, { relay: false });
				}
			} catch {
				// Malformed envelope; counted so a stream of bad messages
				// is observable instead of silently swallowed.
				mParseErrors?.inc();
			}
	}

	function subOnError(err) {
		console.error('sharded bus subscriber error:', err.message);
		// A slot migration makes the source master drop that slot's shard
		// subscriptions; ioredis surfaces the server-initiated unsubscribe here
		// rather than as a clean event. Mark the slot map stale so the next
		// subscribe refreshes before resolving, and re-establish the dropped
		// subscriptions on their new owner.
		if (isCluster(client.redis)) {
			topologyDirty = true;
			scheduleReconcile();
		}
	}

	// Build (or extend) the per-master subscriber connections and the slot ->
	// owner-node-id map. Refreshed lazily and when a channel's owner is unknown.
	// Single-flight: concurrent callers (a batch subscribe in a dirty window, many
	// unknown-owner resolutions, a retry storm) share one CLUSTER SLOTS refresh
	// instead of each issuing its own.
	function subRefreshTopology() {
		if (!topologyRefresh) {
			topologyRefresh = refreshTopologyNow().finally(() => { topologyRefresh = null; });
		}
		return topologyRefresh;
	}

	async function refreshTopologyNow() {
		const masters = client.redis.nodes('master');
		await Promise.all(masters.map(async (node) => {
			const id = await node.cluster('MYID');
			if (!subByNodeId.has(id)) {
				const s = node.duplicate({ enableReadyCheck: false });
				s.on('error', subOnError);
				s.on('smessage', handleSMessage);
				subByNodeId.set(id, s);
			}
		}));
		const slots = await client.redis.cluster('SLOTS');
		// Each entry: [startSlot, endSlot, [ip, port, nodeId], ...replicas].
		slotRanges = slots.map((e) => ({ start: e[0], end: e[1], nodeId: e[2][2] }));
		topologyDirty = false;
	}

	// The node id owning a slot, from the cached slot map (null if unknown). The
	// slot is computed client-side (CRC16 of the channel's hash-tag) - identical
	// to CLUSTER KEYSLOT but with no per-channel round trip.
	function ownerIdForSlot(slot) {
		if (!slotRanges) return null;
		for (const r of slotRanges) {
			if (slot >= r.start && slot <= r.end) return r.nodeId;
		}
		return null;
	}

	// Bound a shard (un)subscribe so a stale-owner hang cannot wedge the caller
	// forever (see SHARD_SUBSCRIBE_TIMEOUT_MS).
	function withSubTimeout(promise, label) {
		let timer;
		const timeout = new Promise((_, reject) => {
			timer = setTimeout(
				() => reject(new Error('sharded bus: ' + label + ' timed out (stale slot owner)')),
				SHARD_SUBSCRIBE_TIMEOUT_MS
			);
			if (timer.unref) timer.unref();
		});
		return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
	}

	// Subscribe one channel on the master that currently owns its slot, recording
	// the owner so a later reshard can be detected and reconciled. A stale cache
	// would resolve the old owner, where ssubscribe hangs, so a timeout triggers
	// one topology refresh + retry on the true owner.
	async function subscribeChannelOnOwner(channel) {
		const slot = keySlot(channel);
		// If a reshard was signaled, the cached owner map may be stale and an
		// ssubscribe on the old owner would hang, so refresh before resolving.
		if (topologyDirty) await subRefreshTopology();
		let ownerId = ownerIdForSlot(slot);
		if (ownerId === null) {
			await subRefreshTopology();
			ownerId = ownerIdForSlot(slot);
		}
		let conn = ownerId !== null ? subByNodeId.get(ownerId) : null;
		if (!conn) throw new Error('sharded bus: no owning node for channel ' + channel);
		try {
			await withSubTimeout(conn.ssubscribe(channel), 'ssubscribe ' + channel);
		} catch {
			await subRefreshTopology();
			ownerId = ownerIdForSlot(slot);
			conn = ownerId !== null ? subByNodeId.get(ownerId) : null;
			if (!conn) throw new Error('sharded bus: no owning node for channel ' + channel);
			await withSubTimeout(conn.ssubscribe(channel), 'ssubscribe ' + channel + ' (retry)');
		}
		channelNode.set(channel, ownerId);
	}

	// Re-establish every wanted channel on its current owner. A slot migration
	// makes the source master drop that slot's shard subscriptions, so on a
	// subscriber error this refreshes the topology and re-subscribes the channels
	// whose owner moved. Debounced (a reshard fans out errors across connections)
	// and refreshes first, so resolution is never stale.
	async function subReconcile() {
		if (!isCluster(client.redis) || !subReady) return;
		try {
			await subRefreshTopology();
		} catch {
			return; // transient; the next error reschedules
		}
		const moved = [];
		for (const channel of subscribedChannels) {
			const ownerId = ownerIdForSlot(keySlot(channel));
			if (ownerId !== null && channelNode.get(channel) !== ownerId) moved.push(channel);
		}
		// Re-subscribe the moved channels in parallel for fast recovery; the
		// topology was refreshed above, so none resolves a stale owner.
		await Promise.all(moved.map((channel) =>
			subscribeChannelOnOwner(channel).then(
				() => mResubscribes?.inc(),
				// Leave a failed one for the next sweep rather than aborting the rest.
				(err) => console.error('sharded bus reconcile ' + channel + ': ' + err.message)
			)
		));
	}

	function scheduleReconcile() {
		if (reconcileTimer !== null) return;
		reconcileTimer = setTimeout(() => {
			reconcileTimer = null;
			subReconcile().catch((err) => console.error('sharded bus reconcile failed:', err.message));
		}, 0);
		if (reconcileTimer.unref) reconcileTimer.unref();
	}

	async function subEnsure() {
		if (subReady) return;
		if (isCluster(client.redis)) {
			await subRefreshTopology();
		} else {
			subStandalone = client.duplicate({ enableReadyCheck: false });
			subStandalone.on('error', subOnError);
			subStandalone.on('smessage', handleSMessage);
		}
		subReady = true;
	}

	async function subSubscribe(channels) {
		if (channels.length === 0) return;
		if (!isCluster(client.redis)) {
			await subStandalone.ssubscribe(...channels);
			return;
		}
		// On a cluster, SSUBSCRIBE requires every channel in one call to share a
		// slot (same owning node is not enough), and node.ssubscribe only resolves
		// on the slot's owning master (Cluster.ssubscribe hangs), so subscribe each
		// channel individually on its owner.
		await Promise.all(channels.map((ch) => subscribeChannelOnOwner(ch)));
	}

	async function subUnsubscribe(channel) {
		if (!isCluster(client.redis)) {
			if (subStandalone) await subStandalone.sunsubscribe(channel);
			return;
		}
		const ownerId = channelNode.get(channel);
		channelNode.delete(channel);
		const conn = ownerId !== undefined ? subByNodeId.get(ownerId) : null;
		if (!conn) return;
		// Best-effort: a channel whose slot already migrated is gone from this
		// node, and sunsubscribe on a non-owner would hang, so cap and ignore.
		await withSubTimeout(conn.sunsubscribe(channel), 'sunsubscribe ' + channel).catch(() => {});
	}

	async function subClose() {
		if (reconcileTimer !== null) {
			clearTimeout(reconcileTimer);
			reconcileTimer = null;
		}
		const all = isCluster(client.redis)
			? [...subByNodeId.values()]
			: (subStandalone ? [subStandalone] : []);
		subByNodeId.clear();
		channelNode.clear();
		subStandalone = null;
		slotRanges = null;
		subReady = false;
		await Promise.all(all.map((s) => s.quit().catch(() => s.disconnect())));
	}

	async function activate(platform) {
		activePlatform = platform;
		if (active) return;

		b?.guard();
		let info;
		try {
			info = await client.redis.info('server');
		} catch (err) {
			b?.failure(err);
			throw err;
		}
		const major = parseRedisVersion(info);
		if (major === null) {
			b?.failure(new Error('could not parse Redis version'));
			throw new Error('sharded bus: could not parse Redis version from INFO server');
		}
		if (major < 7) {
			b?.failure(new Error(`Redis ${major} too old`));
			throw new Error(`sharded bus: requires Redis 7 or newer (server reports ${major}). Use createPubSubBus for older servers.`);
		}
		b?.success();

		await subEnsure();
		active = true;
	}

	async function deactivate() {
		if (!active) return;
		active = false;
		activePlatform = null;
		subscribedChannels.clear();
		await subClose();
		channelBatches = new Map();
		if (relayTimer !== null) {
			clearTimeout(relayTimer);
			relayTimer = null;
		}
		followCounts.clear();
		channelRefcounts.clear();
	}

	async function follow(topic) {
		if (!subReady) {
			throw new Error('sharded bus: activate() must be called before follow()');
		}
		const count = followCounts.get(topic) || 0;
		// Per-instance cap on distinct followed topics. Mirrors the
		// adapter's WS_SUBSCRIPTIONS denial shape but at the bus layer.
		// The reject is structured so callers can distinguish "topic
		// rejected" from a Redis failure.
		if (count === 0 && followCounts.size >= MAX_SHARDED_BUS_TOPICS) {
			throw new Error(
				'sharded bus: distinct topic count exceeded ' +
				MAX_SHARDED_BUS_TOPICS + ' on this instance'
			);
		}
		followCounts.set(topic, count + 1);
		// Channel refcount tracks distinct active topics resolving to
		// the channel, not raw follow-call count. Only bump when this
		// topic transitions from 0 to active.
		if (count > 0) return;

		const channel = channelFor(topic);
		const chCount = channelRefcounts.get(channel) || 0;
		channelRefcounts.set(channel, chCount + 1);
		if (chCount > 0) return;

		subscribedChannels.add(channel);
		b?.guard();
		try {
			await subSubscribe([channel]);
			b?.success();
			mFollows?.inc();
		} catch (err) {
			b?.failure(err);
			subscribedChannels.delete(channel);
			channelRefcounts.delete(channel);
			followCounts.set(topic, count);
			throw err;
		}
	}

	/**
	 * Bulk follow. Groups input topics by shard channel, runs one
	 * SSUBSCRIBE round trip for any channel transitioning from refcount 0
	 * to active. Refcount semantics for individual topics match `follow`:
	 * each call to `followBatch` bumps every input topic's refcount by 1,
	 * and only the channel transitions trigger Redis traffic.
	 *
	 * Pairs with the adapter's `subscribeBatch` hook so an N-topic
	 * subscribe batch turns into one round-trip-per-channel rather than
	 * one round-trip-per-topic. With the adapter's next.7 client-side
	 * coalescing, the win covers initial-mount subscribes too, not just
	 * reconnect resubscribes.
	 *
	 * Empty arrays no-op. Duplicate topics in the input collapse to one
	 * refcount bump.
	 *
	 * @param {string[]} topics
	 */
	async function followBatch(topics) {
		if (!subReady) {
			throw new Error('sharded bus: activate() must be called before followBatch()');
		}
		if (!Array.isArray(topics)) {
			throw new TypeError('sharded bus: followBatch requires an array of topics');
		}
		if (topics.length === 0) return;

		const touchedTopics = new Set();
		const activatedTopics = [];
		const channelsToSubscribe = [];

		for (const topic of topics) {
			if (typeof topic !== 'string' || topic.length === 0) continue;
			if (touchedTopics.has(topic)) continue;
			touchedTopics.add(topic);
			const count = followCounts.get(topic) || 0;
			// Per-instance cap shared with `follow`. New topics past the
			// cap are silently skipped here (rather than rejecting the
			// whole batch) so a partial subscribe can still land for the
			// caller's existing topics.
			if (count === 0 && followCounts.size >= MAX_SHARDED_BUS_TOPICS) {
				continue;
			}
			followCounts.set(topic, count + 1);
			if (count > 0) continue;
			activatedTopics.push(topic);

			const channel = channelFor(topic);
			const chCount = channelRefcounts.get(channel) || 0;
			channelRefcounts.set(channel, chCount + 1);
			if (chCount === 0) channelsToSubscribe.push(channel);
		}

		if (channelsToSubscribe.length === 0) return;

		b?.guard();
		try {
			// ioredis accepts multiple channels in one ssubscribe call; this
			// collapses N new-channel subscribes into one round trip when the
			// caller's topics span multiple shards.
			await subSubscribe(channelsToSubscribe);
			for (const ch of channelsToSubscribe) subscribedChannels.add(ch);
			b?.success();
			mFollows?.inc(channelsToSubscribe.length);
		} catch (err) {
			b?.failure(err);
			// Roll back every refcount bump we did under this call so the
			// caller can retry without leaking stale counts.
			for (const topic of touchedTopics) {
				const cur = followCounts.get(topic) || 0;
				if (cur <= 1) followCounts.delete(topic);
				else followCounts.set(topic, cur - 1);
			}
			for (const topic of activatedTopics) {
				const channel = channelFor(topic);
				const chCur = channelRefcounts.get(channel) || 0;
				if (chCur <= 1) channelRefcounts.delete(channel);
				else channelRefcounts.set(channel, chCur - 1);
			}
			throw err;
		}
	}

	async function unfollow(topic) {
		const count = followCounts.get(topic) || 0;
		if (count === 0) return;
		const newCount = count - 1;
		if (newCount > 0) {
			followCounts.set(topic, newCount);
			return;
		}
		followCounts.delete(topic);

		const channel = channelFor(topic);
		const chCount = channelRefcounts.get(channel) || 0;
		const newChCount = chCount - 1;
		if (newChCount > 0) {
			channelRefcounts.set(channel, newChCount);
			return;
		}
		channelRefcounts.delete(channel);

		if (subReady && subscribedChannels.has(channel)) {
			subscribedChannels.delete(channel);
			try {
				await subUnsubscribe(channel);
				mUnfollows?.inc();
			} catch {
				// SUNSUBSCRIBE failure is non-fatal - the connection
				// will be torn down on deactivate anyway.
			}
		}
	}

	/**
	 * Snapshot every topic this bus is currently following with its
	 * local subscriber count from the supplied platform. Use as the
	 * `subjects` callback on `createPublishRateAggregator` so the
	 * aggregator can broadcast subscriber counts cluster-wide:
	 *
	 *     const bus = createShardedBus(client);
	 *     const aggregator = createPublishRateAggregator(client, {
	 *       subjects: () => bus.localSubjects(platform)
	 *     });
	 *
	 * Topics with 0 local subscribers are omitted - they have nothing
	 * to contribute. Topics subscribed outside the bus's hooks (raw
	 * `ws.subscribe` bypass) are not enumerated; they will not propagate
	 * cluster-wide via this path.
	 *
	 * @param {import('svelte-adapter-uws').Platform} platform
	 * @returns {Array<{topic: string, count: number}>}
	 */
	function localSubjects(platform) {
		if (!platform || typeof platform.subscribers !== 'function') return [];
		const out = [];
		for (const topic of followCounts.keys()) {
			const count = platform.subscribers(topic) | 0;
			if (count > 0) out.push({ topic, count });
		}
		return out;
	}

	const bus = {
		wrap(platform) {
			const wrapped = {
				publish(topic, event, data, opts) {
					const result = platform.publish(topic, event, data, opts);
					if (!opts || opts.relay !== false) {
						const channel = channelFor(topic);
						scheduleRelay(channel, JSON.stringify({ instanceId, topic, event, data }), [topic]);
					}
					return result;
				},
				// Per-event loop. Mirrors `platform.batch` semantics: N submitted
				// messages produce N wire frames per subscriber. Use
				// `publishBatched` instead when you want one wire frame per
				// subscriber per call.
				batch(messages) {
					const results = platform.batch(messages);
					for (let i = 0; i < messages.length; i++) {
						const mes = messages[i];
						if (!mes.options || mes.options.relay !== false) {
							const channel = channelFor(mes.topic);
							scheduleRelay(channel, JSON.stringify({
								instanceId, topic: mes.topic, event: mes.event, data: mes.data
							}), [mes.topic]);
						}
					}
					return results;
				},
				// Wire-batched publish. One SPUBLISH envelope per shard channel
				// per call. Receivers fan out via local `platform.publishBatched`
				// for one wire frame per subscriber. Empty arrays no-op.
				publishBatched(messages) {
					if (!Array.isArray(messages)) {
						throw new TypeError('sharded bus: publishBatched requires an array of messages');
					}
					if (messages.length === 0) return;

					platform.publishBatched(messages);

					/** @type {Map<string, Array<{topic: string, event: string, data: unknown}>>} */
					const byChannel = new Map();
					for (let i = 0; i < messages.length; i++) {
						const mes = messages[i];
						if (mes.options && mes.options.relay === false) continue;
						const ch = channelFor(mes.topic);
						let arr = byChannel.get(ch);
						if (!arr) {
							arr = [];
							byChannel.set(ch, arr);
						}
						arr.push({ topic: mes.topic, event: mes.event, data: mes.data });
					}

					for (const [ch, batch] of byChannel) {
						const topics = new Array(batch.length);
						for (let i = 0; i < batch.length; i++) topics[i] = batch[i].topic;
						scheduleRelay(ch, JSON.stringify({ instanceId, batch }), topics);
					}
				},
				send: platform.send.bind(platform),
				sendTo: platform.sendTo.bind(platform),
				sendCoalesced: platform.sendCoalesced.bind(platform),
				request: platform.request.bind(platform),
				// Binary wire methods. Forwarded like send/sendTo (local fanout, no
				// cross-instance relay - the plugin's own relay() handles that). Without
				// these the wrapped seam hides the binary path and cluster-backed cursor /
				// presence silently fall back to JSON. `undefined` when the underlying
				// platform predates per-frame binary wire, so the plugins' typeof guard
				// degrades gracefully.
				publishWire: platform.publishWire ? platform.publishWire.bind(platform) : undefined,
				sendWire: platform.sendWire ? platform.sendWire.bind(platform) : undefined,
				get connections() { return platform.connections; },
				get requestId() { return platform.requestId; },
				get pressure() { return platform.pressure; },
				onPressure: platform.onPressure.bind(platform),
				onPublishRate: platform.onPublishRate.bind(platform),
				subscribers: platform.subscribers.bind(platform),
				forEachSubscriber: platform.forEachSubscriber.bind(platform),
				subscribe: platform.subscribe.bind(platform),
				unsubscribe: platform.unsubscribe.bind(platform),
				checkSubscribe: platform.checkSubscribe.bind(platform),
				get maxPayloadLength() { return platform.maxPayloadLength; },
				bufferedAmount: platform.bufferedAmount.bind(platform),
				get closedWsAborts() { return platform.closedWsAborts ?? 0; },
				// The subscribe-ack epoch must carry the SAME generation domain
				// the resume hook later compares against. When a replay tracker
				// is wired on the source platform it owns the per-topic seq space,
				// so its cached per-topic generation is the authority - route the
				// ack to it. Without this bridge the ack would carry the source
				// platform's single-worker process generation while the resume
				// hook reads the tracker's small-integer generation, mismatching
				// on every resume and forcing a cold re-read where gap-fill should
				// apply. A deployment may still override the source platform's
				// topicEpoch directly; that path is honoured when no cachedEpoch
				// tracker is present. Read live so post-wrap wiring propagates.
				topicEpoch(t) {
					const r = platform.replay;
					if (r && typeof r.cachedEpoch === 'function') return r.cachedEpoch(t);
					return platform.topicEpoch(t);
				},
				// Framework conventions stashed on the source platform by
				// app init code (e.g. `platform.replay = createReplay(...)`,
				// `platform.redis = ioredisClient`) must survive the wrap so
				// downstream framework auto-routing can discover them on the
				// wrapped seam too. Live getters so post-wrap reassignment
				// propagates.
				get replay() { return platform.replay; },
				get redis() { return platform.redis; },
				get presence() { return platform.presence; },
				topic(t) {
					return {
						publish(event, data) { wrapped.publish(t, event, data); },
						created(data) { wrapped.publish(t, 'created', data); },
						updated(data) { wrapped.publish(t, 'updated', data); },
						deleted(data) { wrapped.publish(t, 'deleted', data); },
						set(value) { wrapped.publish(t, 'set', value); },
						increment(amount) { wrapped.publish(t, 'increment', amount); },
						decrement(amount) { wrapped.publish(t, 'decrement', amount); }
					};
				}
			};
			return wrapped;
		},
		activate,
		deactivate,
		follow,
		followBatch,
		unfollow,
		localSubjects,
		/**
		 * Cluster-wide subscriber count for a topic. Returns the local
		 * count (`platform.subscribers(topic)`) plus the remote sum from
		 * the wired aggregator's `subscribersOf(topic)`. When no
		 * aggregator is wired, returns the local count only.
		 *
		 * Eventually-consistent within the aggregator's `publishInterval`
		 * for the remote contribution; the local read is always live.
		 *
		 * @param {string} topic
		 * @returns {number}
		 */
		subscribers(topic) {
			const local = activePlatform ? activePlatform.subscribers(topic) | 0 : 0;
			if (!subscribersAggregator) return local;
			// Aggregator's subscribersOf already includes self (via its
			// own `subjects()` callback wired to bus.localSubjects).
			// Trust it as the cluster total.
			return subscribersAggregator.subscribersOf(topic);
		},
		hooks: {
			async subscribe(ws, topic) {
				if (topic.startsWith('__')) return;
				let topics = wsFollows.get(ws);
				if (!topics) {
					topics = new Set();
					wsFollows.set(ws, topics);
				}
				if (topics.has(topic)) return;
				topics.add(topic);
				await follow(topic);
			},
			async subscribeBatch(ws, topics) {
				if (!Array.isArray(topics) || topics.length === 0) return;
				let wsTopics = wsFollows.get(ws);
				if (!wsTopics) {
					wsTopics = new Set();
					wsFollows.set(ws, wsTopics);
				}
				const fresh = [];
				for (const topic of topics) {
					if (typeof topic !== 'string' || topic.startsWith('__')) continue;
					if (wsTopics.has(topic)) continue;
					wsTopics.add(topic);
					fresh.push(topic);
				}
				if (fresh.length === 0) return;
				await followBatch(fresh);
			},
			async unsubscribe(ws, topic) {
				const topics = wsFollows.get(ws);
				if (!topics || !topics.has(topic)) return;
				topics.delete(topic);
				if (topics.size === 0) wsFollows.delete(ws);
				await unfollow(topic);
			},
			async close(ws) {
				const topics = wsFollows.get(ws);
				if (!topics) return;
				wsFollows.delete(ws);
				const pending = [];
				for (const t of topics) pending.push(unfollow(t));
				await Promise.all(pending);
			}
		}
	};
	return bus;
}
