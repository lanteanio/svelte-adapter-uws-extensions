/**
 * Redis-backed cluster coordinator for conflict-free documents
 * (`svelte-realtime` `live.doc` / `live.map` / `live.array`).
 *
 * The document replica itself lives in the adapter
 * (`svelte-adapter-uws/plugins/crdt/replica`) and is owned per instance by the
 * realtime layer. This module is pure transport: it carries applied updates
 * between instances so every instance's replica converges, answers a
 * cold-joining instance's sync request with whatever a live peer holds beyond
 * the persisted snapshot, and gates snapshot persistence to one writer per
 * topic so concurrent instances never clobber each other's stores.
 *
 * The model, and why it is correct:
 *
 *   - **Per-instance replicas, no fan-out hop.** Every instance holds its own
 *     replica of each locally-subscribed document and serves its own
 *     subscribers directly. There is no per-topic "owner" that every edit must
 *     round-trip through, and no single point of failure for serving reads.
 *   - **Relay everything, converge everywhere.** When an instance applies an
 *     update from one of its clients it relays the opaque bytes here; every
 *     other instance applies them to its own replica and fans them out to its
 *     own subscribers. Because the merge is commutative and idempotent, order
 *     and overlap do not matter, so every replica converges to the same value.
 *   - **One writer per topic.** Snapshot persistence is gated by a per-topic
 *     lease (`acquirePersist`): the lease holder is the sole persister for that
 *     topic, so two instances can never write divergent snapshots to the same
 *     key. The holder's replica is the most converged (it receives every
 *     relayed update), so its snapshot is the freshest; the lease rotates
 *     naturally when the holder's last local subscriber leaves or it dies.
 *   - **Cold-join freshness.** A persisted snapshot is only as fresh as the
 *     last debounced store. When an instance cold-loads a topic it loads the
 *     snapshot AND broadcasts a sync request carrying its state vector; any
 *     instance holding the topic replies with exactly the structs the joiner
 *     lacks. The joiner applies the reply on top of the snapshot - idempotent,
 *     so applying both is always safe - closing the staleness gap whenever a
 *     live peer exists.
 *
 * The relay channel carries opaque CRDT bytes between TRUSTED instances: a
 * peer has already authorized its client's write before relaying (the realtime
 * `live.doc` guard runs at the update boundary), so relayed updates are
 * pre-authorized and are not re-checked here - the same peer-trust model the
 * cursor and presence relays use. Defense against a foreign publisher on a
 * shared Redis is the bus validator (size cap + shape check) plus the
 * app-key-prefixed channel name; the channel is not `__`-prefixed, so it rides
 * the bus untouched.
 *
 * @module svelte-adapter-uws-extensions/redis/crdt
 */

import { randomBytes } from '../shared/runtime.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { LEASE_RENEW_SCRIPT } from '../shared/lease-scripts.js';

/** Relay message kinds (the `k` field on every envelope). */
const KIND_UPDATE = 'u';
const KIND_SYNC_REQUEST = 'sreq';
const KIND_SYNC_REPLY = 'srep';

/** Coerce a positive-number option, else the default. */
function positive(v, fallback) {
	return typeof v === 'number' && Number.isFinite(v) && v > 0 ? v : fallback;
}

/**
 * @typedef {Object} CrdtClusterOptions
 * @property {number} [persistLeaseMs=6000] - Per-topic persist-lease TTL. The
 *   lease holder is the sole snapshot writer for a topic; the TTL should
 *   exceed the document's `debounceWait` so the holder keeps renewing across
 *   scheduled stores and the lease only rotates when the holder goes quiet or
 *   dies. Default 6s suits the 2s document debounce default.
 * @property {number} [maxEnvelopeBytes=1048576] - Reject inbound relay
 *   envelopes larger than this BEFORE JSON.parse. Defends against a bus-side
 *   DoS on shared-Redis deployments.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional
 *   circuit breaker; when open, outbound relays are skipped (the local replica
 *   and persistence are unaffected - convergence resumes when it closes).
 * @property {(err: unknown) => void} [onError] - Observe a relay-subscriber
 *   failure (the subscriber tears down so a later relay re-subscribes).
 */

/**
 * Create the cluster coordinator. Attach it to the platform the same way the
 * other Redis plugins are wired (`platform.crdt = createCrdtCluster(client)`)
 * and `bus.wrap` forwards it; the realtime `live.doc` layer detects
 * `platform.crdt` and routes the relay / cold-join / persist-gating through it.
 * Without it, `live.doc` runs single-instance.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {CrdtClusterOptions} [options]
 */
export function createCrdtCluster(client, options = {}) {
	const redis = client.redis;
	const channel = client.key('crdt:events');
	const instanceId = randomBytes(8).toString('hex');
	const persistLeaseMs = positive(options.persistLeaseMs, 6000);
	const validator = createBusValidator({
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: false,
		allowedSystemTopics: []
	});
	const b = options.breaker;

	/** @type {{ onUpdate: Function | null, onSyncRequest: Function | null, onSyncReply: Function | null }} */
	let handlers = { onUpdate: null, onSyncRequest: null, onSyncReply: null };
	/** @type {any} */
	let subscriber = null;
	/** @type {Promise<any> | null} */
	let subscriberReady = null;
	let destroyed = false;

	/** Publish one envelope, after the subscriber is up so we never miss a reply we caused. */
	function publish(obj) {
		if (destroyed) return;
		if (b) { try { b.guard(); } catch { return; } }
		const msg = JSON.stringify(obj);
		if (subscriberReady) subscriberReady.then(() => redis.publish(channel, msg).catch(() => {}));
		else redis.publish(channel, msg).catch(() => {});
	}

	/** Bring up the relay subscriber once; idempotent. */
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
			const kind = parsed.k;
			const declKey = parsed.d;
			const topic = parsed.t;
			// `topic` rides the bus validator's topic-shape check (it is the
			// bare document name, never `__`-prefixed); `kind` doubles as the
			// event-shape argument.
			if (typeof declKey !== 'string' || typeof kind !== 'string' || !validator.acceptEnvelope(topic, kind)) return;
			if (kind === KIND_UPDATE) {
				if (handlers.onUpdate && Array.isArray(parsed.b)) handlers.onUpdate(declKey, topic, parsed.b);
			} else if (kind === KIND_SYNC_REQUEST) {
				if (handlers.onSyncRequest) handlers.onSyncRequest(declKey, topic, Array.isArray(parsed.v) ? parsed.v : [], parsed.i);
			} else if (kind === KIND_SYNC_REPLY) {
				// Targeted: only the requester applies a reply addressed to it.
				if (parsed.to !== instanceId) return;
				if (handlers.onSyncReply && Array.isArray(parsed.b)) handlers.onSyncReply(declKey, topic, parsed.b);
			}
		});
		subscriberReady = sub.subscribe(channel).catch((err) => {
			// Failed subscribe: tear down so the next relay re-subscribes fresh.
			try { sub.quit().catch(() => sub.disconnect()); } catch { /* already gone */ }
			if (subscriber === sub) { subscriber = null; subscriberReady = null; }
			if (typeof options.onError === 'function') { try { options.onError(err); } catch { /* host handler */ } }
		});
	}

	return {
		/** This instance's relay identity (stable for the coordinator's life). */
		instanceId,

		/**
		 * Register the inbound handlers and start the relay subscriber. The
		 * realtime layer calls this once per coordinator with:
		 *   - onUpdate(declKey, topic, bytes): apply a peer's update locally + fan out.
		 *   - onSyncRequest(declKey, topic, sv, fromInstance): if held, reply via sendSyncReply.
		 *   - onSyncReply(declKey, topic, bytes): apply a peer's catch-up diff locally + fan out.
		 * @param {{ onUpdate?: Function, onSyncRequest?: Function, onSyncReply?: Function }} h
		 */
		onMessage(h) {
			handlers = {
				onUpdate: typeof h.onUpdate === 'function' ? h.onUpdate : null,
				onSyncRequest: typeof h.onSyncRequest === 'function' ? h.onSyncRequest : null,
				onSyncReply: typeof h.onSyncReply === 'function' ? h.onSyncReply : null
			};
			ensureSubscriber();
		},

		/** Relay one applied update to peers so their replicas converge. */
		relayUpdate(declKey, topic, bytes) {
			publish({ i: instanceId, k: KIND_UPDATE, d: declKey, t: topic, b: bytes });
		},

		/** Broadcast a cold-join sync request carrying this replica's state vector. */
		requestSync(declKey, topic, sv) {
			ensureSubscriber(); // so the targeted reply finds a live subscriber
			publish({ i: instanceId, k: KIND_SYNC_REQUEST, d: declKey, t: topic, v: sv });
		},

		/** Answer a peer's sync request with the structs it lacks (targeted). */
		sendSyncReply(declKey, topic, bytes, toInstance) {
			publish({ i: instanceId, k: KIND_SYNC_REPLY, d: declKey, t: topic, b: bytes, to: toInstance });
		},

		/**
		 * Try to become (or stay) the sole snapshot writer for a topic. Returns
		 * true when this instance holds the per-topic persist lease and may
		 * write; false when another instance holds it (skip - that instance
		 * persists). Throws on a Redis error so the caller's persistence
		 * schedule retries rather than silently dropping the write.
		 * @param {string} topic
		 * @returns {Promise<boolean>}
		 */
		async acquirePersist(topic) {
			if (destroyed) return false;
			const key = client.key('crdt:persist:' + topic);
			const r = await redis.set(key, instanceId, 'NX', 'PX', persistLeaseMs);
			if (r === 'OK') return true; // newly acquired
			// Held by someone: renew only if it is ours (compare-and-pexpire).
			const renew = await redis.eval(LEASE_RENEW_SCRIPT, 1, key, instanceId, persistLeaseMs);
			return Number(renew) === 1;
		},

		/** Tear down the subscriber. Idempotent. */
		destroy() {
			if (destroyed) return;
			destroyed = true;
			handlers = { onUpdate: null, onSyncRequest: null, onSyncReply: null };
			if (subscriber) {
				const s = subscriber;
				subscriber = null;
				subscriberReady = null;
				try { s.quit().catch(() => s.disconnect()); } catch { /* already gone */ }
			}
		}
	};
}
