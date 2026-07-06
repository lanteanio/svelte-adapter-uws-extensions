/**
 * Redis-backed cluster coordinator for server-authoritative smooth entities
 * (`svelte-realtime` `live.smooth`).
 *
 * The authority itself lives in the adapter
 * (`svelte-adapter-uws/plugins/smooth`) and is owned per instance by the
 * realtime layer. This module is pure transport: it carries a topic's
 * commands to the single instance that owns the topic's tick, carries that
 * owner's broadcasts/acks/events back out to every instance's local clients,
 * and gates tick ownership to one instance per topic via a Redis lease.
 *
 * The model, and why it differs from the document relay:
 *
 *   - **One owner per topic, not per-instance replicas.** A document merge is
 *     commutative and idempotent, so every instance can hold its own replica
 *     and converge by relaying opaque bytes (`redis/crdt`). The smooth
 *     authority is the opposite: `apply(state, command, ctx)` is a
 *     single-writer, order-dependent, NON-idempotent step (it reseeds its RNG
 *     per command id and fires one-shot events). A naive N-instance fan-out
 *     where every instance ticks would double-apply commands and double-fire
 *     events. So exactly ONE instance owns a topic's tick; the others forward
 *     their clients' commands to it and relay its output back.
 *   - **Ownership is a per-topic lease.** `SET NX PX` claims it,
 *     compare-and-pexpire renews it, compare-and-delete releases it (the same
 *     lease shape `redis/crdt`'s persist gate and `redis/leader` use). The
 *     lease holder is the sole authority for that topic; it renews while it
 *     holds live entities and releases when its last local subscriber leaves.
 *     On owner death the lease expires and another instance acquires a fresh
 *     authority - clients re-sync, exactly as they would after a
 *     single-instance server restart.
 *   - **Commands forward fire-and-forget; sync is request/response.** A
 *     non-owner relays a command batch as one envelope and expects no reply
 *     (the volatile grain). A cold-joining client needs the owner's catalog
 *     back, so its sync is a correlation-id request the owner answers with a
 *     targeted reply - unlike the document relay, where any peer can answer a
 *     sync idempotently, a smooth sync must reach exactly the one owner and
 *     correlate exactly one reply.
 *   - **Broadcasts fan out; acks are targeted.** The owner relays each
 *     update/event/remove once; EVERY instance re-emits it to its own local
 *     subscribers. An ack is relayed to the single instance the commanding
 *     client is connected to. One-shot events carry a per-topic monotonic
 *     sequence so a receiving instance can drop a redelivered or
 *     lease-overlap-duplicated event (events are non-idempotent and
 *     author-exclusion is structurally insufficient across instances).
 *
 * The relay channel carries opaque entity state and commands between TRUSTED
 * instances: a peer has already authorized its client's command before
 * forwarding (the realtime smooth guard runs at the command boundary), so
 * forwarded commands are pre-authorized and are not re-checked here - the same
 * peer-trust model the cursor, presence, and document relays use. Defense
 * against a foreign publisher on a shared Redis is the bus validator (pre-parse
 * size cap) plus the app-key-prefixed channel name; the channel itself is the
 * clean `smooth:events` (not `__`-prefixed), so it rides the bus untouched. The
 * entity's own wire topic IS `__smooth:`-prefixed, but it travels as a FIELD
 * inside the envelope, so it is validated by shape (`isValidBusTopic`) rather
 * than the envelope-topic gate that would reject a `__`-prefix.
 *
 * @module svelte-adapter-uws-extensions/redis/smooth
 */

import { randomBytes } from '../shared/runtime.js';
import { evalCached } from '../shared/eval-cached.js';
import { createBusValidator, isValidBusTopic } from '../shared/bus-validate.js';
import { LEASE_RENEW_SCRIPT, LEASE_RELEASE_SCRIPT } from '../shared/lease-scripts.js';
import { isCluster, keySlot } from '../shared/cluster.js';

/** Relay message kinds (the `k` field on every envelope). */
const KIND_COMMAND = 'cmd';
const KIND_SYNC_REQUEST = 'sreq';
const KIND_SYNC_REPLY = 'srep';
const KIND_BROADCAST = 'bc';
const KIND_ACK = 'ack';
const KIND_LEAVE = 'leave';
const KIND_SHOOT = 'shoot';

/** Coerce a positive-number option, else the default. */
function positive(v, fallback) {
	return typeof v === 'number' && Number.isFinite(v) && v > 0 ? v : fallback;
}

/**
 * @typedef {Object} SmoothClusterOptions
 * @property {number} [leaseMs=10000] - Per-topic ownership-lease TTL. The lease
 *   holder is the sole tick authority for a topic; the realtime layer renews it
 *   (via `renewOwner`) on a cadence well inside this TTL while it holds live
 *   entities, so the lease only rotates when the holder goes quiet or dies.
 *   Default 10s leaves room for a renew at ~TTL/3 to survive a GC pause plus a
 *   Redis blip while still damping ownership thrash.
 * @property {number} [maxEnvelopeBytes=1048576] - Reject inbound relay
 *   envelopes larger than this BEFORE JSON.parse. Defends against a bus-side
 *   DoS on shared-Redis deployments.
 * @property {number} [snapshotTtlMs] - TTL (ms) for a topic's warm-handoff
 *   snapshot, written by the owner when `live.smooth({ snapshot: true })` is set.
 *   Each owner write refreshes it, so a live owner's snapshot never expires; a
 *   dead owner's self-expires this long after its last write. Default 3x leaseMs.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional
 *   circuit breaker; when open, outbound relays are skipped (the local
 *   authority is unaffected - cross-instance traffic resumes when it closes).
 * @property {(err: unknown) => void} [onError] - Observe a relay-subscriber
 *   failure (the subscriber tears down so a later relay re-subscribes).
 */

/**
 * Create the cluster coordinator. Attach it to the platform the same way the
 * other Redis plugins are wired (`platform.smooth = createSmoothCluster(client)`)
 * and `bus.wrap` forwards it; the realtime `live.smooth` layer detects
 * `platform.smooth` and routes ownership / command-forward / broadcast-relay
 * through it. Without it, `live.smooth` runs single-instance (correct only when
 * every client of a topic lands on one instance).
 *
 * @param {import('./index.js').RedisClient} client
 * @param {SmoothClusterOptions} [options]
 */
export function createSmoothCluster(client, options = {}) {
	const redis = client.redis;
	const channel = client.key('smooth:events');
	const instanceId = randomBytes(8).toString('hex');
	const leaseMs = positive(options.leaseMs, 10000);
	// Warm-handoff snapshot TTL. Each owner write refreshes it, so a live
	// owner's snapshot never expires; a dead owner's self-expires a few lease
	// periods after its last write (default 3x the lease).
	const snapshotTtlMs = positive(options.snapshotTtlMs, leaseMs * 3);
	// The validator's size cap (`acceptRaw`) is the only piece used here. The
	// envelope-topic gate (`acceptEnvelope`) is deliberately NOT used: the
	// entity wire topic is `__smooth:`-prefixed and that gate rejects `__`
	// topics, so the wire topic is shape-checked with `isValidBusTopic` instead.
	const validator = createBusValidator({ maxBytes: options.maxEnvelopeBytes });
	const b = options.breaker;

	/**
	 * @type {{
	 *   onCommand: Function | null, onSync: Function | null,
	 *   onSyncReply: Function | null, onBroadcast: Function | null,
	 *   onAck: Function | null, onLeave: Function | null, onShoot: Function | null
	 * }}
	 */
	let handlers = {
		onCommand: null, onSync: null, onSyncReply: null,
		onBroadcast: null, onAck: null, onLeave: null, onShoot: null
	};
	/** @type {any} */
	let subscriber = null;
	/** @type {Promise<any> | null} */
	let subscriberReady = null;
	let destroyed = false;

	const onCluster = isCluster(redis);
	// On a cluster, every publish goes through the SINGLE master that owns the
	// relay channel's slot. ioredis routes a keyless PUBLISH to a varying node, and
	// the cluster bus then delivers those publishes to a subscriber OUT OF ORDER
	// across nodes (verified empirically on Redis 7.4 and Valkey 9). The receive
	// side dedups broadcasts by a per-owner monotonic seq, so a late-arriving
	// lower-seq broadcast is dropped as a duplicate - which loses a one-shot event
	// (updates still converge latest-wins, but an event fires zero times). Pinning
	// every publish to one node makes that node serialize them, so a subscriber's
	// single bus link delivers them in order. Regular PUBLISH still fans out
	// cluster-wide from that node, so this fixes ordering without narrowing reach.
	// Standalone keeps its single ordered connection unchanged.
	/** @type {Promise<any> | null} resolves to the connection to PUBLISH through (cluster only) */
	let pubTarget = null;
	let degradedPubWarned = false;

	function resolvePubTarget() {
		if (pubTarget) return pubTarget;
		pubTarget = (async () => {
			try {
				const slot = keySlot(channel);
				const ranges = await redis.cluster('SLOTS');
				let ownerId = null;
				for (const e of ranges) {
					if (slot >= e[0] && slot <= e[1]) { ownerId = e[2][2]; break; }
				}
				if (ownerId !== null) {
					for (const node of redis.nodes('master')) {
						try { if ((await node.cluster('MYID')) === ownerId) return node; } catch { /* try next master */ }
					}
				}
			} catch { /* fall through to the degraded fallback */ }
			// Unresolved (transient SLOTS error, or a freshly-resharded owner ioredis
			// has not discovered yet). Re-resolve on the next publish.
			pubTarget = null;
			// Degraded fallback: still pin to a SINGLE node so a burst stays in order.
			// A keyless cluster PUBLISH would route to a varying node and the bus would
			// reorder it, silently dropping the receiver's lower-seq one-shot events -
			// the exact failure this pinning prevents - so any one master beats the
			// cluster client. Only a total master-less outage (no delivery anyway)
			// falls through to redis.
			try {
				const masters = redis.nodes('master');
				if (masters.length) {
					if (!degradedPubWarned) {
						degradedPubWarned = true;
						console.warn('[redis/smooth] relay channel owner unresolved (cluster topology in flux); pinning publishes to a fallback master to keep broadcast order.');
					}
					return masters[0];
				}
			} catch { /* not a cluster / no nodes(): use the client below */ }
			return redis;
		})();
		return pubTarget;
	}

	/** Publish one envelope, after the subscriber is up so we never miss a reply we caused. */
	function publish(obj) {
		if (destroyed) return;
		if (b) { try { b.guard(); } catch { return; } }
		const msg = JSON.stringify(obj);
		if (!onCluster) {
			// Standalone: one connection, PUBLISH delivered in order.
			if (subscriberReady) subscriberReady.then(() => redis.publish(channel, msg).catch(() => {}));
			else redis.publish(channel, msg).catch(() => {});
			return;
		}
		// Cluster: route through the pinned owner so publishes arrive in order.
		const send = () => resolvePubTarget().then((t) => t.publish(channel, msg).catch(() => {
			// The pinned node may have gone (failover / reshard); drop the cache so
			// the next publish re-resolves the current owner. PUBLISH from any live
			// node still reaches every subscriber.
			if (t !== redis) pubTarget = null;
		}));
		if (subscriberReady) subscriberReady.then(send);
		else send();
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
			const topic = parsed.t;
			// `topic` is the entity wire topic (`__smooth:`-prefixed). Validate
			// its shape directly - it must NOT go through the validator's
			// envelope-topic gate, which rejects the `__` prefix.
			if (typeof kind !== 'string' || !isValidBusTopic(topic)) return;
			if (kind === KIND_COMMAND) {
				if (handlers.onCommand && typeof parsed.id === 'string' && Array.isArray(parsed.b)) {
					handlers.onCommand(topic, parsed.id, parsed.o, parsed.b);
				}
			} else if (kind === KIND_SYNC_REQUEST) {
				if (handlers.onSync && typeof parsed.id === 'string') {
					handlers.onSync(topic, parsed.id, parsed.o, parsed.c);
				}
			} else if (kind === KIND_SYNC_REPLY) {
				// Targeted: only the requester applies a reply addressed to it.
				if (parsed.to !== instanceId) return;
				if (handlers.onSyncReply && typeof parsed.c === 'string') handlers.onSyncReply(topic, parsed.c, parsed.p);
			} else if (kind === KIND_BROADCAST) {
				if (handlers.onBroadcast && typeof parsed.e === 'string') {
					// `parsed.i` (the owner that minted this broadcast) rides along
					// so the receiver can reset its per-topic seq watermark on an
					// ownership handoff - a fresh owner restarts the seq counter, so
					// without the owner identity its lower seqs would be dropped.
					handlers.onBroadcast(topic, parsed.e, parsed.d, parsed.x, parsed.s, parsed.i);
				}
			} else if (kind === KIND_ACK) {
				// Targeted: only the instance the commanding client is on applies it.
				if (parsed.to !== instanceId) return;
				if (handlers.onAck && typeof parsed.id === 'string') {
					handlers.onAck(topic, parsed.id, parsed.p);
				}
			} else if (kind === KIND_LEAVE) {
				if (handlers.onLeave && typeof parsed.id === 'string') {
					handlers.onLeave(topic, parsed.id, parsed.o);
				}
			} else if (kind === KIND_SHOOT) {
				// A non-owner forwarded a client's shot; only the owner resolves it.
				// The payload carries the edge-measured DURATIONS (reach width + a
				// rewind age), never an absolute stamp, so the owner rebuilds the
				// rewind on its own ring axis without subtracting a foreign clock.
				if (handlers.onShoot && typeof parsed.id === 'string' && parsed.p && typeof parsed.p === 'object') {
					handlers.onShoot(topic, parsed.id, parsed.o, parsed.p);
				}
			}
		});
		subscriberReady = sub.subscribe(channel).catch((err) => {
			// Failed subscribe: tear down so the next relay re-subscribes fresh.
			try { sub.quit().catch(() => sub.disconnect()); } catch { /* already gone */ }
			if (subscriber === sub) { subscriber = null; subscriberReady = null; }
			if (typeof options.onError === 'function') { try { options.onError(err); } catch { /* host handler */ } }
		});
	}

	/** Lease key for a topic's tick ownership. */
	function ownerKey(wireTopic) {
		return client.key('smooth:owner:' + wireTopic);
	}

	/** Snapshot key for a topic's warm-handoff state (one string blob per topic). */
	function snapKey(wireTopic) {
		return client.key('smooth:snap:' + wireTopic);
	}

	return {
		/** This instance's relay identity (stable for the coordinator's life). */
		instanceId,

		/**
		 * Register the inbound handlers and start the relay subscriber. The
		 * realtime layer calls this once per coordinator with:
		 *   - onCommand(wireTopic, identity, originInstance, batch): owner enqueues the forwarded batch.
		 *   - onSync(wireTopic, identity, originInstance, corr): owner ensures the entity and replies via sendSyncReply.
		 *   - onSyncReply(wireTopic, corr, payload): requester resolves the pending sync for `corr`.
		 *   - onBroadcast(wireTopic, event, data, excludeIdentity, seq, ownerInstance): every instance re-emits to local subscribers (seq-deduped per owner).
		 *   - onAck(wireTopic, identity, payload): the commanding client's instance delivers the ack to its local socket.
		 *   - onLeave(wireTopic, identity, originInstance): owner drops the surrogate and broadcasts the remove.
		 *   - onShoot(wireTopic, identity, originInstance, payload): owner resolves a forwarded shot against its ring (the payload carries edge-measured durations).
		 * @param {{ onCommand?: Function, onSync?: Function, onSyncReply?: Function, onBroadcast?: Function, onAck?: Function, onLeave?: Function, onShoot?: Function }} h
		 */
		onMessage(h) {
			handlers = {
				onCommand: typeof h.onCommand === 'function' ? h.onCommand : null,
				onSync: typeof h.onSync === 'function' ? h.onSync : null,
				onSyncReply: typeof h.onSyncReply === 'function' ? h.onSyncReply : null,
				onBroadcast: typeof h.onBroadcast === 'function' ? h.onBroadcast : null,
				onAck: typeof h.onAck === 'function' ? h.onAck : null,
				onLeave: typeof h.onLeave === 'function' ? h.onLeave : null,
				onShoot: typeof h.onShoot === 'function' ? h.onShoot : null
			};
			ensureSubscriber();
		},

		/**
		 * Forward a client's command batch to the topic's owner (fire-and-forget,
		 * ONE envelope so intra-batch order is preserved). Only the owner's
		 * onCommand handler acts on it; non-owners that receive the broadcast
		 * have no authority for the topic and ignore it.
		 * @param {string} wireTopic
		 * @param {string} identity - the commanding client's stable identity.
		 * @param {string} originInstance - the instance the client is connected to (for ack routing).
		 * @param {any[]} batch
		 */
		relayCommand(wireTopic, identity, originInstance, batch) {
			publish({ i: instanceId, k: KIND_COMMAND, t: wireTopic, id: identity, o: originInstance, b: batch });
		},

		/**
		 * Forward a client's shot to the topic's owner (fire-and-forget; the
		 * authoritative hit rides the owner's existing event broadcast back to
		 * every instance, so a shot needs no correlated reply). Only the owner's
		 * onShoot handler acts on it. The payload carries the EDGE-measured
		 * durations (`reach` width + a rewind `age`), never an absolute timestamp,
		 * so the owner rebuilds the rewind on its own ring axis without subtracting
		 * a clock it does not author.
		 * @param {string} wireTopic
		 * @param {string} shooterIdentity - the firing client's stable identity (its entity lives on the owner).
		 * @param {string} originInstance - the instance the client is connected to.
		 * @param {any} payload - `{ cmd, reach, rewindAge, detect? }`, all edge-measured.
		 */
		relayShoot(wireTopic, shooterIdentity, originInstance, payload) {
			publish({ i: instanceId, k: KIND_SHOOT, t: wireTopic, id: shooterIdentity, o: originInstance, p: payload });
		},

		/**
		 * Ask the topic's owner for the entity catalog (correlation request).
		 * The owner answers via sendSyncReply targeted at `originInstance` and
		 * correlated by `corr`.
		 * @param {string} wireTopic
		 * @param {string} identity
		 * @param {string} originInstance - this instance's id (where the reply must land).
		 * @param {string} corr - caller-minted correlation id for the awaiting request.
		 */
		requestSync(wireTopic, identity, originInstance, corr) {
			ensureSubscriber(); // so the targeted reply finds a live subscriber
			publish({ i: instanceId, k: KIND_SYNC_REQUEST, t: wireTopic, id: identity, o: originInstance, c: corr });
		},

		/**
		 * Answer a sync request with the catalog, targeted at the requester.
		 * @param {string} wireTopic
		 * @param {string} corr - the request's correlation id.
		 * @param {string} toInstance - the requester's instance id.
		 * @param {any} payload - the catalog reply (e.g. `{ ack, states }`).
		 */
		sendSyncReply(wireTopic, corr, toInstance, payload) {
			publish({ i: instanceId, k: KIND_SYNC_REPLY, t: wireTopic, c: corr, to: toInstance, p: payload });
		},

		/**
		 * Relay one of the owner's broadcasts (update / event / remove) to every
		 * other instance so they re-emit it to their local subscribers. The owner
		 * emits to its OWN subscribers locally and echo-suppression keeps it from
		 * re-processing this relay.
		 * @param {string} wireTopic
		 * @param {string} event
		 * @param {any} data
		 * @param {string | undefined} excludeIdentity - author to exclude on the re-emitting side.
		 * @param {number} seq - per-topic monotonic sequence for receive-side dedup.
		 */
		relayBroadcast(wireTopic, event, data, excludeIdentity, seq) {
			publish({ i: instanceId, k: KIND_BROADCAST, t: wireTopic, e: event, d: data, x: excludeIdentity, s: seq });
		},

		/**
		 * Relay an ack to the single instance the commanding client is on.
		 * @param {string} wireTopic
		 * @param {string} identity - the commanding client's identity (resolved to a local socket there).
		 * @param {string} toInstance - the instance the client is connected to.
		 * @param {any} payload - the ack payload.
		 */
		relayAck(wireTopic, identity, toInstance, payload) {
			publish({ i: instanceId, k: KIND_ACK, t: wireTopic, id: identity, to: toInstance, p: payload });
		},

		/**
		 * Tell the owner a client left so it can drop the client's surrogate and
		 * broadcast the entity removal (the entity lives on the owner, the close
		 * fires on the forwarder).
		 * @param {string} wireTopic
		 * @param {string} identity
		 * @param {string} originInstance
		 */
		relayLeave(wireTopic, identity, originInstance) {
			publish({ i: instanceId, k: KIND_LEAVE, t: wireTopic, id: identity, o: originInstance });
		},

		/**
		 * Become (or stay) the topic's tick owner. Resolves true when this
		 * instance holds the lease and may tick; false when another instance
		 * holds it. Claims a free or expired lease (`SET NX PX`); if held,
		 * renews only when it is already ours. Rejects on a Redis error so the
		 * caller can decide how to degrade.
		 * @param {string} wireTopic
		 * @returns {Promise<boolean>}
		 */
		async acquireOwner(wireTopic) {
			if (destroyed) return false;
			const key = ownerKey(wireTopic);
			const r = await redis.set(key, instanceId, 'NX', 'PX', leaseMs);
			if (r === 'OK') return true; // newly acquired (free or expired)
			// Held by someone: renew only if it is ours (compare-and-pexpire).
			const renew = await evalCached(redis, LEASE_RENEW_SCRIPT, 1, key, instanceId, leaseMs);
			return Number(renew) === 1;
		},

		/**
		 * Renew this instance's ownership lease, WITHOUT acquiring a free one.
		 * Resolves true when the lease was ours and its TTL was refreshed; false
		 * when we no longer own it (expired and unclaimed, or taken over). The
		 * realtime layer calls this on its tick while it holds live entities; a
		 * false result means it must stop ticking the topic and re-sync. Rejects
		 * on a Redis error.
		 * @param {string} wireTopic
		 * @returns {Promise<boolean>}
		 */
		async renewOwner(wireTopic) {
			if (destroyed) return false;
			const renew = await evalCached(redis, LEASE_RENEW_SCRIPT, 1, ownerKey(wireTopic), instanceId, leaseMs);
			return Number(renew) === 1;
		},

		/**
		 * Release this instance's ownership lease (compare-and-delete: only when
		 * it is ours) so a sibling can take over within a renew cycle instead of
		 * waiting out the TTL. Best-effort: never throws (the TTL is the safety
		 * net), resolves true when released, false when not ours or on a Redis
		 * error.
		 * @param {string} wireTopic
		 * @returns {Promise<boolean>}
		 */
		async releaseOwner(wireTopic) {
			if (destroyed) return false;
			try {
				const released = await evalCached(redis, LEASE_RELEASE_SCRIPT, 1, ownerKey(wireTopic), instanceId);
				return Number(released) === 1;
			} catch {
				return false;
			}
		},

		/**
		 * Diagnostic read of the topic's current owner instance id. Best-effort:
		 * never throws, resolves null when unowned or on a Redis error.
		 * @param {string} wireTopic
		 * @returns {Promise<string | null>}
		 */
		async currentOwner(wireTopic) {
			if (destroyed) return null;
			try {
				return await redis.get(ownerKey(wireTopic));
			} catch {
				return null;
			}
		},

		/**
		 * Persist a topic's catalog as its warm-handoff snapshot so a sibling
		 * that takes over after this owner dies can seed entities from their last
		 * state instead of resetting them to `initial`. Owner-only by contract:
		 * the realtime layer calls this only while it owns the topic's tick.
		 * Fire-and-forget and best-effort - breaker-guarded, never throws, and
		 * expires after `snapshotTtlMs` (refreshed on every write).
		 * @param {string} wireTopic
		 * @param {any} payload - the owner's catalog (`Array<{ key, state }>`).
		 * @returns {Promise<void>}
		 */
		async writeSnapshot(wireTopic, payload) {
			if (destroyed) return;
			if (b) { try { b.guard(); } catch { return; } }
			try {
				await redis.set(snapKey(wireTopic), JSON.stringify(payload), 'PX', snapshotTtlMs);
			} catch { /* best-effort: the warm handoff is an optimization, not a correctness guarantee */ }
		},

		/**
		 * Read a topic's warm-handoff snapshot (the catalog the previous owner
		 * persisted), or null when absent, expired, or on a Redis/parse error. A
		 * fresh owner calls this on acquire to seed entities from their last known
		 * state. Best-effort: never throws.
		 * @param {string} wireTopic
		 * @returns {Promise<any>}
		 */
		async readSnapshot(wireTopic) {
			if (destroyed) return null;
			if (b) { try { b.guard(); } catch { return null; } }
			try {
				const raw = await redis.get(snapKey(wireTopic));
				return raw == null ? null : JSON.parse(raw);
			} catch {
				return null;
			}
		},

		/** Tear down the subscriber. Idempotent. */
		destroy() {
			if (destroyed) return;
			destroyed = true;
			handlers = {
				onCommand: null, onSync: null, onSyncReply: null,
				onBroadcast: null, onAck: null, onLeave: null, onShoot: null
			};
			if (subscriber) {
				const s = subscriber;
				subscriber = null;
				subscriberReady = null;
				try { s.quit().catch(() => s.disconnect()); } catch { /* already gone */ }
			}
		}
	};
}
