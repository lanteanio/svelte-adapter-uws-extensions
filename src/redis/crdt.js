/**
 * Redis-backed cluster coordinator for conflict-free documents
 * (`svelte-realtime` `live.doc` / `live.map` / `live.array`).
 *
 * The document replica itself lives in the adapter
 * (`svelte-adapter-uws/plugins/crdt/replica`) and is owned per instance by the
 * realtime layer. This module is pure transport: it carries applied updates
 * between instances so every instance's replica converges, answers a
 * cold-joining instance's sync request with whatever a live peer holds beyond
 * the persisted snapshot, repairs the replicas after the relay lost traffic,
 * and gates snapshot persistence to one writer per topic so concurrent
 * instances never clobber each other's stores.
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
 *   - **Repair after a lost relay.** Pub/sub is fire-and-forget: a relay the
 *     breaker refused or Redis failed, and a message published while this
 *     instance's subscriber was reconnecting, never arrive. Each of those marks
 *     the topic, and once the relay works again the coordinator runs a two-way
 *     resync for it: it broadcasts the replica's full state when peers may
 *     lack local edits (peers merge it) and asks every peer for its full state
 *     (this instance merges the replies). The request carries a digest of
 *     this replica's full state, and a peer whose state encodes to the same
 *     digest answers empty, so a round over converged replicas moves no
 *     document bytes. Merging is idempotent, so a resync never harms a
 *     replica that lost nothing. Triggers: the breaker closing
 *     or probing, the subscriber restoring its channel, a failed or refused
 *     publish (retried with a backoff when nothing else signals recovery), and
 *     a freshly acquired persist lease. A lost sync reply is not a resync:
 *     the coordinator answers that requester again on recovery. A receiver
 *     that has to drop an envelope arriving in parts (its reassembly buffer is
 *     full, or the parts stopped arriving) resyncs that topic too.
 *   - **One writer per topic.** Snapshot persistence is gated by a per-topic
 *     lease (`acquirePersist`): the lease holder is the sole persister for that
 *     topic, so two instances can never write divergent snapshots to the same
 *     key. The lease rotates when the holder's last local subscriber leaves or
 *     it dies.
 *   - **Sync before the first write.** A replica only holds what reached it,
 *     so a freshly acquired lease (or one renewed while a resync for the topic
 *     is running) waits for a sync round - bounded to a third of the lease TTL
 *     - before the write is allowed; whatever the round merged also schedules
 *     a store of the converged state. A topic whose own edits have not reached
 *     the peers, or whose round could not reach them, refuses persistence
 *     (rejects, so the schedule retries and the replica stays loaded) until a
 *     resync delivered them.
 *   - **Cold-join freshness.** A persisted snapshot is only as fresh as the
 *     last debounced store. When an instance cold-loads a topic it loads the
 *     snapshot AND broadcasts a sync request carrying its state vector; any
 *     instance holding the topic replies with exactly the updates the joiner
 *     lacks. The joiner applies the reply on top of the snapshot - idempotent,
 *     so applying both is always safe - closing the staleness gap whenever a
 *     live peer exists.
 *
 * The coordinator reads a replica's state through the realtime layer's own
 * `onSyncRequest` handler: asked for the diff against an empty state vector on
 * a local reply address, the handler answers synchronously through
 * `sendSyncReply` with the full state, and no answer means the instance no
 * longer holds the topic. A resync request carries a round id (`c`), the
 * sha256 digest of the requester's full state (`h`) and asks every peer to
 * answer (`a: 1`); a peer holding nothing or the same state answers with an empty
 * reply, so on a standalone server, where PUBLISH reports how many subscribers
 * received the request, a round ends as soon as every peer answered. Peers
 * that predate these fields ignore them and answer as for a cold join.
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

import { createHash } from 'node:crypto';
import { randomBytes, setTimer, clearTimer, setImmediateTimer, monotonicNow } from '../shared/runtime.js';
import { guardBreaker } from '../shared/breaker-admission.js';
import { evalCached } from '../shared/eval-cached.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { LEASE_RENEW_SCRIPT } from '../shared/lease-scripts.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure } from '../shared/diagnostic.js';
import { openSubscriber, SUBSCRIBER_RESTORING, SUBSCRIBER_RESTORED } from '../shared/redis-subscriber.js';
import { isCluster } from '../shared/cluster.js';
import { operationError } from '../shared/errors.js';

/** Relay message kinds (the `k` field on every envelope). */
const KIND_UPDATE = 'u';
const KIND_SYNC_REQUEST = 'sreq';
const KIND_SYNC_REPLY = 'srep';
/** One slice of an envelope larger than `maxEnvelopeBytes`. */
const KIND_PART = 'part';

// Bounds on the envelopes being reassembled from parts: base64 characters
// held across all of them, how many at once, and how long one may take.
// When a bound is hit the sender holding the most gives way, so one peer's
// large resync cannot starve the others.
const PART_BUFFER_MAX_CHARS = 64 * 1024 * 1024;
const PART_ASSEMBLIES_MAX = 256;
const PART_TIMEOUT_MS = 30000;
// Envelopes dropped while their parts were arriving, remembered so their
// remaining parts do not start a new assembly that can never complete.
const PART_DROPPED_MAX = 1024;

/** startRound's answer when the breaker refused the round. */
const REFUSED = Symbol('refused');

// Retry cadence for a resync the relay could not carry, when no breaker
// transition or subscriber restore signals recovery first.
const RESYNC_RETRY_FIRST_MS = 1000;
const RESYNC_RETRY_MAX_MS = 30000;

// Resync rounds whose publishes may be in flight at once, so a recovery over
// many held documents does not encode and publish them all in one task.
const RESYNC_CONCURRENCY = 8;

// The topics remembered for a resync are swept for ones no longer held once
// the set grows past this, then past twice its size after the last sweep.
const KNOWN_SWEEP_MIN = 1024;
// Replica reads one sweep step makes before yielding the event loop; each
// read encodes a document's full state.
const SWEEP_SLICE = 64;

/** Digest of a replica's full state, so a peer holding the same state can say so without sending it. */
function digestOf(bytes) {
	return createHash('sha256').update(Uint8Array.from(bytes)).digest('base64');
}

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
 *   dies. Default 6s suits the 2s document debounce default. A freshly
 *   acquired lease waits up to a third of it for the sync round.
 * @property {number} [maxEnvelopeBytes=1048576] - Reject inbound relay
 *   messages larger than this BEFORE JSON.parse. Defends against a bus-side
 *   DoS on shared-Redis deployments. A larger envelope (a big update or full
 *   document state) is sent as parts that each fit, so every instance must
 *   use the same value.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional
 *   circuit breaker; while it is open no relay is published, and every topic
 *   whose traffic it refused is resynced with the peers once it lets traffic
 *   through again.
 * @property {(err: unknown) => void} [onError] - Observe a relay-subscriber
 *   failure (the subscriber tears down, is opened again, and resyncs), and a
 *   relay envelope too large to send even in parts (code
 *   `RELAY_ENVELOPE_TOO_LARGE`; its topic keeps retrying, the other topics
 *   are unaffected).
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
	// Where a local state read is answered; never a peer's (hex) identity.
	const localReplyTo = 'local:' + instanceId;
	const persistLeaseMs = positive(options.persistLeaseMs, 6000);
	const syncWindowMs = Math.max(1, Math.floor(persistLeaseMs / 3));
	// PUBLISH counts every receiver only on a standalone server; a Cluster
	// reports the receiving node's subscribers alone.
	const countsReceivers = !isCluster(redis);
	const validator = createBusValidator({
		label: 'crdt cluster',
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: false,
		allowedSystemTopics: []
	});
	const b = options.breaker;

	/** @type {{ onUpdate: Function | null, onSyncRequest: Function | null, onSyncReply: Function | null }} */
	let handlers = { onUpdate: null, onSyncRequest: null, onSyncReply: null };
	let wired = false;
	/** @type {any} */
	let subscriber = null;
	/** @type {Promise<any> | null} */
	let subscriberReady = null;
	// Whether this instance's subscriber currently receives the channel.
	let subscribed = false;
	// Set when the subscriber missed traffic it will not get back by itself.
	let inboundLost = false;
	// One report per failure streak: every relay retries a failed subscribe.
	let subscribeFailing = false;
	let destroyed = false;

	/** @type {Map<string, string>} topic -> declKey, for every topic seen held */
	const known = new Map();
	let sweepAt = KNOWN_SWEEP_MIN;
	/** @type {Iterator<[string, string]> | null} the sweep in progress */
	let sweeping = null;
	/** @type {Map<string, number>} topics holding local edits the peers may lack -> when last marked */
	const unrelayed = new Map();
	let marks = 0;
	/** @type {Map<string, Set<Promise<number | null>>>} topic -> update publishes Redis has not confirmed */
	const inflight = new Map();
	/** @type {Set<string>} topics whose replica may lack edits the peers sent */
	const stale = new Set();
	/** @type {Map<string, Map<string, { declKey: string, v: any[], c: string | undefined }>>} topic -> requester -> a sync reply the relay lost */
	const owed = new Map();
	/** @type {WeakMap<object, any[]>} sync reply envelope -> the state vector it answered */
	const replyVectors = new WeakMap();
	/** @type {Set<string>} topics waiting for a resync round */
	const queue = new Set();
	let activeRounds = 0;
	/** @type {Map<string, any>} topic -> the resync round in flight */
	const rounds = new Map();
	/** @type {any} */
	let retryTimer = null;
	let retryMs = 0;
	/** @type {unknown} */
	let lastRelayError = null;
	// Counts the relays the breaker refused, so a loop can tell that the
	// breaker just refused one; a relay lost for any other reason concerns
	// its own topic only.
	let breakerRefusals = 0;
	/** @type {{ declKey: string, topic: string, bytes: any } | null} */
	let capture = null;
	/** @type {{ declKey: string, topic: string, to: string, c: string | undefined, v: any[], replied: boolean } | null} */
	let answering = null;

	/** Remember a topic this instance holds, so a recovery can resync it. */
	function remember(declKey, topic) {
		if (typeof declKey !== 'string' || typeof topic !== 'string') return;
		known.set(topic, declKey);
		if (known.size > sweepAt && sweeping === null) {
			sweeping = known.entries();
			setImmediateTimer(sweep);
		}
	}

	/**
	 * Forget the remembered topics this instance no longer holds, a slice per
	 * event-loop turn so a large set never stalls the loop.
	 */
	function sweep() {
		if (destroyed || sweeping === null) { sweeping = null; return; }
		let reads = 0;
		while (reads < SWEEP_SLICE) {
			const next = sweeping.next();
			if (next.done) {
				sweeping = null;
				sweepAt = Math.max(KNOWN_SWEEP_MIN, known.size * 2);
				return;
			}
			const [topic, declKey] = next.value;
			if (rounds.has(topic) || unrelayed.has(topic) || stale.has(topic) || queue.has(topic)) continue;
			reads++;
			if (readState(declKey, topic) === null) { known.delete(topic); owed.delete(topic); }
		}
		setImmediateTimer(sweep);
	}

	/**
	 * Read the local replica's full state through the realtime handler (see the
	 * module comment), or null when this instance does not hold the topic.
	 * @returns {any[] | null}
	 */
	function readState(declKey, topic) {
		const h = handlers.onSyncRequest;
		if (!h) return null;
		const prev = capture;
		const slot = { declKey, topic, bytes: null };
		capture = slot;
		try {
			h(declKey, topic, [], localReplyTo);
		} catch {
			/* a handler that throws holds nothing readable */
		} finally {
			capture = prev;
		}
		return Array.isArray(slot.bytes) ? slot.bytes : null;
	}

	/**
	 * A relay was refused or failed: resync its topic once Redis takes traffic
	 * again. A lost request leaves this replica behind; lost content leaves the
	 * peers behind.
	 */
	function relayLost(obj, err, permanent = false) {
		lastRelayError = err;
		const topic = obj.t;
		if (typeof topic === 'string' && known.has(topic)) {
			if (obj.k === KIND_SYNC_REQUEST) stale.add(topic);
			else if (obj.k === KIND_UPDATE && Array.isArray(obj.b) && obj.b.length > 0) unrelayed.set(topic, ++marks);
			else if (!permanent && obj.k === KIND_SYNC_REPLY && Array.isArray(obj.b) && obj.b.length > 0 && typeof obj.to === 'string') {
				// The requester lacks this instance's state, but no local
				// edit is at risk: answer it again once the relay recovers.
				let peers = owed.get(topic);
				if (!peers) owed.set(topic, peers = new Map());
				peers.set(obj.to, { declKey: obj.d, v: replyVectors.get(obj) ?? [], c: obj.c });
			}
		}
		scheduleRetry();
	}

	/** Hand a failure to the host's onError, which must not break the relay. */
	function report(err) {
		if (typeof options.onError === 'function') { try { options.onError(err); } catch { /* host handler */ } }
	}

	/**
	 * Publish one envelope, after the subscriber is up so we never miss a reply
	 * we caused. Resolves to the receiver count Redis reports, or null when the
	 * breaker refused the publish, Redis failed it, or it is too large for the
	 * relay; never rejects. A publish given a `verdict` runs under an admission
	 * its caller took and reports to: it records whether Redis took it, or the
	 * first error, there instead of on the breaker.
	 * @param {any} obj
	 * @param {{ reached: boolean, err: unknown } | null} [verdict]
	 * @returns {Promise<number | null>}
	 */
	function publish(obj, verdict = null) {
		if (destroyed) return Promise.resolve(null);
		const msgs = encode(obj);
		if (msgs === null) {
			const err = operationError('RELAY_ENVELOPE_TOO_LARGE', 'crdt: the envelope exceeds the relay size limit even when sent in parts');
			relayLost(obj, err, true);
			report(err);
			return Promise.resolve(null);
		}
		if (b && verdict === null) {
			try { b.guard(); } catch (err) { breakerRefusals++; relayLost(obj, err); return Promise.resolve(null); }
		}
		const send = () => Promise.all(msgs.map((msg) => redis.publish(channel, msg))).then((counts) => {
			if (verdict) verdict.reached = true;
			else b?.success();
			return Number(counts[0]);
		}, (err) => {
			if (verdict) { if (verdict.err === null) verdict.err = err; }
			else b?.failure(err);
			relayLost(obj, err);
			return null;
		});
		const sent = subscriberReady ? subscriberReady.then(send) : send();
		if (obj.k === KIND_UPDATE && typeof obj.t === 'string') {
			// Persistence waits for these: an edit is not safe to unload
			// until Redis confirmed its relay.
			const topic = obj.t;
			let pending = inflight.get(topic);
			if (!pending) inflight.set(topic, pending = new Set());
			pending.add(sent);
			sent.then(() => {
				pending.delete(sent);
				if (pending.size === 0 && inflight.get(topic) === pending) inflight.delete(topic);
			});
		}
		return sent;
	}

	/**
	 * Serialize an envelope into the messages that carry it: itself when it
	 * fits `maxEnvelopeBytes`, else parts whose base64 slices of its bytes
	 * rebuild it on the receiver (coordinators without parts ignore them).
	 * Null when even parts cannot carry it.
	 * @returns {string[] | null}
	 */
	function encode(obj) {
		const msg = JSON.stringify(obj);
		if (Buffer.byteLength(msg) <= validator.maxBytes) return [msg];
		if (!Array.isArray(obj.b)) return null;
		const data = Buffer.from(Uint8Array.from(obj.b)).toString('base64');
		const { i, d, t, b: _bytes, ...rest } = obj;
		/** @type {any} */
		const head = { i, k: KIND_PART, d, t, p: randomBytes(6).toString('hex') };
		// A receiver drops the parts of a reply addressed to someone else.
		if (typeof obj.to === 'string') head.to = obj.to;
		const overhead = Buffer.byteLength(JSON.stringify({ ...head, n: 1e9, of: 1e9, e: rest, x: '' }));
		const room = validator.maxBytes - overhead;
		if (room < 1 || data.length > PART_BUFFER_MAX_CHARS) return null;
		const of = Math.ceil(data.length / room);
		const out = [];
		for (let n = 0; n < of; n++) {
			/** @type {any} */
			const part = { ...head, n, of, x: data.slice(n * room, (n + 1) * room) };
			if (n === 0) part.e = rest;
			out.push(JSON.stringify(part));
		}
		return out;
	}

	/** @type {Map<string, { from: string, of: number, d: string, t: string, e: any, parts: Map<number, string>, chars: number, timer: any }>} */
	const assemblies = new Map();
	let partChars = 0;
	/** @type {Map<string, { chars: number, count: number }>} sender -> what its assemblies hold */
	const senders = new Map();
	/** @type {Map<string, number>} dropped assembly -> until when its parts are ignored */
	const dropped = new Map();

	/**
	 * Remove an assembly. A `lost` one carried traffic this instance will not
	 * get otherwise: its topic is resynced, and its remaining parts are ignored.
	 */
	function dropAssembly(key, lost = false) {
		const a = assemblies.get(key);
		if (!a) return;
		assemblies.delete(key);
		partChars -= a.chars;
		const sender = /** @type {{ chars: number, count: number }} */ (senders.get(a.from));
		sender.chars -= a.chars;
		if (--sender.count === 0) senders.delete(a.from);
		if (a.timer !== null) clearTimer(a.timer);
		if (lost) partsLost(key, a.t);
	}

	/** Envelope parts were lost: ignore the rest of them and resync their topic. */
	function partsLost(key, topic) {
		const at = monotonicNow();
		for (const [k, until] of dropped) {
			if (until > at && dropped.size < PART_DROPPED_MAX) break;
			dropped.delete(k);
		}
		dropped.set(key, at + PART_TIMEOUT_MS);
		if (typeof topic === 'string' && known.has(topic)) {
			stale.add(topic);
			scheduleRetry();
		}
	}

	function isDropped(key) {
		const until = dropped.get(key);
		if (until === undefined) return false;
		if (until > monotonicNow()) return true;
		dropped.delete(key);
		return false;
	}

	/** The sender holding the most by `by`, other than `except`, or null. */
	function heaviestSender(by, except) {
		let best = null;
		let most = -1;
		for (const [from, held] of senders) {
			if (from !== except && held[by] > most) { best = from; most = held[by]; }
		}
		return best === null ? null : { from: best, most };
	}

	/** Drop the assembly of `from` holding the most characters. */
	function evictFrom(from) {
		let victim = null;
		let most = -1;
		for (const [key, a] of assemblies) {
			if (a.from === from && a.chars > most) { victim = key; most = a.chars; }
		}
		if (victim !== null) dropAssembly(victim, true);
	}

	/**
	 * Add one part; returns the rebuilt envelope once every part arrived. Parts
	 * may arrive in any order (a Redis Cluster routes each PUBLISH on its own).
	 */
	function assemble(parsed) {
		const { i: from, p: id, n, of, x, d, t } = parsed;
		if (typeof from !== 'string' || typeof id !== 'string' || typeof x !== 'string') return null;
		if (!Number.isInteger(of) || of < 1 || !Number.isInteger(n) || n < 0 || n >= of) return null;
		const key = from + ' ' + id;
		let a = assemblies.get(key);
		if (!a) {
			if (isDropped(key)) return null;
			if (assemblies.size >= PART_ASSEMBLIES_MAX) {
				// Room comes from the sender with the most assemblies, unless
				// that is this one.
				const own = senders.get(from)?.count ?? 0;
				const other = heaviestSender('count', from);
				if (other === null || other.most <= own) { partsLost(key, t); return null; }
				evictFrom(other.from);
			}
			a = { from, of, d, t, e: null, parts: new Map(), chars: 0, timer: null };
			assemblies.set(key, a);
			let sender = senders.get(from);
			if (!sender) senders.set(from, sender = { chars: 0, count: 0 });
			sender.count++;
			a.timer = setTimer(() => { a.timer = null; dropAssembly(key, true); }, PART_TIMEOUT_MS);
			if (a.timer && a.timer.unref) a.timer.unref();
		}
		if (a.of !== of || a.d !== d || a.t !== t || a.parts.has(n)) return null;
		while (partChars + x.length > PART_BUFFER_MAX_CHARS) {
			// The sender holding the most characters gives way; this part's
			// own envelope when that is its sender.
			const own = /** @type {{ chars: number, count: number }} */ (senders.get(from)).chars + x.length;
			const other = heaviestSender('chars', from);
			if (other === null || other.most <= own) { dropAssembly(key, true); return null; }
			evictFrom(other.from);
		}
		if (n === 0) {
			const e = parsed.e;
			if (!e || typeof e !== 'object' || Array.isArray(e) || e.k === KIND_PART) { dropAssembly(key); return null; }
			a.e = e;
		}
		a.parts.set(n, x);
		a.chars += x.length;
		/** @type {{ chars: number, count: number }} */ (senders.get(from)).chars += x.length;
		partChars += x.length;
		if (a.parts.size < a.of) return null;
		dropAssembly(key);
		let data = '';
		for (let k = 0; k < a.of; k++) data += a.parts.get(k);
		return { ...a.e, i: from, d: a.d, t: a.t, b: Array.from(Buffer.from(data, 'base64')) };
	}

	/** The rejection while a topic's local edits have not reached the peers, or its replica has not synced with them. */
	function relayPending(message = 'crdt: local edits to the document have not reached the other instances') {
		const err = operationError('CRDT_RELAY_PENDING', message);
		if (lastRelayError !== null) err.cause = lastRelayError;
		return err;
	}

	/**
	 * Wait, at most the sync window, for the topic's update publishes started
	 * before this call; rejects while one of them is still unconfirmed.
	 */
	async function awaitRelayed(topic) {
		const pending = inflight.get(topic);
		if (!pending) return;
		const started = [...pending];
		let timer = null;
		await Promise.race([
			Promise.all(started),
			new Promise((resolve) => {
				timer = setTimer(resolve, syncWindowMs);
				if (timer && timer.unref) timer.unref();
			})
		]);
		if (timer !== null) clearTimer(timer);
		if (started.some((p) => pending.has(p))) throw relayPending();
	}

	/** Apply one relay envelope from a peer. */
	function receive(parsed) {
		if (!parsed || typeof parsed !== 'object') return;
		if (parsed.i === instanceId) return; // echo suppression
		const kind = parsed.k;
		const declKey = parsed.d;
		const topic = parsed.t;
		// `topic` rides the bus validator's topic-shape check (it is the
		// bare document name, never `__`-prefixed); `kind` doubles as the
		// event-shape argument.
		if (typeof declKey !== 'string' || typeof kind !== 'string' || !validator.acceptEnvelope(topic, kind)) return;
		if (kind === KIND_PART) {
			// Only the addressee of a reply rebuilds it.
			if (typeof parsed.to === 'string' && parsed.to !== instanceId) return;
			const whole = assemble(parsed);
			if (whole) receive(whole);
		} else if (kind === KIND_UPDATE) {
			if (handlers.onUpdate && Array.isArray(parsed.b)) handlers.onUpdate(declKey, topic, parsed.b);
		} else if (kind === KIND_SYNC_REQUEST) {
			answerSyncRequest(parsed, declKey, topic);
		} else if (kind === KIND_SYNC_REPLY) {
			// Targeted: only the requester applies a reply addressed to it.
			if (parsed.to !== instanceId || !Array.isArray(parsed.b)) return;
			if (parsed.b.length > 0 && handlers.onSyncReply) handlers.onSyncReply(declKey, topic, parsed.b);
			noteReply(topic, parsed.i, parsed.c);
		}
	}

	/** Bring up the relay subscriber once; idempotent. */
	function ensureSubscriber() {
		if (subscriber || destroyed) return;
		const sub = openSubscriber(client, 'crdt');
		subscriber = sub;
		sub.on('error', (err) => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.CRDT_SUBSCRIBER, { error: err }));
		sub.on(SUBSCRIBER_RESTORING, () => {
			if (subscriber === sub) subscribed = false;
		});
		sub.on(SUBSCRIBER_RESTORED, () => {
			if (subscriber !== sub || destroyed) return;
			// Whatever peers published while the channel was down is gone.
			subscribed = true;
			resyncAll();
		});
		sub.on('message', (ch, message) => {
			if (ch !== channel) return;
			if (!validator.acceptRaw(message)) return; // size cap before parse
			let parsed;
			try { parsed = JSON.parse(message); } catch { return; }
			receive(parsed);
		});
		subscriberReady = sub.subscribe(channel).then(() => {
			if (subscriber !== sub) return;
			subscribeFailing = false;
			subscribed = true;
			if (inboundLost) {
				inboundLost = false;
				resyncAll();
			}
		}, (err) => {
			// Failed subscribe: tear down so the retry below re-subscribes
			// fresh, then resyncs what peers published meanwhile.
			try { sub.quit().catch(() => sub.disconnect()); } catch { /* already gone */ }
			if (subscriber === sub) { subscriber = null; subscriberReady = null; subscribed = false; }
			inboundLost = true;
			scheduleRetry();
			report(err);
			if (!subscribeFailing) {
				subscribeFailing = true;
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.CRDT_SUBSCRIBE, { error: err });
			}
		});
	}

	/** Answer a peer's sync request through the realtime handler. */
	function answerSyncRequest(parsed, declKey, topic) {
		const from = parsed.i;
		const v = Array.isArray(parsed.v) ? parsed.v : [];
		const ctx = { declKey, topic, to: from, c: typeof parsed.c === 'string' ? parsed.c : undefined, v, replied: false };
		if (typeof parsed.h === 'string' && typeof from === 'string') {
			// A resync request carries the digest of the requester's full
			// state: a replica holding exactly that state answers empty.
			const full = readState(declKey, topic);
			if (full !== null && full.length > 0 && digestOf(full) !== parsed.h) {
				ctx.replied = true;
				sendReply(declKey, topic, full, from, ctx.c, v);
			}
		} else if (handlers.onSyncRequest) {
			const prev = answering;
			answering = ctx;
			try {
				handlers.onSyncRequest(declKey, topic, v, from);
			} finally {
				answering = prev;
			}
		}
		// A resync round counts its answers, so a peer holding nothing says so.
		if (parsed.a === 1 && !ctx.replied && typeof from === 'string') {
			publish({ i: instanceId, k: KIND_SYNC_REPLY, d: declKey, t: topic, b: [], to: from, c: ctx.c });
		}
	}

	/** Publish a sync reply targeted at one requester. */
	function sendReply(declKey, topic, bytes, to, c, v) {
		/** @type {any} */
		const env = { i: instanceId, k: KIND_SYNC_REPLY, d: declKey, t: topic, b: bytes, to };
		if (c !== undefined) env.c = c;
		if (v !== undefined) replyVectors.set(env, v);
		publish(env);
	}

	/** Count a peer's answer toward the topic's resync round. */
	function noteReply(topic, from, c) {
		const round = rounds.get(topic);
		if (!round || typeof from !== 'string') return;
		if (c !== undefined && c !== round.id) return;
		round.replied.add(from);
		if (round.expected !== null && round.replied.size >= round.expected) finishRound(round);
	}

	/**
	 * End a round. It resolves true when Redis confirmed its sync request, so
	 * every peer received it; false when the request was lost or still
	 * unconfirmed when the window closed.
	 */
	function finishRound(round) {
		if (round.done) return;
		round.done = true;
		if (round.timer !== null) { clearTimer(round.timer); round.timer = null; }
		if (rounds.get(round.topic) === round) rounds.delete(round.topic);
		// A request not known to have reached the peers leaves the replica
		// unsynced: the next write runs another round.
		if (!round.asked && !destroyed && known.has(round.topic)) stale.add(round.topic);
		round.resolve(round.asked);
	}

	/**
	 * Start a two-way resync of one topic: broadcast the replica's full state
	 * when peers may lack local edits, and ask every peer for its full state.
	 * Returns the round, null when this instance no longer holds the topic, or
	 * REFUSED when the breaker refused it. The round settles when every peer
	 * answered or the sync window closed.
	 * @param {string} topic
	 */
	function startRound(topic) {
		const existing = rounds.get(topic);
		if (existing) return existing;
		queue.delete(topic);
		const declKey = known.get(topic);
		if (declKey === undefined) {
			stale.delete(topic);
			unrelayed.delete(topic);
			return null;
		}
		// One admission covers both publishes of the round; a round that
		// publishes nothing hands it back unused.
		let release;
		try {
			release = guardBreaker(b);
		} catch (err) {
			// Refused before reading the replica: the breaker transition or
			// the retry resyncs it.
			lastRelayError = err;
			breakerRefusals++;
			stale.add(topic);
			scheduleRetry();
			return REFUSED;
		}
		const full = readState(declKey, topic);
		stale.delete(topic);
		if (full === null) {
			release();
			known.delete(topic);
			unrelayed.delete(topic);
			return null;
		}
		// The mark stays until Redis confirmed the push, and a loss recorded
		// meanwhile keeps it.
		const mark = unrelayed.get(topic);
		/** @type {any} */
		const round = { id: randomBytes(6).toString('hex'), topic, expected: null, replied: new Set(), asked: false, done: false, timer: null, resolve: null, promise: null, sent: null };
		round.promise = new Promise((resolve) => { round.resolve = resolve; });
		rounds.set(topic, round);
		round.timer = setTimer(() => { round.timer = null; finishRound(round); }, syncWindowMs);
		if (round.timer && round.timer.unref) round.timer.unref();
		const verdict = { reached: false, err: /** @type {unknown} */ (null) };
		const pushed = mark === undefined
			? Promise.resolve(0)
			: publish({ i: instanceId, k: KIND_UPDATE, d: declKey, t: topic, b: full }, verdict).then((p) => {
				if (p !== null && unrelayed.get(topic) === mark) unrelayed.delete(topic);
				return p;
			});
		const asked = publish({ i: instanceId, k: KIND_SYNC_REQUEST, d: declKey, t: topic, v: [], c: round.id, a: 1, h: digestOf(full) }, verdict);
		round.sent = Promise.all([pushed, asked]).then(([p, n]) => {
			// The round's one admission gets one outcome.
			if (verdict.err !== null) b?.failure(verdict.err);
			else if (verdict.reached) b?.success();
			else release();
			if (n !== null) round.asked = true;
			if (p === null || n === null) {
				// relayLost marked the topic again; nothing will answer.
				finishRound(round);
				return;
			}
			if (unrelayed.size === 0 && stale.size === 0 && !inboundLost) retryMs = 0;
			// Every receiver but this instance's own subscriber is a peer.
			round.expected = countsReceivers ? Math.max(0, n - (subscribed ? 1 : 0)) : null;
			if (round.expected !== null && round.replied.size >= round.expected) finishRound(round);
		});
		return round;
	}

	/** Run the queued resync rounds, a bounded number at a time. */
	function pump() {
		if (destroyed) return;
		while (activeRounds < RESYNC_CONCURRENCY && queue.size > 0) {
			const topic = /** @type {string} */ (queue.values().next().value);
			queue.delete(topic);
			const round = startRound(topic);
			if (round === REFUSED) {
				// The breaker refuses rounds (open, or probing with its probe
				// taken): every further one would be refused after reading its
				// replica. The breaker transition or the retry resumes. A
				// relay lost for another reason, such as an envelope too
				// large to carry, concerns its own topic only.
				for (const rest of queue) stale.add(rest);
				queue.clear();
				return;
			}
			if (!round) continue;
			activeRounds++;
			Promise.race([round.sent, round.promise]).then(() => {
				activeRounds--;
				// Traffic for the topic lost while its round ran needs another.
				if (stale.has(topic) && !rounds.has(topic)) scheduleRetry();
				pump();
			});
		}
	}

	/** Resync every remembered topic: this instance may have missed peer traffic. */
	function resyncAll() {
		for (const topic of known.keys()) queue.add(topic);
		pump();
	}

	/** The relay may carry traffic again: resync what it lost and reopen the subscriber. */
	function recover() {
		if (destroyed) return;
		if (wired && !subscriber) ensureSubscriber();
		for (const topic of unrelayed.keys()) queue.add(topic);
		for (const topic of stale) queue.add(topic);
		pump();
		answerOwed();
	}

	/** Answer the sync requests whose replies the relay lost, again. */
	function answerOwed() {
		for (const [topic, peers] of owed) {
			for (const [to, req] of peers) {
				if (b && b.state === 'broken') { scheduleRetry(); return; }
				peers.delete(to);
				if (peers.size === 0) owed.delete(topic);
				const before = breakerRefusals;
				answerSyncRequest({ i: to, v: req.v, c: req.c }, req.declKey, topic);
				if (breakerRefusals !== before) return; // refused again: re-owed, retried later
			}
		}
	}

	function scheduleRetry() {
		if (retryTimer !== null || destroyed) return;
		retryMs = retryMs === 0 ? RESYNC_RETRY_FIRST_MS : Math.min(retryMs * 2, RESYNC_RETRY_MAX_MS);
		retryTimer = setTimer(() => {
			retryTimer = null;
			recover();
		}, retryMs);
		if (retryTimer && retryTimer.unref) retryTimer.unref();
	}

	/**
	 * Join the topic's resync round, starting one when none runs. Resolves
	 * false when the round could not reach the peers.
	 * @returns {Promise<boolean>}
	 */
	function syncRound(topic) {
		const round = rounds.get(topic) ?? startRound(topic);
		if (round === REFUSED) return Promise.resolve(false);
		return round ? round.promise : Promise.resolve(true);
	}

	/**
	 * Reject while the topic's local edits have not reached the peers, or its
	 * replica may lack edits the peers sent.
	 */
	function assertSynced(topic, reached) {
		if (unrelayed.has(topic)) throw relayPending();
		if (!reached || stale.has(topic)) {
			throw relayPending('crdt: the document has not synced with the other instances since the relay lost traffic');
		}
	}

	const unsubscribeBreaker = b && typeof b.subscribe === 'function'
		? b.subscribe((from, to) => {
			if (from !== to && (to === 'healthy' || to === 'probing')) recover();
		})
		: null;

	return {
		/** This instance's relay identity (stable for the coordinator's life). */
		instanceId,

		/**
		 * Register the inbound handlers and start the relay subscriber. The
		 * realtime layer calls this once per coordinator with:
		 *   - onUpdate(declKey, topic, bytes): apply a peer's update locally + fan out.
		 *   - onSyncRequest(declKey, topic, sv, fromInstance): if held, reply via
		 *     sendSyncReply. The coordinator also asks it locally to read the
		 *     replica's state, so it must reply synchronously.
		 *   - onSyncReply(declKey, topic, bytes): apply a peer's catch-up diff locally + fan out.
		 * @param {{ onUpdate?: Function, onSyncRequest?: Function, onSyncReply?: Function }} h
		 */
		onMessage(h) {
			handlers = {
				onUpdate: typeof h.onUpdate === 'function' ? h.onUpdate : null,
				onSyncRequest: typeof h.onSyncRequest === 'function' ? h.onSyncRequest : null,
				onSyncReply: typeof h.onSyncReply === 'function' ? h.onSyncReply : null
			};
			wired = true;
			ensureSubscriber();
		},

		/** Relay one applied update to peers so their replicas converge. */
		relayUpdate(declKey, topic, bytes) {
			remember(declKey, topic);
			publish({ i: instanceId, k: KIND_UPDATE, d: declKey, t: topic, b: bytes });
		},

		/** Broadcast a cold-join sync request carrying this replica's state vector. */
		requestSync(declKey, topic, sv) {
			remember(declKey, topic);
			ensureSubscriber(); // so the targeted reply finds a live subscriber
			publish({ i: instanceId, k: KIND_SYNC_REQUEST, d: declKey, t: topic, v: sv });
		},

		/** Answer a peer's sync request with the updates it lacks (targeted). */
		sendSyncReply(declKey, topic, bytes, toInstance) {
			if (toInstance === localReplyTo) {
				// A local state read (see readState), never published.
				if (capture && capture.declKey === declKey && capture.topic === topic) capture.bytes = bytes;
				return;
			}
			remember(declKey, topic);
			if (answering && answering.to === toInstance && answering.topic === topic && answering.declKey === declKey) {
				answering.replied = true;
				sendReply(declKey, topic, bytes, toInstance, answering.c, answering.v);
			} else {
				sendReply(declKey, topic, bytes, toInstance, undefined, undefined);
			}
		},

		/**
		 * Try to become (or stay) the sole snapshot writer for a topic. Returns
		 * true when this instance holds the per-topic persist lease and may
		 * write; false when another instance holds it (skip - that instance
		 * persists). A newly acquired lease, and one renewed while a resync of
		 * the topic runs, first waits for a sync round with the peers (bounded
		 * to a third of the lease TTL). Throws on a Redis error, and with
		 * code `CRDT_RELAY_PENDING` while local edits to the topic have not
		 * reached the peers (a relay that failed, or one Redis has not
		 * confirmed within that window) or the replica has not synced with
		 * them after the relay lost traffic (a sync round the breaker refused,
		 * or whose request failed or was not confirmed within that window), so
		 * the caller's persistence schedule retries rather than dropping the
		 * write, storing a snapshot without the peers' edits, or unloading the
		 * only replica holding local edits.
		 * @param {string} topic
		 * @returns {Promise<boolean>}
		 */
		async acquirePersist(topic) {
			if (destroyed) return false;
			let synced = false;
			let reached = true;
			if (known.has(topic) && (unrelayed.has(topic) || stale.has(topic) || queue.has(topic) || rounds.has(topic))) {
				reached = await syncRound(topic);
				synced = true;
				if (destroyed) return false;
			}
			await awaitRelayed(topic);
			if (destroyed) return false;
			assertSynced(topic, reached);
			const key = client.key('crdt:persist:' + topic);
			const r = await redis.set(key, instanceId, 'NX', 'PX', persistLeaseMs);
			if (r !== 'OK') {
				// Held by someone: renew only if it is ours (compare-and-pexpire).
				const renew = await evalCached(redis, LEASE_RENEW_SCRIPT, 1, key, instanceId, persistLeaseMs);
				if (Number(renew) !== 1) return false;
			} else if (!synced && known.has(topic)) {
				// Newly acquired: the previous writer may have held edits this
				// replica never received.
				reached = await syncRound(topic);
				if (destroyed) return false;
				await awaitRelayed(topic);
			}
			if (destroyed) return false;
			// Edits marked or relays started while the lease was being taken
			// count as well.
			assertSynced(topic, reached);
			return true;
		},

		/** Tear down the subscriber. Idempotent. */
		destroy() {
			if (destroyed) return;
			destroyed = true;
			handlers = { onUpdate: null, onSyncRequest: null, onSyncReply: null };
			if (unsubscribeBreaker) unsubscribeBreaker();
			if (retryTimer !== null) { clearTimer(retryTimer); retryTimer = null; }
			for (const round of [...rounds.values()]) finishRound(round);
			queue.clear();
			unrelayed.clear();
			inflight.clear();
			stale.clear();
			owed.clear();
			known.clear();
			for (const key of [...assemblies.keys()]) dropAssembly(key);
			dropped.clear();
			if (subscriber) {
				const s = subscriber;
				subscriber = null;
				subscriberReady = null;
				subscribed = false;
				try { s.quit().catch(() => s.disconnect()); } catch { /* already gone */ }
			}
		}
	};
}
