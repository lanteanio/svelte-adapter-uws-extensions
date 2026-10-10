/**
 * Which sequence a frame this package publishes may carry, and how the buses
 * carry the origin's choice to other instances.
 *
 * A multi-worker adapter refuses any publish that would draw its per-worker
 * topic counter: the call must say `{ seq: false }`, or carry a positive
 * integer seq together with `relay: false`. `hasMultipleWorkers` is the
 * adapter's own predicate for that runtime, line for line the one in its
 * src/runtime/handler/cluster-sequence-policy.js, so the two packages always
 * agree on when the rule applies (test/shared/sequence-authority-parity.test.js
 * fails when the adapter's version changes).
 *
 * Each worker of such a runtime runs its own copy of every module here, with
 * its own Redis subscriber, so it is an instance like any other: a frame a
 * module fans out through Redis reaches the sibling workers by that route,
 * and the adapter's in-process relay would deliver it to them a second time.
 * Those local publishes say `relay: false`, which a single worker ignores.
 * What Redis does not carry (during an outage, or an envelope over the size
 * cap) misses the sibling workers as it misses every other instance.
 */

import { workerData } from 'node:worker_threads';

/** @param {any} [data] */
export function hasMultipleWorkers(data = workerData) {
	return Number.isInteger(data?.totalWorkers) && data.totalWorkers > 1;
}

/** Fixed for the worker's lifetime, like the adapter's own reading. */
export const MULTI_WORKER_RUNTIME = hasMultipleWorkers();

const RECEIVED_COUNTER = Object.freeze({ relay: false });
const RECEIVED_UNSEQUENCED = Object.freeze({ relay: false, seq: false });

/**
 * Options for a frame delivered from a source that already reached every
 * worker (a bus relay, a LISTEN on every instance) and carries no seq. A
 * single worker stamps its counter as before; a multi-worker runtime has no
 * counter to give it, so the frame goes out unsequenced.
 */
export const EXTERNAL_DELIVERY = MULTI_WORKER_RUNTIME ? RECEIVED_UNSEQUENCED : RECEIVED_COUNTER;

/** Options for a delivered frame that makes no monotonic promise on any topology. */
export const UNSEQUENCED_DELIVERY = RECEIVED_UNSEQUENCED;

/** Options for a publish that makes no monotonic promise on any topology. */
export const UNSEQUENCED_PUBLISH = Object.freeze({ seq: false });

/**
 * Options for a frame that has no seq of its own and is published on this
 * worker's own fan-out: the counter on a single worker, unsequenced on a
 * multi-worker runtime. `undefined` keeps the single-worker call unchanged.
 */
export const LOCAL_DEFAULT = MULTI_WORKER_RUNTIME ? Object.freeze({ seq: false }) : undefined;

/**
 * The `s` field a bus envelope carries for a publish's `seq` option: the
 * number for an authoritative number or bigint, `false` for `seq: false`
 * or `seq: null`, and `undefined` (omitted) otherwise. The local platform
 * validates the authority before the bus carries it, including the safe
 * integer bound for bigint values.
 *
 * @param {unknown} seq
 * @returns {number | false | undefined}
 */
export function carriedSeq(seq) {
	if (typeof seq === 'number') return seq;
	if (seq === false || seq === null) return false;
	if (typeof seq === 'bigint') return Number(seq);
	return undefined;
}

/**
 * The `seq` option for a frame a bus received. A positive safe integer is the
 * origin's authority; `false` is the origin's declaration that the frame makes
 * no monotonic promise; anything else (an older peer, a counter-stamped
 * origin, a malformed field) falls back to this worker's default.
 *
 * @param {unknown} s
 * @returns {number | false | undefined}
 */
export function receivedSeq(s) {
	if (typeof s === 'number') return Number.isSafeInteger(s) && s >= 1 ? s : (MULTI_WORKER_RUNTIME ? false : undefined);
	if (s === false) return false;
	return MULTI_WORKER_RUNTIME ? false : undefined;
}

/**
 * Local options for a frame a bus received from another instance:
 * `relay: false` (every worker has its own subscriber), the seq the envelope
 * carried (see {@link receivedSeq}), and the de-herd window and compression
 * intent when the envelope carried them. The two plain cases share frozen
 * objects; the adapter only reads publish options.
 *
 * @param {unknown} s
 * @param {unknown} [j]
 * @param {boolean} [compress]
 * @returns {Record<string, any>}
 */
export function receivedOptions(s, j, compress) {
	const seq = receivedSeq(s);
	if (j === undefined && typeof compress !== 'boolean') {
		if (seq === undefined) return RECEIVED_COUNTER;
		if (seq === false) return RECEIVED_UNSEQUENCED;
	}
	/** @type {Record<string, any>} */
	const opts = { relay: false };
	if (seq !== undefined) opts.seq = seq;
	if (j !== undefined) opts.jitterMs = j;
	if (typeof compress === 'boolean') opts.compress = compress;
	return opts;
}

const RELAY_OFF = Object.freeze({ relay: false });

/**
 * Local options for a bus publish that the bus also relays through Redis. On
 * a multi-worker runtime the bus is the fan-out to the sibling workers, so the
 * local call says `relay: false` - unless `viaRedis` is false because the bus
 * does not carry this frame at all (its receivers refuse the envelope by
 * rule). The adapter's relay then reaches the siblings, as it can for any
 * frame without an authoritative seq; one with an authoritative seq has no
 * other carrier and stays on this worker. A single worker passes the caller's
 * options through untouched.
 *
 * @template {Record<string, any> | undefined | null} T
 * @param {T} options
 * @param {boolean} [viaRedis]
 * @returns {T | Record<string, any>}
 */
export function relayedLocally(options, viaRedis = true) {
	if (!MULTI_WORKER_RUNTIME) return options;
	if (!viaRedis && !(options != null && (typeof options.seq === 'number' || typeof options.seq === 'bigint'))) return options;
	return options ? { ...options, relay: false } : RELAY_OFF;
}

/**
 * {@link relayedLocally} for a batch, entry by entry, with `viaRedis(entry)`
 * answering for each. An entry that is already local, or that is not an
 * object, is passed through so the platform judges it as before; a single
 * worker gets the caller's array back.
 *
 * @param {any} messages
 * @param {(entry: any) => boolean} viaRedis
 * @returns {any}
 */
export function relayedBatchLocally(messages, viaRedis) {
	if (!MULTI_WORKER_RUNTIME || !Array.isArray(messages)) return messages;
	const local = new Array(messages.length);
	for (let i = 0; i < messages.length; i++) {
		const m = messages[i];
		if (!m || typeof m !== 'object' || (m.options && m.options.relay === false)) {
			local[i] = m;
			continue;
		}
		const options = relayedLocally(m.options, viaRedis(m));
		local[i] = options === m.options ? m : { ...m, options };
	}
	return local;
}

/**
 * One batch entry as a bus envelope carries it: the topic, event and data, plus
 * the de-herd window (`j`) and the seq (`s`, see {@link carriedSeq}) when the
 * entry's options set them, exactly as a single publish's envelope does.
 *
 * @param {{ topic: string, event: string, data?: unknown, options?: any }} m
 */
export function relayEntry(m) {
	/** @type {Record<string, any>} */
	const entry = { topic: m.topic, event: m.event, data: m.data };
	const o = m.options;
	if (o) {
		if (typeof o.jitterMs === 'number' && o.jitterMs > 0) entry.j = o.jitterMs;
		const s = carriedSeq(o.seq);
		if (s !== undefined) entry.s = s;
	}
	return entry;
}
