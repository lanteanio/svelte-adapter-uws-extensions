/**
 * Shared helpers for the sorted-set and stream Redis replay backends.
 *
 * `parseReplayOptions(prefix, options)` validates and normalizes the
 * size/ttl/durability/minReplicas/replicationTimeoutMs fields. The prefix is
 * spliced into thrown error messages so the caller is identifiable.
 *
 * `awaitReplication(...)` issues `WAIT minReplicas timeoutMs` and throws
 * `ReplicationTimeoutError` if fewer than `minReplicas` replicas ack.
 *
 * @module svelte-adapter-uws-extensions/shared/replay-helpers
 */
import { redactConnectionUrl } from './sensitive.js';
import { MAX_RESUME_TOPICS, MAX_STORE_PAYLOAD_BYTES } from './caps.js';
import { checkReplayAccess, RESUME_PREAUTHORIZED } from './replay-gate.js';
import { now } from './runtime.js';

/** In-flight authorization checks per resume frame. */
const RESUME_AUTH_CONCURRENCY = 32;

/**
 * How often one resume hook may report topic-set truncation. Any socket can
 * send the frame at will, so this cannot be per-frame - but a process-lifetime
 * one-shot is the other extreme: a burst during startup silences every later
 * occurrence, including from other connections, for as long as the process
 * lives. The window is the middle: repeated enough to show a pattern in the
 * logs, bounded enough not to be the amplifier it guards against.
 */
const RESUME_TRUNCATION_WARN_INTERVAL_MS = 60_000;

/**
 * Thrown when `WAIT` reports fewer replicas than `minReplicas` within
 * `replicationTimeoutMs`. The data is in the master; callers should skip
 * the local broadcast so live consumers are not committed to state that
 * could be lost if the master fails before replicas catch up.
 */
export class ReplicationTimeoutError extends Error {
	constructor(ack, minReplicas, timeoutMs) {
		super(`replication timed out: ${ack}/${minReplicas} replicas acked within ${timeoutMs}ms`);
		this.name = 'ReplicationTimeoutError';
		this.ack = ack;
		this.minReplicas = minReplicas;
		this.timeoutMs = timeoutMs;
	}
}

/**
 * Thrown by replay backends when the underlying storage call fails (Redis
 * eval / Postgres query / circuit breaker open). Wraps the original error in
 * `.cause`. Caller policy: catch this to fall back to a best-effort local
 * `platform.publish`, or set `localFanoutOnStorageFailure: true` at
 * construction time to have the backend do that for you.
 */
export class ReplayStorageError extends Error {
	constructor(op, cause) {
		// The cause message can embed a connection DSN (pg auth/SSL failures,
		// ioredis target errors); redact so the password never reaches logs.
		super(`replay storage failed during ${op}: ${redactConnectionUrl(cause?.message ?? String(cause))}`);
		this.name = 'ReplayStorageError';
		this.op = op;
		// Redacted too. `util.inspect` prints `cause` - which is what
		// console.error(err) and every structured logger's error serializer
		// do - so attaching the raw original would hand the DSN straight
		// back to the logs this message was just cleaned for.
		this.cause = redactCause(cause);
	}
}

/**
 * A copy of `cause` with its string fields redacted, keeping the prototype
 * so `instanceof` checks on the original error class still hold.
 * @param {any} cause
 */
function redactCause(cause) {
	if (!cause || typeof cause !== 'object') {
		return typeof cause === 'string' ? redactConnectionUrl(cause) : cause;
	}
	// Return the ORIGINAL when there is nothing to redact. Copying
	// unconditionally would break `err.cause === theErrorIThrew`, which is a
	// reasonable thing for a caller to check, and the copy only earns its
	// keep when it is actually hiding a credential.
	const msg = typeof cause.message === 'string' ? cause.message : '';
	const stk = typeof cause.stack === 'string' ? cause.stack : '';
	if (redactConnectionUrl(msg) === msg && redactConnectionUrl(stk) === stk) return cause;
	const safe = Object.create(Object.getPrototypeOf(cause));
	// `message` and `stack` are re-defined below with their original hidden
	// shape. Plain assignment would make them own ENUMERABLE properties, so
	// an app that does `JSON.stringify(err)` into an error response would
	// start shipping the whole server stack trace to the client - an
	// information disclosure introduced by the redactor itself. Skipping
	// them in the copy loop also keeps a FROZEN cause from carrying over a
	// non-configurable descriptor that the redefine would then throw on.
	const redefined = new Set();
	if (typeof cause.message === 'string') redefined.add('message');
	if (typeof cause.stack === 'string') redefined.add('stack');
	for (const k of Reflect.ownKeys(cause)) {
		if (redefined.has(k)) continue;
		let desc;
		try {
			desc = Object.getOwnPropertyDescriptor(cause, k);
		} catch {
			continue;
		}
		if (!desc) continue;
		if ('value' in desc && typeof desc.value === 'string') {
			desc = { ...desc, value: redactConnectionUrl(desc.value) };
		}
		try { Object.defineProperty(safe, k, desc); } catch { /* never lose the cause over one property */ }
	}
	if (typeof cause.message === 'string') defineHidden(safe, 'message', redactConnectionUrl(cause.message));
	if (typeof cause.stack === 'string') defineHidden(safe, 'stack', redactConnectionUrl(cause.stack));
	return safe;
}

/**
 * @param {object} target
 * @param {string} key
 * @param {any} value
 */
function defineHidden(target, key, value) {
	Object.defineProperty(target, key, { value, writable: true, enumerable: false, configurable: true });
}

/**
 * Thrown by replay backends when the caller-supplied `data` cannot be
 * serialized to JSON (typically because it contains a `BigInt`, a circular
 * reference, or another value `JSON.stringify` refuses to encode). Distinct
 * from `ReplayStorageError` because this is a caller-input bug, not a
 * transient storage failure - so `localFanoutOnStorageFailure: true` does
 * NOT cause the publish to fall back to `platform.publish`. The local
 * fanout would either re-throw on its own serializer or silently degrade
 * the durability promise; neither is the right answer to malformed input.
 * Wraps the original `TypeError` from `JSON.stringify` in `.cause`.
 */
export class ReplaySerializationError extends Error {
	constructor(op, cause) {
		super(`replay payload could not be serialized during ${op}: ${redactConnectionUrl(cause?.message ?? String(cause))}`);
		this.name = 'ReplaySerializationError';
		this.op = op;
		this.cause = redactCause(cause);
	}
}

/**
 * Validate the replay options shared by the sorted-set and stream backends.
 * Returns normalized values with defaults filled in. Throws with the given
 * prefix in front of every error message so callers can identify which
 * backend is reporting.
 *
 * @param {string} prefix
 * @param {Record<string, any>} options
 * @returns {{ maxSize: number, ttl: number, replicated: boolean, minReplicas: number, replicationTimeoutMs: number, localFanoutOnStorageFailure: boolean }}
 */
export function parseReplayOptions(prefix, options) {
	if (options.size !== undefined) {
		if (typeof options.size !== 'number' || options.size < 1 || !Number.isInteger(options.size)) {
			throw new Error(`${prefix}: size must be a positive integer, got ${options.size}`);
		}
	}
	if (options.ttl !== undefined) {
		if (typeof options.ttl !== 'number' || options.ttl < 0 || !Number.isInteger(options.ttl)) {
			throw new Error(`${prefix}: ttl must be a non-negative integer, got ${options.ttl}`);
		}
	}
	if (options.durability !== undefined && options.durability !== 'replicated') {
		throw new Error(`${prefix}: durability must be 'replicated' or undefined, got ${options.durability}`);
	}
	const replicated = options.durability === 'replicated';
	let minReplicas = 1;
	let replicationTimeoutMs = 1000;
	if (replicated) {
		if (options.minReplicas !== undefined) {
			if (typeof options.minReplicas !== 'number' || !Number.isInteger(options.minReplicas) || options.minReplicas < 1) {
				throw new Error(`${prefix}: minReplicas must be a positive integer, got ${options.minReplicas}`);
			}
			minReplicas = options.minReplicas;
		}
		if (options.replicationTimeoutMs !== undefined) {
			if (typeof options.replicationTimeoutMs !== 'number' || !Number.isInteger(options.replicationTimeoutMs) || options.replicationTimeoutMs < 0) {
				throw new Error(`${prefix}: replicationTimeoutMs must be a non-negative integer, got ${options.replicationTimeoutMs}`);
			}
			replicationTimeoutMs = options.replicationTimeoutMs;
		}
	}
	if (options.localFanoutOnStorageFailure !== undefined &&
		typeof options.localFanoutOnStorageFailure !== 'boolean') {
		throw new Error(`${prefix}: localFanoutOnStorageFailure must be a boolean, got ${options.localFanoutOnStorageFailure}`);
	}
	// Number.isInteger, not `typeof === 'number'`: NaN is a number and
	// `NaN < 1` is false, so NaN would pass a typeof-only guard and then make
	// every `bytes > maxDataBytes` comparison false, silently disabling the
	// very cap it configures.
	if (options.maxDataBytes !== undefined &&
		(!Number.isInteger(options.maxDataBytes) || options.maxDataBytes < 1)) {
		throw new Error(`${prefix}: maxDataBytes must be a positive integer (bytes), got ${options.maxDataBytes}`);
	}
	return {
		maxSize: options.size || 1000,
		ttl: options.ttl || 0,
		replicated,
		minReplicas,
		replicationTimeoutMs,
		maxDataBytes: options.maxDataBytes ?? MAX_STORE_PAYLOAD_BYTES,
		localFanoutOnStorageFailure: options.localFanoutOnStorageFailure === true
	};
}

/**
 * Issue `WAIT minReplicas timeoutMs` and account the result against the
 * given breaker / metrics. Throws `ReplicationTimeoutError` if fewer than
 * `minReplicas` replicas ack within the timeout.
 *
 * @param {import('ioredis').Redis} redis
 * @param {number} minReplicas
 * @param {number} timeoutMs
 * @param {import('./breaker.js').CircuitBreaker | undefined} b
 * @param {{ inc(): void } | null | undefined} mReplications
 * @param {{ inc(): void } | null | undefined} mReplicationTimeouts
 */
export async function awaitReplication(redis, minReplicas, timeoutMs, b, mReplications, mReplicationTimeouts) {
	let ack;
	try {
		ack = await redis.wait(minReplicas, timeoutMs);
	} catch (err) {
		b?.failure(err);
		throw err;
	}
	if (ack < minReplicas) {
		mReplicationTimeouts?.inc();
		throw new ReplicationTimeoutError(ack, minReplicas, timeoutMs);
	}
	mReplications?.inc();
}

/**
 * Group-commit state per redis client: concurrent replicated publishes share
 * WAIT round trips instead of each paying their own. Keyed by the client
 * (WeakMap - a dropped client frees its state) and, inside it, by the
 * `minReplicas:timeoutMs` pair, so every window is homogeneous and each
 * waiter gets exactly the guarantee and the latency bound it configured.
 * @type {WeakMap<object, Map<string, { inflight: boolean, queue: Array<{ resolve: () => void, reject: (err: any) => void, b: any, mReplications: any, mReplicationTimeouts: any }> }>>}
 */
const waitGroups = new WeakMap();

/**
 * Group-committed `WAIT`: the publish-path form of `awaitReplication`.
 *
 * Redis's WAIT acknowledges every write this client sent BEFORE the WAIT was
 * issued, so one WAIT can confirm a whole burst: a caller whose write has
 * completed joins the pending window, and the window's single WAIT settles
 * everyone in it. A caller that arrives while a WAIT is already in flight
 * joins the NEXT window (its write may have landed after the in-flight WAIT
 * sampled the replication offset, so being settled by it would claim a
 * durability that was never checked). Under a publish burst of N this is one
 * or two WAIT round trips instead of N; a lone publish pays exactly the one
 * WAIT it always did, with no added latency for anyone (waiters piggyback,
 * nothing is held back to fill a window).
 *
 * Per-waiter accounting is preserved: each caller's own metrics increment and
 * its own breaker records the failure, and an `ack < minReplicas` outcome
 * rejects every waiter in the window with the same ReplicationTimeoutError
 * a solo WAIT would have thrown.
 *
 * @param {import('ioredis').Redis} redis
 * @param {number} minReplicas
 * @param {number} timeoutMs
 * @param {import('./breaker.js').CircuitBreaker | undefined} b
 * @param {{ inc(): void } | null | undefined} mReplications
 * @param {{ inc(): void } | null | undefined} mReplicationTimeouts
 * @returns {Promise<void>}
 */
export function awaitReplicationGrouped(redis, minReplicas, timeoutMs, b, mReplications, mReplicationTimeouts) {
	let groups = waitGroups.get(redis);
	if (!groups) {
		groups = new Map();
		waitGroups.set(redis, groups);
	}
	const key = minReplicas + ':' + timeoutMs;
	let g = groups.get(key);
	if (!g) {
		g = { inflight: false, queue: [] };
		groups.set(key, g);
	}
	return new Promise((resolve, reject) => {
		g.queue.push({ resolve, reject, b, mReplications, mReplicationTimeouts });
		if (!g.inflight) {
			g.inflight = true;
			_drainWaitWindows(redis, minReplicas, timeoutMs, g);
		}
	});
}

async function _drainWaitWindows(redis, minReplicas, timeoutMs, g) {
	while (g.queue.length > 0) {
		const window = g.queue;
		g.queue = [];
		let ack;
		let err = null;
		try {
			ack = await redis.wait(minReplicas, timeoutMs);
		} catch (e) {
			err = e;
		}
		for (const w of window) {
			if (err !== null) {
				w.b?.failure(err);
				w.reject(err);
			} else if (ack < minReplicas) {
				w.mReplicationTimeouts?.inc();
				w.reject(new ReplicationTimeoutError(ack, minReplicas, timeoutMs));
			} else {
				w.mReplications?.inc();
				w.resolve();
			}
		}
	}
	g.inflight = false;
}

/**
 * Shared `hooks.ws.resume` hook body for the replay stores. Loops over the
 * client's per-topic lastSeenSeqs. For each topic it compares the client's
 * presented epoch (ctx.lastSeenEpochs, threaded by the adapter; absent ->
 * the baseline 0) to the topic's stored epoch. On a MATCH it gap-fills via
 * the store's replay() pipeline, which already detects + emits truncation
 * per topic. On a MISMATCH the seq space reset since the client last saw
 * it, so it SKIPS gap-fill for that topic and emits a `rehydrate` marker on
 * the same `__replay:{topic}` channel the client already handles for
 * `truncated`/`denied`, telling the client to drop its stale offset and
 * re-read from scratch instead of being served a reset seq space as if it
 * were contiguous.
 *
 * Additive epoch semantics: a topic the client presented NO epoch for (old
 * client, or one that never received an epoch) is always a match and
 * gap-fills exactly as before - never compared against the stored epoch, so
 * the byte-identical old path is preserved even though the first publish
 * bumps a fresh topic's epoch 0 -> 1. Only a presented integer epoch that
 * differs from the stored one is a reset.
 *
 * Resume is the reconnect-storm hot path, so the store round-trips are
 * batched: every needed epoch read goes through ONE `currentEpochs` call
 * (one pipeline/query round trip instead of one per topic), and the
 * per-topic gap-fills run CONCURRENTLY (a client resuming N topics pays one
 * replay latency, not the sum). Each topic's own frame order is preserved
 * by its replay call, and topics are independent per-topic channels, so
 * cross-topic interleaving on the socket is free. A gap-fill rejection
 * propagates (Promise.all - the sibling fills still run to completion with
 * their rejections observed).
 *
 * Covered watermark: resolves `{ [topic]: highestSeqCovered }` for every topic
 * it gap-filled, where the value is the seq the store's `replay()` reported
 * covering. The adapter's recovery barrier reads this return value to dedup
 * the live frames it buffered during the resume window EXACTLY (a frame with
 * seq <= the watermark was already delivered by the gap-fill and is skipped;
 * anything newer flushes). A topic that was NOT gap-filled - a rehydrate
 * mismatch, or a replay that reported nothing (denied) - is simply absent
 * from the map, so the adapter falls back to its conservative pre-window
 * floor for that topic (at-least-once rather than a gap).
 *
 * @param {{
 *   currentEpochs: (topics: string[]) => Promise<Map<string, number>>,
 *   replay: (ws: any, topic: string, sinceSeq: number, platform: any, reqId?: any, preAuthorized?: symbol) => Promise<number | undefined>,
 *   onTruncate?: () => void
 * }} store
 * @returns {(ws: any, ctx: any) => Promise<Record<string, number> | undefined>}
 */
export function createResumeHook({ currentEpochs, replay, authorize = checkReplayAccess, onTruncate }) {
	// Per-hook, NOT module-global. A module-level latch is shared by every
	// store in the process and by every test file that touches one, which made
	// the report both order-dependent and permanently silenced after whoever
	// happened to trigger it first. `-Infinity` so the first occurrence always
	// reports.
	let truncationWarnedAt = -Infinity;
	return async (ws, ctx) => {
		if (!ctx || !ctx.lastSeenSeqs || !ctx.platform) return;
		const presented = (ctx.lastSeenEpochs && typeof ctx.lastSeenEpochs === 'object')
			? ctx.lastSeenEpochs
			: null;
		// Cap the client-presented topic set: the resume frame is raw wire
		// input and every accepted topic costs pre-authorization state and
		// queries. Truncate rather than reject, so the legitimate prefix
		// still resumes.
		//
		// Built by iterating, NOT `Object.entries(...).slice(...)`, because
		// Object.entries materializes the whole client-supplied map first -
		// which is the allocation spike the bound exists to prevent.
		const entries = [];
		let truncated = false;
		for (const topic in ctx.lastSeenSeqs) {
			if (!Object.prototype.hasOwnProperty.call(ctx.lastSeenSeqs, topic)) continue;
			if (entries.length >= MAX_RESUME_TOPICS) {
				truncated = true;
				break;
			}
			entries.push([topic, ctx.lastSeenSeqs[topic]]);
		}
		if (truncated) {
			// Count every overflowing frame even while its operator log is
			// throttled. This callback is wired to a label-free counter by all
			// three stores, so client-controlled topic names never become
			// metric labels.
			onTruncate?.();
			// A silently partial `covered` map reads to the client as "these
			// are your watermarks" when the rest were never looked at. This is
			// an OPERATOR-side report only: the map is consumed by the adapter
			// per topic and never forwarded, so there is nowhere in the current
			// wire contract to hand the client an overflow marker it could act
			// on. A client whose topic set exceeds the bound must be given a
			// smaller one; the log is what surfaces that.
			const nowMs = now();
			if (nowMs - truncationWarnedAt >= RESUME_TRUNCATION_WARN_INTERVAL_MS) {
				truncationWarnedAt = nowMs;
				console.warn(
					`replay resume: more than ${MAX_RESUME_TOPICS} topics presented, ${MAX_RESUME_TOPICS} accepted ` +
					'(MAX_RESUME_TOPICS); the remainder were not inspected or resumed. Further occurrences on this ' +
					`store are reported at most once per ${RESUME_TRUNCATION_WARN_INTERVAL_MS}ms.`
				);
			}
		}
		// Authorize BEFORE any per-topic work. Without this, an unauthorized
		// topic name still reached currentEpochs, whose stores memoize every
		// topic they are asked about - so a spray of forged names evicted the
		// real topics' memoized epochs, and the next `subscribed` ack carried
		// epoch 0 for a topic whose real epoch had climbed, sending healthy
		// clients into a full rehydrate instead of a gap-fill. The store's
		// own per-topic check still runs inside replay(); it passes for
		// everything that got through here, so the client sees one denial per
		// denied topic, not two.
		// Bounded concurrency, not a serial loop: these checks are per-topic
		// independent, and `platform.checkSubscribe` runs the app's subscribe
		// hook, which is commonly DB-backed. Serially at MAX_RESUME_TOPICS a
		// 1ms-latency authorizer takes over two minutes for one client frame,
		// and the adapter awaits this hook inline in the message handler.
		const authorized = [];
		const verdicts = new Array(entries.length);
		let next = 0;
		async function authWorker() {
			while (next < entries.length) {
				const i = next++;
				verdicts[i] = await authorize(ws, entries[i][0], ctx.platform, ctx.reqId);
			}
		}
		const workers = [];
		for (let i = 0; i < Math.min(RESUME_AUTH_CONCURRENCY, entries.length); i++) workers.push(authWorker());
		await Promise.all(workers);
		for (let i = 0; i < entries.length; i++) {
			if (verdicts[i]) authorized.push(entries[i]);
		}

		const epochTopics = [];
		if (presented !== null) {
			for (const [topic] of authorized) {
				if (Number.isInteger(presented[topic])) epochTopics.push(topic);
			}
		}
		const have = epochTopics.length > 0 ? await currentEpochs(epochTopics) : null;
		const fills = [];
		const fillTopics = [];
		for (const [topic, sinceSeq] of authorized) {
			// Normalize wire-supplied sinceSeq. Reject non-integers (fractional,
			// NaN, Infinity, non-number) by falling through to 0 (resume from
			// start). Negative values also fall through; the store's replay()
			// validates internally as defense-in-depth.
			const seq = Number.isInteger(sinceSeq) && sinceSeq >= 0 ? sinceSeq : 0;
			const want = presented && Number.isInteger(presented[topic]) ? presented[topic] : null;
			if (want !== null) {
				const haveEpoch = /** @type {Map<string, number>} */ (have).get(topic) ?? 0;
				if (want !== haveEpoch) {
					ctx.platform.send(ws, '__replay:' + topic, 'rehydrate', { epoch: haveEpoch });
					continue;
				}
			}
			fills.push(replay(ws, topic, seq, ctx.platform, ctx.reqId, RESUME_PREAUTHORIZED));
			fillTopics.push(topic);
		}
		const results = await Promise.all(fills);
		// Prototype-free so a hostile topic name (e.g. __proto__) still lands
		// as an own property instead of being silently swallowed by the
		// Object.prototype setter, which would drop that topic's watermark.
		/** @type {Record<string, number>} */
		const covered = Object.create(null);
		for (let i = 0; i < results.length; i++) {
			if (typeof results[i] === 'number') covered[fillTopics[i]] = /** @type {number} */ (results[i]);
		}
		return covered;
	};
}
