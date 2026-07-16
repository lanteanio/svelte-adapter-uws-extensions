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
		super(`replay storage failed during ${op}: ${cause?.message ?? cause}`);
		this.name = 'ReplayStorageError';
		this.op = op;
		this.cause = cause;
	}
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
		super(`replay payload could not be serialized during ${op}: ${cause?.message ?? cause}`);
		this.name = 'ReplaySerializationError';
		this.op = op;
		this.cause = cause;
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
	return {
		maxSize: options.size || 1000,
		ttl: options.ttl || 0,
		replicated,
		minReplicas,
		replicationTimeoutMs,
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
 *   replay: (ws: any, topic: string, sinceSeq: number, platform: any) => Promise<number | undefined>
 * }} store
 * @returns {(ws: any, ctx: any) => Promise<Record<string, number> | undefined>}
 */
export function createResumeHook({ currentEpochs, replay }) {
	return async (ws, ctx) => {
		if (!ctx || !ctx.lastSeenSeqs || !ctx.platform) return;
		const presented = (ctx.lastSeenEpochs && typeof ctx.lastSeenEpochs === 'object')
			? ctx.lastSeenEpochs
			: null;
		const entries = Object.entries(ctx.lastSeenSeqs);
		const epochTopics = [];
		if (presented !== null) {
			for (const [topic] of entries) {
				if (Number.isInteger(presented[topic])) epochTopics.push(topic);
			}
		}
		const have = epochTopics.length > 0 ? await currentEpochs(epochTopics) : null;
		const fills = [];
		const fillTopics = [];
		for (const [topic, sinceSeq] of entries) {
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
			fills.push(replay(ws, topic, seq, ctx.platform));
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
