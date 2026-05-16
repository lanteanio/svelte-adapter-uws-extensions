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
