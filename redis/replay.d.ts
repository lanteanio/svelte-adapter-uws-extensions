import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisReplayOptions {
	/**
	 * Storage backend. `'sortedset'` (default) uses ZADD/ZRANGEBYSCORE
	 * for compatibility back to Redis 5. `'stream'` uses XADD/XRANGE
	 * with `<seq>-0` IDs; listpack encoding is more compact and
	 * range queries filter natively by sequence number. Both backends
	 * implement the same external contract.
	 *
	 * @default 'sortedset'
	 */
	storage?: 'sortedset' | 'stream';
	/** Max messages per topic. @default 1000 */
	size?: number;
	/** TTL in seconds for replay keys (0 = no expiry). @default 0 */
	ttl?: number;
	/**
	 * Opt into per-publish replication signalling. After the write,
	 * runs `WAIT minReplicas replicationTimeoutMs`; throws
	 * `ReplicationTimeoutError` and skips the local broadcast when
	 * fewer than `minReplicas` replicas ack within the timeout.
	 * Use for loss-sensitive flows where the caller wants a
	 * "replicated" signal before declaring success.
	 */
	durability?: 'replicated';
	/**
	 * Minimum replicas that must ack before publish is considered
	 * durable. Only consulted when `durability: 'replicated'`.
	 * @default 1
	 */
	minReplicas?: number;
	/**
	 * Per-publish replication timeout in milliseconds. `0` blocks
	 * indefinitely (Redis WAIT semantics). Only consulted when
	 * `durability: 'replicated'`.
	 * @default 1000
	 */
	replicationTimeoutMs?: number;
	/**
	 * Default TTL in seconds for the dedup cache used by
	 * `publishIdempotent` (stream backend only). `0` disables expiry.
	 * Per-call override via the `idempotencyTtl` option.
	 * @default 172800 (48 hours)
	 */
	idempotencyTtl?: number;
	/**
	 * When `true`, `publish()` falls back to a best-effort
	 * `platform.publish(topic, event, data)` if the underlying storage
	 * call fails (Redis down, breaker open, etc.) instead of throwing.
	 * The `replay_storage_fallbacks_total{topic}` counter increments
	 * on each fallback, so observability is preserved.
	 *
	 * Default `false` is the safe choice for production: storage failure
	 * surfaces as `ReplayStorageError` so reconnecting clients don't see
	 * messages that were delivered live but never persisted. Set `true`
	 * for dev environments running without Redis, or for use cases where
	 * loss of replay durability is acceptable as long as live delivery
	 * keeps working.
	 *
	 * Does not affect `publishIdempotent`, which always throws on
	 * storage failure to preserve its exactly-once contract.
	 *
	 * @default false
	 */
	localFanoutOnStorageFailure?: boolean;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface PublishIdempotentOptions {
	/** Logical producer identifier; scopes the dedup cache. */
	producerId: string;
	/** Request identifier; the dedup key within the producer's namespace. */
	requestId: string;
	/** Override the default `idempotencyTtl` for this call. */
	idempotencyTtl?: number;
}

export interface PublishIdempotentResult {
	/** Sequence number of the entry (newly written or returned from cache). */
	seq: number;
	/** `true` when the call was served from the dedup cache. No XADD, no broadcast. */
	isDuplicate: boolean;
}

/**
 * Thrown by `publish()` when `durability: 'replicated'` is set and
 * the Redis WAIT command reports fewer replicas than `minReplicas`
 * within `replicationTimeoutMs`. Carries the actual ack count for
 * caller introspection.
 */
export class ReplicationTimeoutError extends Error {
	readonly name: 'ReplicationTimeoutError';
	readonly ack: number;
	readonly minReplicas: number;
	readonly timeoutMs: number;
	constructor(ack: number, minReplicas: number, timeoutMs: number);
}

/**
 * Thrown by `publish()` and `publishIdempotent()` when the underlying
 * storage call fails (Redis eval, circuit breaker open, connection refused).
 * The original error is preserved in `.cause`. Catch this to fall back to a
 * best-effort `platform.publish(topic, event, data)`, or set
 * `localFanoutOnStorageFailure: true` to have the backend do that for you.
 *
 * Always thrown by `publishIdempotent` even when the option is set, since
 * silent fanout would issue the message but leave the dedup cache empty,
 * breaking the exactly-once contract on a retry.
 */
export class ReplayStorageError extends Error {
	readonly name: 'ReplayStorageError';
	readonly op: string;
	readonly cause: unknown;
	constructor(op: string, cause: unknown);
}

/**
 * Thrown by `publish()` and `publishIdempotent()` when the caller-supplied
 * `data` cannot be serialized to JSON (typically a `BigInt`, a circular
 * reference, or another value `JSON.stringify` refuses). Distinct from
 * `ReplayStorageError` because this is a caller-input bug, not a transient
 * storage failure - so `localFanoutOnStorageFailure: true` does NOT cause
 * the publish to fall back to `platform.publish`. The original `TypeError`
 * is preserved in `.cause`.
 */
export class ReplaySerializationError extends Error {
	readonly name: 'ReplaySerializationError';
	readonly op: string;
	readonly cause: unknown;
	constructor(op: string, cause: unknown);
}

export interface BufferedMessage {
	seq: number;
	topic: string;
	event: string;
	data: unknown;
}

export interface ReplayGap {
	/** True when the buffer no longer holds the next sequence the consumer needs. */
	truncated: boolean;
	/** The first sequence the consumer is missing (`lastSeenSeq + 1`), or null when caught up. */
	missingFrom: number | null;
}

/**
 * Hook function compatible with `hooks.ws.resume` from `svelte-adapter-uws`.
 * Loops over the client's per-topic `lastSeenSeqs` and gap-fills via the
 * underlying `replay()` pipeline.
 */
export type ResumeHook = (
	ws: any,
	ctx: {
		lastSeenSeqs?: Record<string, number>;
		platform: Platform;
		sessionId?: string;
	}
) => Promise<void>;

export interface RedisReplayBuffer {
	/**
	 * Publish a message through the buffer. Stores it in Redis with a
	 * sequence number, then calls platform.publish() as normal.
	 */
	publish(platform: Platform, topic: string, event: string, data?: unknown): Promise<boolean>;

	/** Get the current sequence number for a topic. Returns 0 if unknown. */
	seq(topic: string): Promise<number>;

	/**
	 * Publish with caller-provided idempotency. Stream backend only --
	 * not present on the sorted-set backend. On a fresh `(producerId, requestId)`
	 * tuple, performs the same INCR + XADD + broadcast as `publish()`
	 * and caches `seq` in a dedup hash with TTL `idempotencyTtl`. On
	 * a repeat tuple within the TTL, returns the cached seq, skips
	 * the XADD, and skips the local broadcast (the original publish
	 * already broadcast; live consumers either got it then or will
	 * pick it up via `replay()` on reconnect).
	 *
	 * @returns `{ seq, isDuplicate }` - `isDuplicate` is `true` when
	 *   served from the dedup cache.
	 */
	publishIdempotent?(
		platform: Platform,
		topic: string,
		event: string,
		data: unknown,
		opts: PublishIdempotentOptions
	): Promise<PublishIdempotentResult>;

	/**
	 * Inspect whether the buffer still holds the next sequence after
	 * `lastSeenSeq`. Returns `{ truncated: true, missingFrom: lastSeenSeq + 1 }`
	 * when the next message is not in the buffer but the seq counter has
	 * advanced past it; otherwise `{ truncated: false, missingFrom: null }`.
	 *
	 * Useful for clients that want to decide between an incremental replay
	 * and a full reload before opening a WebSocket. `lastSeenSeq` of 0
	 * always returns `{ truncated: false, missingFrom: null }` (a fresh
	 * client has no history to lose).
	 */
	gap(topic: string, lastSeenSeq: number): Promise<ReplayGap>;

	/** Get all buffered messages after a given sequence number. */
	since(topic: string, since: number): Promise<BufferedMessage[]>;

	/**
	 * Send buffered messages to a single connection. Sends each missed
	 * message on `__replay:{topic}`, then an end marker.
	 *
	 * If the buffer has been trimmed past `sinceSeq`, a `truncated` event
	 * is sent before the messages so the client knows data was lost.
	 *
	 * @param reqId - Optional correlation ID for disambiguating concurrent replays.
	 */
	replay(ws: any, topic: string, sinceSeq: number, platform: Platform, reqId?: string): Promise<void>;

	/** Clear all replay buffers. */
	clear(): Promise<void>;

	/** Clear the buffer for a single topic. */
	clearTopic(topic: string): Promise<void>;

	/**
	 * Returns a hook function for `hooks.ws.resume`. Iterates over the
	 * client's per-topic `lastSeenSeqs` and gap-fills via `replay()`,
	 * which already detects + emits truncation per topic.
	 *
	 * @example
	 * ```js
	 * const replay = createReplay(redis);
	 * export const resume = replay.resumeHook();
	 * ```
	 */
	resumeHook(): ResumeHook;
}

/**
 * Create a Redis-backed replay buffer.
 */
export function createReplay(client: RedisClient, options?: RedisReplayOptions): RedisReplayBuffer;
