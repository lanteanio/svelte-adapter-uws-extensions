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
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
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

export interface RedisReplayBuffer {
	/**
	 * Publish a message through the buffer. Stores it in Redis with a
	 * sequence number, then calls platform.publish() as normal.
	 */
	publish(platform: Platform, topic: string, event: string, data?: unknown): Promise<boolean>;

	/** Get the current sequence number for a topic. Returns 0 if unknown. */
	seq(topic: string): Promise<number>;

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
}

/**
 * Create a Redis-backed replay buffer.
 */
export function createReplay(client: RedisClient, options?: RedisReplayOptions): RedisReplayBuffer;
