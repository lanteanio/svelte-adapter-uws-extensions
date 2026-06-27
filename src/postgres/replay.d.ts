import type { Platform } from 'svelte-adapter-uws';
import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface PgReplayOptions {
	/** Table name. @default 'svti_replay' */
	table?: string;
	/** Max messages per topic. @default 1000 */
	size?: number;
	/** TTL in seconds (0 = no expiry). @default 0 */
	ttl?: number;
	/** Auto-create table on first use. @default true */
	autoMigrate?: boolean;
	/** Cleanup interval in ms (0 to disable). @default 60000 */
	cleanupInterval?: number;
	/**
	 * When `true`, `publish()` falls back to a best-effort
	 * `platform.publish(topic, event, data)` if the underlying Postgres
	 * call fails (connection refused, breaker open, etc.) instead of
	 * throwing. The `replay_storage_fallbacks_total{topic}` counter
	 * increments on each fallback.
	 *
	 * Default `false` is the safe choice for production: storage failure
	 * surfaces as `ReplayStorageError` so reconnecting clients don't see
	 * messages that were delivered live but never persisted. Set `true`
	 * for dev environments running without Postgres, or for use cases
	 * where loss of replay durability is acceptable as long as live
	 * delivery keeps working.
	 *
	 * @default false
	 */
	localFanoutOnStorageFailure?: boolean;
	/**
	 * Right-to-erasure: extract a buffered event's authoring userId at publish
	 * time into a `user_id` column so `live.forget` can `DELETE WHERE user_id`.
	 * Without it, buffered events are not user-purgeable.
	 */
	forgetUserId?: (event: { topic: string; event: string; data: unknown }) => string | null | undefined;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

/**
 * Thrown by `publish()` when the underlying Postgres query fails (connection
 * refused, circuit breaker open, etc). The original error is preserved in
 * `.cause`. Catch this to fall back to a best-effort
 * `platform.publish(topic, event, data)`, or set
 * `localFanoutOnStorageFailure: true` to have the backend do that for you.
 */
export class ReplayStorageError extends Error {
	readonly name: 'ReplayStorageError';
	readonly op: string;
	readonly cause: unknown;
	constructor(op: string, cause: unknown);
}

/**
 * Thrown by `publish()` when the caller-supplied `data` cannot be serialized
 * to JSON (typically a `BigInt`, a circular reference, or another value
 * `JSON.stringify` refuses). Distinct from `ReplayStorageError` because this
 * is a caller-input bug, not a transient storage failure - so
 * `localFanoutOnStorageFailure: true` does NOT cause the publish to fall
 * back to `platform.publish`. The original `TypeError` is preserved in
 * `.cause`.
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
 * For each topic in `lastSeenSeqs` it compares the client's presented epoch
 * (`lastSeenEpochs`, threaded by the adapter; absent is the baseline 0) to the
 * topic's stored epoch: on a match it gap-fills via the underlying `replay()`
 * pipeline, on a mismatch it skips gap-fill and emits a `rehydrate` marker on
 * `__replay:{topic}` so the client re-reads a reset seq space from scratch.
 */
export type ResumeHook = (
	ws: any,
	ctx: {
		lastSeenSeqs?: Record<string, number>;
		lastSeenEpochs?: Record<string, number>;
		platform: Platform;
		sessionId?: string;
	}
) => Promise<void>;

export interface PgReplayBuffer {
	/**
	 * Publish a message through the buffer. Stores it in Postgres with a
	 * sequence number, then calls platform.publish() as normal.
	 */
	publish(platform: Platform, topic: string, event: string, data?: unknown): Promise<boolean>;

	/** Get the current sequence number for a topic. Returns 0 if unknown. */
	seq(topic: string): Promise<number>;

	/**
	 * Inspect whether the buffer still holds the next sequence after
	 * `lastSeenSeq`. Returns `{ truncated: true, missingFrom: lastSeenSeq + 1 }`
	 * when the next row is not in the buffer but the seq counter has advanced
	 * past it; otherwise `{ truncated: false, missingFrom: null }`.
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

	/** Clear all replay data. */
	clear(): Promise<void>;

	/** Clear replay data for a single topic. */
	clearTopic(topic: string): Promise<void>;

	/**
	 * Right-to-erasure: delete a user's buffered events from every topic (needs
	 * `forgetUserId`). Leaves the seq space intact (holes read as truncation on
	 * resume). A no-op without the extractor.
	 */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;

	/**
	 * Current stored generation of a topic's seq space. A topic whose seq
	 * space has never reset reads as the baseline `0`. Bumped whenever the
	 * authoritative seq numbering for the topic restarts (`clearTopic`, or a
	 * publish landing on a fresh/cleared empty seq space). The resume hook
	 * compares this to the client's presented epoch to choose gap-fill (match)
	 * or cold-rehydrate (mismatch).
	 */
	currentEpoch(topic: string): Promise<number>;

	/**
	 * Synchronous best-effort read of a topic's epoch from the in-process
	 * cache (populated by `currentEpoch` / a bump). Returns the baseline `0`
	 * for a topic this process has not yet observed. Wire it to the platform's
	 * `topicEpoch(topic)` so the synchronous subscribe-ack carries the
	 * per-topic generation a resuming client presents back.
	 */
	cachedEpoch(topic: string): number;

	/** Stop the cleanup timer. */
	destroy(): void;

	/**
	 * Returns a hook function for `hooks.ws.resume`. Iterates over the
	 * client's per-topic `lastSeenSeqs` and gap-fills via `replay()`,
	 * which already detects + emits truncation per topic.
	 *
	 * @example
	 * ```js
	 * const replay = createReplay(pg);
	 * export const resume = replay.resumeHook();
	 * ```
	 */
	resumeHook(): ResumeHook;
}

/**
 * Create a Postgres-backed replay buffer.
 */
export function createReplay(client: PgClient, options?: PgReplayOptions): PgReplayBuffer;
