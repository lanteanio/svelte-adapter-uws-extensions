import type { Platform } from 'svelte-adapter-uws';
import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface PgReplayOptions {
	/** Table name. @default 'ws_replay' */
	table?: string;
	/** Max messages per topic. @default 1000 */
	size?: number;
	/** TTL in seconds (0 = no expiry). @default 0 */
	ttl?: number;
	/** Auto-create table on first use. @default true */
	autoMigrate?: boolean;
	/** Cleanup interval in ms (0 to disable). @default 60000 */
	cleanupInterval?: number;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface BufferedMessage {
	seq: number;
	topic: string;
	event: string;
	data: unknown;
}

export interface PgReplayBuffer {
	/**
	 * Publish a message through the buffer. Stores it in Postgres with a
	 * sequence number, then calls platform.publish() as normal.
	 */
	publish(platform: Platform, topic: string, event: string, data?: unknown): Promise<boolean>;

	/** Get the current sequence number for a topic. Returns 0 if unknown. */
	seq(topic: string): Promise<number>;

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

	/** Stop the cleanup timer. */
	destroy(): void;
}

/**
 * Create a Postgres-backed replay buffer.
 */
export function createReplay(client: PgClient, options?: PgReplayOptions): PgReplayBuffer;
