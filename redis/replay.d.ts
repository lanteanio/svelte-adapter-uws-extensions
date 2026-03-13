import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';

export interface RedisReplayOptions {
	/** Max messages per topic. @default 1000 */
	size?: number;
	/** TTL in seconds for replay keys (0 = no expiry). @default 0 */
	ttl?: number;
}

export interface BufferedMessage {
	seq: number;
	topic: string;
	event: string;
	data: unknown;
}

export interface RedisReplayBuffer {
	/**
	 * Publish a message through the buffer. Stores it in Redis with a
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
	 */
	replay(ws: any, topic: string, sinceSeq: number, platform: Platform): Promise<void>;

	/** Clear all replay buffers. */
	clear(): Promise<void>;

	/** Clear the buffer for a single topic. */
	clearTopic(topic: string): Promise<void>;
}

/**
 * Create a Redis-backed replay buffer.
 */
export function createReplay(client: RedisClient, options?: RedisReplayOptions): RedisReplayBuffer;
