/**
 * Test utilities for svelte-adapter-uws-extensions.
 *
 * In-memory mocks for Redis, Postgres, Platform, and WebSocket that mirror
 * the real APIs closely enough to test extension-consuming code without
 * running any infrastructure.
 *
 * @module svelte-adapter-uws-extensions/testing
 */

import type { RedisClient } from '../redis/index.js';
import type { PgClient } from '../postgres/index.js';

// -- Mock Redis ---------------------------------------------------------------

export interface MockRedisClient extends RedisClient {
	/** Direct access to the string store (key -> value). */
	readonly _store: Map<string, string>;
	/** Direct access to sorted sets (key -> Array<{score, member}>). */
	readonly _sortedSets: Map<string, Array<{ score: number; member: string }>>;
	/** Direct access to hashes (key -> Map<field, value>). */
	readonly _hashes: Map<string, Map<string, string>>;
}

/**
 * Create an in-memory Redis client mock.
 * Supports strings, hashes, sorted sets, pub/sub, pipelines, scan,
 * and Lua script evaluation for all extension scripts.
 */
export function mockRedisClient(keyPrefix?: string): MockRedisClient;

// -- Mock Platform ------------------------------------------------------------

export interface MockPlatform {
	/** All `publish()` calls recorded as `{ topic, event, data, options }`. */
	published: Array<{ topic: string; event: string; data: any; options?: any }>;
	/** All `send()` calls recorded as `{ ws, topic, event, data }`. */
	sent: Array<{ ws: any; topic: string; event: string; data: any }>;
	connections: number;
	publish(topic: string, event: string, data?: any, options?: any): boolean;
	send(ws: any, topic: string, event: string, data?: any): number;
	batch(messages: Array<{ topic: string; event: string; data?: any }>): boolean[];
	sendTo(filter: any, topic: string, event: string, data?: any): number;
	subscribers(topic: string): number;
	topic(t: string): {
		publish(event: string, data?: any): void;
		created(data?: any): void;
		updated(data?: any): void;
		deleted(data?: any): void;
		set(value?: any): void;
		increment(amount?: number): void;
		decrement(amount?: number): void;
	};
	/** Clear all recorded publish/send calls. */
	reset(): void;
}

/**
 * Create an in-memory Platform mock that records all publish/send calls.
 */
export function mockPlatform(): MockPlatform;

// -- Mock WebSocket -----------------------------------------------------------

export interface MockWs<T extends Record<string, any> = Record<string, any>> {
	getUserData(): T;
	subscribe(topic: string): boolean;
	unsubscribe(topic: string): boolean;
	isSubscribed(topic: string): boolean;
	getBufferedAmount(): number;
	/** Simulate socket close. After this, subscribe/unsubscribe/getBufferedAmount throw. */
	close(): void;
	/** Whether close() has been called. */
	readonly _closed: boolean;
	/** Currently subscribed topics. */
	readonly _topics: Set<string>;
}

/**
 * Create an in-memory WebSocket mock. Throws after close(), matching uWS behavior.
 */
export function mockWs<T extends Record<string, any> = Record<string, any>>(userData?: T): MockWs<T>;

// -- Mock Postgres ------------------------------------------------------------

export interface MockPgClient {
	readonly pool: {};
	query(textOrObj: string | { text: string; values?: any[] }, values?: any[]): Promise<{ rows: any[]; rowCount: number }>;
	end(): Promise<void>;
	/** Get all stored rows (Postgres replay mock). */
	_getRows(): any[];
	/** Get sequence counters by topic (Postgres replay mock). */
	_getSeqCounters(): Map<string, number>;
	/** Reset all state. */
	_reset(): void;
}

/**
 * Create an in-memory Postgres client mock.
 * Parses SQL to simulate the replay buffer table operations.
 */
export function mockPgClient(): MockPgClient;
