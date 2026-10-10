import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface PgDeadLetterOptions {
	/** Table name. @default 'svti_dead_letter' */
	table?: string;
	/** Retention target; oldest ids are evicted in bounded passes. @default 1000 */
	max?: number;
	/**
	 * Drop records older than this many ms (0 = no TTL). Reads exclude expired
	 * rows using the database clock. Bounded cleanup deletes them after writes
	 * and while idle. @default 0
	 */
	ttlMs?: number;
	/** Auto-create the table on first use. @default true */
	autoMigrate?: boolean;
	/**
	 * Maximum jitter delay between bounded cleanup passes (ms). 0 disables
	 * the timer; write-time cleanup remains. @default 60000
	 */
	cleanupInterval?: number;
	/** Circuit breaker for fault isolation. */
	breaker?: CircuitBreaker;
	/** Prometheus registry for the `dead_letter_added_total` counter. */
	metrics?: MetricsRegistry;
	/**
	 * Right-to-erasure: extract a record's owning userId at write time into a
	 * `user_id` column so `live.forget` can `DELETE WHERE user_id`. Without it,
	 * records are not user-purgeable.
	 */
	forgetUserId?: (record: DeadLetterRecord) => string | null | undefined;
}

/** A retained, undeliverable outbound-webhook event. */
export interface DeadLetterRecord {
	id: string;
	webhookId: string;
	topic: string;
	event: string;
	data: unknown;
	attempts: number;
	error: string;
	failedAt: number;
}

/**
 * Postgres dead-letter store. Matches svelte-realtime's in-memory
 * `DeadLetterStore` but every method is async. Wire it with
 * `configureWebhooks({ deadLetter: createDeadLetter(client) })` on
 * svelte-realtime >= 0.6.0-next.40.
 */
export interface PgDeadLetterStore {
	/** Returns the committed row ID even if later retention cleanup fails. A rejected insert can still have an ambiguous transport outcome; add() has no caller deduplication key. */
	add(rec: Omit<DeadLetterRecord, 'id'>): Promise<string>;
	get(id: string): Promise<DeadLetterRecord | null>;
	remove(id: string): Promise<boolean>;
	count(filter?: { topic?: string }): Promise<number>;
	list(filter?: { topic?: string; limit?: number }): Promise<DeadLetterRecord[]>;
	summary(): Promise<{ total: number; byTopic: Record<string, number>; oldest: number | null; newest: number | null }>;
	/** Right-to-erasure: delete every record stamped with a user's id (needs `forgetUserId`). */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;
	clear(): Promise<void>;
	/** Stop retention maintenance. */
	destroy(): void;
}

/**
 * Create a Postgres-backed dead-letter store for svelte-realtime's
 * outbound-webhook DLQ - undeliverable events survive restarts and are shared
 * across the cluster.
 */
export function createDeadLetter(client: PgClient, options?: PgDeadLetterOptions): PgDeadLetterStore;
