import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface PgDeadLetterOptions {
	/** Table name. @default 'svti_dead_letter' */
	table?: string;
	/** Max retained records (oldest evicted first). @default 1000 */
	max?: number;
	/** Drop records older than this many ms on write (0 = no TTL). @default 0 */
	ttlMs?: number;
	/** Auto-create the table on first use. @default true */
	autoMigrate?: boolean;
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
	add(rec: Omit<DeadLetterRecord, 'id'>): Promise<string>;
	get(id: string): Promise<DeadLetterRecord | null>;
	remove(id: string): Promise<boolean>;
	count(filter?: { topic?: string }): Promise<number>;
	list(filter?: { topic?: string; limit?: number }): Promise<DeadLetterRecord[]>;
	summary(): Promise<{ total: number; byTopic: Record<string, number>; oldest: number | null; newest: number | null }>;
	/** Right-to-erasure: delete every record stamped with a user's id (needs `forgetUserId`). */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;
	clear(): Promise<void>;
}

/**
 * Create a Postgres-backed dead-letter store for svelte-realtime's
 * outbound-webhook DLQ - undeliverable events survive restarts and are shared
 * across the cluster.
 */
export function createDeadLetter(client: PgClient, options?: PgDeadLetterOptions): PgDeadLetterStore;
