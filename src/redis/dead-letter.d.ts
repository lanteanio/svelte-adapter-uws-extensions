import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisDeadLetterOptions {
	/** Max retained records (oldest evicted first). @default 1000 */
	max?: number;
	/** Drop records older than this many ms on write (0 = no TTL). @default 0 */
	ttlMs?: number;
	/** Circuit breaker for fault isolation. */
	breaker?: CircuitBreaker;
	/** Prometheus registry for the `dead_letter_added_total` counter. */
	metrics?: MetricsRegistry;
	/**
	 * Right-to-erasure: map a record to its owning userId so `live.forget` can
	 * drop the user's undelivered payloads (scanned at purge time over the bounded
	 * collection). Without it, records are not user-purgeable.
	 */
	forgetUserId?: (record: DeadLetterRecord) => string | null | undefined;
	/**
	 * How long a per-user forget tombstone lives (its PX). A delivery whose total
	 * retry duration outlives this resurrects the erased payload once the tombstone
	 * expires; size it above your max retry budget. @default 600000 (10 minutes)
	 */
	forgetTombstoneMs?: number;
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
 * The dead-letter store interface. Matches svelte-realtime's in-memory
 * `DeadLetterStore` but every method is async (Redis I/O). Wire it with
 * `configureWebhooks({ deadLetter: createDeadLetter(client) })` on
 * svelte-realtime >= 0.6.0-next.40.
 */
export interface RedisDeadLetterStore {
	/**
	 * Retain an undeliverable event; resolves the new record id, or `null` when a
	 * forget tombstone drops it (the delivery was in flight when `live.forget`
	 * ran). `startedAt` is the delivery start as a monotonic stamp (threaded by
	 * svelte-realtime's `_fireWebhookOut`), used only for the forget-race check,
	 * never stored.
	 */
	add(rec: Omit<DeadLetterRecord, 'id'> & { startedAt?: number }): Promise<string | null>;
	get(id: string): Promise<DeadLetterRecord | null>;
	remove(id: string): Promise<boolean>;
	count(filter?: { topic?: string }): Promise<number>;
	list(filter?: { topic?: string; limit?: number }): Promise<DeadLetterRecord[]>;
	summary(): Promise<{ total: number; byTopic: Record<string, number>; oldest: number | null; newest: number | null }>;
	/** Right-to-erasure: drop every record belonging to a user (needs `forgetUserId`). */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;
	clear(): Promise<void>;
}

/**
 * Create a Redis-backed dead-letter store for svelte-realtime's outbound-webhook
 * DLQ - undeliverable events survive restarts and are shared across the cluster.
 */
export function createDeadLetter(client: RedisClient, options?: RedisDeadLetterOptions): RedisDeadLetterStore;
