/**
 * Cluster-shared outbound-webhook delivery controls (retry budget + endpoint
 * ejection), backed by Redis. Cluster substitutes for the adapter's in-process
 * `createRetryBudget` / `createWebhookBreaker`, implementing the same interface
 * svelte-realtime's delivery consumes.
 *
 * @module svelte-adapter-uws-extensions/redis/webhook-controls
 */

import type { RedisClient } from './index.js';

/** A retry budget: `take` consumes one token, resolving to whether a retry may
 * proceed. Keyed by the webhook registration id. */
export interface RetryBudget {
	take(key?: string): Promise<boolean>;
}

/** An endpoint-ejection circuit breaker. `guard` throws when the key's circuit
 * is open; `success`/`failure` record the terminal delivery outcome (returning a
 * never-rejecting promise). */
export interface WebhookBreaker {
	guard(key?: string): void;
	success(key?: string): Promise<void>;
	failure(err: any, key?: string): Promise<void>;
	stateOf(key?: string): 'healthy' | 'broken' | 'probing';
	destroy(): void;
}

/** Options for {@link createRetryBudget}. */
export interface RedisRetryBudgetOptions {
	/** Max retries allowed per window, per endpoint (default 100). */
	capacity?: number;
	/** Refill window in ms (default 10000). */
	intervalMs?: number;
	/** Backend circuit breaker for Redis fault isolation (shared `createCircuitBreaker`). */
	breaker?: object;
}

/** Options for {@link createWebhookBreaker}. */
export interface RedisWebhookBreakerOptions {
	/** Combined fleet failures before an endpoint's circuit opens (default 5). */
	failureThreshold?: number;
	/** Ms an open circuit waits before allowing a half-open probe (default 30000). */
	resetMs?: number;
	/** Backend circuit breaker for Redis fault isolation (shared `createCircuitBreaker`). */
	breaker?: object;
}

/** Thrown by {@link createWebhookBreaker}'s `guard` when a key's circuit is open. */
export declare class WebhookCircuitOpenError extends Error {
	readonly code: 'WEBHOOK_CIRCUIT_OPEN';
}

/**
 * Create a Redis-backed retry budget shared across the fleet - the cluster
 * substitute for the adapter's in-process `createRetryBudget`.
 */
export function createRetryBudget(client: RedisClient, options?: RedisRetryBudgetOptions): RetryBudget;

/**
 * Create a Redis-backed endpoint-ejection breaker shared across the fleet - the
 * cluster substitute for the adapter's in-process `createWebhookBreaker`.
 */
export function createWebhookBreaker(client: RedisClient, options?: RedisWebhookBreakerOptions): WebhookBreaker;
