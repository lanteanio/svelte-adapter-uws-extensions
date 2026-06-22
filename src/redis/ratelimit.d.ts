import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisRateLimitOptions {
	/** Tokens available per interval. Must be a positive integer. */
	points: number;
	/** Refill interval in milliseconds. Must be positive. */
	interval: number;
	/** Auto-ban duration in ms when exhausted. 0 = no ban. @default 0 */
	blockDuration?: number;
	/** Key extraction mode. @default 'ip' */
	keyBy?: 'ip' | 'connection' | ((ws: any) => string);
	/**
	 * Optional per-connection tenant resolver. When set, the bucket key is scoped by
	 * the returned tenant id (joined to the key with a NUL, so it stays unambiguous even
	 * for IPv6 keys), so two tenants sharing an IP / connection / custom key get
	 * independent buckets. Return null/undefined for an unscoped connection; omit for a
	 * single-tenant deploy (byte-identical).
	 */
	tenant?: (ws: any) => string | null | undefined;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface ConsumeResult {
	/** Whether the request was permitted. */
	allowed: boolean;
	/** Tokens left in the bucket (0 if banned or exhausted). */
	remaining: number;
	/** Milliseconds until the bucket refills or the ban expires. */
	resetMs: number;
}

export interface RedisRateLimiter {
	/** Attempt to consume tokens. */
	consume(ws: any, cost?: number): Promise<ConsumeResult>;
	/** Clear the bucket for a key (optionally scoped to a tenant). */
	reset(key: string, tenant?: string | null): Promise<void>;
	/** Manually ban a key (optionally scoped to a tenant). */
	ban(key: string, duration?: number, tenant?: string | null): Promise<void>;
	/** Remove a ban (optionally scoped to a tenant). */
	unban(key: string, tenant?: string | null): Promise<void>;
	/** Reset all state, or only one tenant's buckets when a tenant id is given. */
	clear(tenant?: string | null): Promise<void>;
}

/**
 * Create a Redis-backed rate limiter using atomic Lua scripts.
 */
export function createRateLimit(client: RedisClient, options: RedisRateLimitOptions): RedisRateLimiter;
