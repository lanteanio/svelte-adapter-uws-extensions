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
	/**
	 * Key extraction mode. @default 'ip'
	 *
	 * In 'ip' mode (the default) the bucket key is `userData.remoteAddress`, which the adapter
	 * resolves from ADDRESS_HEADER / XFF_DEPTH. Behind an address-rewriting proxy (docker
	 * userland-proxy, an L4 load balancer, a non-XFF proxy) with ADDRESS_HEADER unset, every
	 * client arrives as the same gateway address and the per-IP bucket collapses into one shared
	 * global bucket. Set ADDRESS_HEADER (and XFF_DEPTH) so the real client IP is resolved, or pass
	 * an explicit keyBy. The limiter logs a one-shot warning the first time it denies on a
	 * loopback/private key while ADDRESS_HEADER is unset.
	 */
	keyBy?: 'ip' | 'connection' | ((ws: any) => string);
	/**
	 * Optional per-connection tenant resolver. When set, the bucket key is scoped by
	 * the returned tenant id (joined to the key with a NUL, so it stays unambiguous even
	 * for IPv6 keys), so two tenants sharing an IP / connection / custom key get
	 * independent buckets. Return null/undefined for an unscoped connection; omit for a
	 * single-tenant deploy (byte-identical).
	 */
	tenant?: (ws: any) => string | null | undefined;
	/**
	 * Opt-in degraded mode for `consume()`: when Redis is unreachable (or the
	 * breaker is open), decide on an in-process token bucket with the same
	 * semantics instead of rejecting the promise. `true` reuses the configured
	 * points/interval; the object form sets a tighter per-instance budget
	 * (e.g. `points / instanceCount` keeps the fleet-wide allowance roughly
	 * constant while degraded). Floor state is per process, so N instances
	 * allow up to N times the floor budget in the worst case. Admin ops
	 * (`reset` / `ban` / `unban` / `clear` / `purgeUser`) still reject while
	 * the store is down - only the request-path verdict degrades. When a
	 * metrics registry is configured, floor-decided verdicts count in
	 * `ratelimit_storage_fallbacks_total`. @default false
	 */
	localFloorOnStorageFailure?: boolean | { points?: number; interval?: number };
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
	/**
	 * Right-to-erasure: clear a user's bucket. Meaningful only when `keyBy`
	 * resolves to the userId; a no-op for ip/connection buckets. Counters-only,
	 * so no PII is involved.
	 */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;
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
