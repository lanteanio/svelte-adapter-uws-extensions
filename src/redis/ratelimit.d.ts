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
	/**
	 * Tuning for the fleet-wide emergency scale reader. The factor is read
	 * through a lazily-refreshed cache (at most one background GET per
	 * `refreshMs` window, never per check); lower values propagate an
	 * incident clamp faster at the cost of more reads. @default { refreshMs: 1000 }
	 */
	emergency?: { refreshMs?: number };
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

/**
 * Operator surface for the fleet-wide emergency scale: one shared Redis key
 * holding a factor applied to every limiter's budget at check time. `0.2`
 * tightens every limit sharing the Redis to 20% of its configured points
 * within the readers' refresh window (~1s); `2` doubles them; clearing (or
 * TTL expiry) restores neutral. The same factor keeps applying on the
 * in-process floor paths while the store is unreachable.
 */
export interface RateLimitEmergency {
	/**
	 * Apply a factor fleet-wide. Applies a one-hour TTL by default so a
	 * forgotten incident clamp expires on its own; pass `{ ttlMs: 0 }` for a
	 * persistent factor.
	 */
	set(scale: number, opts?: { ttlMs?: number }): Promise<void>;
	/** Restore the neutral factor. */
	clear(): Promise<void>;
	/** Read the currently stored factor (1 when unset). */
	get(): Promise<number>;
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
	/** The fleet-wide emergency scale surface (shared across every limiter on this client). */
	emergency: RateLimitEmergency;
}

/**
 * Create a Redis-backed rate limiter using atomic Lua scripts.
 */
export function createRateLimit(client: RedisClient, options: RedisRateLimitOptions): RedisRateLimiter;

/**
 * Standalone emergency-scale surface for ops scripts that have a Redis
 * client but no limiter instance. Flips the same shared key every limiter
 * reads.
 */
export function createRateLimitEmergency(client: RedisClient): RateLimitEmergency;

/** One dimension of a composite rate limit. */
export interface CompositeDimension {
	/** Budget per interval for this dimension. Positive integer. */
	points: number;
	/** Refill interval in milliseconds. Positive. */
	interval: number;
	/** Auto-ban duration (ms) when this dimension trips. 0 = no ban. @default 0 */
	blockDuration?: number;
	/**
	 * Key extraction for this dimension. REQUIRED: each dimension must count
	 * along its own axis (account, endpoint, action, ...), so there is no
	 * shared default to silently collapse the composite into one budget.
	 */
	keyBy: 'ip' | 'connection' | ((ws: any) => string);
}

export interface CompositeConsumeResult {
	/** Whether the request was permitted (all dimensions had budget). */
	allowed: boolean;
	/** The dimension that denied (declaration order decides when several would), or null. */
	tripped: string | null;
	/** Post-verdict remaining points per dimension. */
	remaining: Record<string, number>;
	/** Retry-after of the tripped dimension on deny; the soonest refill across dimensions on allow. */
	resetMs: number;
}

export interface CompositeRateLimitOptions {
	/**
	 * The dimensions consulted per check (2 to 8), most-strict-wins: a request
	 * is admitted only when EVERY dimension has budget, and consumes from all
	 * of them atomically or from none.
	 */
	dimensions: Record<string, CompositeDimension>;
	/** Optional per-connection tenant resolver; scopes every dimension's keys. */
	tenant?: (ws: any) => string | null | undefined;
	/**
	 * Opt-in degraded mode: when the store is unreachable, decide on
	 * in-process buckets mirroring each dimension's own budget with the same
	 * all-or-nothing semantics. Boolean only. @default false
	 */
	localFloorOnStorageFailure?: boolean;
	/** Tuning for the fleet-wide emergency scale reader. @default { refreshMs: 1000 } */
	emergency?: { refreshMs?: number };
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface CompositeRateLimiter {
	/** Consult every dimension atomically; consume from all or none. */
	consume(ws: any, cost?: number): Promise<CompositeConsumeResult>;
	/** Clear one dimension bucket. */
	reset(dimension: string, key: string, tenant?: string | null): Promise<void>;
	/** Ban one key on one dimension. */
	ban(dimension: string, key: string, duration?: number, tenant?: string | null): Promise<void>;
	/** Lift a ban on one dimension bucket. */
	unban(dimension: string, key: string, tenant?: string | null): Promise<void>;
	/** Clear buckets (one tenant's hash-tag space, or the whole composite key space). */
	clear(tenant?: string | null): Promise<void>;
	/** The fleet-wide emergency scale surface (the same shared key the single-dimension limiter reads). */
	emergency: RateLimitEmergency;
}

/**
 * Create a composite multi-dimension rate limiter: several budgets, one
 * atomic verdict. On Redis Cluster every dimension key for a check shares
 * one hash tag (per tenant), so a tenant's composite buckets concentrate on
 * one shard - the standard trade for atomic multi-scope limiting.
 */
export function createCompositeRateLimit(client: RedisClient, options: CompositeRateLimitOptions): CompositeRateLimiter;
