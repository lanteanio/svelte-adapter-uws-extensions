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
	/** Clear the bucket for a key. */
	reset(key: string): Promise<void>;
	/** Manually ban a key. */
	ban(key: string, duration?: number): Promise<void>;
	/** Remove a ban. */
	unban(key: string): Promise<void>;
	/** Reset all state. */
	clear(): Promise<void>;
}

/**
 * Create a Redis-backed rate limiter using atomic Lua scripts.
 */
export function createRateLimit(client: RedisClient, options: RedisRateLimitOptions): RedisRateLimiter;
