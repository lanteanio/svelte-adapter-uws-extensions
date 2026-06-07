import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface ClockSkewSamplerOptions {
	/**
	 * How often to sample, in milliseconds.
	 * @default 30000
	 */
	intervalMs?: number;
	/**
	 * Redis `TIME` reads per sample; the per-read skew estimates are reduced by
	 * median to reject jitter. Must be `>= 1`.
	 * @default 5
	 */
	samples?: number;
	/**
	 * Absolute skew (ms) at or above which `onWarn` fires (and below `tripMs`).
	 * @default 100
	 */
	warnMs?: number;
	/**
	 * Absolute skew (ms) at or above which `onTrip` fires.
	 * @default 500
	 */
	tripMs?: number;
	/**
	 * Take the first sample at construction instead of waiting one interval.
	 * @default true
	 */
	immediate?: boolean;
	/** Called after every successful sample with the signed median skew. */
	onSkew?: (skewMs: number) => void;
	/** Called when `warnMs <= |skew| < tripMs`. */
	onWarn?: (skewMs: number) => void;
	/** Called when `|skew| >= tripMs`. */
	onTrip?: (skewMs: number) => void;
	/**
	 * Called when a sample fails (Redis error). The last good gauge value is
	 * retained.
	 */
	onError?: (err: Error) => void;
	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface ClockSkewSampler {
	/**
	 * The most recent signed skew in milliseconds (positive = local clock ahead
	 * of Redis), or `null` before the first successful sample.
	 */
	current(): number | null;
	/**
	 * Run one sample immediately and return the signed median skew (or `null` on
	 * failure). Also updates the gauge and fires callbacks.
	 */
	sample(): Promise<number | null>;
	/**
	 * Stop the interval and await any in-flight sample. Idempotent. Never throws.
	 */
	stop(): Promise<void>;
}

/**
 * Create a clock-skew sampler that measures this instance's wall clock against
 * the Redis server clock and exposes it as the `platform_clock_skew_ms` gauge.
 * Starts sampling immediately; call `stop()` on shutdown.
 */
export function createClockSkewSampler(
	client: RedisClient,
	options?: ClockSkewSamplerOptions
): ClockSkewSampler;
