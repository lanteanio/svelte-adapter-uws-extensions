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
	/**
	 * Opt-in self-fencing: turn the trip threshold from an alert into a state.
	 * The sampler enters the FENCED state after `tripSamples` (default 2)
	 * consecutive trip-level samples and releases after `releaseSamples`
	 * (default 3) consecutive below-warn samples; warn-level samples hold the
	 * current state (deliberate hysteresis). While fenced, `fenced()` reads
	 * true and the `platform_clock_fenced` gauge reads 1; consumers that stamp
	 * or order by this instance's clock stand down (`attachClockFence` + the
	 * clustered `live.smooth` authority gate consume it that way). A failed
	 * sample never changes fence state. @default false
	 */
	fence?: boolean | { tripSamples?: number; releaseSamples?: number };
	/** Called on every fence-state transition. */
	onFence?: (fenced: boolean) => void;
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
	 * Whether the sampler is in the FENCED state. Always `false` when the
	 * `fence` option is off.
	 */
	fenced(): boolean;
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
 * Attach a sampler's fence surface to the platform as `platform.clockFence`,
 * so framework layers that stamp or order by this instance's clock (the
 * clustered `live.smooth` authority renewal gate today) stand down while the
 * clock is untrustworthy. `bus.wrap` forwards it, so the wrapped platform a
 * clustered app hands to the framework carries it too. Requires a sampler
 * constructed with the `fence` option. Returns the platform.
 */
export function attachClockFence(
	platform: object,
	sampler: ClockSkewSampler
): object;

/**
 * Create a clock-skew sampler that measures this instance's wall clock against
 * the Redis server clock and exposes it as the `platform_clock_skew_ms` gauge.
 * Starts sampling immediately; call `stop()` on shutdown.
 */
export function createClockSkewSampler(
	client: RedisClient,
	options?: ClockSkewSamplerOptions
): ClockSkewSampler;
