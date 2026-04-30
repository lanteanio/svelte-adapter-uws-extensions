import type { MetricsRegistry } from '../prometheus/index.js';

export type PressureReason = 'NONE' | 'PUBLISH_RATE' | 'SUBSCRIBERS' | 'MEMORY';

export interface PressureSnapshot {
	readonly active: boolean;
	readonly subscriberRatio: number;
	readonly publishRate: number;
	readonly memoryMB: number;
	readonly reason: PressureReason;
}

/**
 * A per-class rule. Either:
 * - an array of reasons -- this class is rejected when `pressure.reason` is in the list
 * - a predicate -- this class is rejected when the predicate returns true for the current snapshot
 */
export type AdmissionRule = readonly PressureReason[] | ((snapshot: PressureSnapshot) => boolean);

export interface AdmissionControlOptions {
	/**
	 * Map of class name to admission rule. Each class is independently
	 * configured. `shouldAccept(className, ...)` with an unknown name
	 * throws -- typos surface immediately rather than silently accepting.
	 */
	classes: Record<string, AdmissionRule>;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
}

export interface AdmissionControl {
	/**
	 * Returns `true` when this class is admitted under the platform's
	 * current pressure snapshot, `false` when the class should be shed.
	 *
	 * Reads `platform.pressure` -- a property access, no I/O. Safe to
	 * call on every request hot path. The reason-precedence math
	 * (memory > publish rate > subscribers > none) lives in the
	 * adapter; this method only checks the resolved `reason` against
	 * the configured rule.
	 *
	 * Throws if `className` was not configured at construction time.
	 */
	shouldAccept(className: string, platform: { readonly pressure: PressureSnapshot }): boolean;
}

/**
 * Create a pressure-aware admission controller. Complements (does not
 * replace) the dependency circuit breaker -- the breaker answers "is
 * the backend up?", admission control answers "are we OK to take more
 * work right now?"
 */
export function createAdmissionControl(options: AdmissionControlOptions): AdmissionControl;
