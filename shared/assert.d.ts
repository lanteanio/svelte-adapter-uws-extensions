import type { MetricsRegistry } from '../prometheus/index.js';

/**
 * Production-safe invariant check.
 *
 * On violation: increments the per-category counter on the live module-
 * level Map, increments the bound Prometheus counter (if
 * `wireAssertionMetrics` was called), and logs a structured
 * `[extensions/assert] {...}` line.
 *
 * Test mode (`process.env.VITEST` or `NODE_ENV === 'test'`): also throws,
 * so vitest surfaces the failure as a test error.
 *
 * Production: does NOT throw - a thrown exception inside a Redis pubsub
 * callback or a publish hot-path microtask could leave a half-applied
 * transaction or a corrupted local index. Counter + log only.
 */
export function assert(cond: boolean, category: string, context?: Record<string, unknown>): void;

/**
 * Dev-time DX hint. Full no-op when `NODE_ENV === 'production'`. On
 * violation in non-prod modes logs a warning. Does NOT throw, even in
 * test mode (matches adapter shape: dev hints should not gate test runs).
 */
export function devAssert(cond: boolean, message: string, context?: Record<string, unknown>): void;

/**
 * Read the live per-category violation counter Map. The Map is the live
 * state - not a snapshot - so consumers holding the reference see updates
 * automatically. Mirrors the adapter's `platform.assertions` shape.
 */
export function getAssertionCounters(): Map<string, number>;

/**
 * Wire the assertion counter into a Prometheus registry. Registers
 * `extensions_assertion_violations_total{category}` as a counter that
 * `assert` increments on every violation alongside the in-memory counter
 * Map. Calling twice replaces the bound counter (most-recent registry
 * wins).
 */
export function wireAssertionMetrics(metrics: MetricsRegistry): void;
