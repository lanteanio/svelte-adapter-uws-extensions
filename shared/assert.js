/**
 * Production-assertion helpers for svelte-adapter-uws-extensions.
 *
 * Mirrors the adapter's two-tier shape from 0.5.0-next.8:
 *
 * - `assert(cond, category, context)` — production-safe invariant check.
 *   On violation: increments the per-category counter on the live module-
 *   level Map, logs a structured `[extensions/assert] {...}` line, and
 *   (in test mode only) throws so vitest surfaces the failure as a test
 *   error. In production it does NOT throw — a thrown exception inside a
 *   Redis pubsub callback or a publish hot-path microtask could leave a
 *   half-applied transaction or a corrupted local index. Counter + log
 *   give us observability without the corruption risk.
 *
 * - `devAssert(cond, message, context)` — dev-time DX hint. Full no-op
 *   when `NODE_ENV === 'production'`. On violation in non-prod modes
 *   logs a warning. Use for cosmetic checks, schema-mismatch hints, etc.
 *   Does NOT throw, even in test mode (matches adapter shape: dev hints
 *   should not gate test runs).
 *
 * Test mode detection matches the adapter exactly:
 * `process.env.VITEST || process.env.NODE_ENV === 'test'`.
 *
 * @module svelte-adapter-uws-extensions/shared/assert
 */

import { stripInternal } from './sensitive.js';

const IS_TEST_MODE = !!process.env.VITEST || process.env.NODE_ENV === 'test';
const IS_PROD = process.env.NODE_ENV === 'production';

/**
 * Per-category violation counter. Survives the lifetime of the process.
 * Read via `getAssertionCounters()` or `wireAssertionMetrics(metrics)`.
 *
 * @type {Map<string, number>}
 */
const counters = new Map();

/**
 * Optional Prometheus counter bound via `wireAssertionMetrics(metrics)`.
 * When set, every `assert` violation increments it alongside the in-memory
 * counter Map.
 *
 * @type {{ inc(labels: { category: string }): void } | null}
 */
let boundCounter = null;

/**
 * Production-safe invariant check. See module-level JSDoc for the
 * counter / log / test-mode-throws contract.
 *
 * @param {boolean} cond - The condition that should hold. If falsy, the
 *   assertion has been violated.
 * @param {string} category - Stable category string for the metric label.
 *   Convention: `<module>.<invariant>` (e.g. `registry.session-shadow.consistency`).
 * @param {Record<string, unknown>} [context] - Serialisable extra context
 *   for the structured log entry. PII keys are stripped via
 *   `stripInternal` before logging.
 */
export function assert(cond, category, context) {
	if (cond) return;
	counters.set(category, (counters.get(category) || 0) + 1);
	if (boundCounter) {
		try { boundCounter.inc({ category }); } catch { /* metrics path is best-effort */ }
	}
	const safeContext = context ? stripInternal(context) : undefined;
	const payload = JSON.stringify(safeContext === undefined
		? { category }
		: { category, context: safeContext });
	console.error('[extensions/assert] ' + payload);
	if (IS_TEST_MODE) {
		throw new Error('extensions assertion failed: ' + category + ' ' + payload);
	}
	// Production: counter + log only. Throwing here could corrupt state
	// (Redis pubsub callbacks, publish hot-path microtasks).
}

/**
 * Dev-time DX hint. Full no-op in production; warning log in dev / test.
 *
 * @param {boolean} cond
 * @param {string} message
 * @param {Record<string, unknown>} [context]
 */
export function devAssert(cond, message, context) {
	if (IS_PROD) return;
	if (cond) return;
	const safeContext = context ? stripInternal(context) : undefined;
	const suffix = safeContext === undefined ? '' : ' ' + JSON.stringify(safeContext);
	console.warn('[extensions/devAssert] ' + message + suffix);
}

/**
 * Read the live counter Map. Mirrors the adapter's `platform.assertions`
 * shape. The Map is the live state -- not a snapshot -- so consumers
 * holding the reference see updates automatically.
 *
 * @returns {Map<string, number>}
 */
export function getAssertionCounters() {
	return counters;
}

/**
 * Wire the assertion counter into a Prometheus registry. Registers
 * `extensions_assertion_violations_total{category}` as a counter that
 * `assert` increments on every violation alongside the in-memory counter
 * Map. Cardinality is bounded by the number of distinct categories
 * declared by the assert call sites (~25 today, all module-level
 * constants — never user-input-driven).
 *
 * Calling twice replaces the bound counter (the most-recent registry
 * wins). Pre-existing in-memory counter values are NOT replayed into the
 * new Prometheus counter; the wiring takes effect for subsequent
 * violations only.
 *
 * @param {import('../prometheus/index.js').MetricsRegistry} metrics
 */
export function wireAssertionMetrics(metrics) {
	if (!metrics || typeof metrics.counter !== 'function') {
		throw new Error('wireAssertionMetrics: metrics registry is required');
	}
	boundCounter = metrics.counter(
		'extensions_assertion_violations_total',
		'Production-assertion violations by category',
		['category']
	);
}

/**
 * Test-only helper. Resets the counter Map and clears the bound
 * Prometheus counter binding. Never call this from production code.
 */
export function _resetCountersForTesting() {
	counters.clear();
	boundCounter = null;
}
