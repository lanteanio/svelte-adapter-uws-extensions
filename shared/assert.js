/**
 * Production-assertion helpers for svelte-adapter-uws-extensions.
 *
 * Mirrors the adapter's tiered shape:
 *
 * - `assert(cond, category, context)` - production-safe invariant check
 *   (SOFT tier). On violation: increments the per-category counter on the
 *   live module-level Map, logs a structured `[extensions/assert] {...}`
 *   line, and (in test mode only) throws so vitest surfaces the failure as
 *   a test error. In production it does NOT throw - a thrown exception
 *   inside a Redis pubsub callback or a publish hot-path microtask could
 *   leave a half-applied transaction or a corrupted local index. Counter +
 *   log give us observability without the corruption risk.
 *
 * - `fatal(cond, category, context)` - HARD tier for genuinely
 *   unrecoverable state. Shares the same counter Map and the same bound
 *   Prometheus counter (the violation is labelled `severity="fatal"`),
 *   logs a `[extensions/fatal] {...}` line, and - in production only -
 *   schedules a DEFERRED process termination with exit code 78 AFTER the
 *   current callback frame unwinds (a synchronous exit inside a Redis
 *   callback risks the same half-applied state `assert` guards against).
 *   In test mode it throws instead of exiting so the runner sees it without
 *   killing the harness. The deferred exit is injectable via
 *   `setFatalSink` so the simulator captures fatals instead of exiting.
 *
 * - `devAssert(cond, message, context)` - dev-time DX hint. Full no-op
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
import { microtask } from './runtime.js';

// Process exit code for a hard-tier invariant violation. Distinct from a
// config-error exit (1) and a graceful shutdown (0) so ops can tell a
// crash-on-bad-state apart from a crash-on-bad-config.
const FATAL_EXIT_CODE = 78;

/**
 * Test-mode and production-mode checks are evaluated per call (not at
 * module load) so a test that flips `process.env.NODE_ENV` or sets
 * `process.env.VITEST` partway through its run gets the updated mode
 * on the next `assert` / `devAssert` invocation. Module-load snapshots
 * are stale by the time the test mutates env, which is the typical
 * shape for vitest setup files that re-enter mode mid-run.
 */
function isTestMode() {
	return !!process.env.VITEST || process.env.NODE_ENV === 'test';
}
function isProd() {
	return process.env.NODE_ENV === 'production';
}

/**
 * Per-category violation counter. Survives the lifetime of the process.
 * Read via `getAssertionCounters()` or `wireAssertionMetrics(metrics)`.
 *
 * @type {Map<string, number>}
 */
const counters = new Map();

/**
 * Optional Prometheus counter bound via `wireAssertionMetrics(metrics)`.
 * When set, every `assert` / `fatal` violation increments it alongside the
 * in-memory counter Map, labelled by `category` and `severity`.
 *
 * @type {{ inc(labels: { category: string, severity: string }): void } | null}
 */
let boundCounter = null;

/**
 * Injectable sink for the hard-tier termination. Defaults to the real
 * `process.exit`. The simulator swaps this so a `fatal` is captured instead
 * of killing the harness; tests swap it to assert the exit was scheduled.
 *
 * @type {{ exit(code: number): void }}
 */
let fatalSink = { exit: (code) => process.exit(code) };

/**
 * Record a violation against the live counter Map and the bound Prometheus
 * counter (if wired). One place so `assert` and `fatal` stay in lockstep on
 * the counting + the severity label.
 *
 * @param {string} category
 * @param {'soft' | 'fatal'} severity
 */
function recordViolation(category, severity) {
	counters.set(category, (counters.get(category) || 0) + 1);
	if (boundCounter) {
		try { boundCounter.inc({ category, severity }); } catch { /* metrics path is best-effort */ }
	}
}

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
	recordViolation(category, 'soft');
	const safeContext = context ? stripInternal(context) : undefined;
	const payload = JSON.stringify(safeContext === undefined
		? { category }
		: { category, context: safeContext });
	console.error('[extensions/assert] ' + payload);
	if (isTestMode()) {
		throw new Error('extensions assertion failed: ' + category + ' ' + payload);
	}
	// Production: counter + log only. Throwing here could corrupt state
	// (Redis pubsub callbacks, publish hot-path microtasks).
}

/**
 * Hard-tier invariant check for genuinely unrecoverable state. See the
 * module-level JSDoc for the full contract. Shares the counter Map and the
 * bound Prometheus counter with `assert` (one metric namespace), labelled
 * `severity="fatal"`. On violation in production: schedules a DEFERRED
 * `process.exit(78)` (via the injectable sink, in a microtask so the current
 * callback frame unwinds first). In test mode: throws instead of exiting.
 *
 * @param {boolean} cond - The condition that must hold. If falsy, the
 *   hard-tier invariant has been violated.
 * @param {string} category - Stable category string for the metric label.
 * @param {Record<string, unknown>} [context] - Serialisable extra context;
 *   PII keys are stripped via `stripInternal` before logging.
 */
export function fatal(cond, category, context) {
	if (cond) return;
	recordViolation(category, 'fatal');
	const safeContext = context ? stripInternal(context) : undefined;
	const payload = JSON.stringify(safeContext === undefined
		? { category, severity: 'fatal' }
		: { category, context: safeContext, severity: 'fatal' });
	console.error('[extensions/fatal] ' + payload);
	if (isTestMode()) {
		throw new Error('extensions fatal: ' + category + ' ' + payload);
	}
	// Production: defer the termination so the current callback frame completes
	// before the process goes down (a synchronous exit inside a Redis pubsub
	// callback risks the half-applied state `assert` already guards against).
	// Flush the metric/log above first; the exit rides a microtask after.
	microtask(() => { fatalSink.exit(FATAL_EXIT_CODE); });
}

/**
 * Install a custom hard-tier termination sink. The simulator uses this to
 * capture fatals into its result set instead of exiting the harness; tests
 * use it to assert an exit was scheduled without killing the runner. Never
 * call this from production code.
 *
 * @param {{ exit(code: number): void }} sink
 */
export function setFatalSink(sink) {
	if (!sink || typeof sink.exit !== 'function') {
		throw new Error('setFatalSink: sink must expose an exit(code) function');
	}
	fatalSink = sink;
}

/**
 * Restore the default termination sink (`process.exit`). Test/sim teardown.
 */
export function resetFatalSink() {
	fatalSink = { exit: (code) => process.exit(code) };
}

/**
 * Dev-time DX hint. Full no-op in production; warning log in dev / test.
 *
 * @param {boolean} cond
 * @param {string} message
 * @param {Record<string, unknown>} [context]
 */
export function devAssert(cond, message, context) {
	if (isProd()) return;
	if (cond) return;
	const safeContext = context ? stripInternal(context) : undefined;
	const suffix = safeContext === undefined ? '' : ' ' + JSON.stringify(safeContext);
	console.warn('[extensions/devAssert] ' + message + suffix);
}

/**
 * Read the live counter Map. Mirrors the adapter's `platform.assertions`
 * shape. The Map is the live state - not a snapshot - so consumers
 * holding the reference see updates automatically.
 *
 * @returns {Map<string, number>}
 */
export function getAssertionCounters() {
	return counters;
}

/**
 * Wire the assertion counter into a Prometheus registry. Registers
 * `extensions_assertion_violations_total{category,severity}` as a counter
 * that both `assert` (severity="soft") and `fatal` (severity="fatal")
 * increment on every violation alongside the in-memory counter Map.
 * Cardinality is bounded by the number of distinct categories declared by
 * the call sites (all module-level constants - never user-input-driven)
 * times the two severities.
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
		'Production-assertion violations by category and severity',
		['category', 'severity']
	);
}

/**
 * Test-only helper. Resets the counter Map and clears the bound
 * Prometheus counter binding. Never call this from production code.
 */
export function _resetCountersForTesting() {
	counters.clear();
	boundCounter = null;
	resetFatalSink();
}
