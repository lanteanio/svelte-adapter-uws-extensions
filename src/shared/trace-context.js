// W3C trace-context validation, kept as a local copy of the adapter's
// svelte-adapter-uws/observability implementation rather than an import so the
// bus modules stay free of a hard adapter dependency. normalizeTraceContext and
// injectTraceContext must behave exactly like the adapter's; the parity test
// drives both over the same vectors against the installed adapter.

const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const ZERO_TRACE_ID = '00000000000000000000000000000000';
const ZERO_SPAN_ID = '0000000000000000';
const MAX_TRACESTATE_BYTES = 512;
const MAX_TRACESTATE_MEMBERS = 32;

/** Span attributes shared by every Redis producer and consumer operation. */
export const REDIS_TRACE_ATTRIBUTES = Object.freeze({ 'messaging.system': 'redis' });
/** Span attributes shared by every Postgres producer and consumer operation. */
export const POSTGRES_TRACE_ATTRIBUTES = Object.freeze({ 'messaging.system': 'postgresql' });

function readHeader(carrier, name) {
	if (carrier == null) return undefined;
	try {
		if (typeof carrier.get === 'function') return carrier.get(name) ?? undefined;
		const direct = carrier[name] ?? carrier[name.toLowerCase()] ?? carrier[name.toUpperCase()];
		if (direct !== undefined) return direct;
		for (const key of Object.keys(carrier)) {
			if (key.toLowerCase() === name) return carrier[key];
		}
	} catch { /* hostile getter or proxy */ }
	return undefined;
}

function validTracestate(value) {
	if (value === undefined || value === null || value === '') return null;
	if (typeof value !== 'string' || value.length > MAX_TRACESTATE_BYTES) return null;
	for (let i = 0; i < value.length; i++) {
		const code = value.charCodeAt(i);
		if (code < 0x20 || code > 0x7e) return null;
	}
	const members = value.split(',');
	if (members.length > MAX_TRACESTATE_MEMBERS) return null;
	const keys = new Set();
	for (const raw of members) {
		const member = raw.trim();
		const equal = member.indexOf('=');
		if (equal < 1 || equal === member.length - 1) return null;
		const key = member.slice(0, equal);
		const validKey = key.includes('@')
			? /^[a-z0-9][a-z0-9_\-*\/]{0,240}@[a-z][a-z0-9_\-*\/]{0,13}$/.test(key)
			: /^[a-z0-9][a-z0-9_\-*\/]{0,255}$/.test(key);
		if (!validKey || keys.has(key)) return null;
		keys.add(key);
		const stateValue = member.slice(equal + 1);
		if (stateValue.length > 256 || stateValue.includes('=')) return null;
	}
	return value;
}

/**
 * Validate and normalize a W3C trace context. Invalid input is ignored rather
 * than propagated across a trust boundary.
 *
 * @param {unknown} value
 * @returns {{ traceparent: string, tracestate?: string } | null}
 */
export function normalizeTraceContext(value) {
	if (value === null || typeof value !== 'object') return null;
	const traceparent = readHeader(value, 'traceparent');
	if (typeof traceparent !== 'string') return null;
	const match = TRACEPARENT_RE.exec(traceparent);
	if (match === null || match[1] === ZERO_TRACE_ID || match[2] === ZERO_SPAN_ID) return null;
	const tracestate = validTracestate(readHeader(value, 'tracestate'));
	return tracestate === null ? { traceparent } : { traceparent, tracestate };
}

/**
 * Write a validated context onto a carrier. A null or invalid context writes
 * nothing, so an application field named `traceparent` inside `data` is never
 * touched and an envelope stays byte-identical when there is nothing to carry.
 *
 * @template {Record<string, any>} T
 * @param {T} carrier
 * @param {unknown} [context]
 * @returns {T}
 */
export function injectTraceContext(carrier, context = null) {
	const normalized = normalizeTraceContext(context);
	if (normalized === null || carrier === null || typeof carrier !== 'object') return carrier;
	if (typeof carrier.set === 'function') {
		carrier.set('traceparent', normalized.traceparent);
		if (normalized.tracestate !== undefined) carrier.set('tracestate', normalized.tracestate);
		return carrier;
	}
	carrier.traceparent = normalized.traceparent;
	if (normalized.tracestate !== undefined) carrier.tracestate = normalized.tracestate;
	return carrier;
}

/**
 * The Platform's live context, validated. A direct property read rather than
 * the case-insensitive header lookup: the Platform is a known object, and
 * scanning its keys for a missing context costs more than the publish.
 *
 * @param {unknown} platform
 * @returns {{ traceparent: string, tracestate?: string } | null}
 */
export function liveTraceContext(platform) {
	let live;
	try {
		live = /** @type {any} */ (platform)?.traceContext;
	} catch {
		return null;
	}
	return live == null ? null : normalizeTraceContext(live);
}

/**
 * The context a caller passed explicitly as `options.traceContext`, for the
 * job and task producers that accept one. `undefined` when the option is
 * absent, so the caller falls back to the Platform's live context; `null`
 * when it is present but null or invalid, which opts that call out of
 * tracing entirely.
 *
 * @param {unknown} options
 * @returns {{ traceparent: string, tracestate?: string } | null | undefined}
 */
export function explicitTraceContext(options) {
	if (options === null || typeof options !== 'object' ||
		!Object.prototype.hasOwnProperty.call(options, 'traceContext')) {
		return undefined;
	}
	return normalizeTraceContext(/** @type {any} */ (options).traceContext);
}

/**
 * The trace context carried by one of this package's own envelopes or rows.
 * Those are written with exact lowercase field names, so a missing
 * traceparent field is answered without the header-style key scan.
 *
 * @param {any} carrier
 * @returns {{ traceparent: string, tracestate?: string } | null}
 */
export function carriedTraceContext(carrier) {
	if (carrier === null || typeof carrier !== 'object' || typeof carrier.traceparent !== 'string') return null;
	return normalizeTraceContext(carrier);
}

/**
 * True when the Platform has a tracing provider configured. The Redis buses
 * gate every trace step on this, the same condition under which the adapter
 * itself does trace work, so a publish on a Platform without a provider reads
 * no context and allocates nothing for tracing. The adapter fixes its
 * provider at build time, so the buses read this once per wrapped or
 * activated Platform.
 *
 * @param {unknown} platform
 */
export function traceEnabled(platform) {
	try {
		const trace = /** @type {any} */ (platform)?.trace;
		return trace != null && trace.enabled === true && typeof trace.run === 'function';
	} catch {
		return false;
	}
}

function currentContext(platform, fallback) {
	try {
		const current = platform?.trace?.current?.();
		return normalizeTraceContext(current) ?? fallback;
	} catch {
		return fallback;
	}
}

/**
 * Run one producer/consumer operation through the adapter's structural trace
 * surface. With no provider it restores a valid parent through withContext
 * when available; with no context and no provider it is a direct callback.
 *
 * @template T
 * @param {unknown} platform
 * @param {string} name
 * @param {'producer' | 'consumer'} kind
 * @param {{ traceparent: string, tracestate?: string } | null} parent
 * @param {Readonly<Record<string, string | number | boolean>>} attributes
 * @param {(context: { traceparent: string, tracestate?: string } | null) => T} fn
 * @returns {T}
 */
export function runTraceOperation(platform, name, kind, parent, attributes, fn) {
	if (traceEnabled(platform)) {
		return platform.trace.run(name, { kind, parent, attributes }, () => fn(currentContext(platform, parent)));
	}
	if (parent !== null) {
		let withContext = null;
		try {
			if (typeof platform?.trace?.withContext === 'function') {
				withContext = platform.trace.withContext.bind(platform.trace);
			}
		} catch {
			// A tracing observer cannot break transport delivery.
		}
		if (withContext !== null) return withContext(parent, () => fn(parent));
	}
	return fn(parent);
}

/** Restore context without creating a span. */
export function withTraceContext(platform, context, fn) {
	if (context !== null) {
		let withContext = null;
		try {
			if (typeof platform?.trace?.withContext === 'function') {
				withContext = platform.trace.withContext.bind(platform.trace);
			}
		} catch {
			// Trace integration is observational.
		}
		if (withContext !== null) return withContext(context, fn);
	}
	return fn();
}
