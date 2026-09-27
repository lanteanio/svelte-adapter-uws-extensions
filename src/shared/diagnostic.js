// The diagnostic contract shared with svelte-adapter-uws/observability, kept
// as a local copy rather than an import so the bus modules do not load the
// adapter's observability module. Everything an operator or a
// collector can observe is identical to the adapter's: the console prefix, the
// one-line grammar, the record envelope, the validation rules, and the
// Symbol.for keys of the process-wide sink registry, so a sink installed
// through either package receives both packages' records. The parity test
// pins each of those against the installed adapter.

import { wallIso } from './runtime.js';
import { redactConnectionUrl } from './sensitive.js';
import { EXTENSIONS_ERROR_IDS, extensionsConsoleLine, extensionsErrorDefinition } from '../error-registry.js';

export const PACKAGE_SOURCE = 'svelte-adapter-uws-extensions';
export const DIAGNOSTIC_SCHEMA_VERSION = 1;
export const DIAGNOSTIC_PREFIX = 'lantean/diagnostic';
export const TELEMETRY_LEVELS = Object.freeze(['debug', 'info', 'warn', 'error', 'fatal']);
export const DATA_CLASSES = Object.freeze(['operational', 'pseudonymous', 'application', 'secret']);

const MAX_TEXT = 512;
const LEVELS = new Set(TELEMETRY_LEVELS);
const CLASSES = new Set(DATA_CLASSES);
const DOT_NAME = /^[a-z][a-z0-9-]*(?:\.[a-z0-9-]+)+$/;
const SOURCE = /^[a-z0-9@][a-z0-9@/._-]{0,127}$/;
const UNSAFE_TEXT = /[\u0000-\u001f\u007f-\uffff]/g;

// Symbol.for makes the sink process-wide even when more than one package (or
// more than one installed copy) participates in the same server process.
const OPERATIONAL_EVENT_SINK = Symbol.for('lantean.operational-event-sink.v1');
const OPERATIONAL_EVENT_SINK_REGISTRY = Symbol.for('lantean.operational-event-sink-registry.v1');

function unicodeEscape(character) {
	return '\\u' + character.charCodeAt(0).toString(16).padStart(4, '0');
}

export function normalizeDiagnosticMessage(value, fallback = 'unknown') {
	const text = String(value ?? fallback)
		.replace(UNSAFE_TEXT, unicodeEscape)
		.replace(/\s+/g, ' ')
		.trim();
	return (text || fallback).slice(0, MAX_TEXT);
}

function asciiJson(value) {
	return JSON.stringify(value).replace(/[\u007f-\uffff]/g, unicodeEscape);
}

export function isDiagnosticDotName(value) {
	return DOT_NAME.test(value);
}

export function isDiagnosticSeverity(value) {
	return LEVELS.has(value);
}

function checkedSource(value) {
	const source = String(value ?? '');
	if (!SOURCE.test(source)) throw new TypeError('diagnostic source is invalid');
	return source;
}

function checkedDotName(value, field) {
	const name = String(value ?? '');
	if (!isDiagnosticDotName(name)) throw new TypeError(`diagnostic ${field} must be a dot-name`);
	return name;
}

function checkedSeverity(value) {
	if (!isDiagnosticSeverity(value)) throw new TypeError('diagnostic severity is invalid');
	return value;
}

function checkedDataClass(value) {
	if (!CLASSES.has(value)) throw new TypeError('diagnostic dataClass is invalid');
	return value;
}

/**
 * Build the package-neutral diagnostic envelope shared by the adapter,
 * extensions, and realtime packages.
 *
 * @param {{ source: string, component: string, event: string, severity: string, message: string, occurredAt?: string | null, dataClass?: string, attributes?: unknown }} input
 */
export function createDiagnostic(input) {
	if (!input || typeof input !== 'object') throw new TypeError('diagnostic input is required');
	const occurredAt = input.occurredAt ?? null;
	if (occurredAt !== null && (typeof occurredAt !== 'string' || !occurredAt)) {
		throw new TypeError('diagnostic occurredAt must be a string or null');
	}
	const severity = checkedSeverity(input.severity);
	return {
		schemaVersion: DIAGNOSTIC_SCHEMA_VERSION,
		occurredAt,
		source: checkedSource(input.source),
		component: checkedDotName(input.component, 'component'),
		event: checkedDotName(input.event, 'event'),
		severity,
		level: severity,
		dataClass: checkedDataClass(input.dataClass ?? 'operational'),
		message: normalizeDiagnosticMessage(input.message),
		attributes: input.attributes ?? null
	};
}

/** @param {Parameters<typeof createDiagnostic>[0]} input */
export function formatDiagnostic(input) {
	const record = createDiagnostic(input);
	// Keep the physical console line ASCII outside the package-owned English
	// message. JSON escapes retain the exact structured value for collectors
	// while preventing C1, bidi controls, and right-to-left payload text from
	// reordering the surrounding terminal line.
	const suffix = asciiJson(record);
	return `[${DIAGNOSTIC_PREFIX} source=${record.source} component=${record.component} event=${record.event} severity=${record.severity}] ${record.message} ${suffix}`;
}

/**
 * Bounded, throw-proof projection of a caught value for diagnostic
 * attributes: name, code, and message only - never the stack, never
 * arbitrary own properties.
 *
 * @param {unknown} error
 * @returns {{ name: string, code: string | null, message: string }}
 */
export function diagnosticError(error) {
	let name = 'Error';
	let code = null;
	let message = 'Uninspectable thrown value';
	try { if (typeof (/** @type {any} */ (error))?.name === 'string') name = /** @type {any} */ (error).name; } catch { /* hostile getter */ }
	try { if ((/** @type {any} */ (error))?.code != null) code = String((/** @type {any} */ (error)).code); } catch { /* hostile getter */ }
	try { message = String((/** @type {any} */ (error))?.message ?? error); } catch { /* hostile toString */ }
	return { name: name.slice(0, 80), code: code === null ? null : code.slice(0, 80), message: message.slice(0, 512) };
}

function defaultOperationalEventSink(record) {
	const method = record.severity === 'debug' ? 'debug'
		: record.severity === 'info' ? 'info'
			: record.severity === 'warn' ? 'warn' : 'error';
	// Rendering and writing are separate protected steps so a console method
	// that throws is reported as that, not as a record that could not render.
	let line;
	try {
		line = formatDiagnostic(record);
	} catch {
		// An unserializable attribute (BigInt, circular reference) must not
		// erase the event: drop the attributes, keep the envelope.
		try {
			line = formatDiagnostic(createDiagnostic({ ...record, attributes: undefined }));
		} catch {
			try { console.error(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RENDER_COLLAPSE, String(record?.event))); } catch { /* console gone */ }
			return;
		}
	}
	try {
		console[method](line);
	} catch {
		try { console.error(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_CONSOLE_WRITE, String(record?.event))); } catch { /* console gone */ }
	}
}

function sinkFailureFallback(record) {
	// A broken observer must not erase the event it was meant to report. The
	// original goes out first and on its own; only building the notice below
	// can fail, and that is one record's worth of loss.
	defaultOperationalEventSink(record);
	/** @type {any} */
	let notice;
	try {
		const definition = extensionsErrorDefinition(EXTENSIONS_ERROR_IDS.SINK_FAILED);
		notice = createDiagnostic({
			source: PACKAGE_SOURCE,
			component: definition.component,
			event: definition.event,
			severity: definition.severity,
			message: definition.problemPrefix,
			occurredAt: wallIso(),
			attributes: { originalSource: record.source, originalEvent: record.event }
		});
	} catch {
		try { console.error(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_SINK_NOTICE, String(record?.event))); } catch { /* console gone */ }
		return;
	}
	defaultOperationalEventSink(notice);
}

// The two sink slots live on globalThis so the adapter, extensions and realtime
// packages share one registration even when a bundler gives each its own copy
// of this module. globalThis inherits from Object.prototype, so a plain
// assignment is a [[Set]] an accessor on either key can swallow - and a
// swallowed publication reads back undefined, which is how every copy ends up
// with its own sink and events go to console while a sink is installed.
// defineProperty consults no prototype.
/**
 * @param {symbol} key
 * @param {unknown} value
 */
function defineGlobal(key, value) {
	Object.defineProperty(globalThis, key, { value, writable: true, enumerable: true, configurable: true });
}

function operationalEventSinkRegistry() {
	let registry = globalThis[OPERATIONAL_EVENT_SINK_REGISTRY];
	if (!registry || !Array.isArray(registry.entries)) {
		const inherited = globalThis[OPERATIONAL_EVENT_SINK];
		registry = {
			entries: typeof inherited === 'function'
				? [{ sink: inherited, active: true }]
				: []
		};
		defineGlobal(OPERATIONAL_EVENT_SINK_REGISTRY, registry);
	}
	return registry;
}

function syncOperationalEventSink(registry) {
	const current = registry.entries[registry.entries.length - 1];
	if (current) defineGlobal(OPERATIONAL_EVENT_SINK, current.sink);
	else delete globalThis[OPERATIONAL_EVENT_SINK];
}

/**
 * Install the process-wide operational event sink shared by the adapter,
 * extensions, and realtime packages. Pass null to restore console output.
 * The returned disposer removes only this registration. If registrations are
 * nested, the newest still-active sink resumes; disposing an older registration
 * out of order prevents it from ever being resurrected later.
 *
 * @param {((record: ReturnType<typeof createDiagnostic>) => void | Promise<void>) | null} sink
 */
export function setOperationalEventSink(sink) {
	if (sink !== null && typeof sink !== 'function') {
		throw new TypeError('operational event sink must be a function or null');
	}
	const registry = operationalEventSinkRegistry();
	if (sink === null) {
		for (const entry of registry.entries) entry.active = false;
		registry.entries.length = 0;
		delete globalThis[OPERATIONAL_EVENT_SINK];
		return () => {};
	}
	const entry = { sink, active: true };
	registry.entries.push(entry);
	defineGlobal(OPERATIONAL_EVENT_SINK, sink);
	return () => {
		if (!entry.active) return;
		entry.active = false;
		const index = registry.entries.indexOf(entry);
		if (index !== -1) registry.entries.splice(index, 1);
		syncOperationalEventSink(registry);
	};
}

/**
 * Emit one canonical operational record. A configured sink receives the
 * structured record; otherwise a canonical JSON diagnostic line is written to
 * console. Sink throws and rejections fail over to console and never escape.
 *
 * @param {Parameters<typeof createDiagnostic>[0]} input
 */
export function emitOperationalEvent(input) {
	let record;
	try {
		record = createDiagnostic({ ...input, occurredAt: input?.occurredAt ?? wallIso() });
	} catch (err) {
		// Telemetry must never turn the failure it reports into a crash: many
		// callers emit from inside a catch block on a timer or a Redis error
		// handler. An invalid record shape is a programming error for tests to
		// surface; at runtime the last honest act is a plain console line.
		try {
			console.error(
				extensionsConsoleLine(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RECORD_SHAPE),
				/** @type {any} */ (err)?.message ?? err,
				input?.event
			);
		} catch { /* console gone */ }
		return null;
	}
	// Frozen at the trust boundary so a sink that mutates a validated field
	// and then throws cannot hand the fallback a record it can no longer render.
	Object.freeze(record);
	const sink = globalThis[OPERATIONAL_EVENT_SINK];
	if (typeof sink !== 'function') {
		defaultOperationalEventSink(record);
		return record;
	}
	try {
		const result = sink(record);
		if (result && typeof result.then === 'function') {
			Promise.resolve(result).catch(() => sinkFailureFallback(record));
		}
	} catch {
		sinkFailureFallback(record);
	}
	return record;
}

/**
 * Replace every occurrence of each listed string with `<redacted>`. Every
 * match of every value is marked first and each marked run is replaced once,
 * so values that overlap leave no fragment of one another behind. A string
 * passed on its own counts as one value.
 *
 * @param {string} text
 * @param {Iterable<unknown> | string} values
 */
function redactValues(text, values) {
	/** @type {Uint8Array | null} */
	let marked = null;
	for (const value of typeof values === 'string' ? [values] : values) {
		if (typeof value !== 'string' || value.length === 0) continue;
		let covered = 0;
		for (let at = text.indexOf(value); at !== -1; at = text.indexOf(value, at + 1)) {
			marked ??= new Uint8Array(text.length);
			const end = at + value.length;
			if (end > covered) {
				marked.fill(1, Math.max(at, covered), end);
				covered = end;
			}
		}
	}
	if (marked === null) return text;
	let out = '';
	let i = 0;
	while (i < text.length) {
		const start = i;
		if (marked[i] === 0) {
			while (i < text.length && marked[i] === 0) i++;
			out += text.slice(start, i);
		} else {
			while (i < text.length && marked[i] === 1) i++;
			out += '<redacted>';
		}
	}
	return out;
}

// ioredis reports a reply that matches no queued command as "Command queue
// state error. ... Last reply: <the reply>" (or "Last error: <its text>"),
// and the reply is whatever the server sent: after a slot migration the
// server-initiated sunsubscribe naming a shard channel, a late pub/sub message
// with its channel and payload, or any command's data. Only a pub/sub reply
// kind, or an error's capitalized code, is kept.
const UNMATCHED_REPLY = /^(Command queue state error\. If you can reproduce this, please report it\. Last (reply|error): )([\s\S]*)$/;
const PUSH_REPLY_KINDS = new Set(['message', 'pmessage', 'smessage', 'subscribe', 'psubscribe', 'ssubscribe', 'unsubscribe', 'punsubscribe', 'sunsubscribe', 'pong']);
// A command the server does not know is answered with "ERR unknown command
// '<name>', with args beginning with: '<arg>' ..." (backticks before Redis 7),
// the one Redis and Valkey reply that repeats a command's arguments (keys,
// channels). They are cut.
const UNKNOWN_COMMAND_ARGS = /^((?:ERR )?unknown command ['`][^'`]*['`], with args beginning with: )[\s\S]*$/;

/**
 * Cut the parts of a backend error that repeat what was sent or received.
 *
 * @param {string} message
 */
function cutEchoedData(message) {
	const unmatched = UNMATCHED_REPLY.exec(message);
	if (unmatched) {
		const [, head, which, rest] = unmatched;
		const kind = which === 'reply' ? /^[a-z]+/.exec(rest)?.[0] : /^[A-Z]+\b/.exec(rest)?.[0];
		if (kind === undefined || (which === 'reply' && !PUSH_REPLY_KINDS.has(kind))) return head + '<redacted>';
		return rest.length > kind.length ? head + kind + ' <redacted>' : head + kind;
	}
	return message.replace(UNKNOWN_COMMAND_ARGS, '$1<redacted>');
}

/**
 * diagnosticError's projection with the message cleaned before it is capped.
 * Capping first could cut a connection string in two and leave a password
 * fragment the URL redaction no longer recognizes, so the whole message is
 * cleaned and only then cut to the projection's length. The echo cuts go
 * first (they drop the bulk of an oversized message), then the listed values,
 * so one that contains a connection string still matches exactly.
 *
 * @param {unknown} error
 * @param {Iterable<unknown> | string | true | undefined} redact
 */
function redactedError(error, redact) {
	const projected = diagnosticError(error);
	if (redact === true) return { ...projected, message: '<redacted>' };
	let message = projected.message;
	try { message = String(/** @type {any} */ (error)?.message ?? error); } catch { /* hostile toString: keep the projection's text */ }
	message = cutEchoedData(message);
	if (redact) message = redactValues(message, redact);
	message = redactConnectionUrl(message);
	return { ...projected, message: message.slice(0, MAX_TEXT) };
}

/**
 * Emit a registry-indexed failure. The registry entry is the one place the
 * component, event, severity, and message are stated, so the emitted line and
 * the documented search key cannot drift apart. A caught error is projected to
 * name, code, and message under `attributes.error`.
 *
 * The echoes Redis and ioredis produce themselves (an unknown command's
 * arguments, a reply ioredis could not match) are cut for every caller.
 * `redact` lists further exact strings to remove: a channel or key built from
 * a topic or a client identity, for a server or a proxy in front of it that
 * repeats one in an error. `redact: true` drops the message and keeps the name
 * and code, for errors whose text can quote data no caller can enumerate.
 *
 * @param {string} id - An EXTENSIONS_ERROR_IDS value whose entry is a diagnostic emission.
 * @param {{ error?: unknown, redact?: Iterable<unknown> | string | true, detail?: string, attributes?: Record<string, unknown>, dataClass?: string }} [options]
 */
export function emitRegisteredFailure(id, options = {}) {
	const definition = extensionsErrorDefinition(id);
	if (definition.emission !== 'direct') throw new TypeError('Extensions error id is not a diagnostic emission: ' + id);
	const hasError = Object.prototype.hasOwnProperty.call(options, 'error');
	let attributes = options.attributes ?? null;
	if (hasError) {
		// Backend errors embed connection strings in some failure modes (auth
		// failed, host unreachable, TLS mismatch); redact every URL in the
		// projected message so a password never reaches a sink or the console.
		attributes = { ...(options.attributes || {}), error: redactedError(options.error, options.redact) };
	}
	return emitOperationalEvent({
		source: PACKAGE_SOURCE,
		component: definition.component,
		event: definition.event,
		severity: definition.severity,
		dataClass: options.dataClass ?? definition.dataClass ?? (hasError ? 'application' : 'operational'),
		message: definition.problemPrefix + (options.detail ?? ''),
		attributes
	});
}
