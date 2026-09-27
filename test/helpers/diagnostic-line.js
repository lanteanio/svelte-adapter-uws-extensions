import { expect } from 'vitest';
import { parseDiagnostic } from 'svelte-adapter-uws/observability';
import { EXTENSIONS_ERROR_REGISTRY, extensionsErrorDefinition } from '../../src/error-registry.js';
import { setOperationalEventSink } from '../../src/shared/diagnostic.js';

const ID_BY_EVENT = new Map(EXTENSIONS_ERROR_REGISTRY.map((entry) => [entry.event, entry.id]));

/**
 * Capture every operational record emitted until `stop()` is called, through
 * the same process-wide sink an application installs. `ids()` names each
 * record by its registry entry.
 */
export function captureRegistered() {
	/** @type {any[]} */
	const records = [];
	const stop = setOperationalEventSink((record) => { records.push(record); });
	return {
		records,
		ids: () => records.map((record) => ID_BY_EVENT.get(record.event) ?? record.event),
		stop
	};
}

/**
 * Assert that a printed console line is the canonical diagnostic for one
 * registry entry, and return its record.
 *
 * The line must start with the entry's documented prefix (the search key in
 * docs/errors.md) and must parse with the adapter's own reader, which is what
 * an operator's log pipeline runs over it.
 *
 * @param {unknown} line
 * @param {string} id - an EXTENSIONS_ERROR_IDS value
 */
export function registeredRecord(line, id) {
	const entry = extensionsErrorDefinition(id);
	expect(typeof line).toBe('string');
	const text = /** @type {string} */ (line);
	expect(text.startsWith(entry.messagePrefix), text).toBe(true);
	const parsed = parseDiagnostic(text);
	expect(parsed?.format).toBe('canonical');
	expect(parsed.record).toMatchObject({
		source: 'svelte-adapter-uws-extensions',
		component: entry.component,
		event: entry.event,
		severity: entry.severity
	});
	return parsed.record;
}
