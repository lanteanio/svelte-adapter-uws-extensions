import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'acorn';
import { TELEMETRY_LEVELS } from '../src/shared/diagnostic.js';
import { EXTENSIONS_ERROR_IDS, EXTENSIONS_ERROR_REGISTRY } from '../src/error-registry.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const outputPath = join(root, 'docs', 'errors.md');
const DIAGNOSTIC_PREFIX = 'lantean/diagnostic';

// The emitted-event inventory is derived from the runtime source at generation
// time, so a new diagnostic emission makes --check fail until --write reruns.
// The registry module is excluded from the scan: its event fields declare the
// index itself, and counting them as emissions would make the ghost-entry
// validation below vacuous.
const EVENT_SCAN_DIRECTORIES = Object.freeze(['src']);
const EVENT_SCAN_EXCLUDED = Object.freeze(['src/error-registry.js']);
// A diagnostic event is a dot-name; the one-segment `event: 'created'` of a
// bus envelope literal is not one and must not enter the inventory.
const EVENT_LITERAL = /event: '([a-z][a-z0-9-]*(?:\.[a-z0-9-]+)+)'/;
const EVENT_CONTEXT_LINES = 6;

const FAILURE_SEVERITIES = new Set(['fatal', 'error', 'warn']);
const INFORMATIONAL_SEVERITIES = new Set(['debug', 'info']);

function eventScanFiles() {
	const files = [];
	const walk = (relativeDirectory) => {
		for (const entry of readdirSync(join(root, relativeDirectory), { withFileTypes: true })) {
			const relativePath = relativeDirectory + '/' + entry.name;
			if (entry.isDirectory()) walk(relativePath);
			else if (/\.(?:js|mjs|cjs)$/.test(entry.name)) files.push(relativePath);
		}
	};
	for (const directory of EVENT_SCAN_DIRECTORIES) walk(directory);
	return files.filter((file) => !EVENT_SCAN_EXCLUDED.includes(file)).sort();
}

/** Validate literal diagnostic severities without treating comments as code. */
export function auditDiagnosticSeverities(source, file = '<source>') {
	const errors = [];
	const allowed = new Set(TELEMETRY_LEVELS);
	const pending = [parse(source, { ecmaVersion: 'latest', sourceType: 'module', locations: true })];
	while (pending.length) {
		const node = pending.pop();
		if (node.type === 'Property' && node.kind === 'init') {
			const key = node.computed || node.key.type === 'Literal' ? node.key.value : node.key.name;
			const value = node.value;
			if (key === 'severity' && value.type === 'Literal' && typeof value.value === 'string' && !allowed.has(value.value)) {
				errors.push(`${file}:${value.loc.start.line}: invalid diagnostic severity ${JSON.stringify(value.value)}`);
			}
		}
		for (const value of Object.values(node)) {
			if (Array.isArray(value)) {
				for (const child of value) if (child && typeof child.type === 'string') pending.push(child);
			} else if (value && typeof value.type === 'string') pending.push(value);
		}
	}
	return errors.sort();
}

export function auditRuntimeDiagnosticSeverities() {
	return [...eventScanFiles(), 'src/error-registry.js'].flatMap((file) =>
		auditDiagnosticSeverities(readFileSync(join(root, file), 'utf8'), file));
}

/**
 * Every diagnostic event the runtime emits as a literal, with the component
 * and severity read from the surrounding emission when they are literals too.
 * Also collects which EXTENSIONS_ERROR_IDS keys the runtime references,
 * because registry-backed emissions go through emitRegisteredFailure(),
 * extensionsError(), and extensionsConsoleLine() rather than event literals.
 *
 * @returns {{ events: { event: string, components: string[], severities: string[], sources: string[] }[], idKeyReferences: Set<string> }}
 */
export function scanEmittedEvents() {
	const byEvent = new Map();
	const idKeyReferences = new Set();
	for (const file of eventScanFiles()) {
		const content = readFileSync(join(root, file), 'utf8');
		for (const reference of content.matchAll(/EXTENSIONS_ERROR_IDS\.([A-Z0-9_]+)/g)) {
			idKeyReferences.add(reference[1]);
		}
		const lines = content.split(/\r?\n/);
		for (let index = 0; index < lines.length; index++) {
			const match = EVENT_LITERAL.exec(lines[index]);
			if (!match) continue;
			let record = byEvent.get(match[1]);
			if (!record) {
				record = { event: match[1], components: new Set(), severities: new Set(), sources: new Set() };
				byEvent.set(match[1], record);
			}
			record.sources.add(file);
			const first = Math.max(0, index - EVENT_CONTEXT_LINES);
			const last = Math.min(lines.length - 1, index + EVENT_CONTEXT_LINES);
			for (let context = first; context <= last; context++) {
				const component = /component: '([a-z][a-z0-9.-]*)'/.exec(lines[context]);
				if (component) record.components.add(component[1]);
				const severity = /(?:severity|level): '([a-z]+)'/.exec(lines[context]);
				if (severity) record.severities.add(severity[1]);
			}
		}
	}
	const events = [...byEvent.values()]
		.map((record) => ({
			event: record.event,
			components: [...record.components].sort(),
			severities: [...record.severities].sort(),
			sources: [...record.sources].sort()
		}))
		.sort((a, b) => (a.event < b.event ? -1 : a.event > b.event ? 1 : 0));
	return { events, idKeyReferences };
}

/**
 * A registry entry nothing in the runtime emits is a ghost: its documentation
 * promises text that can never appear in a log. An entry counts as emitted
 * when its event is in the scanned inventory, when the runtime references its
 * EXTENSIONS_ERROR_IDS key, or when one of its declared source files carries
 * the event, the stable ID, or the key reference.
 *
 * @param {typeof EXTENSIONS_ERROR_REGISTRY} entries
 * @param {ReturnType<typeof scanEmittedEvents>} scan
 * @returns {string[]}
 */
export function findGhostRegistryEntries(entries = EXTENSIONS_ERROR_REGISTRY, scan = scanEmittedEvents()) {
	const errors = [];
	const keyById = new Map(Object.entries(EXTENSIONS_ERROR_IDS).map(([key, value]) => [value, key]));
	const scannedEvents = new Set(scan.events.map((record) => record.event));
	for (const entry of entries) {
		if (scannedEvents.has(entry.event)) continue;
		const key = keyById.get(entry.id);
		if (key && scan.idKeyReferences.has(key)) continue;
		let referenced = false;
		for (const source of entry.sources || []) {
			const sourcePath = resolve(root, source);
			if (isAbsolute(source) || relative(root, sourcePath).startsWith('..') || !existsSync(sourcePath)) continue;
			const content = readFileSync(sourcePath, 'utf8');
			if (content.includes("'" + entry.event + "'") ||
				content.includes(entry.id) ||
				(key && content.includes('EXTENSIONS_ERROR_IDS.' + key))) {
				referenced = true;
				break;
			}
		}
		if (!referenced) {
			errors.push((entry.id || 'registry entry') + ': event ' + entry.event +
				' is not emitted by any scanned runtime source and is not referenced from its declared sources');
		}
	}
	return errors;
}

/**
 * An emitted event whose severity is a failure band must be indexed. Events
 * emitted only at informational severities are exempt; promoting one to a
 * warning or an error fails generation until an entry exists.
 *
 * @param {ReturnType<typeof scanEmittedEvents>} scan
 * @param {typeof EXTENSIONS_ERROR_REGISTRY} entries
 * @returns {string[]}
 */
export function findUnindexedFailures(scan = scanEmittedEvents(), entries = EXTENSIONS_ERROR_REGISTRY) {
	const indexed = new Set(entries.map((entry) => entry.event));
	const unindexed = [];
	for (const record of scan.events) {
		if (indexed.has(record.event)) continue;
		const informational = record.severities.length > 0 &&
			record.severities.every((severity) => INFORMATIONAL_SEVERITIES.has(severity));
		if (!informational) unindexed.push(record);
	}
	return unindexed.map((record) => record.event + ' (severity ' +
		(record.severities.join('/') || 'not a literal') + ', emitted by ' + record.sources.join(', ') +
		') is a failure with no entry in EXTENSIONS_ERROR_REGISTRY; add one with cause, consequence,' +
		' automatic recovery, and next action, then rerun --write');
}

export function validateErrorRegistry(entries) {
	const errors = [];
	const ids = new Set();
	const anchors = new Set();
	const prefixes = new Set();
	const events = new Set();
	for (const [index, entry] of entries.entries()) {
		const label = entry.id || 'entry ' + (index + 1);
		if (!/^EXTENSIONS-ERR-[A-Z0-9-]+$/.test(entry.id || '')) errors.push(label + ': invalid stable id');
		if (ids.has(entry.id)) errors.push(label + ': duplicate stable id');
		ids.add(entry.id);
		if (!/^extensions-err-[a-z0-9-]+$/.test(entry.anchor || '')) errors.push(label + ': invalid anchor');
		if (anchors.has(entry.anchor)) errors.push(label + ': duplicate anchor');
		anchors.add(entry.anchor);
		if (prefixes.has(entry.messagePrefix)) errors.push(label + ': duplicate messagePrefix');
		prefixes.add(entry.messagePrefix);
		if (events.has(entry.event)) errors.push(label + ': duplicate event');
		events.add(entry.event);
		for (const field of ['event', 'messagePrefix', 'cause', 'consequence', 'automaticRecovery', 'nextAction', 'help']) {
			if (typeof entry[field] !== 'string' || !entry[field].trim()) errors.push(label + ': missing ' + field);
		}
		if (!/^[a-z][a-z0-9-]*(?:\.[a-z0-9-]+)+$/.test(entry.event || '')) errors.push(label + ': event is not a dot-name');
		// The emission shapes produce different lines. Validating the declared
		// prefix against the shape is what stops the reference promising text no
		// log will ever contain.
		const head = '[' + DIAGNOSTIC_PREFIX + ' source=svelte-adapter-uws-extensions component=' + entry.component +
			' event=' + entry.event + ' severity=';
		if (entry.emission === 'thrown') {
			if (entry.component !== null || entry.severity !== null || entry.problemPrefix !== null) {
				errors.push(label + ': a thrown entry carries no component, severity, or problemPrefix');
			}
		} else if (entry.emission === 'direct') {
			if (typeof entry.component !== 'string' || !/^[a-z][a-z0-9-]*(?:\.[a-z0-9-]+)+$/.test(entry.component) ||
				typeof entry.problemPrefix !== 'string' || !entry.problemPrefix) {
				errors.push(label + ': a direct entry needs a dot-name component and a problemPrefix');
			}
			if (!FAILURE_SEVERITIES.has(entry.severity)) errors.push(label + ': invalid operational severity');
			if (entry.messagePrefix !== head + entry.severity + '] ' + entry.problemPrefix) {
				errors.push(label + ': direct prefix does not match component, event, severity, and problemPrefix');
			}
		} else if (entry.emission === 'head') {
			if (typeof entry.component !== 'string' || entry.severity !== null || entry.problemPrefix !== null) {
				errors.push(label + ': a head entry carries a component but no fixed severity or problemPrefix');
			}
			if (entry.messagePrefix !== head) errors.push(label + ': head prefix does not stop where the variation begins');
		} else if (entry.emission === 'console') {
			// A console entry's prefix is the literal line beginning, so it must
			// not claim the diagnostic head it never carries, it must state a
			// real failure severity even though no event pipeline enforces one,
			// and it must open with the package's family tag.
			if (entry.component !== null || entry.problemPrefix !== null) {
				errors.push(label + ': a console entry carries no component or problemPrefix');
			}
			if (!FAILURE_SEVERITIES.has(entry.severity)) errors.push(label + ': invalid operational severity');
			if (entry.messagePrefix.startsWith('[' + DIAGNOSTIC_PREFIX)) {
				errors.push(label + ': a console prefix must not claim the diagnostic line head');
			}
			if (!/^\[extensions\] /.test(entry.messagePrefix)) {
				errors.push(label + ': a console prefix must open with the [extensions] family tag');
			}
		} else {
			errors.push(label + ': invalid emission');
		}
		if (entry.help !== 'docs/errors.md#' + entry.anchor) errors.push(label + ': help route does not match anchor');
		if (entry.link !== null && !/^https:\/\/svti\.me\/[a-z0-9-]+$/.test(entry.link)) errors.push(label + ': invalid link');
		if (!Array.isArray(entry.sources) || entry.sources.length === 0) {
			errors.push(label + ': missing sources');
		} else {
			for (const source of entry.sources) {
				const sourcePath = resolve(root, source);
				if (isAbsolute(source) || relative(root, sourcePath).startsWith('..') || !existsSync(sourcePath)) {
					errors.push(label + ': missing or out-of-root source: ' + source);
				}
			}
		}
		if (entry.code !== null && (typeof entry.code !== 'string' || !entry.code)) errors.push(label + ': invalid code');
	}
	return errors;
}

function cell(value) {
	return String(value).replace(/\|/g, '\\|').replace(/\r?\n/g, ' ');
}

/**
 * An inline code span that renders `value` exactly: the fence is one backtick
 * longer than the longest run inside it, and a value that starts or ends with a
 * backtick, or starts and ends with a space, is padded with one space each
 * side - the one case CommonMark strips a space, which it then strips again.
 * @param {string} value
 */
function codeSpan(value) {
	const text = String(value);
	let longest = 0;
	for (const run of text.match(/`+/g) || []) longest = Math.max(longest, run.length);
	const fence = '`'.repeat(longest + 1);
	const pad = /^`|`$/.test(text) || (/^ /.test(text) && / $/.test(text)) ? ' ' : '';
	return fence + pad + text + pad + fence;
}

export function renderErrorReference(entries = EXTENSIONS_ERROR_REGISTRY, options = {}) {
	const scan = options.scan || scanEmittedEvents();
	const errors = [
		...validateErrorRegistry(entries),
		...findGhostRegistryEntries(entries, scan),
		...findUnindexedFailures(scan, entries)
	];
	if (errors.length) throw new Error(errors.join('\n'));
	const indexedEvents = new Set(entries.map((entry) => entry.event));
	const unindexed = scan.events.filter((record) => !indexedEvents.has(record.event));
	const scannedNames = new Set(scan.events.map((record) => record.event));
	const thrownEntries = entries.filter((entry) => entry.emission === 'thrown');
	const diagnosticEntries = entries.filter((entry) => entry.emission === 'direct' || entry.emission === 'head');
	const consoleEntries = entries.filter((entry) => entry.emission === 'console');
	const emittedCount = scan.events.length + diagnosticEntries.filter((entry) => !scannedNames.has(entry.event)).length;
	const lines = [
		'# Error reference',
		'',
		'Search this page with the exact stable ID, event, or beginning of the line you saw.',
		'Three kinds of failure are indexed here, each with its cause, what it means for traffic,',
		'whether anything recovers on its own, and what to do next:',
		'',
		'- ' + thrownEntries.length + ' thrown errors. The Error message starts with the indexed prefix and ends with the',
		'  stable ID tag and this document\'s route.',
		'- ' + diagnosticEntries.length + ' diagnostic events. Each is one canonical line',
		'  `[' + DIAGNOSTIC_PREFIX + ' source=svelte-adapter-uws-extensions component=<component> event=<event> severity=<severity>] <message> {json}`,',
		'  the same grammar the adapter prints, so `parseDiagnostic()` from `svelte-adapter-uws/observability`',
		'  parses it and a sink installed through either package receives the record. The indexed prefix is',
		'  that line up to and including the invariant part of the message.',
		'- ' + consoleEntries.length + ' plain console lines, printed through the registry so each carries its stable ID tag',
		'  and the printed text cannot drift from the indexed prefix.',
		'',
		'The diagnostic inventory is derived at generation time by scanning `src/` for emitted events: a',
		'failure event cannot be added to the runtime without an entry here, because the generator fails',
		'the build until one exists, and an entry whose event nothing emits fails it too. Plain console',
		'output is held to the same rule from the other side: `scripts/check-console-index.js` walks every',
		'`console.warn` and `console.error` under `src/` and fails the build unless the line prints through',
		'this registry or carries a recorded reason an operator who read it needs nothing more.',
		'',
		'The adapter ships its own reference for its runtime at',
		'[svelte-adapter-uws docs/errors.md](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/errors.md).',
		'',
		'| Stable ID | Code or event | Searchable message prefix |',
		'|---|---|---|'
	];
	for (const entry of entries) {
		lines.push('| [' + entry.id + '](#' + entry.anchor + ') | `' +
			cell(entry.code || entry.event) + '` | ' + codeSpan(cell(entry.messagePrefix)) + ' |');
	}
	lines.push(
		'',
		'## Emitted diagnostic event coverage',
		'',
		'The runtime emits ' + emittedCount + ' distinct diagnostic events. ' + (unindexed.length === 0
			? 'Every one of them is indexed above with a stable ID and full operator guidance.'
			: 'The ' + diagnosticEntries.length + ' indexed above carry stable IDs and full operator guidance; the remaining ' +
				unindexed.length + ' are informational.'),
		'The split is enforced by severity rather than by a list: an emitted event is exempt from the',
		'indexed reference only while every severity it is emitted at is informational, so promoting one',
		'to a warning or an error fails generation until it is indexed.',
		'',
		'Indexed events:',
		''
	);
	for (const entry of diagnosticEntries) {
		lines.push('- `' + entry.event + '` - [' + entry.id + '](#' + entry.anchor + ')');
	}
	lines.push(
		'',
		'Indexed console lines (no diagnostic event; the searchable key is the printed prefix and',
		'the stable ID tag on the line):',
		''
	);
	for (const entry of consoleEntries) {
		lines.push('- ' + codeSpan(cell(entry.messagePrefix)) + ' - [' + entry.id + '](#' + entry.anchor + ')');
	}
	if (unindexed.length > 0) {
		lines.push(
			'',
			'### Informational events',
			'',
			'These carry no stable ID and no recovery guidance because they report normal operation',
			'rather than a failure.',
			'',
			'| Event | Component | Severity | Emitting sources |',
			'|---|---|---|---|'
		);
		for (const record of unindexed) {
			lines.push(
				'| `' + record.event + '` | ' +
				(record.components.length ? record.components.map((component) => '`' + component + '`').join(', ') : 'varies by call site') + ' | ' +
				(record.severities.length ? record.severities.join(', ') : 'varies by call site') + ' | ' +
				record.sources.map((source) => '[' + source + '](../' + source + ')').join(', ') + ' |'
			);
		}
	}
	for (const entry of entries) {
		lines.push(
			'',
			'<a id="' + entry.anchor + '"></a>',
			'## `' + entry.id + '`',
			'',
			'- **Code/event:** `' + (entry.code || entry.event) + '`',
			'- **Message prefix:** ' + codeSpan(entry.messagePrefix),
			'- **Cause:** ' + entry.cause,
			'- **Consequence:** ' + entry.consequence,
			'- **Automatic recovery:** ' + entry.automaticRecovery,
			'- **Next action:** ' + entry.nextAction,
			...(entry.link ? ['- **Operator shortlink:** `' + entry.link + '`'] : []),
			'- **Runtime help:** `' + entry.help + '`',
			'- **Runtime sources:** ' + entry.sources.map((source) => '[' + source + '](../' + source + ')').join(', ')
		);
	}
	return lines.join('\n') + '\n';
}

/**
 * Content comparison, independent of line endings: a CRLF working copy of the
 * committed LF document is not stale.
 */
export function errorReferenceMatches(onDisk, generated) {
	return onDisk.replace(/\r\n/g, '\n') === generated;
}

export function checkErrorReference({ write = false } = {}) {
	const severityErrors = auditRuntimeDiagnosticSeverities();
	if (severityErrors.length) throw new Error(severityErrors.join('\n'));
	const expected = renderErrorReference();
	if (write) writeFileSync(outputPath, expected);
	const actual = existsSync(outputPath) ? readFileSync(outputPath, 'utf8') : '';
	if (!errorReferenceMatches(actual, expected)) throw new Error('docs/errors.md is stale; run node scripts/generate-error-reference.js --write');
	console.log('generate-error-reference: ' + EXTENSIONS_ERROR_REGISTRY.length + ' stable extension errors match docs/errors.md');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	try {
		checkErrorReference({ write: process.argv.includes('--write') });
	} catch (error) {
		console.error('generate-error-reference FAILED: ' + (error instanceof Error ? error.message : String(error)));
		process.exitCode = 1;
	}
}
