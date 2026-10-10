import { readFileSync } from 'node:fs';
import { afterEach, describe, expect, it } from 'vitest';
import { mockRedisClient } from './helpers/mock-redis.js';
import { mockPlatform } from './helpers/mock-platform.js';
import { mockWs } from './helpers/mock-ws.js';
import { createConnectionRegistry } from '../src/redis/registry.js';
import { createGroup } from '../src/redis/groups.js';
import { CircuitBrokenError } from '../src/shared/breaker.js';
import { setOperationalEventSink, emitRegisteredFailure, TELEMETRY_LEVELS } from '../src/shared/diagnostic.js';
import {
	EXTENSIONS_ERROR_IDS,
	EXTENSIONS_ERROR_REGISTRY,
	extensionsConsoleLine,
	extensionsError,
	extensionsErrorDefinition
} from '../src/error-registry.js';
import {
	auditDiagnosticSeverities,
	auditRuntimeDiagnosticSeverities,
	errorReferenceMatches,
	findGhostRegistryEntries,
	findUnindexedFailures,
	renderErrorReference,
	scanEmittedEvents,
	validateErrorRegistry
} from '../scripts/generate-error-reference.js';
import { auditConsoleFailures, auditRuntimeConsoleFailures, staleExclusions } from '../scripts/check-console-index.js';

const read = (relative) => readFileSync(new URL('../' + relative, import.meta.url), 'utf8');
const keyById = new Map(Object.entries(EXTENSIONS_ERROR_IDS).map(([key, id]) => [id, key]));
let registry;
let group;

afterEach(async () => {
	setOperationalEventSink(null);
	await registry?.destroy();
	registry = undefined;
	group?.destroy();
	group = undefined;
});

describe('generated extensions error reference', () => {
	const scan = scanEmittedEvents();

	it('is valid, complete, and generated from the registry', () => {
		expect(validateErrorRegistry(EXTENSIONS_ERROR_REGISTRY)).toEqual([]);
		expect(findGhostRegistryEntries(EXTENSIONS_ERROR_REGISTRY, scan)).toEqual([]);
		expect(findUnindexedFailures(scan, EXTENSIONS_ERROR_REGISTRY)).toEqual([]);
		const rendered = renderErrorReference(EXTENSIONS_ERROR_REGISTRY, { scan });
		expect(errorReferenceMatches(read('docs/errors.md'), rendered)).toBe(true);
		for (const entry of EXTENSIONS_ERROR_REGISTRY) {
			expect(rendered).toContain('<a id="' + entry.anchor + '"></a>');
			expect(rendered).toContain(entry.help);
		}
	});

	it('wires every entry through the registry helper that produces its emission kind', () => {
		// A direct emission derives component, event, severity, and message from
		// the entry inside emitRegisteredFailure(), so proving the site calls the
		// helper with the entry's key proves the emitted line matches the index.
		for (const entry of EXTENSIONS_ERROR_REGISTRY) {
			const key = keyById.get(entry.id);
			const sources = entry.sources.map(read).join('\n');
			// A direct entry is emitted either through emitRegisteredFailure() or,
			// where the emission must not travel through the sink it reports on,
			// by reading the definition and building the record from it.
			const helper = entry.emission === 'direct' ? '(?:emitRegisteredFailure|extensionsErrorDefinition)'
				: entry.emission === 'thrown' ? 'extensionsError'
					: entry.emission === 'console' ? 'extensionsConsoleLine'
						: null;
			const expected = helper === null
				? new RegExp("event: '" + entry.event.replace(/[.]/g, '[.]') + "'")
				: new RegExp(helper + '[(]\\s*EXTENSIONS_ERROR_IDS[.]' + key + '\\b');
			expect(expected.test(sources), `${entry.id} via ${expected}`).toBe(true);
		}
	});

	it('fails generation for a ghost entry and for an unindexed failure event', () => {
		const ghost = {
			...EXTENSIONS_ERROR_REGISTRY.find((entry) => entry.emission === 'direct'),
			id: 'EXTENSIONS-ERR-GHOST',
			event: 'redis.ghost.failed',
			anchor: 'extensions-err-ghost',
			help: 'docs/errors.md#extensions-err-ghost',
			messagePrefix: 'ghost',
			sources: ['src/redis/lock.js']
		};
		expect(findGhostRegistryEntries([ghost], scan)).toEqual([
			expect.stringContaining('EXTENSIONS-ERR-GHOST: event redis.ghost.failed is not emitted')
		]);
		const failure = { event: 'redis.synthetic.failed', components: ['runtime.redis'], severities: ['error'], sources: ['src/redis/synthetic.js'] };
		const informational = { event: 'redis.synthetic.started', components: ['runtime.redis'], severities: ['info'], sources: ['src/redis/synthetic.js'] };
		expect(findUnindexedFailures({ events: [failure, informational], idKeyReferences: new Set() }, EXTENSIONS_ERROR_REGISTRY)).toEqual([
			expect.stringContaining('redis.synthetic.failed (severity error')
		]);
		expect(validateErrorRegistry([{
			...EXTENSIONS_ERROR_REGISTRY[0],
			id: 'EXTENSIONS-ERR-MISSING-SOURCE',
			anchor: 'extensions-err-missing-source',
			help: 'docs/errors.md#extensions-err-missing-source',
			messagePrefix: 'missing source',
			event: 'registry.missing.source',
			sources: ['src/not-present.js']
		}])).toContain('EXTENSIONS-ERR-MISSING-SOURCE: missing or out-of-root source: src/not-present.js');
	});

	it('emits the circuit-open failure with a stable id and help route', () => {
		const error = new CircuitBrokenError();
		expect(error.message).toBe(extensionsError(EXTENSIONS_ERROR_IDS.CIRCUIT_OPEN));
		expect(error.message).toContain('[' + EXTENSIONS_ERROR_IDS.CIRCUIT_OPEN + ']');
		expect(error.message).toContain('docs/errors.md#extensions-err-circuit-open');
	});

	it('renders console lines from the registry with the stable id tag', () => {
		// A prefix that takes a detail ends in its own separator, as the
		// adapter's do, so the helper inserts nothing between the two.
		const collapse = extensionsErrorDefinition(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RENDER_COLLAPSE);
		expect(collapse.messagePrefix.endsWith(': ')).toBe(true);
		expect(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RENDER_COLLAPSE, 'redis.x.failed'))
			.toBe(collapse.messagePrefix + 'redis.x.failed [' + collapse.id + ']');
		const shape = extensionsErrorDefinition(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RECORD_SHAPE);
		expect(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RECORD_SHAPE))
			.toBe(shape.messagePrefix + ' [' + shape.id + ']');
		expect(() => extensionsConsoleLine(EXTENSIONS_ERROR_IDS.CIRCUIT_OPEN)).toThrow(/not console-emitted/);
		expect(() => extensionsError(EXTENSIONS_ERROR_IDS.GROUPS_SUBSCRIBER)).toThrow(/not a thrown failure/);
	});

	it('emits registry prefixes through real request validation and lookup paths', async () => {
		registry = createConnectionRegistry(mockRedisClient('errors:'), {
			identify: () => 'user',
			heartbeat: 60000
		});
		await expect(registry.request('', 'event')).rejects.toThrow(
			extensionsError('EXTENSIONS-ERR-REGISTRY-REQUEST-TARGET')
		);
		await expect(registry.request('user', '')).rejects.toThrow(
			extensionsError('EXTENSIONS-ERR-REGISTRY-REQUEST-EVENT')
		);
		await expect(registry.request('offline-user', 'event')).rejects.toThrow(
			extensionsError('EXTENSIONS-ERR-REGISTRY-REQUEST-OFFLINE', 'offline-user" is offline')
		);
		await expect(registry.requestSession('', 'event')).rejects.toThrow(
			extensionsError('EXTENSIONS-ERR-REGISTRY-SESSION-TARGET')
		);
		await expect(registry.requestSession('session', '')).rejects.toThrow(
			extensionsError('EXTENSIONS-ERR-REGISTRY-SESSION-EVENT')
		);
		await expect(registry.requestSession('offline-session', 'event')).rejects.toThrow(
			extensionsError('EXTENSIONS-ERR-REGISTRY-SESSION-OFFLINE', 'offline-session" is offline')
		);
		await expect(registry.send('', 'topic', 'event')).rejects.toThrow(extensionsError('EXTENSIONS-ERR-REGISTRY-SEND-TARGET'));
		await expect(registry.sendCoalesced('', { key: 'k', topic: 'topic', event: 'event' })).rejects.toThrow(
			extensionsError('EXTENSIONS-ERR-REGISTRY-SEND-COALESCED-TARGET')
		);
	});

	it('a real subscriber failure reaches the sink as exactly the indexed record', async () => {
		const client = mockRedisClient('errors:');
		const duplicates = [];
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (...args) => {
			const dup = duplicate(...args);
			duplicates.push(dup);
			return dup;
		};
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		group = createGroup(client, 'lobby');
		await group.join(mockWs(), mockPlatform());
		expect(duplicates).toHaveLength(1);
		const onError = duplicates[0]._listeners.get('error');
		expect(typeof onError).toBe('function');
		onError(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }));

		const entry = extensionsErrorDefinition(EXTENSIONS_ERROR_IDS.GROUPS_SUBSCRIBER);
		expect(records).toHaveLength(1);
		expect(records[0]).toMatchObject({
			source: 'svelte-adapter-uws-extensions',
			component: entry.component,
			event: entry.event,
			severity: entry.severity,
			message: entry.problemPrefix,
			attributes: { group: 'lobby', error: { name: 'Error', code: 'ECONNRESET', message: 'read ECONNRESET' } }
		});
	});
});

// The reference can only describe lines that go through the registry, so the
// gate starts from the console call sites instead.
describe('console call-site gate', () => {
	const consoleEntry = EXTENSIONS_ERROR_REGISTRY.find((entry) => entry.emission === 'console');
	const consoleKey = keyById.get(consoleEntry.id);
	const consoleMethodFor = (entry) => entry.severity === 'warn' ? 'warn' : 'error';
	const declaredSite = consoleEntry.sources[0].replace(/^src\//, '');

	it('accounts for every console failure under src', () => {
		const { failures, indexed, modules } = auditRuntimeConsoleFailures();
		expect(failures).toEqual([]);
		// A tree that stopped being scanned would also report no failures.
		expect(modules).toBeGreaterThan(50);
		expect(indexed).toBeGreaterThan(0);
	});

	it('refuses a new console failure that is neither indexed nor recorded', () => {
		const source = "console.warn('[extensions] the widget lane collapsed:', err);\n";
		const { failures } = auditConsoleFailures(source, 'redis/widget.js', new Map());
		expect(failures).toHaveLength(1);
		expect(failures[0]).toContain('redis/widget.js:1');
		expect(failures[0]).toContain('the widget lane collapsed');
	});

	it('accepts a line printed through the registry, and one recorded with a reason', () => {
		const indexedSource = `console.${consoleMethodFor(consoleEntry)}(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.${consoleKey}), err);\n`;
		// At the file the entry declares as a source - the gate refuses the
		// same call anywhere the entry does not name.
		const result = auditConsoleFailures(indexedSource, declaredSite, new Map());
		expect(result.failures).toEqual([]);
		expect(result.indexed).toBe(1);

		const recorded = new Map([['redis/widget.js::[extensions] the widget lane collapsed:', 'narrates an indexed failure']]);
		const raw = "console.warn('[extensions] the widget lane collapsed:', err);\n";
		expect(auditConsoleFailures(raw, 'redis/widget.js', recorded).failures).toEqual([]);
	});

	// The call SHAPE is not the contract - the id is. extensionsConsoleLine
	// throws on an id it cannot resolve, and it is called from inside catch
	// blocks, so a dangling id replaces the failure being reported with a
	// TypeError that also skips whatever recovery the catch was performing.
	it('refuses an extensionsConsoleLine call whose id is not a console entry', () => {
		const dangling = 'console.error(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.WIDGET), err);\n';
		const { failures } = auditConsoleFailures(dangling, 'redis/widget.js', new Map());
		expect(failures).toHaveLength(1);
		expect(failures[0]).toContain('not a registry entry');

		// A real id whose emission is not `console` is refused the same way,
		// because extensionsConsoleLine refuses it at runtime too.
		const thrownEntry = EXTENSIONS_ERROR_REGISTRY.find((entry) => entry.emission !== 'console');
		const wrongEmission = `console.error(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.${keyById.get(thrownEntry.id)}), err);\n`;
		expect(auditConsoleFailures(wrongEmission, 'redis/widget.js', new Map()).failures[0])
			.toContain('rather than "console"');
	});

	// A warn-level line indexed as an error misfiles wherever severity routes,
	// and the reference then describes it at the wrong urgency.
	it('refuses a console method that disagrees with the entry severity', () => {
		const errorEntry = EXTENSIONS_ERROR_REGISTRY.find((entry) => entry.emission === 'console' && entry.severity === 'error');
		const source = `console.warn(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.${keyById.get(errorEntry.id)}));\n`;
		const { failures } = auditConsoleFailures(source, errorEntry.sources[0].replace(/^src\//, ''), new Map());
		expect(failures).toHaveLength(1);
		expect(failures[0]).toContain('severity');
	});

	// Every printer spelling the header claims to follow, including the
	// object-access forms, so each claim is exercised rather than asserted.
	it('follows every console spelling and binding the header claims', () => {
		const emit = "('[extensions] the widget lane collapsed:', err);";
		for (const source of [
			`console.error${emit}`,
			`console['error']${emit}`,
			`globalThis.console.error${emit}`,
			`globalThis['console'].error${emit}`,
			`const fail = console.error.bind(console);\nfail${emit}`,
			`const { error: fail } = console;\nfail${emit}`,
			`const c = console;\nc.error${emit}`,
			`let fail;\nfail = console.warn;\nfail${emit}`
		]) {
			const { failures } = auditConsoleFailures(source, 'redis/widget.js', new Map());
			expect(failures, source).toHaveLength(1);
		}
		// console.log is not a failure and must not be conscripted.
		expect(auditConsoleFailures(`console.log${emit}`, 'redis/widget.js', new Map()).failures).toEqual([]);
	});

	// An entry is checked against the call sites it names, and the prose is
	// written per site - so a site the entry does not name is one nobody
	// checked the prose against.
	it('refuses a call site the entry does not declare as a source', () => {
		const source = `console.${consoleMethodFor(consoleEntry)}(extensionsConsoleLine(EXTENSIONS_ERROR_IDS.${consoleKey}));\n`;
		const { failures } = auditConsoleFailures(source, 'redis/somewhere-else.js', new Map());
		expect(failures).toHaveLength(1);
		expect(failures[0]).toContain('sources do not name this file');
		expect(auditConsoleFailures(source, declaredSite, new Map()).failures).toEqual([]);
	});

	it('reports an exclusion that no longer names a file', () => {
		expect(staleExclusions()).toEqual([]);
	});

	// Writing to the stream directly skips every classification above.
	it('refuses a failure written straight to stderr', () => {
		const { failures } = auditConsoleFailures("process.stderr.write('[extensions] the lane collapsed\\n');\n", 'redis/widget.js', new Map());
		expect(failures).toHaveLength(1);
		expect(failures[0]).toContain('bypasses the console index');
	});

	// The decision is keyed on the text an operator would search for, so
	// rewording the line asks for the decision again instead of inheriting one
	// made about different words.
	it('does not let a reworded line inherit an earlier decision', () => {
		const recorded = new Map([['redis/widget.js::[extensions] the widget lane collapsed:', 'narrates an indexed failure']]);
		const reworded = "console.warn('[extensions] the widget lane failed:', err);\n";
		expect(auditConsoleFailures(reworded, 'redis/widget.js', recorded).failures).toHaveLength(1);
	});

	it('refuses a line with no invariant text of its own', () => {
		for (const source of [
			"console.warn('[extensions] ' + detail);\n",
			'console.error(`[redis pubsub] ${prefix}`);\n'
		]) {
			const { failures } = auditConsoleFailures(source, 'redis/widget.js', new Map());
			expect(failures, source).toHaveLength(1);
			expect(failures[0]).toContain('no invariant text');
		}
		// An interpolation in the MIDDLE is fine: the words around it are what
		// an operator searches for, and they are what the key carries.
		const interpolated = 'console.warn(`[extensions] the widget lane collapsed after ${ms}ms`);\n';
		const key = 'redis/widget.js::[extensions] the widget lane collapsed after {}ms';
		expect(auditConsoleFailures(interpolated, 'redis/widget.js', new Map([[key, 'recorded']])).failures).toEqual([]);

		// EVERY leading tag is stripped, not just the first - otherwise a
		// second tag reads as the line's own invariant words.
		const twoTags = "console.warn('[extensions] [redis/cursor] ' + detail);\n";
		expect(auditConsoleFailures(twoTags, 'redis/widget.js', new Map()).failures[0]).toContain('no invariant text');
	});

	// The key normalises whitespace runs, so it is not collision-proof and the
	// check that catches one is load-bearing.
	it('refuses two lines whose keys collide through whitespace', () => {
		const source = "console.warn('[extensions] the widget lane\\n collapsed:', a);\nconsole.warn('[extensions] the widget lane collapsed:', b);\n";
		const recorded = new Map([['redis/widget.js::[extensions] the widget lane collapsed:', 'judged once']]);
		const { failures } = auditConsoleFailures(source, 'redis/widget.js', recorded);
		expect(failures).toHaveLength(1);
		expect(failures[0]).toContain('the same key');
	});

	// A printer taken as an injectable default, so a test can capture its
	// output, is still console.error.
	it('sees a console printer bound under another name', () => {
		const emit = "log('[extensions] the relay lane collapsed:', err);";
		for (const source of [
			`const { log = console.error } = options;\n${emit}\n`,
			`const log = console.error;\n${emit}\n`,
			`function report(log = console.warn) {\n\t${emit}\n}\n`
		]) {
			const { failures } = auditConsoleFailures(source, 'redis/relay.js', new Map());
			expect(failures, source).toHaveLength(1);
			expect(failures[0]).toContain('alias');
		}
	});

	it('reports an allowlist entry that no longer matches anything', () => {
		const stale = new Map([['redis/gone.js::[extensions] a line that was deleted:', 'no longer emitted']]);
		const { failures } = auditRuntimeConsoleFailures(stale);
		expect(failures.some((f) => f.includes('stale UNINDEXED entry'))).toBe(true);
	});

	// console.log and friends are not failures - an informational line has
	// nothing to index.
	it('leaves informational console output alone', () => {
		const source = "console.log('[extensions] listening on ' + port);\nconsole.info('[extensions] ready');\n";
		const { failures } = auditConsoleFailures(source, 'redis/widget.js', new Map());
		expect(failures).toEqual([]);
	});
});


describe('diagnostic severity contract', () => {
	it('accepts only real telemetry levels at every literal emission and registry declaration', () => {
		expect(auditRuntimeDiagnosticSeverities()).toEqual([]);
		for (const severity of TELEMETRY_LEVELS) {
			expect(auditDiagnosticSeverities(`emit({ severity: '${severity}' });`)).toEqual([]);
		}
	});

	it('rejects syslog aliases and ignores documentation and unrelated level fields', () => {
		for (const severity of ['warning', 'critical', 'notice', 'err', 'trace']) {
			expect(auditDiagnosticSeverities(`emit({ severity: '${severity}' });`, 'fixture.js')).toEqual([
				`fixture.js:1: invalid diagnostic severity "${severity}"`
			]);
		}
		for (const literal of ['null', '3', 'false', '1n']) {
			expect(auditDiagnosticSeverities(`emit({ severity: ${literal} });`)).toHaveLength(1);
		}
		expect(auditDiagnosticSeverities('emit({ "severity": "warning" });')).toHaveLength(1);
		expect(auditDiagnosticSeverities('emit({ ["severity"]: "warning" });')).toHaveLength(1);
		expect(auditDiagnosticSeverities("/** severity: 'warning' */\nconst x = { level: 'siege' }; // severity: 'critical'\n")).toEqual([]);
	});

	it('delivers a registered warning to the operational sink', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		try {
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_EVENTS_OFF);
			expect(records).toHaveLength(1);
			expect(records[0]).toMatchObject({ severity: 'warn', event: 'redis.presence.keyspace-events.off' });
		} finally {
			setOperationalEventSink(null);
		}
	});
});
