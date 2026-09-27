import { afterEach, describe, expect, it, vi } from 'vitest';
import { createRequire } from 'node:module';
import * as lead from 'svelte-adapter-uws/observability';
import {
	DIAGNOSTIC_PREFIX,
	DIAGNOSTIC_SCHEMA_VERSION,
	createDiagnostic,
	diagnosticError,
	emitOperationalEvent,
	emitRegisteredFailure,
	formatDiagnostic,
	setOperationalEventSink
} from '../../src/shared/diagnostic.js';
import { EXTENSIONS_ERROR_IDS, extensionsErrorDefinition } from '../../src/error-registry.js';

// The extensions copy of the diagnostic contract must be indistinguishable
// from the adapter's for everything an operator, a collector, or a sink can
// observe: same prefix, same line grammar, same envelope, same validation, and
// the same process-wide sink registry. Each assertion below drives BOTH
// implementations and compares them, so a change on either side that breaks
// interoperability fails here rather than in a log pipeline.

const SAMPLE = {
	source: 'svelte-adapter-uws-extensions',
	component: 'runtime.redis.clock',
	event: 'redis.clock.sample.failed',
	severity: 'error',
	message: 'Clock sample failed.',
	occurredAt: '2026-01-01T00:00:00.000Z',
	attributes: { operation: 'sample', error: { name: 'Error', code: null, message: 'redis unavailable' } }
};

afterEach(() => {
	setOperationalEventSink(null);
	lead.setOperationalEventSink(null);
	vi.restoreAllMocks();
});

describe('diagnostic contract parity with the adapter', () => {
	it('spells the prefix and schema version the way the adapter does', () => {
		expect(DIAGNOSTIC_PREFIX).toBe(lead.DIAGNOSTIC_PREFIX);
		expect(DIAGNOSTIC_SCHEMA_VERSION).toBe(lead.DIAGNOSTIC_SCHEMA_VERSION);
	});

	it('renders byte-identical records and lines for the same input', () => {
		expect(createDiagnostic(SAMPLE)).toEqual(lead.createDiagnostic(SAMPLE));
		expect(formatDiagnostic(SAMPLE)).toBe(lead.formatDiagnostic(SAMPLE));
		const hostile = { ...SAMPLE, message: 'external=' + String.fromCodePoint(0, 0x85, 0x202e, 0x2066, 0x5e9), attributes: { name: 'a' + String.fromCodePoint(0x2028) + 'b' } };
		expect(formatDiagnostic(hostile)).toBe(lead.formatDiagnostic(hostile));
	});

	it('matches the adapter on every normalization edge: length cut, whitespace, value kinds, escaping', () => {
		const ch = (...codes) => String.fromCharCode(...codes);
		const vectors = [
			{ message: 'a'.repeat(511) },
			{ message: 'a'.repeat(512) },
			{ message: 'a'.repeat(513) },
			{ message: 'a'.repeat(600) },
			{ message: 'a'.repeat(510) + '  ' + 'b'.repeat(40) },
			{ message: 'a'.repeat(511) + ' ' + 'b'.repeat(40) },
			{ message: 'tabs\there\tand\nnewlines\r\nand   runs   of   spaces' },
			{ message: '   leading and trailing   ' },
			{ message: '' },
			{ message: '   ' },
			{ message: undefined },
			{ message: null },
			{ message: 42 },
			{ message: { toString() { return 'object message'; } } },
			{ message: 'quote " backslash \\ brace { bracket ]' },
			{ message: 'embedded {"schemaVersion":1} json' },
			{ message: 'separators ' + ch(0x2028, 0x2029) + ' bidi ' + ch(0x202e, 0x2066) + ' nel ' + ch(0x85) },
			{ message: 'emoji ' + ch(0xd83d, 0xde00) + ' lone ' + ch(0xd800) },
			{ attributes: undefined },
			{ attributes: {} },
			{ attributes: { n: 1, f: 1.5, t: true, z: null, s: 'x', nested: { a: [1, 2, { b: 'c' }] } } },
			{ attributes: { long: 'y'.repeat(5000) } },
			{ attributes: { text: 'line' + ch(0x2028) + 'break "q" \\ ' + ch(0x7f) } },
			{ dataClass: 'operational' },
			{ dataClass: 'pseudonymous' }
		];
		for (const vector of vectors) {
			const input = { ...SAMPLE, ...vector };
			const label = JSON.stringify(vector, (_k, v) => (typeof v === 'string' && v.length > 40 ? v.slice(0, 40) + '...' : v));
			let ours;
			let theirs;
			try { ours = { record: createDiagnostic(input), line: formatDiagnostic(input) }; } catch (error) { ours = { error: error.message }; }
			try { theirs = { record: lead.createDiagnostic(input), line: lead.formatDiagnostic(input) }; } catch (error) { theirs = { error: error.message }; }
			expect(ours, label).toEqual(theirs);
			if (ours.line !== undefined) {
				expect(lead.parseDiagnostic(ours.line) === null, label).toBe(lead.parseDiagnostic(theirs.line) === null);
			}
		}
	});

	it('is parsed by the adapter parser as a canonical record', () => {
		const parsed = lead.parseDiagnostic(formatDiagnostic(SAMPLE));
		expect(parsed?.format).toBe('canonical');
		expect(parsed?.record).toEqual(createDiagnostic(SAMPLE));
	});

	it('rejects the same invalid shapes the adapter rejects', () => {
		for (const bad of [
			{ ...SAMPLE, severity: 'nope' },
			{ ...SAMPLE, component: 'Redis Groups] event=x' },
			{ ...SAMPLE, event: 'nodots' },
			{ ...SAMPLE, source: 'Bad Source' },
			{ ...SAMPLE, dataClass: 'private' },
			{ ...SAMPLE, occurredAt: 42 }
		]) {
			let ours = null;
			let theirs = null;
			try { createDiagnostic(bad); } catch (error) { ours = error.message; }
			try { lead.createDiagnostic(bad); } catch (error) { theirs = error.message; }
			expect(ours, JSON.stringify(bad)).not.toBeNull();
			expect(ours).toBe(theirs);
		}
	});

	it('delivers extensions records to a sink installed through the adapter, and adapter records to a sink installed here', () => {
		const viaAdapter = [];
		const restore = lead.setOperationalEventSink((record) => viaAdapter.push(record));
		const record = emitOperationalEvent(SAMPLE);
		expect(viaAdapter).toEqual([record]);
		restore();

		const viaExtensions = [];
		setOperationalEventSink((r) => viaExtensions.push(r));
		const adapterRecord = lead.emitOperationalEvent({ ...SAMPLE, source: 'svelte-adapter-uws' });
		expect(viaExtensions).toEqual([adapterRecord]);
	});

	it('nested registrations resume the newest still-active sink across both packages', () => {
		const seen = [];
		const disposeA = setOperationalEventSink(() => seen.push('A'));
		const disposeB = lead.setOperationalEventSink(() => seen.push('B'));
		disposeA();
		emitOperationalEvent(SAMPLE);
		disposeB();
		emitOperationalEvent(SAMPLE);
		expect(seen).toEqual(['B']);
	});
});

describe('operational event emission', () => {
	it('falls back to canonical console output when no sink is installed', () => {
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		emitOperationalEvent(SAMPLE);
		expect(error).toHaveBeenCalledOnce();
		expect(error.mock.calls[0][0]).toBe(formatDiagnostic(SAMPLE));
	});

	it('keeps the event when a custom sink throws or rejects, and reports the sink once per record', async () => {
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		setOperationalEventSink(() => { throw new Error('sink down'); });
		emitOperationalEvent(SAMPLE);
		expect(error.mock.calls.map((call) => call[0])).toEqual([
			formatDiagnostic(SAMPLE),
			expect.stringContaining('event=operational.sink.failed')
		]);
		expect(JSON.parse(error.mock.calls[1][0].slice(error.mock.calls[1][0].indexOf('{'))).attributes).toEqual({
			originalSource: SAMPLE.source,
			originalEvent: SAMPLE.event
		});
		// The notice is built from the registry entry rather than through
		// emitRegisteredFailure, so pin that its line starts with the indexed
		// prefix: the message is the problem text, not the full line head.
		const notice = extensionsErrorDefinition(EXTENSIONS_ERROR_IDS.SINK_FAILED);
		expect(error.mock.calls[1][0].startsWith(notice.messagePrefix + ' {')).toBe(true);

		error.mockClear();
		setOperationalEventSink(() => Promise.reject(new Error('async down')));
		emitOperationalEvent(SAMPLE);
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(error).toHaveBeenCalledTimes(2);
		expect(error.mock.calls[1][0]).toContain('event=operational.sink.failed');
	});

	it('hands a sink a frozen record, so a sink that corrupts it and throws cannot change what the fallback prints', () => {
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		setOperationalEventSink((record) => {
			record.attributes = { size: 10n };
			throw new Error('sink down');
		});
		emitOperationalEvent(SAMPLE);
		expect(error.mock.calls[0][0]).toBe(formatDiagnostic(SAMPLE));
	});

	it('registers the sink even where an accessor on Object.prototype would swallow a plain assignment', () => {
		const keys = [Symbol.for('lantean.operational-event-sink.v1'), Symbol.for('lantean.operational-event-sink-registry.v1')];
		for (const key of keys) delete globalThis[key];
		for (const key of keys) {
			Object.defineProperty(Object.prototype, key, { get() { return undefined; }, set() {}, configurable: true });
		}
		try {
			const records = [];
			setOperationalEventSink((record) => { records.push(record); });
			const error = vi.spyOn(console, 'error').mockImplementation(() => {});
			emitOperationalEvent(SAMPLE);
			expect(records).toHaveLength(1);
			expect(error).not.toHaveBeenCalled();
		} finally {
			for (const key of keys) delete Object.prototype[key];
		}
	});

	it('never throws out of the emitting path for an unserializable attribute or an invalid shape', () => {
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		expect(() => emitOperationalEvent({ ...SAMPLE, attributes: { size: 10n } })).not.toThrow();
		// The envelope survives with its attributes stripped rather than being lost.
		expect(error.mock.calls[0][0]).toContain('event=redis.clock.sample.failed');
		expect(JSON.parse(error.mock.calls[0][0].slice(error.mock.calls[0][0].indexOf('{'))).attributes).toBeNull();

		error.mockClear();
		expect(emitOperationalEvent({ ...SAMPLE, severity: 'loud' })).toBeNull();
		expect(error.mock.calls[0][0]).toContain('[EXTENSIONS-ERR-DIAGNOSTIC-RECORD-SHAPE]');
	});

	it('emits a registered failure from the registry entry alone, with the error projected and redacted', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		const definition = extensionsErrorDefinition(EXTENSIONS_ERROR_IDS.POSTGRES_POOL_IDLE_CLIENT);
		const err = Object.assign(new Error('connect ECONNREFUSED postgres://app:hunter2@db.internal:5432/main'), { code: 'ECONNREFUSED' });
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.POSTGRES_POOL_IDLE_CLIENT, { error: err, attributes: { pool: 'primary' } });
		expect(records).toHaveLength(1);
		expect(records[0]).toMatchObject({
			source: 'svelte-adapter-uws-extensions',
			component: definition.component,
			event: definition.event,
			severity: definition.severity,
			dataClass: 'application',
			message: definition.problemPrefix,
			attributes: {
				pool: 'primary',
				error: { name: 'Error', code: 'ECONNREFUSED', message: 'connect ECONNREFUSED postgres://app:***@db.internal:5432/main' }
			}
		});
		// The documented search key is the head of the line an operator sees.
		expect(formatDiagnostic(records[0]).startsWith(definition.messagePrefix + ' {')).toBe(true);
		expect(JSON.stringify(records[0])).not.toContain('hunter2');
	});

	it('redacts a connection string before capping the message, so a password cut at the cap leaves no fragment', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		// The password starts at character 503 and runs past the 512 cap.
		const head = 'x'.repeat(490) + ' redis://app:';
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, { error: new Error(head + 'hunter2secret@cache.internal:6379 refused') });
		const message = records[0].attributes.error.message;
		expect(message).toBe((head + '***@cache.internal:6379 refused').slice(0, 512));
		expect(JSON.stringify(records[0])).not.toMatch(/hunter|secret/);
	});

	it('removes each listed value from the message, leaving no fragment where values overlap', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		const report = (message, redact) => {
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE, { error: new Error(message), redact, attributes: { slot: 1 } });
			return records[records.length - 1].attributes.error.message;
		};
		// A backend or proxy that repeats what it refused.
		const channel = 'uws:sharded:user:42:inbox';
		expect(report("proxy refused '" + channel + "' (user:42)", ['user:42', channel, '', 7])).toBe("proxy refused '<redacted>' (<redacted>)");
		expect(report('refused abc and bc', ['ab', 'bc'])).toBe('refused <redacted> and <redacted>');
		// A single string is one value, not a list of characters.
		expect(report('Connection is closed for user:42.', 'user:42')).toBe('Connection is closed for <redacted>.');
		// A listed value that itself holds a connection string still matches whole.
		expect(report('refused tenant:redis://app:pw@host', ['tenant:redis://app:pw@host'])).toBe('refused <redacted>');
	});

	it('passes a real ACL refusal through: Redis does not name the key or channel it refused', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, { error: new Error('NOPERM No permissions to access a channel'), redact: ['uws:sharded:user:42'] });
		expect(records[0].attributes.error.message).toBe('NOPERM No permissions to access a channel');
	});

	it("cuts the arguments an unknown-command error repeats", () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, {
			error: new Error("ERR unknown command 'ssubscribe', with args beginning with: 'uws:sharded:user:42:inbox' ")
		});
		expect(records[0].attributes.error.message).toBe("ERR unknown command 'ssubscribe', with args beginning with: <redacted>");
		// Before Redis 7 the name and arguments were quoted with backticks.
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, {
			error: new Error('ERR unknown command ' + '`ssubscribe`, with args beginning with: `uws:sharded:user:42:inbox`, ')
		});
		expect(records[1].attributes.error.message).toBe('ERR unknown command `ssubscribe`, with args beginning with: <redacted>');
	});

	it("cuts the unmatched-reply error the installed ioredis builds, not only the text written here", () => {
		const { default: DataHandler } = createRequire(import.meta.url)('ioredis/built/DataHandler.js');
		let captured = null;
		const handler = { redis: { commandQueue: { shift: () => undefined }, emit: (_event, err) => { captured = err; } } };
		DataHandler.prototype.shiftCommand.call(handler, [Buffer.from('sunsubscribe'), Buffer.from('uws:sharded:user:42:inbox'), 0]);
		expect(captured).toBeInstanceOf(Error);
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER, { error: captured });
		expect(records[0].attributes.error.message).toMatch(/Last reply: sunsubscribe <redacted>$/);
		expect(JSON.stringify(records[0])).not.toContain('user:42');
	});

	it('drops the message for redact: true and keeps the name and code', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		const err = Object.assign(new SyntaxError('Unexpected token, "alice@example.com" is not valid JSON'), { code: 'E_PARSE' });
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.POSTGRES_NOTIFY_PARSE, { error: err, redact: true, attributes: { channel: 'changes' } });
		expect(records[0].attributes.error).toEqual({ name: 'SyntaxError', code: 'E_PARSE', message: '<redacted>' });
	});

	it('keeps only the pub/sub kind or error code of a reply ioredis could not match to a command', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		const state = 'Command queue state error. If you can reproduce this, please report it.';
		const messages = [
			state + ' Last reply: sunsubscribe,uws:sharded:user:42,0',
			state + ' Last reply: message,uws:pubsub,{"topic":"user:42"}',
			state + ' Last error: NOPERM No permissions to access a channel',
			// Any other reply is data: an HKEYS of a presence hash, a status, a number.
			state + ' Last reply: alice,bob',
			state + ' Last reply: 5',
			// Other libraries' and applications' "Last error:" text is not ioredis's reply.
			'All sentinels are unreachable. Retrying from scratch after 10ms. Last error: connect ECONNREFUSED 10.0.0.5:26379',
			'Too many Cluster redirections. Last error: MOVED 3999 10.0.0.7:6381'
		];
		for (const message of messages) emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, { error: new Error(message) });
		expect(records.map((r) => r.attributes.error.message)).toEqual([
			state + ' Last reply: sunsubscribe <redacted>',
			state + ' Last reply: message <redacted>',
			state + ' Last error: NOPERM <redacted>',
			state + ' Last reply: <redacted>',
			state + ' Last reply: <redacted>',
			messages[5],
			messages[6]
		]);
	});

	it('cleans a large message in linear time', () => {
		const records = [];
		setOperationalEventSink((record) => records.push(record));
		const started = Date.now();
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, { error: new Error('a://'.repeat(32 * 1024)) });
		expect(Date.now() - started).toBeLessThan(1000);
		expect(records[0].attributes.error.message).toHaveLength(512);
	});

	it('refuses to emit a thrown or console entry as a diagnostic', () => {
		expect(() => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.CIRCUIT_OPEN)).toThrow(/not a diagnostic emission/);
		expect(() => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RECORD_SHAPE)).toThrow(/not a diagnostic emission/);
	});

	it('projects hostile thrown values without throwing', () => {
		const trap = new Proxy({}, { get() { throw new Error('trap'); } });
		expect(diagnosticError(trap)).toEqual({ name: 'Error', code: null, message: 'Uninspectable thrown value' });
		expect(diagnosticError('plain')).toEqual({ name: 'Error', code: null, message: 'plain' });
		expect(diagnosticError({ name: 'X'.repeat(200), code: 12, message: 'm'.repeat(600) })).toEqual({
			name: 'X'.repeat(80), code: '12', message: 'm'.repeat(512)
		});
	});
});
