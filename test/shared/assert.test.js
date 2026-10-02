import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
	assert,
	fatal,
	setFatalSink,
	resetFatalSink,
	devAssert,
	getAssertionCounters,
	wireAssertionMetrics,
	_resetCountersForTesting
} from '../../src/shared/assert.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { diagnosticError, formatDiagnostic } from '../../src/shared/diagnostic.js';

function diagnosticRecord(line) {
	return JSON.parse(line.slice(line.indexOf('{')));
}

describe('shared/assert', () => {
	beforeEach(() => {
		_resetCountersForTesting();
	});

	it('keeps bridge diagnostics on one physical line', () => {
		const line = formatDiagnostic({
			source: 'svelte-adapter-uws-extensions', component: 'runtime.redis',
			event: 'redis.warning', severity: 'warn', message: 'warning',
			attributes: { value: 'a\u2028b\u2029c' }
		});
		expect(line).not.toContain('\u2028');
		expect(line).not.toContain('\u2029');
		expect(line).toContain('\\u2028');
		expect(line).toContain('\\u2029');
		expect(JSON.parse(line.slice(line.indexOf('{'))).attributes.value).toBe('a\u2028b\u2029c');
	});

	it('ASCII-escapes control, bidi, and right-to-left external values', () => {
		const hostile = String.fromCodePoint(0, 0x85, 0x202e, 0x2066, 0x5e9);
		const line = formatDiagnostic({
			source: 'svelte-adapter-uws-extensions',
			component: 'runtime.redis',
			event: 'redis.warning',
			severity: 'warn',
			message: 'external=' + hostile,
			attributes: { name: hostile }
		});
		expect(line).not.toMatch(/[\u0000-\u001f\u007f-\uffff]/);
		for (const escaped of ['\\u0000', '\\u0085', '\\u202e', '\\u2066', '\\u05e9']) {
			expect(line).toContain(escaped);
		}
		const record = diagnosticRecord(line);
		expect(record.message).toBe('external=\\u0000\\u0085\\u202e\\u2066\\u05e9');
		expect(record.attributes.name).toBe(hostile);
	});

	it('contains thrown values whose accessors and string conversion also throw', () => {
		const hostile = new Proxy({}, {
			get() { throw new Error('getter failed'); }
		});
		expect(diagnosticError(hostile)).toEqual({ name: 'Error', code: null, message: 'Uninspectable thrown value' });
	});

	describe('assert', () => {
		it('is a no-op when the condition is true', () => {
			expect(() => assert(true, 'test.passes')).not.toThrow();
			expect(getAssertionCounters().size).toBe(0);
		});

		it('throws in test mode when the condition is false (vitest sets VITEST)', () => {
			expect(() => assert(false, 'test.fails-in-test')).toThrow(/extensions assertion failed/);
		});

		it('increments the in-memory counter on every violation', () => {
			expect(() => assert(false, 'first')).toThrow();
			expect(() => assert(false, 'first')).toThrow();
			expect(() => assert(false, 'second')).toThrow();
			expect(getAssertionCounters().get('first')).toBe(2);
			expect(getAssertionCounters().get('second')).toBe(1);
		});

		it('logs a structured line on violation', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			expect(() => assert(false, 'shape.test', { x: 1 })).toThrow();
			expect(errSpy).toHaveBeenCalledTimes(1);
			const line = errSpy.mock.calls[0][0];
			expect(line).toMatch(/^\[lantean\/diagnostic source=svelte-adapter-uws-extensions component=runtime\.assertion event=invariant\.violated severity=warn\]/);
			const record = diagnosticRecord(line);
			expect(record.attributes).toEqual({
				category: 'shape.test',
				context: { x: 1 }
			});
			errSpy.mockRestore();
		});

		it('strips sensitive keys from context before logging', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			expect(() =>
				assert(false, 'shape.with-pii', {
					userId: 'u-1',
					password: 'leaked',
					token: 'shh',
					authorization: 'bearer xxx'
				})
			).toThrow();
			const parsed = diagnosticRecord(errSpy.mock.calls[0][0]);
			expect(parsed.attributes.context).toHaveProperty('userId');
			expect(parsed.attributes.context).not.toHaveProperty('password');
			expect(parsed.attributes.context).not.toHaveProperty('token');
			expect(parsed.attributes.context).not.toHaveProperty('authorization');
			errSpy.mockRestore();
		});

		it('omits the context field when no context was passed', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			expect(() => assert(false, 'no.context')).toThrow();
			const parsed = diagnosticRecord(errSpy.mock.calls[0][0]);
			expect(parsed.attributes).toEqual({ category: 'no.context', context: null });
			errSpy.mockRestore();
		});
	});

	describe('devAssert', () => {
		it('is a no-op when the condition is true', () => {
			expect(() => devAssert(true, 'all good')).not.toThrow();
		});

		it('logs and then throws on violation in test mode, as the adapter does', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			let thrown = null;
			try { devAssert(false, 'shape mismatch', { got: 'string' }); } catch (err) { thrown = err; }
			expect(thrown).toBeInstanceOf(Error);
			expect(thrown.message).toBe('extensions devAssert: shape mismatch');
			expect(thrown.context).toEqual({ got: 'string' });
			expect(errSpy).toHaveBeenCalledTimes(1);
			expect(errSpy.mock.calls[0][0]).toMatch(/^\[lantean\/diagnostic source=svelte-adapter-uws-extensions component=runtime\.assertion event=invariant\.violated severity=error\] shape mismatch/);
			expect(diagnosticRecord(errSpy.mock.calls[0][0]).attributes).toEqual({
				category: 'development.assertion',
				context: { got: 'string' }
			});
			errSpy.mockRestore();
		});

		it('logs the message and context when violated', () => {
			const warnSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			expect(() => devAssert(false, 'mismatch', { hint: 'check options' })).toThrow('extensions devAssert: mismatch');
			const line = warnSpy.mock.calls[0][0];
			expect(line).toContain('mismatch');
			expect(line).toContain('hint');
			expect(line).toContain('check options');
			warnSpy.mockRestore();
		});

		it('strips internal keys from the context it throws, as from the line it logs', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			let thrown = null;
			try { devAssert(false, 'leak check', { password: 'hunter2', hint: 'ok' }); } catch (err) { thrown = err; }
			expect(thrown.context).not.toHaveProperty('password');
			expect(thrown.context.hint).toBe('ok');
			expect(errSpy.mock.calls[0][0]).not.toContain('hunter2');
			errSpy.mockRestore();
		});

		it('throws with a null context when none is given', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			let thrown = null;
			try { devAssert(false, 'bare'); } catch (err) { thrown = err; }
			expect(thrown.context).toBeNull();
			errSpy.mockRestore();
		});
	});

	describe('test-mode detection is per-call (not module-load snapshot)', () => {
		it('assert: respects a NODE_ENV flip between calls', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			const originalVitest = process.env.VITEST;
			const originalNodeEnv = process.env.NODE_ENV;
			try {
				// Simulate prod-mode mid-run: no VITEST, NODE_ENV=production.
				delete process.env.VITEST;
				process.env.NODE_ENV = 'production';
				// Should NOT throw (prod path counts + logs only).
				expect(() => assert(false, 'mode.prod-skip')).not.toThrow();

				// Flip back to test mode for the next call.
				process.env.NODE_ENV = 'test';
				expect(() => assert(false, 'mode.test-throws')).toThrow(/extensions assertion failed/);
			} finally {
				if (originalVitest === undefined) delete process.env.VITEST;
				else process.env.VITEST = originalVitest;
				if (originalNodeEnv === undefined) delete process.env.NODE_ENV;
				else process.env.NODE_ENV = originalNodeEnv;
				errSpy.mockRestore();
			}
		});

		it('devAssert: respects a NODE_ENV flip from prod to dev between calls', () => {
			const warnSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			const originalVitest = process.env.VITEST;
			const originalNodeEnv = process.env.NODE_ENV;
			try {
				// Prod mode: devAssert is a no-op (no warning).
				delete process.env.VITEST;
				process.env.NODE_ENV = 'production';
				devAssert(false, 'mode.prod-noop');
				expect(warnSpy).not.toHaveBeenCalled();

				// Flip to dev: the next call logs and throws.
				process.env.NODE_ENV = 'development';
				expect(() => devAssert(false, 'mode.dev-warns')).toThrow('extensions devAssert: mode.dev-warns');
				expect(warnSpy).toHaveBeenCalledTimes(1);
				expect(warnSpy.mock.calls[0][0]).toContain('mode.dev-warns');
			} finally {
				if (originalVitest === undefined) delete process.env.VITEST;
				else process.env.VITEST = originalVitest;
				if (originalNodeEnv === undefined) delete process.env.NODE_ENV;
				else process.env.NODE_ENV = originalNodeEnv;
				warnSpy.mockRestore();
			}
		});
	});

	describe('wireAssertionMetrics', () => {
		it('rejects an absent metrics registry', () => {
			expect(() => wireAssertionMetrics(null)).toThrow(/metrics/);
			expect(() => wireAssertionMetrics({})).toThrow(/metrics/);
		});

		it('registers the violations counter and increments on every assert', async () => {
			const metrics = createMetrics();
			wireAssertionMetrics(metrics);

			expect(() => assert(false, 'wire.test')).toThrow();
			expect(() => assert(false, 'wire.test')).toThrow();
			expect(() => assert(false, 'wire.other')).toThrow();

			const out = await metrics.serialize();
			expect(out).toMatch(/extensions_assertion_violations_total\{category="wire.test",severity="soft"\}\s+2/);
			expect(out).toMatch(/extensions_assertion_violations_total\{category="wire.other",severity="soft"\}\s+1/);
		});

		it('does not increment when assert passes', async () => {
			const metrics = createMetrics();
			wireAssertionMetrics(metrics);

			assert(true, 'wire.passes');
			assert(true, 'wire.passes');

			const out = await metrics.serialize();
			expect(out).not.toMatch(/extensions_assertion_violations_total\{category="wire.passes"\}/);
		});

		it('replaces the bound counter when called twice (most-recent wins)', async () => {
			const m1 = createMetrics();
			const m2 = createMetrics();
			wireAssertionMetrics(m1);
			wireAssertionMetrics(m2);

			expect(() => assert(false, 'replace.test')).toThrow();

			const out1 = await m1.serialize();
			const out2 = await m2.serialize();
			expect(out1).not.toMatch(/replace\.test"/);
			expect(out2).toMatch(/extensions_assertion_violations_total\{category="replace.test",severity="soft"\}\s+1/);
		});

		it('survives a counter.inc throwing without losing the assertion violation', async () => {
			const metrics = {
				counter() {
					return {
						inc() { throw new Error('metrics down'); }
					};
				}
			};
			wireAssertionMetrics(metrics);
			expect(() => assert(false, 'flaky.metrics')).toThrow();
			// The in-memory counter should still have advanced.
			expect(getAssertionCounters().get('flaky.metrics')).toBe(1);
		});

		it('labels a fatal violation severity="fatal" on the same counter', async () => {
			const metrics = createMetrics();
			wireAssertionMetrics(metrics);
			const exit = vi.fn();
			setFatalSink({ exit });
			const originalVitest = process.env.VITEST;
			const originalNodeEnv = process.env.NODE_ENV;
			try {
				delete process.env.VITEST;
				process.env.NODE_ENV = 'production';
				fatal(false, 'wire.fatal', { reason: 'corrupt' });
			} finally {
				if (originalVitest === undefined) delete process.env.VITEST;
				else process.env.VITEST = originalVitest;
				if (originalNodeEnv === undefined) delete process.env.NODE_ENV;
				else process.env.NODE_ENV = originalNodeEnv;
			}
			const out = await metrics.serialize();
			expect(out).toMatch(/extensions_assertion_violations_total\{category="wire.fatal",severity="fatal"\}\s+1/);
		});
	});

	describe('fatal', () => {
		it('is a no-op when the condition is true', () => {
			expect(() => fatal(true, 'fatal.passes')).not.toThrow();
			expect(getAssertionCounters().size).toBe(0);
		});

		it('throws in test mode when the condition is false', () => {
			expect(() => fatal(false, 'fatal.fails-in-test')).toThrow(/extensions fatal/);
		});

		it('increments the same counter map as assert (one namespace)', () => {
			expect(() => assert(false, 'fatal.shared')).toThrow();
			expect(() => fatal(false, 'fatal.shared')).toThrow();
			expect(getAssertionCounters().get('fatal.shared')).toBe(2);
		});

		it('logs a structured line with severity: fatal and strips PII', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			expect(() => fatal(false, 'fatal.log', { topic: 'room', password: 'leaked' })).toThrow();
			const line = errSpy.mock.calls[0][0];
			expect(line).toMatch(/^\[lantean\/diagnostic source=svelte-adapter-uws-extensions component=runtime\.assertion event=invariant\.violated severity=fatal\]/);
			const parsed = diagnosticRecord(line);
			expect(parsed.severity).toBe('fatal');
			expect(parsed.attributes.category).toBe('fatal.log');
			expect(parsed.attributes.context).toHaveProperty('topic');
			expect(parsed.attributes.context).not.toHaveProperty('password');
			errSpy.mockRestore();
		});

		it('setFatalSink rejects a sink without an exit function', () => {
			expect(() => setFatalSink(null)).toThrow(/exit/);
			expect(() => setFatalSink({})).toThrow(/exit/);
		});

		it('in production, defers exit(78) through the injectable sink without killing the process', async () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			const exit = vi.fn();
			setFatalSink({ exit });
			const originalVitest = process.env.VITEST;
			const originalNodeEnv = process.env.NODE_ENV;
			try {
				delete process.env.VITEST;
				process.env.NODE_ENV = 'production';
				expect(() => fatal(false, 'fatal.defer', { topic: 'room' })).not.toThrow();
				// Not exited synchronously; metric + log already flushed.
				expect(exit).not.toHaveBeenCalled();
				expect(getAssertionCounters().get('fatal.defer')).toBe(1);
				expect(errSpy).toHaveBeenCalledTimes(1);
				// Deferred via microtask: fires after the current frame.
				await Promise.resolve();
				expect(exit).toHaveBeenCalledTimes(1);
				expect(exit).toHaveBeenCalledWith(78);
			} finally {
				if (originalVitest === undefined) delete process.env.VITEST;
				else process.env.VITEST = originalVitest;
				if (originalNodeEnv === undefined) delete process.env.NODE_ENV;
				else process.env.NODE_ENV = originalNodeEnv;
				errSpy.mockRestore();
			}
		});

		it('resetFatalSink restores the default sink', () => {
			expect(() => resetFatalSink()).not.toThrow();
		});
	});
});
