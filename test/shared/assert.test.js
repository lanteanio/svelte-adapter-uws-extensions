import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
	assert,
	devAssert,
	getAssertionCounters,
	wireAssertionMetrics,
	_resetCountersForTesting
} from '../../shared/assert.js';
import { createMetrics } from '../../prometheus/index.js';

describe('shared/assert', () => {
	beforeEach(() => {
		_resetCountersForTesting();
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
			expect(line).toMatch(/^\[extensions\/assert\]/);
			const json = line.slice('[extensions/assert] '.length);
			expect(JSON.parse(json)).toEqual({
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
			const json = errSpy.mock.calls[0][0].slice('[extensions/assert] '.length);
			const parsed = JSON.parse(json);
			expect(parsed.context).toHaveProperty('userId');
			expect(parsed.context).not.toHaveProperty('password');
			expect(parsed.context).not.toHaveProperty('token');
			expect(parsed.context).not.toHaveProperty('authorization');
			errSpy.mockRestore();
		});

		it('omits the context field when no context was passed', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			expect(() => assert(false, 'no.context')).toThrow();
			const json = errSpy.mock.calls[0][0].slice('[extensions/assert] '.length);
			const parsed = JSON.parse(json);
			expect(parsed).toEqual({ category: 'no.context' });
			errSpy.mockRestore();
		});
	});

	describe('devAssert', () => {
		it('is a no-op when the condition is true', () => {
			expect(() => devAssert(true, 'all good')).not.toThrow();
		});

		it('does not throw on violation, even in test mode', () => {
			const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			expect(() => devAssert(false, 'shape mismatch', { got: 'string' })).not.toThrow();
			expect(warnSpy).toHaveBeenCalledTimes(1);
			expect(warnSpy.mock.calls[0][0]).toMatch(/^\[extensions\/devAssert\] shape mismatch/);
			warnSpy.mockRestore();
		});

		it('logs the message and context when violated', () => {
			const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			devAssert(false, 'mismatch', { hint: 'check options' });
			const line = warnSpy.mock.calls[0][0];
			expect(line).toContain('mismatch');
			expect(line).toContain('hint');
			expect(line).toContain('check options');
			warnSpy.mockRestore();
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
			expect(out).toMatch(/extensions_assertion_violations_total\{category="wire.test"\}\s+2/);
			expect(out).toMatch(/extensions_assertion_violations_total\{category="wire.other"\}\s+1/);
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
			expect(out1).not.toMatch(/replace\.test"\}\s+1/);
			expect(out2).toMatch(/extensions_assertion_violations_total\{category="replace.test"\}\s+1/);
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
	});
});
