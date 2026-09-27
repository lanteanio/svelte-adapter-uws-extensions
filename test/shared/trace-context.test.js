import { describe, expect, it } from 'vitest';
import * as lead from 'svelte-adapter-uws/observability';
import {
	POSTGRES_TRACE_ATTRIBUTES,
	REDIS_TRACE_ATTRIBUTES,
	injectTraceContext,
	explicitTraceContext,
	liveTraceContext,
	normalizeTraceContext,
	runTraceOperation,
	traceEnabled,
	withTraceContext
} from '../../src/shared/trace-context.js';

const A = '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01';
const B = '00-fedcba9876543210fedcba9876543210-fedcba9876543210-00';

// The local validator is a copy of the adapter's; these vectors drive both and
// require identical answers, so the copy cannot drift from the adapter's.
const PARITY_VECTORS = [
	{ traceparent: A, tracestate: 'vendor=value' },
	{ traceparent: A, tracestate: 'bad=value=again' },
	{ traceparent: A.toUpperCase() },
	{ traceparent: '00-00000000000000000000000000000000-0123456789abcdef-01' },
	{ traceparent: '00-0123456789abcdef0123456789abcdef-0000000000000000-01' },
	{ traceparent: '01-0123456789abcdef0123456789abcdef-0123456789abcdef-01' },
	{ traceparent: 'ff-0123456789abcdef0123456789abcdef-0123456789abcdef-01' },
	{ traceparent: A + '-extra' },
	{ traceparent: A, tracestate: '' },
	{ traceparent: A, tracestate: ' vendor=value , other=x ' },
	{ traceparent: A, tracestate: 'vendor=value,,other=x' },
	{ traceparent: A, tracestate: 'VENDOR=value' },
	{ traceparent: A, tracestate: `${'a'.repeat(256)}=value` },
	{ traceparent: A, tracestate: `${'a'.repeat(257)}=value` },
	{ traceparent: A, tracestate: `${'a'.repeat(241)}@system=value` },
	{ traceparent: A, tracestate: `${'a'.repeat(242)}@system=value` },
	{ traceparent: A, tracestate: 'tenant@1system=value' },
	{ traceparent: A, tracestate: 'vendor=one,vendor=two' },
	{ traceparent: A, tracestate: `vendor=${'a'.repeat(256)}` },
	{ traceparent: A, tracestate: `vendor=${'a'.repeat(257)}` },
	{ traceparent: A, tracestate: Array.from({ length: 32 }, (_, i) => `k${i}=v`).join(',') },
	{ traceparent: A, tracestate: Array.from({ length: 33 }, (_, i) => `k${i}=v`).join(',') },
	{ traceparent: A, tracestate: 'vendor=va' + String.fromCharCode(0xe9) + 'lue' },
	{ Traceparent: A, TraceState: 'vendor=value' },
	{ TRACEPARENT: A },
	new Map([['traceparent', A]]),
	{ traceparent: 42 },
	null,
	'string',
	{ data: { traceparent: A } }
];

describe('shared trace context', () => {
	it('agrees with the adapter validator on every vector, including header casing and Headers-like carriers', () => {
		for (const vector of PARITY_VECTORS) {
			expect(normalizeTraceContext(vector), JSON.stringify(vector instanceof Map ? [...vector] : vector)).toEqual(lead.normalizeTraceContext(vector));
		}
		expect(normalizeTraceContext({ Traceparent: A, TraceState: 'vendor=value' })).toEqual({ traceparent: A, tracestate: 'vendor=value' });
		expect(normalizeTraceContext(new Headers({ traceparent: A }))).toEqual({ traceparent: A });
	});

	it('injects the way the adapter injects: validated fields only, never into data, Headers supported', () => {
		for (const context of [{ traceparent: A, tracestate: 'vendor=value' }, { traceparent: 'garbage' }, null, undefined]) {
			const ours = injectTraceContext({ data: { traceparent: 'application-value' } }, context);
			const theirs = lead.injectTraceContext({ data: { traceparent: 'application-value' } }, context);
			expect(ours).toEqual(theirs);
		}
		expect(injectTraceContext({ data: { traceparent: 'application-value' } }, { traceparent: A })).toEqual({
			data: { traceparent: 'application-value' },
			traceparent: A
		});
		const headers = injectTraceContext(new Headers(), { traceparent: A, tracestate: 'vendor=value' });
		expect(headers.get('traceparent')).toBe(A);
		expect(headers.get('tracestate')).toBe('vendor=value');
	});

	it('reads an explicit option as absent, opted out, or a validated context', () => {
		expect(explicitTraceContext(undefined)).toBeUndefined();
		expect(explicitTraceContext({ relay: false })).toBeUndefined();
		expect(explicitTraceContext(Object.create({ traceContext: { traceparent: A } }))).toBeUndefined();
		expect(explicitTraceContext({ traceContext: null })).toBeNull();
		expect(explicitTraceContext({ traceContext: { traceparent: 'garbage' } })).toBeNull();
		expect(explicitTraceContext({ traceContext: { traceparent: B } })).toEqual({ traceparent: B });
	});

	it('reads the Platform live context directly and validates it', () => {
		expect(liveTraceContext({ traceContext: { traceparent: A } })).toEqual({ traceparent: A });
		expect(liveTraceContext({ traceContext: { traceparent: 'garbage' } })).toBeNull();
		expect(liveTraceContext({ traceContext: null })).toBeNull();
		expect(liveTraceContext({})).toBeNull();
		expect(liveTraceContext(null)).toBeNull();
		expect(liveTraceContext({ get traceContext() { throw new Error('trap'); } })).toBeNull();
	});

	it('exposes frozen span attributes shared by every operation', () => {
		expect(Object.isFrozen(REDIS_TRACE_ATTRIBUTES)).toBe(true);
		expect(Object.isFrozen(POSTGRES_TRACE_ATTRIBUTES)).toBe(true);
		expect(REDIS_TRACE_ATTRIBUTES).toEqual({ 'messaging.system': 'redis' });
		expect(POSTGRES_TRACE_ATTRIBUTES).toEqual({ 'messaging.system': 'postgresql' });
	});

	it('reports tracing enabled only for a provider-backed run()', () => {
		expect(traceEnabled({ trace: { enabled: true, run() {} } })).toBe(true);
		expect(traceEnabled({ trace: { enabled: false, run() {} } })).toBe(false);
		expect(traceEnabled({ trace: { enabled: true } })).toBe(false);
		expect(traceEnabled({})).toBe(false);
		expect(traceEnabled(new Proxy({}, { get() { throw new Error('trap'); } }))).toBe(false);
	});

	it('uses the structural trace surface only when a provider is enabled', () => {
		const calls = [];
		const platform = {
			traceContext: { traceparent: A },
			trace: {
				enabled: true,
				current: () => ({ traceparent: B }),
				run(name, options, fn) {
					calls.push({ name, options });
					return fn();
				}
			}
		};
		const result = runTraceOperation(platform, 'redis.publish', 'producer', { traceparent: A }, REDIS_TRACE_ATTRIBUTES, (context) => context);
		expect(result).toEqual({ traceparent: B });
		expect(calls).toEqual([{
			name: 'redis.publish',
			options: { kind: 'producer', parent: { traceparent: A }, attributes: REDIS_TRACE_ATTRIBUTES }
		}]);
	});

	it('leaves a non-object carrier untouched, like the adapter', () => {
		for (const carrier of [null, undefined, 'carrier', 5, true]) {
			expect(injectTraceContext(carrier, { traceparent: A })).toBe(lead.injectTraceContext(carrier, { traceparent: A }));
			expect(() => injectTraceContext(carrier, { traceparent: A })).not.toThrow();
		}
	});

	it('never retries a callback that throws inside a restored context', () => {
		let calls = 0;
		const platform = {
			trace: {
				enabled: false,
				withContext(_context, fn) { return fn(); }
			}
		};
		const explode = () => {
			calls += 1;
			throw new Error('boom');
		};
		expect(() => runTraceOperation(platform, 'redis.receive', 'consumer', { traceparent: A }, {}, explode)).toThrow('boom');
		expect(calls).toBe(1);
		expect(() => withTraceContext(platform, { traceparent: A }, explode)).toThrow('boom');
		expect(calls).toBe(2);
	});
});
