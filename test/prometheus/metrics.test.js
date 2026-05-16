import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
	createMetrics,
	wirePublishRateMetrics,
	wireClusterPublishRateMetrics,
	connectionMetricsHook
} from '../../prometheus/index.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../redis/pubsub.js';
import { createPresence } from '../../redis/presence.js';
import { createReplay as createRedisReplay } from '../../redis/replay.js';
import { createRateLimit } from '../../redis/ratelimit.js';
import { createGroup } from '../../redis/groups.js';
import { createCursor } from '../../redis/cursor.js';
import { createReplay as createPgReplay } from '../../postgres/replay.js';
import { createNotifyBridge } from '../../postgres/notify.js';

function mockWs(userData = {}) {
	const subscriptions = new Set();
	return {
		getUserData: () => ({ ...userData, __subscriptions: subscriptions }),
		subscribe(topic) { subscriptions.add(topic); },
		unsubscribe(topic) { subscriptions.delete(topic); },
		getBufferedAmount() { return 0; }
	};
}

describe('prometheus metrics', () => {
	describe('registry', () => {
		it('creates counters, gauges, and histograms', () => {
			const m = createMetrics();
			const c = m.counter('test_total', 'A counter');
			const g = m.gauge('test_value', 'A gauge');
			const h = m.histogram('test_duration', 'A histogram');

			expect(c).toBeDefined();
			expect(g).toBeDefined();
			expect(h).toBeDefined();
		});

		it('applies prefix to metric names', () => {
			const m = createMetrics({ prefix: 'app_' });
			m.counter('requests_total', 'Requests');
			m.counter('errors_total', 'Errors');

			const output = m.serialize();
			expect(output).toContain('app_requests_total');
			expect(output).toContain('app_errors_total');
		});
	});

	describe('counter', () => {
		it('increments with no labels', () => {
			const m = createMetrics();
			const c = m.counter('hits_total', 'Total hits');
			c.inc();
			c.inc();
			c.inc(3);

			const output = m.serialize();
			expect(output).toContain('# TYPE hits_total counter');
			expect(output).toContain('hits_total 5');
		});

		it('increments with labels', () => {
			const m = createMetrics();
			const c = m.counter('http_requests_total', 'HTTP requests', ['method']);
			c.inc({ method: 'GET' });
			c.inc({ method: 'GET' });
			c.inc({ method: 'POST' });

			const output = m.serialize();
			expect(output).toContain('http_requests_total{method="GET"} 2');
			expect(output).toContain('http_requests_total{method="POST"} 1');
		});

		it('handles label values with special characters', () => {
			const m = createMetrics();
			const c = m.counter('test_total', 'Test', ['path']);
			c.inc({ path: 'hello "world"' });

			const output = m.serialize();
			expect(output).toContain('path="hello \\"world\\""');
		});
	});

	describe('gauge', () => {
		it('sets, increments, and decrements', () => {
			const m = createMetrics();
			const g = m.gauge('temperature', 'Temperature');
			g.set(20);
			g.inc(5);
			g.dec(3);

			const output = m.serialize();
			expect(output).toContain('# TYPE temperature gauge');
			expect(output).toContain('temperature 22');
		});

		it('supports collect callback', () => {
			const m = createMetrics();
			const g = m.gauge('dynamic_value', 'Dynamic');
			let currentValue = 42;
			g.collect(() => {
				g.set(currentValue);
			});

			currentValue = 42;
			let output = m.serialize();
			expect(output).toContain('dynamic_value 42');

			currentValue = 99;
			output = m.serialize();
			expect(output).toContain('dynamic_value 99');
		});

		it('supports labels', () => {
			const m = createMetrics();
			const g = m.gauge('connections', 'Connections', ['server']);
			g.set({ server: 'a' }, 10);
			g.set({ server: 'b' }, 20);

			const output = m.serialize();
			expect(output).toContain('connections{server="a"} 10');
			expect(output).toContain('connections{server="b"} 20');
		});
	});

	describe('histogram', () => {
		it('records observations with default buckets', () => {
			const m = createMetrics();
			const h = m.histogram('request_size', 'Request size');
			h.observe(3);
			h.observe(7);
			h.observe(42);

			const output = m.serialize();
			expect(output).toContain('# TYPE request_size histogram');
			expect(output).toContain('request_size_bucket{le="5"} 1');
			expect(output).toContain('request_size_bucket{le="10"} 2');
			expect(output).toContain('request_size_bucket{le="50"} 3');
			expect(output).toContain('request_size_bucket{le="+Inf"} 3');
			expect(output).toContain('request_size_sum 52');
			expect(output).toContain('request_size_count 3');
		});

		it('supports custom buckets', () => {
			const m = createMetrics();
			const h = m.histogram('latency', 'Latency', [], [10, 50, 100]);
			h.observe(5);
			h.observe(75);

			const output = m.serialize();
			expect(output).toContain('latency_bucket{le="10"} 1');
			expect(output).toContain('latency_bucket{le="50"} 1');
			expect(output).toContain('latency_bucket{le="100"} 2');
			expect(output).toContain('latency_bucket{le="+Inf"} 2');
		});

		it('supports labels', () => {
			const m = createMetrics();
			const h = m.histogram('size', 'Size', ['type'], [10, 100]);
			h.observe({ type: 'a' }, 5);
			h.observe({ type: 'b' }, 50);

			const output = m.serialize();
			expect(output).toContain('size_bucket{type="a",le="10"} 1');
			expect(output).toContain('size_bucket{type="b",le="10"} 0');
			expect(output).toContain('size_bucket{type="b",le="100"} 1');
		});
	});

	describe('label validation', () => {
		it('throws when calling inc() without labels on a labeled counter', () => {
			const m = createMetrics();
			const c = m.counter('x_total', 'help', ['room']);
			expect(() => c.inc()).toThrow('missing required labels');
		});

		it('throws when calling inc(n) without labels on a labeled counter', () => {
			const m = createMetrics();
			const c = m.counter('x_total', 'help', ['room']);
			expect(() => c.inc(5)).toThrow('missing required labels');
		});

		it('throws when a required label is missing from the labels object', () => {
			const m = createMetrics();
			const c = m.counter('x_total', 'help', ['room', 'user']);
			expect(() => c.inc({ room: 'lobby' })).toThrow('missing required label "user"');
		});

		it('throws when gauge.set() is called without required labels', () => {
			const m = createMetrics();
			const g = m.gauge('active', 'help', ['server']);
			expect(() => g.set(10)).toThrow('missing required labels');
		});

		it('throws when histogram.observe() is called without required labels', () => {
			const m = createMetrics();
			const h = m.histogram('latency', 'help', ['method']);
			expect(() => h.observe(42)).toThrow('missing required labels');
		});

		it('does not throw for unlabeled metrics called without labels', () => {
			const m = createMetrics();
			const c = m.counter('simple_total', 'help');
			expect(() => c.inc()).not.toThrow();
			expect(() => c.inc(3)).not.toThrow();
		});

		it('does not throw when all required labels are provided', () => {
			const m = createMetrics();
			const c = m.counter('x_total', 'help', ['room', 'user']);
			expect(() => c.inc({ room: 'lobby', user: 'alice' })).not.toThrow();
		});
	});

	describe('histogram negative-observation rejection (defense-in-depth)', () => {
		it('rejects a negative observation by default (mirrors counter.inc()`s rule)', () => {
			const m = createMetrics();
			const h = m.histogram('latency', 'help');
			expect(() => h.observe(-1)).toThrow('histogram value must not be negative');
			expect(() => h.observe(-0.5)).toThrow('histogram value must not be negative');
			expect(() => h.observe(Number.NEGATIVE_INFINITY)).toThrow();
		});

		it('rejects a negative observation with labels', () => {
			const m = createMetrics();
			const h = m.histogram('latency', 'help', ['method']);
			expect(() => h.observe({ method: 'GET' }, -1)).toThrow('histogram value must not be negative');
		});

		it('rejects a negative observation in the bare-value overload', () => {
			const m = createMetrics();
			const h = m.histogram('latency', 'help');
			// observe(value) (no labels arg) - the constructor swaps positional args internally.
			expect(() => h.observe(-42)).toThrow('histogram value must not be negative');
		});

		it('still accepts zero and positive observations', () => {
			const m = createMetrics();
			const h = m.histogram('latency', 'help');
			expect(() => h.observe(0)).not.toThrow();
			expect(() => h.observe(1)).not.toThrow();
			expect(() => h.observe(1.5)).not.toThrow();
			expect(() => h.observe(Number.MAX_SAFE_INTEGER)).not.toThrow();
		});

		it('allows negative observations when allowNegative: true is set at registration', () => {
			const m = createMetrics();
			// 5th positional arg is allowNegative.
			const h = m.histogram('signed_latency', 'help', [], [-100, -10, 0, 10, 100], true);
			expect(() => h.observe(-50)).not.toThrow();
			expect(() => h.observe(50)).not.toThrow();
			expect(() => h.observe(0)).not.toThrow();
			// The bucket counts should land correctly.
			const out = h.serialize();
			expect(out).toContain('signed_latency_count 3');
		});

		it('rejects NaN regardless of allowNegative (uses the existing assertFinite path)', () => {
			const m = createMetrics();
			const h1 = m.histogram('h1', 'help');
			const h2 = m.histogram('h2', 'help', [], undefined, true);
			expect(() => h1.observe(NaN)).toThrow();
			expect(() => h2.observe(NaN)).toThrow();
		});
	});

	describe('per-metric maxSeries cap', () => {
		let warnSpy;
		beforeEach(() => {
			warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			warnSpy.mockClear();
		});

		it('counter: drops new labelsets past the cap, keeps existing ones', () => {
			const m = createMetrics();
			const c = m.counter('drops', 'help', ['k'], 3);
			c.inc({ k: 'a' });
			c.inc({ k: 'b' });
			c.inc({ k: 'c' });
			// Existing keys keep getting incremented even after cap reached.
			c.inc({ k: 'a' }, 10);
			// New label values past the cap are silently dropped.
			c.inc({ k: 'd' });
			c.inc({ k: 'e' });

			const out = m.serialize();
			expect(out).toContain('drops{k="a"} 11');
			expect(out).toContain('drops{k="b"} 1');
			expect(out).toContain('drops{k="c"} 1');
			expect(out).not.toContain('k="d"');
			expect(out).not.toContain('k="e"');
		});

		it('gauge: drops new labelsets via set/inc/dec past the cap', () => {
			const m = createMetrics();
			const g = m.gauge('g', 'help', ['k'], 2);
			g.set({ k: 'a' }, 1);
			g.inc({ k: 'b' }, 5);
			g.set({ k: 'c' }, 100); // dropped
			g.inc({ k: 'd' }, 1);   // dropped
			g.dec({ k: 'e' }, 1);   // dropped

			const out = m.serialize();
			expect(out).toContain('g{k="a"} 1');
			expect(out).toContain('g{k="b"} 5');
			expect(out).not.toContain('k="c"');
			expect(out).not.toContain('k="d"');
			expect(out).not.toContain('k="e"');
		});

		it('histogram: drops new labelsets at observe time past the cap', () => {
			const m = createMetrics();
			const h = m.histogram('lat', 'help', ['route'], [10, 100], false, 2);
			h.observe({ route: '/a' }, 5);
			h.observe({ route: '/b' }, 50);
			h.observe({ route: '/c' }, 500); // dropped, no new series

			const out = m.serialize();
			expect(out).toContain('lat_count{route="/a"}');
			expect(out).toContain('lat_count{route="/b"}');
			expect(out).not.toContain('route="/c"');
		});

		it('warns once per metric and increments the lazy dropped-series counter', () => {
			const m = createMetrics();
			const c = m.counter('foo', 'help', ['k'], 1);
			c.inc({ k: 'a' });
			c.inc({ k: 'b' }); // first drop
			c.inc({ k: 'c' }); // second drop, no extra warn
			c.inc({ k: 'd' }); // third drop, still no extra warn

			expect(warnSpy).toHaveBeenCalledTimes(1);
			expect(warnSpy.mock.calls[0][0]).toContain('hit maxSeries cap 1');

			const out = m.serialize();
			expect(out).toContain('prometheus_series_dropped_total{metric="foo"} 3');
		});

		it('dropped-series counter is lazily registered - not present until first drop', () => {
			const m = createMetrics();
			m.counter('clean', 'help', ['k'], 5);
			expect(m.serialize()).not.toContain('prometheus_series_dropped_total');
		});

		it('per-metric maxSeries overrides the registry default', () => {
			const m = createMetrics({ maxSeries: 2 });
			const small = m.counter('small', 'help', ['k']);
			const big = m.counter('big', 'help', ['k'], 100);

			for (let i = 0; i < 50; i++) {
				small.inc({ k: 's' + i });
				big.inc({ k: 'b' + i });
			}

			const out = m.serialize();
			// small honored registry default (2); big honored its own override.
			expect((out.match(/^small\{/gm) || []).length).toBe(2);
			expect((out.match(/^big\{/gm) || []).length).toBe(50);
		});

		it('Infinity disables the cap', () => {
			const m = createMetrics({ maxSeries: Infinity });
			const c = m.counter('uncapped', 'help', ['k']);
			for (let i = 0; i < 50; i++) c.inc({ k: 'v' + i });
			const out = m.serialize();
			expect((out.match(/^uncapped\{/gm) || []).length).toBe(50);
		});

		it('rejects invalid maxSeries at construction (per-metric)', () => {
			const m = createMetrics();
			expect(() => m.counter('x', 'help', ['k'], 0)).toThrow('maxSeries');
			expect(() => m.counter('x', 'help', ['k'], -1)).toThrow('maxSeries');
			expect(() => m.counter('x', 'help', ['k'], 1.5)).toThrow('maxSeries');
		});

		it('rejects invalid maxSeries at construction (registry default)', () => {
			expect(() => createMetrics({ maxSeries: 0 })).toThrow('maxSeries');
			expect(() => createMetrics({ maxSeries: -1 })).toThrow('maxSeries');
			expect(() => createMetrics({ maxSeries: 1.5 })).toThrow('maxSeries');
		});

		it('dropped-series counter respects prefix', () => {
			const m = createMetrics({ prefix: 'app_' });
			const c = m.counter('foo', 'help', ['k'], 1);
			c.inc({ k: 'a' });
			c.inc({ k: 'b' });
			const out = m.serialize();
			expect(out).toContain('app_prometheus_series_dropped_total');
			expect(out).toContain('metric="app_foo"');
		});

		it('default cap is 10000 (large enough that typical metrics never hit it)', () => {
			const m = createMetrics();
			const c = m.counter('typical', 'help', ['k']);
			// Operate well under the default - sanity check the constant.
			for (let i = 0; i < 100; i++) c.inc({ k: 'v' + i });
			const out = m.serialize();
			expect(out).not.toContain('prometheus_series_dropped_total');
			expect((out.match(/^typical\{/gm) || []).length).toBe(100);
		});
	});

	describe('histogram maxBuckets cap', () => {
		it('rejects bucket arrays longer than the default 32', () => {
			const m = createMetrics();
			const tooMany = Array.from({ length: 33 }, (_, i) => i + 1);
			expect(() => m.histogram('h', 'help', [], tooMany))
				.toThrow('buckets length 33 exceeds maxBuckets 32');
		});

		it('accepts bucket arrays exactly at the cap', () => {
			const m = createMetrics();
			const exact = Array.from({ length: 32 }, (_, i) => i + 1);
			expect(() => m.histogram('h', 'help', [], exact)).not.toThrow();
		});

		it('default bucket array (DEFAULT_BUCKETS) is well under the cap', () => {
			const m = createMetrics();
			expect(() => m.histogram('h', 'help')).not.toThrow();
		});

		it('per-metric maxBuckets override beats registry default', () => {
			const m = createMetrics();
			// 7th positional arg overrides
			const tight = Array.from({ length: 10 }, (_, i) => i + 1);
			expect(() => m.histogram('h1', 'help', [], tight, false, undefined, 5))
				.toThrow('buckets length 10 exceeds maxBuckets 5');
			expect(() => m.histogram('h2', 'help', [], tight, false, undefined, 10)).not.toThrow();
		});

		it('registry-wide maxBuckets default applies to all histograms', () => {
			const m = createMetrics({ maxBuckets: 4 });
			expect(() => m.histogram('h1', 'help', [], [1, 2, 3, 4])).not.toThrow();
			expect(() => m.histogram('h2', 'help', [], [1, 2, 3, 4, 5]))
				.toThrow('exceeds maxBuckets 4');
		});

		it('Infinity disables the cap (opt-out)', () => {
			const m = createMetrics({ maxBuckets: Infinity });
			const lots = Array.from({ length: 200 }, (_, i) => i + 1);
			expect(() => m.histogram('big', 'help', [], lots)).not.toThrow();
		});

		it('rejects invalid maxBuckets at registry construction', () => {
			expect(() => createMetrics({ maxBuckets: 0 })).toThrow('maxBuckets must be a positive integer');
			expect(() => createMetrics({ maxBuckets: -1 })).toThrow('maxBuckets must be a positive integer');
			expect(() => createMetrics({ maxBuckets: 1.5 })).toThrow('maxBuckets must be a positive integer');
		});

		it('rejects invalid maxBuckets per-metric', () => {
			const m = createMetrics();
			expect(() => m.histogram('h', 'help', [], [1, 2], false, undefined, 0))
				.toThrow('maxBuckets');
			expect(() => m.histogram('h2', 'help', [], [1, 2], false, undefined, -1))
				.toThrow('maxBuckets');
			expect(() => m.histogram('h3', 'help', [], [1, 2], false, undefined, 1.5))
				.toThrow('maxBuckets');
		});

		it('rejects non-array buckets at registration', () => {
			const m = createMetrics();
			expect(() => m.histogram('h', 'help', [], 'not an array'))
				.toThrow('buckets must be an array');
		});

		it('error message points the developer at the configurable knobs', () => {
			const m = createMetrics();
			const tooMany = Array.from({ length: 50 }, (_, i) => i + 1);
			expect(() => m.histogram('h', 'help', [], tooMany))
				.toThrow('Pass a smaller bucket array');
		});
	});

	describe('serialize', () => {
		it('returns empty string for empty registry', () => {
			const m = createMetrics();
			expect(m.serialize()).toBe('');
		});

		it('includes HELP and TYPE lines', () => {
			const m = createMetrics();
			m.counter('foo_total', 'A foo counter');

			const output = m.serialize();
			expect(output).toContain('# HELP foo_total A foo counter');
			expect(output).toContain('# TYPE foo_total counter');
		});
	});

	describe('handler', () => {
		it('writes prometheus content type and body', () => {
			const m = createMetrics();
			m.counter('test_total', 'Test').inc();

			let status, headers = {}, body;
			const res = {
				writeStatus(s) { status = s; },
				writeHeader(k, v) { headers[k] = v; },
				end(b) { body = b; }
			};

			m.handler(res, {});

			expect(status).toBe('200 OK');
			expect(headers['Content-Type']).toBe('text/plain; version=0.0.4; charset=utf-8');
			expect(body).toContain('test_total 1');
		});
	});

	describe('authedHandler', () => {
		function mockRes() {
			const captured = { status: null, headers: {}, body: null, corked: false };
			const res = {
				_captured: captured,
				writeStatus(s) { captured.status = s; },
				writeHeader(k, v) { captured.headers[k] = v; },
				end(b) { captured.body = b; },
				cork(fn) { captured.corked = true; fn(); },
				onAborted(_fn) { captured.onAborted = _fn; }
			};
			return res;
		}

		it('throws if predicate is not a function', () => {
			const m = createMetrics();
			expect(() => m.authedHandler()).toThrow('predicate must be a function');
			expect(() => m.authedHandler('token')).toThrow('predicate must be a function');
			expect(() => m.authedHandler(123)).toThrow('predicate must be a function');
		});

		it('serves metrics when predicate returns true', async () => {
			const m = createMetrics();
			m.counter('hits_total', 'help').inc();
			const handler = m.authedHandler(() => true);
			const res = mockRes();

			await handler(res, {});

			expect(res._captured.status).toBe('200 OK');
			expect(res._captured.headers['Content-Type']).toBe('text/plain; version=0.0.4; charset=utf-8');
			expect(res._captured.body).toContain('hits_total 1');
			expect(res._captured.corked).toBe(true);
		});

		it('returns 401 when predicate returns false', async () => {
			const m = createMetrics();
			m.counter('hits_total', 'help').inc();
			const handler = m.authedHandler(() => false);
			const res = mockRes();

			await handler(res, {});

			expect(res._captured.status).toBe('401 Unauthorized');
			expect(res._captured.body).toBe('Unauthorized\n');
			// Body must not leak metrics data
			expect(res._captured.body).not.toContain('hits_total');
		});

		it('awaits async predicate', async () => {
			const m = createMetrics();
			m.counter('hits_total', 'help').inc();
			let resolved = false;
			const handler = m.authedHandler(async () => {
				await new Promise((r) => setTimeout(r, 5));
				resolved = true;
				return true;
			});
			const res = mockRes();

			await handler(res, {});

			expect(resolved).toBe(true);
			expect(res._captured.status).toBe('200 OK');
		});

		it('predicate that throws is treated as denial (no info leak)', async () => {
			const m = createMetrics();
			const handler = m.authedHandler(() => {
				throw new Error('database is on fire');
			});
			const res = mockRes();

			await handler(res, {});

			expect(res._captured.status).toBe('401 Unauthorized');
			expect(res._captured.body).toBe('Unauthorized\n');
			// Error message must not leak
			expect(res._captured.body).not.toContain('database is on fire');
		});

		it('predicate that rejects is treated as denial', async () => {
			const m = createMetrics();
			const handler = m.authedHandler(async () => {
				throw new Error('async fail');
			});
			const res = mockRes();

			await handler(res, {});

			expect(res._captured.status).toBe('401 Unauthorized');
		});

		it('predicate receives res and req', async () => {
			const m = createMetrics();
			let receivedRes, receivedReq;
			const handler = m.authedHandler((res, req) => {
				receivedRes = res;
				receivedReq = req;
				return true;
			});
			const res = mockRes();
			const req = { getHeader: () => 'ok' };

			await handler(res, req);

			expect(receivedRes).toBe(res);
			expect(receivedReq).toBe(req);
		});

		it('token-based predicate pattern works end-to-end', async () => {
			const m = createMetrics();
			m.counter('hits_total', 'help').inc();
			const expectedToken = 'secret-token';
			const handler = m.authedHandler((res, req) => req.getHeader('x-scrape-token') === expectedToken);

			// Wrong token
			const res1 = mockRes();
			await handler(res1, { getHeader: () => 'wrong' });
			expect(res1._captured.status).toBe('401 Unauthorized');

			// Right token
			const res2 = mockRes();
			await handler(res2, { getHeader: (h) => h === 'x-scrape-token' ? expectedToken : '' });
			expect(res2._captured.status).toBe('200 OK');
			expect(res2._captured.body).toContain('hits_total 1');
		});

		it('does not write response when client aborted before predicate resolved', async () => {
			const m = createMetrics();
			const handler = m.authedHandler(async () => {
				await new Promise((r) => setTimeout(r, 10));
				return true;
			});
			const res = mockRes();
			// Simulate abort: fire onAborted synchronously before predicate resolves
			const promise = handler(res, {});
			res._captured.onAborted();
			await promise;

			expect(res._captured.status).toBe(null);
			expect(res._captured.body).toBe(null);
		});
	});

	describe('mapTopic', () => {
		it('applies mapTopic to topic labels', () => {
			const m = createMetrics({
				mapTopic: (t) => t.startsWith('room:') ? 'room:*' : t
			});
			expect(m.mapTopic('room:123')).toBe('room:*');
			expect(m.mapTopic('chat')).toBe('chat');
		});

		it('defaults to identity', () => {
			const m = createMetrics();
			expect(m.mapTopic('anything')).toBe('anything');
		});

		it('rejects non-function mapTopic', () => {
			expect(() => createMetrics({ mapTopic: 'bad' })).toThrow('mapTopic must be a function');
			expect(() => createMetrics({ mapTopic: 42 })).toThrow('mapTopic must be a function');
		});
	});

	describe('labelNames validation', () => {
		it('rejects non-array labelNames for counter', () => {
			const m = createMetrics();
			expect(() => m.counter('c_total', 'help', 'room')).toThrow('labelNames must be an array');
		});

		it('rejects non-array labelNames for gauge', () => {
			const m = createMetrics();
			expect(() => m.gauge('g_val', 'help', 'server')).toThrow('labelNames must be an array');
		});

		it('rejects non-array labelNames for histogram', () => {
			const m = createMetrics();
			expect(() => m.histogram('h_dur', 'help', 'method')).toThrow('labelNames must be an array');
		});

		it('rejects duplicate labelNames', () => {
			const m = createMetrics();
			expect(() => m.counter('d_total', 'help', ['room', 'room'])).toThrow('duplicate label');
			expect(() => m.gauge('d_val', 'help', ['a', 'b', 'a'])).toThrow('duplicate label');
		});

		it('accepts array labelNames', () => {
			const m = createMetrics();
			expect(() => m.counter('ok_total', 'help', ['room'])).not.toThrow();
		});
	});

	describe('pubsub integration', () => {
		let client, platform, metrics;

		beforeEach(() => {
			client = mockRedisClient();
			platform = mockPlatform();
			metrics = createMetrics();
		});

		it('counts relayed messages and batch sizes', async () => {
			const bus = createPubSubBus(client, { metrics });
			const wrapped = bus.wrap(platform);

			wrapped.publish('chat', 'msg', { text: 'a' });
			wrapped.publish('chat', 'msg', { text: 'b' });
			await new Promise((r) => setTimeout(r, 10));

			const output = metrics.serialize();
			expect(output).toContain('pubsub_messages_relayed_total 2');
			expect(output).toContain('pubsub_relay_batch_size');
		});

		it('counts received and echo-suppressed messages', async () => {
			const bus = createPubSubBus(client, { metrics });
			await bus.activate(platform);

			// Message from another instance
			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'other',
				topic: 'chat',
				event: 'msg',
				data: null
			}));

			// Echo from same instance would need to know the instanceId,
			// so just verify the received counter works
			const output = metrics.serialize();
			expect(output).toContain('pubsub_messages_received_total 1');

			await bus.deactivate();
		});
	});

	describe('presence integration', () => {
		let client, platform, metrics;

		beforeEach(() => {
			client = mockRedisClient();
			platform = mockPlatform();
			metrics = createMetrics();
		});

		it('counts joins and leaves', async () => {
			const presence = createPresence(client, { metrics, key: 'id' });
			const ws = mockWs({ id: 'user1', name: 'Alice' });

			await presence.join(ws, 'room', platform);

			let output = metrics.serialize();
			expect(output).toContain('presence_joins_total{topic="room"} 1');

			await presence.leave(ws, platform, 'room');

			output = metrics.serialize();
			expect(output).toContain('presence_leaves_total{topic="room"} 1');

			presence.destroy();
		});

		it('works without metrics (zero overhead)', async () => {
			const presence = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: 'user1' });

			await presence.join(ws, 'room', platform);
			await presence.leave(ws, platform, 'room');
			// Should not throw
			presence.destroy();
		});
	});

	describe('replay (redis) integration', () => {
		let client, platform, metrics;

		beforeEach(() => {
			client = mockRedisClient();
			platform = mockPlatform();
			metrics = createMetrics();
		});

		it('counts publishes', async () => {
			const replay = createRedisReplay(client, { metrics });
			await replay.publish(platform, 'chat', 'msg', { text: 'hi' });
			await replay.publish(platform, 'chat', 'msg', { text: 'ho' });

			const output = metrics.serialize();
			expect(output).toContain('replay_publishes_total{topic="chat"} 2');
		});

		it('counts replayed messages', async () => {
			const replay = createRedisReplay(client, { metrics });
			await replay.publish(platform, 'chat', 'msg', { text: 'a' });
			await replay.publish(platform, 'chat', 'msg', { text: 'b' });
			await replay.publish(platform, 'chat', 'msg', { text: 'c' });

			const ws = mockWs();
			await replay.replay(ws, 'chat', 0, platform);

			const output = metrics.serialize();
			expect(output).toContain('replay_messages_replayed_total{topic="chat"} 3');
		});
	});

	describe('ratelimit integration', () => {
		let client, metrics;

		beforeEach(() => {
			client = mockRedisClient();
			metrics = createMetrics();
		});

		it('counts allowed and denied requests', async () => {
			const limiter = createRateLimit(client, {
				points: 2,
				interval: 60000,
				metrics
			});
			const ws = mockWs({ remoteAddress: '1.2.3.4' });

			await limiter.consume(ws);
			await limiter.consume(ws);
			await limiter.consume(ws); // should be denied

			const output = metrics.serialize();
			expect(output).toContain('ratelimit_allowed_total 2');
			expect(output).toContain('ratelimit_denied_total 1');
		});

		it('counts bans', async () => {
			const limiter = createRateLimit(client, {
				points: 10,
				interval: 60000,
				metrics
			});

			await limiter.ban('bad-actor', 5000);

			const output = metrics.serialize();
			expect(output).toContain('ratelimit_bans_total 1');
		});
	});

	describe('groups integration', () => {
		let client, platform, metrics;

		beforeEach(() => {
			client = mockRedisClient();
			platform = mockPlatform();
			metrics = createMetrics();
		});

		it('counts joins, leaves, and publishes', async () => {
			const group = createGroup(client, 'lobby', { metrics });
			const ws = mockWs({ id: 'u1' });

			await group.join(ws, platform);
			await group.publish(platform, 'chat', { msg: 'hi' });
			await group.leave(ws, platform);

			const output = metrics.serialize();
			expect(output).toContain('group_joins_total{group="lobby"} 1');
			expect(output).toContain('group_leaves_total{group="lobby"} 1');
			expect(output).toContain('group_publishes_total{group="lobby"} 1');

			group.destroy();
		});

		it('counts rejected joins', async () => {
			const group = createGroup(client, 'tiny', { maxMembers: 1, metrics });
			const ws1 = mockWs({ id: 'u1' });
			const ws2 = mockWs({ id: 'u2' });

			await group.join(ws1, platform);
			await group.join(ws2, platform);

			const output = metrics.serialize();
			expect(output).toContain('group_joins_total{group="tiny"} 1');
			expect(output).toContain('group_joins_rejected_total{group="tiny"} 1');

			group.destroy();
		});
	});

	describe('cursor integration', () => {
		let client, platform, metrics;

		beforeEach(() => {
			client = mockRedisClient();
			platform = mockPlatform();
			metrics = createMetrics();
		});

		it('counts updates and broadcasts', async () => {
			const cursor = createCursor(client, {
				throttle: 0,
				metrics
			});
			const ws = mockWs({ id: 'u1' });

			cursor.update(ws, 'doc', { x: 10, y: 20 }, platform);
			cursor.update(ws, 'doc', { x: 30, y: 40 }, platform);

			const output = metrics.serialize();
			expect(output).toContain('cursor_updates_total{topic="doc"} 2');
			expect(output).toContain('cursor_broadcasts_total{topic="doc"}');

			cursor.destroy();
		});

		it('counts throttled updates', async () => {
			const cursor = createCursor(client, {
				throttle: 100000,
				metrics
			});
			const ws = mockWs({ id: 'u1' });

			cursor.update(ws, 'doc', { x: 1 }, platform);
			cursor.update(ws, 'doc', { x: 2 }, platform);
			cursor.update(ws, 'doc', { x: 3 }, platform);

			const output = metrics.serialize();
			expect(output).toContain('cursor_updates_total{topic="doc"} 3');
			expect(output).toContain('cursor_throttled_total{topic="doc"} 2');

			cursor.destroy();
		});
	});

	describe('replay (postgres) integration', () => {
		let pgClient, platform, metrics;

		beforeEach(() => {
			pgClient = mockPgClient();
			platform = mockPlatform();
			metrics = createMetrics();
		});

		it('counts publishes', async () => {
			const replay = createPgReplay(pgClient, { metrics, cleanupInterval: 0 });
			await replay.publish(platform, 'events', 'created', { id: 1 });
			await replay.publish(platform, 'events', 'created', { id: 2 });

			const output = metrics.serialize();
			expect(output).toContain('replay_publishes_total{topic="events"} 2');

			replay.destroy();
		});

		it('counts replayed messages', async () => {
			const replay = createPgReplay(pgClient, { metrics, cleanupInterval: 0 });
			await replay.publish(platform, 'events', 'created', { id: 1 });
			await replay.publish(platform, 'events', 'created', { id: 2 });

			const ws = mockWs();
			await replay.replay(ws, 'events', 0, platform);

			const output = metrics.serialize();
			expect(output).toContain('replay_messages_replayed_total{topic="events"} 2');

			replay.destroy();
		});
	});

	describe('notify integration', () => {
		it('works without metrics (no crash)', () => {
			const pgClient = {
				pool: {},
				query: async () => ({ rows: [] }),
				createClient: () => ({
					on: () => {},
					connect: async () => {},
					query: async () => {},
					end: async () => {},
					removeListener: () => {}
				}),
				end: async () => {}
			};

			const bridge = createNotifyBridge(pgClient, { channel: 'test' });
			expect(bridge).toBeDefined();
			expect(typeof bridge.activate).toBe('function');
		});
	});

	describe('metric name validation', () => {
		it('rejects invalid metric names', () => {
			const m = createMetrics();
			expect(() => m.counter('bad name', 'help')).toThrow('invalid Prometheus metric name');
			expect(() => m.counter('123start', 'help')).toThrow('invalid Prometheus metric name');
			expect(() => m.counter('has-dash', 'help')).toThrow('invalid Prometheus metric name');
			expect(() => m.counter('has.dot', 'help')).toThrow('invalid Prometheus metric name');
		});

		it('accepts valid metric names', () => {
			const m = createMetrics();
			expect(() => m.counter('valid_name', 'help')).not.toThrow();
			expect(() => m.counter('_leading_underscore', 'help')).not.toThrow();
			expect(() => m.counter('with:colon', 'help')).not.toThrow();
			expect(() => m.gauge('UPPERCASE', 'help')).not.toThrow();
			expect(() => m.histogram('mix_123', 'help')).not.toThrow();
		});

		it('validates metric names with prefix', () => {
			const m = createMetrics({ prefix: 'app_' });
			expect(() => m.counter('valid_total', 'help')).not.toThrow();
		});
	});

	describe('label name validation', () => {
		it('rejects invalid label names', () => {
			const m = createMetrics();
			expect(() => m.counter('x_total', 'help', ['bad-label'])).toThrow('invalid Prometheus label name');
			expect(() => m.counter('y_total', 'help', ['123start'])).toThrow('invalid Prometheus label name');
			expect(() => m.counter('z_total', 'help', ['has space'])).toThrow('invalid Prometheus label name');
		});

		it('rejects __ prefixed label names', () => {
			const m = createMetrics();
			expect(() => m.counter('x_total', 'help', ['__reserved'])).toThrow('__ prefix is reserved');
		});

		it('accepts valid label names', () => {
			const m = createMetrics();
			expect(() => m.counter('x_total', 'help', ['valid'])).not.toThrow();
			expect(() => m.counter('y_total', 'help', ['_underscore'])).not.toThrow();
			expect(() => m.counter('z_total', 'help', ['camelCase'])).not.toThrow();
		});
	});

	describe('HELP text escaping', () => {
		it('escapes backslashes and newlines in HELP text', () => {
			const m = createMetrics();
			m.counter('esc_total', 'line1\nline2');
			const output = m.serialize();
			expect(output).toContain('# HELP esc_total line1\\nline2');
			expect(output).not.toContain('# HELP esc_total line1\nline2');
		});

		it('escapes backslashes in HELP text', () => {
			const m = createMetrics();
			m.counter('bs_total', 'path\\to\\file');
			const output = m.serialize();
			expect(output).toContain('# HELP bs_total path\\\\to\\\\file');
		});
	});

	describe('invalid schema output prevention', () => {
		it('rejects counter with spaces in name', () => {
			const m = createMetrics();
			expect(() => m.counter('bad name', 'help')).toThrow('invalid Prometheus metric name');
		});

		it('rejects gauge with invalid label', () => {
			const m = createMetrics();
			expect(() => m.gauge('test', 'help', ['bad-label'])).toThrow('invalid Prometheus label name');
		});

		it('rejects histogram with invalid metric name', () => {
			const m = createMetrics();
			expect(() => m.histogram('123bad', 'help')).toThrow('invalid Prometheus metric name');
		});
	});

	describe('help text validation', () => {
		it('rejects non-string help for counter', () => {
			const m = createMetrics();
			expect(() => m.counter('x_total', 42)).toThrow('help must be a string');
			expect(() => m.counter('y_total', null)).toThrow('help must be a string');
			expect(() => m.counter('z_total', undefined)).toThrow('help must be a string');
		});

		it('rejects non-string help for gauge', () => {
			const m = createMetrics();
			expect(() => m.gauge('x_val', {})).toThrow('help must be a string');
		});

		it('rejects non-string help for histogram', () => {
			const m = createMetrics();
			expect(() => m.histogram('x_dur', false)).toThrow('help must be a string');
		});
	});

	describe('histogram bucket sorting', () => {
		it('sorts unsorted custom buckets', () => {
			const m = createMetrics();
			const h = m.histogram('sorted_test', 'test', [], [100, 10, 50]);
			h.observe(5);
			h.observe(75);

			const output = m.serialize();
			const bucketLines = output.split('\n').filter((l) => l.includes('_bucket'));
			const leValues = bucketLines.map((l) => {
				const match = l.match(/le="([^"]+)"/);
				return match ? match[1] : null;
			}).filter(Boolean);

			expect(leValues).toEqual(['10', '50', '100', '+Inf']);
		});

		it('sorted buckets produce correct counts', () => {
			const m = createMetrics();
			const h = m.histogram('sort_counts', 'test', [], [100, 10, 50]);
			h.observe(5);
			h.observe(75);

			const output = m.serialize();
			expect(output).toContain('sort_counts_bucket{le="10"} 1');
			expect(output).toContain('sort_counts_bucket{le="50"} 1');
			expect(output).toContain('sort_counts_bucket{le="100"} 2');
			expect(output).toContain('sort_counts_bucket{le="+Inf"} 2');
		});
	});

	describe('histogram bucket validation', () => {
		it('rejects NaN bucket values', () => {
			const m = createMetrics();
			expect(() => m.histogram('x_dur', 'help', [], [1, NaN, 10])).toThrow('finite numbers');
		});

		it('rejects Infinity bucket values', () => {
			const m = createMetrics();
			expect(() => m.histogram('x_dur', 'help', [], [1, Infinity])).toThrow('finite numbers');
		});

		it('rejects -Infinity bucket values', () => {
			const m = createMetrics();
			expect(() => m.histogram('x_dur', 'help', [], [-Infinity, 1])).toThrow('finite numbers');
		});

		it('deduplicates bucket values', () => {
			const m = createMetrics();
			const h = m.histogram('dedup_test', 'test', [], [1, 5, 1, 10, 5]);
			h.observe(3);

			const output = m.serialize();
			const leValues = output.split('\n')
				.filter((l) => l.includes('_bucket') && !l.includes('+Inf'))
				.map((l) => l.match(/le="([^"]+)"/)?.[1])
				.filter(Boolean);

			expect(leValues).toEqual(['1', '5', '10']);
		});

		it('rejects non-number bucket values', () => {
			const m = createMetrics();
			expect(() => m.histogram('x_dur', 'help', [], [1, 'ten'])).toThrow('finite numbers');
		});
	});

	describe('sample value validation', () => {
		it('counter.inc() rejects NaN', () => {
			const m = createMetrics();
			const c = m.counter('c_total', 'help');
			expect(() => c.inc(NaN)).toThrow('finite number');
		});

		it('counter.inc() rejects Infinity', () => {
			const m = createMetrics();
			const c = m.counter('c_total', 'help');
			expect(() => c.inc(Infinity)).toThrow('finite number');
		});

		it('gauge.set() rejects undefined', () => {
			const m = createMetrics();
			const g = m.gauge('g_val', 'help');
			expect(() => g.set(undefined)).toThrow('finite number');
		});

		it('gauge.set() rejects Infinity', () => {
			const m = createMetrics();
			const g = m.gauge('g_val', 'help');
			expect(() => g.set(Infinity)).toThrow('finite number');
		});

		it('gauge.inc() rejects NaN', () => {
			const m = createMetrics();
			const g = m.gauge('g_val', 'help');
			expect(() => g.inc(NaN)).toThrow('finite number');
		});

		it('gauge.dec() rejects Infinity', () => {
			const m = createMetrics();
			const g = m.gauge('g_val', 'help');
			expect(() => g.dec(Infinity)).toThrow('finite number');
		});

		it('histogram.observe() rejects Infinity', () => {
			const m = createMetrics();
			const h = m.histogram('h_dur', 'help');
			expect(() => h.observe(Infinity)).toThrow('finite number');
		});

		it('histogram.observe() rejects NaN', () => {
			const m = createMetrics();
			const h = m.histogram('h_dur', 'help');
			expect(() => h.observe(NaN)).toThrow('finite number');
		});
	});

	describe('mapTopic in extensions', () => {
		it('maps topic labels through mapTopic', async () => {
			const client = mockRedisClient();
			const platform = mockPlatform();
			const metrics = createMetrics({
				mapTopic: (t) => t.startsWith('room:') ? 'room:*' : t
			});

			const replay = createRedisReplay(client, { metrics });
			await replay.publish(platform, 'room:123', 'msg', { text: 'a' });
			await replay.publish(platform, 'room:456', 'msg', { text: 'b' });
			await replay.publish(platform, 'global', 'msg', { text: 'c' });

			const output = metrics.serialize();
			expect(output).toContain('replay_publishes_total{topic="room:*"} 2');
			expect(output).toContain('replay_publishes_total{topic="global"} 1');
		});
	});

	describe('wirePublishRateMetrics', () => {
		it('throws on missing platform / metrics', () => {
			const m = createMetrics();
			expect(() => wirePublishRateMetrics(null, m)).toThrow('platform is required');
			expect(() => wirePublishRateMetrics({ pressure: {} }, null)).toThrow('metrics registry is required');
		});

		it('throws on invalid topN', () => {
			const m = createMetrics();
			const platform = mockPlatform();
			expect(() => wirePublishRateMetrics(platform, m, { topN: 0 })).toThrow('topN must be a positive integer');
			expect(() => wirePublishRateMetrics(platform, m, { topN: -1 })).toThrow('topN must be a positive integer');
			expect(() => wirePublishRateMetrics(platform, m, { topN: 1.5 })).toThrow('topN must be a positive integer');
		});

		it('emits zero gauges when pressure has no topPublishers', () => {
			const m = createMetrics();
			const platform = mockPlatform();
			wirePublishRateMetrics(platform, m);

			const output = m.serialize();
			expect(output).toContain('# TYPE ws_topic_publish_rate gauge');
			expect(output).toContain('# TYPE ws_topic_publish_bytes gauge');
			// No samples emitted since there are no topPublishers.
			expect(output).not.toMatch(/ws_topic_publish_rate\{/);
			expect(output).not.toMatch(/ws_topic_publish_bytes\{/);
		});

		it('reads topPublishers from the pressure snapshot at scrape time', () => {
			const m = createMetrics();
			const platform = mockPlatform();
			wirePublishRateMetrics(platform, m);

			platform._setPressure({
				active: false,
				subscriberRatio: 0,
				publishRate: 0,
				memoryMB: 0,
				reason: 'NONE',
				topPublishers: [
					{ topic: 'chat', messagesPerSec: 1500, bytesPerSec: 32000 },
					{ topic: 'cursors', messagesPerSec: 800, bytesPerSec: 12000 }
				]
			});

			const output = m.serialize();
			expect(output).toContain('ws_topic_publish_rate{topic="chat"} 1500');
			expect(output).toContain('ws_topic_publish_rate{topic="cursors"} 800');
			expect(output).toContain('ws_topic_publish_bytes{topic="chat"} 32000');
			expect(output).toContain('ws_topic_publish_bytes{topic="cursors"} 12000');
		});

		it('caps cardinality at topN', () => {
			const m = createMetrics();
			const platform = mockPlatform();
			wirePublishRateMetrics(platform, m, { topN: 2 });

			platform._setPressure({
				active: false, reason: 'NONE',
				subscriberRatio: 0, publishRate: 0, memoryMB: 0,
				topPublishers: [
					{ topic: 'a', messagesPerSec: 10, bytesPerSec: 100 },
					{ topic: 'b', messagesPerSec: 9,  bytesPerSec: 90 },
					{ topic: 'c', messagesPerSec: 8,  bytesPerSec: 80 },
					{ topic: 'd', messagesPerSec: 7,  bytesPerSec: 70 }
				]
			});

			const output = m.serialize();
			expect(output).toContain('ws_topic_publish_rate{topic="a"} 10');
			expect(output).toContain('ws_topic_publish_rate{topic="b"} 9');
			expect(output).not.toMatch(/ws_topic_publish_rate\{topic="c"\}/);
			expect(output).not.toMatch(/ws_topic_publish_rate\{topic="d"\}/);
		});

		it('honors mapTopic from the registry', () => {
			const m = createMetrics({
				mapTopic: (t) => t.startsWith('chat:') ? 'chat:*' : t
			});
			const platform = mockPlatform();
			wirePublishRateMetrics(platform, m);

			platform._setPressure({
				active: false, reason: 'NONE',
				subscriberRatio: 0, publishRate: 0, memoryMB: 0,
				topPublishers: [
					{ topic: 'chat:room1', messagesPerSec: 100, bytesPerSec: 1000 },
					{ topic: 'chat:room2', messagesPerSec: 50,  bytesPerSec: 500 }
				]
			});

			const output = m.serialize();
			// Both rooms collapse onto the same label; the second `set()`
			// overwrites the first, so the visible value is the last entry's.
			expect(output).toContain('ws_topic_publish_rate{topic="chat:*"} 50');
		});

		it('inline mapTopic option overrides the registry mapTopic', () => {
			const m = createMetrics({ mapTopic: () => 'wrong' });
			const platform = mockPlatform();
			wirePublishRateMetrics(platform, m, { mapTopic: (t) => 'override-' + t });

			platform._setPressure({
				active: false, reason: 'NONE',
				subscriberRatio: 0, publishRate: 0, memoryMB: 0,
				topPublishers: [{ topic: 'a', messagesPerSec: 1, bytesPerSec: 1 }]
			});

			const output = m.serialize();
			expect(output).toContain('ws_topic_publish_rate{topic="override-a"} 1');
		});

		it('reflects pressure changes between scrapes (collect at scrape time)', () => {
			const m = createMetrics();
			const platform = mockPlatform();
			wirePublishRateMetrics(platform, m);

			platform._setPressure({
				active: false, reason: 'NONE',
				subscriberRatio: 0, publishRate: 0, memoryMB: 0,
				topPublishers: [{ topic: 'a', messagesPerSec: 100, bytesPerSec: 1000 }]
			});
			expect(m.serialize()).toContain('ws_topic_publish_rate{topic="a"} 100');

			platform._setPressure({
				active: false, reason: 'NONE',
				subscriberRatio: 0, publishRate: 0, memoryMB: 0,
				topPublishers: [{ topic: 'a', messagesPerSec: 50, bytesPerSec: 500 }]
			});
			expect(m.serialize()).toContain('ws_topic_publish_rate{topic="a"} 50');
		});
	});

	describe('connectionMetricsHook', () => {
		it('throws on missing metrics', () => {
			expect(() => connectionMetricsHook(null)).toThrow('metrics registry is required');
		});

		it('throws when userClose is not a function', () => {
			const m = createMetrics();
			expect(() => connectionMetricsHook(m, 'nope')).toThrow('userClose must be a function');
		});

		it('returns a function with the close-hook signature', () => {
			const m = createMetrics();
			const hook = connectionMetricsHook(m);
			expect(typeof hook).toBe('function');
			// Take two parameters but tolerate any arity at call time.
			expect(hook.length).toBeGreaterThanOrEqual(2);
		});

		it('observes duration / messages / bytes from ctx', async () => {
			const m = createMetrics();
			const close = connectionMetricsHook(m);

			await close({}, {
				code: 1000,
				duration: 5000,         // 5 seconds
				messagesIn: 12,
				messagesOut: 8,
				bytesIn: 2048,
				bytesOut: 1024
			});

			const out = m.serialize();
			expect(out).toContain('ws_connection_duration_seconds_count 1');
			expect(out).toContain('ws_connection_duration_seconds_sum 5');
			expect(out).toContain('ws_connection_messages_in_count 1');
			expect(out).toContain('ws_connection_messages_in_sum 12');
			expect(out).toContain('ws_connection_messages_out_sum 8');
			expect(out).toContain('ws_connection_bytes_in_sum 2048');
			expect(out).toContain('ws_connection_bytes_out_sum 1024');
		});

		it('counts close codes as labels', async () => {
			const m = createMetrics();
			const close = connectionMetricsHook(m);

			await close({}, { code: 1000, duration: 100, messagesIn: 0, messagesOut: 0, bytesIn: 0, bytesOut: 0 });
			await close({}, { code: 1000, duration: 100, messagesIn: 0, messagesOut: 0, bytesIn: 0, bytesOut: 0 });
			await close({}, { code: 1006, duration: 100, messagesIn: 0, messagesOut: 0, bytesIn: 0, bytesOut: 0 });

			const out = m.serialize();
			expect(out).toContain('ws_connection_close_total{code="1000"} 2');
			expect(out).toContain('ws_connection_close_total{code="1006"} 1');
		});

		it('skips fields that are missing from ctx', async () => {
			const m = createMetrics();
			const close = connectionMetricsHook(m);
			await close({}, { code: 1000 }); // only code; no telemetry fields

			const out = m.serialize();
			expect(out).toContain('ws_connection_close_total{code="1000"} 1');
			// Histograms with zero observations emit only TYPE / HELP lines,
			// no _count or _sum samples.
			expect(out).not.toMatch(/ws_connection_duration_seconds_count\b/);
			expect(out).not.toMatch(/ws_connection_messages_in_count\b/);
		});

		it('skips negative-valued telemetry without observing', async () => {
			const m = createMetrics();
			const close = connectionMetricsHook(m);
			await close({}, { code: 1000, duration: -1, messagesIn: -5, messagesOut: 5, bytesIn: 0, bytesOut: 0 });

			const out = m.serialize();
			// duration and messagesIn were negative -> not observed.
			expect(out).not.toMatch(/ws_connection_duration_seconds_count\b/);
			expect(out).not.toMatch(/ws_connection_messages_in_count\b/);
			// messagesOut was non-negative -> observed.
			expect(out).toContain('ws_connection_messages_out_count 1');
			expect(out).toContain('ws_connection_messages_out_sum 5');
		});

		it('composes with a user-supplied close hook (called after metrics)', async () => {
			const m = createMetrics();
			const order = [];
			const userClose = async (ws, ctx) => { order.push(['user', ctx.code]); };
			const close = connectionMetricsHook(m, userClose);

			await close({}, {
				code: 4001,
				duration: 250, messagesIn: 1, messagesOut: 1, bytesIn: 100, bytesOut: 100
			});

			expect(order).toEqual([['user', 4001]]);
			expect(m.serialize()).toContain('ws_connection_close_total{code="4001"} 1');
		});

		it('does not throw if ctx is undefined', async () => {
			const m = createMetrics();
			const close = connectionMetricsHook(m);
			await expect(close({}, undefined)).resolves.toBeUndefined();
		});
	});

	describe('wireClusterPublishRateMetrics', () => {
		function makeAggregator(topPublishers) {
			return { topPublishers };
		}

		it('rejects bad inputs', () => {
			const m = createMetrics();
			expect(() => wireClusterPublishRateMetrics(null, m)).toThrow('aggregator is required');
			expect(() => wireClusterPublishRateMetrics({ topPublishers: [] }, null)).toThrow('metrics registry is required');
			const agg = makeAggregator([]);
			expect(() => wireClusterPublishRateMetrics(agg, m, { topN: 0 })).toThrow('topN must be a positive integer');
			expect(() => wireClusterPublishRateMetrics(agg, m, { topN: 1.5 })).toThrow('topN must be a positive integer');
		});

		it('emits cluster_topic_publish_rate and cluster_topic_publish_bytes from aggregator.topPublishers at scrape time', async () => {
			const m = createMetrics();
			const agg = makeAggregator([
				{ topic: 'chat:room1', messagesPerSec: 300, bytesPerSec: 3000 },
				{ topic: 'audit:org1', messagesPerSec: 50, bytesPerSec: 500 }
			]);
			wireClusterPublishRateMetrics(agg, m);

			const out = await m.serialize();
			expect(out).toMatch(/cluster_topic_publish_rate\{topic="chat:room1"\}\s+300/);
			expect(out).toMatch(/cluster_topic_publish_rate\{topic="audit:org1"\}\s+50/);
			expect(out).toMatch(/cluster_topic_publish_bytes\{topic="chat:room1"\}\s+3000/);
		});

		it('caps emitted gauges at topN', async () => {
			const m = createMetrics();
			const agg = makeAggregator([
				{ topic: 'a', messagesPerSec: 5, bytesPerSec: 50 },
				{ topic: 'b', messagesPerSec: 4, bytesPerSec: 40 },
				{ topic: 'c', messagesPerSec: 3, bytesPerSec: 30 }
			]);
			wireClusterPublishRateMetrics(agg, m, { topN: 2 });

			const out = await m.serialize();
			expect(out).toMatch(/cluster_topic_publish_rate\{topic="a"\}/);
			expect(out).toMatch(/cluster_topic_publish_rate\{topic="b"\}/);
			expect(out).not.toMatch(/cluster_topic_publish_rate\{topic="c"\}/);
		});

		it('honors mapTopic for cardinality control', async () => {
			const m = createMetrics();
			const agg = makeAggregator([
				{ topic: 'org:42:items', messagesPerSec: 10, bytesPerSec: 100 }
			]);
			wireClusterPublishRateMetrics(agg, m, {
				mapTopic: (t) => t.split(':')[0]
			});

			const out = await m.serialize();
			expect(out).toMatch(/cluster_topic_publish_rate\{topic="org"\}\s+10/);
			expect(out).not.toMatch(/cluster_topic_publish_rate\{topic="org:42:items"\}/);
		});

		it('handles an empty topPublishers list', async () => {
			const m = createMetrics();
			const agg = makeAggregator([]);
			wireClusterPublishRateMetrics(agg, m);

			const out = await m.serialize();
			expect(out).toMatch(/# TYPE cluster_topic_publish_rate gauge/);
			expect(out).not.toMatch(/cluster_topic_publish_rate\{/);
		});

		it('reflects aggregator updates between scrapes', async () => {
			const m = createMetrics();
			let snap = [{ topic: 'a', messagesPerSec: 1, bytesPerSec: 10 }];
			const agg = { get topPublishers() { return snap; } };
			wireClusterPublishRateMetrics(agg, m);

			let out = await m.serialize();
			expect(out).toMatch(/cluster_topic_publish_rate\{topic="a"\}\s+1/);

			snap = [{ topic: 'a', messagesPerSec: 999, bytesPerSec: 9999 }];
			out = await m.serialize();
			expect(out).toMatch(/cluster_topic_publish_rate\{topic="a"\}\s+999/);
		});
	});
});
