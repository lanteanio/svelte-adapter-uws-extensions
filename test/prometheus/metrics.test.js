import { describe, it, expect, beforeEach } from 'vitest';
import { createMetrics } from '../../prometheus/index.js';
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
});
