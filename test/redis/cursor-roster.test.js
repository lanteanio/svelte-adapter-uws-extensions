import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createRoster } from '../../src/redis/cursor/roster.js';
import { resetRuntimeEnv, setRuntimeEnv } from '../../src/shared/runtime.js';
import { installSteppedClock } from '../helpers/stepped-clock.js';

const trackers = [];
afterEach(() => { for (const cursor of trackers.splice(0)) cursor.destroy(); });
function setup(options = {}) {
	const client = mockRedisClient();
	const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, ...options });
	trackers.push(cursor);
	const platform = mockPlatform();
	const ws = mockWs({ id: 'watcher' });
	const relay = (event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'board', event, payload }));
	return { cursor, client, platform, ws, relay };
}

describe('cursor snapshot roster', () => {
	it('keeps a join that arrives before the persisted snapshot catches up', async () => {
		const { cursor, platform, ws, relay } = setup();
		await cursor.attach(ws, 'board', platform);
		await relay('join', { key: 'peer:1', user: { id: 'peer' } });
		await relay('update', { key: 'peer:1', data: { x: 7 } });
		platform.reset();
		await cursor.snapshot(ws, 'board', platform);
		expect(platform.sent.find((frame) => frame.event === 'catalog').data).toEqual([{ key: 'peer:1', user: { id: 'peer' } }]);
		expect(platform.sent.find((frame) => frame.event === 'bulk').data).toEqual([{ key: 'peer:1', data: { x: 7 } }]);
	});
	it('merges joins and removals arriving while a snapshot read is pending', async () => {
		const { cursor, client, platform, ws, relay } = setup();
		await cursor.attach(ws, 'board', platform);
		let release;
		const pending = new Promise((resolve) => { release = resolve; });
		client.redis.hgetall = () => pending;
		platform.reset();
		const snapshot = cursor.snapshot(ws, 'board', platform);
		await Promise.resolve(); await Promise.resolve();
		await relay('remove', { key: 'peer:gone' });
		await relay('join', { key: 'peer:new', user: { id: 'new' } });
		await relay('update', { key: 'peer:new', data: { x: 9 } });
		release({ 'peer:gone': JSON.stringify({ user: { id: 'gone' }, data: { x: 0 }, ts: Date.now() }) });
		await snapshot;
		expect(platform.sent.find((frame) => frame.event === 'catalog').data).toEqual([{ key: 'peer:new', user: { id: 'new' } }]);
	});
	it('includes a local mover before its first position flush', async () => {
		const { cursor, platform, ws } = setup({ topicThrottle: 100 });
		await cursor.attach(ws, 'board', platform);
		cursor.update(mockWs({ id: 'local' }), 'board', { x: 1 }, platform);
		platform.reset();
		await cursor.snapshot(ws, 'board', platform);
		expect(platform.sent.find((frame) => frame.event === 'catalog').data.map((entry) => entry.user)).toEqual([{ id: 'local' }]);
	});
	it('does not allocate state when a socket closes during authorization', async () => {
		const { cursor, platform, ws } = setup();
		let authorize;
		platform.checkSubscribe = () => new Promise((resolve) => { authorize = resolve; });
		const joining = cursor.hooks.subscribe(ws, '__cursor:board', { platform });
		ws.getBufferedAmount = () => { throw new Error('closed'); };
		authorize(null);
		await joining;
		expect(platform.sent).toHaveLength(0);
		cursor.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 7 } }, platform });
		expect(cursor.stats().activeTopicsTotal).toBe(0);
	});
});

describe('cursor roster at ttl 1 across a cached-clock step', () => {
	// Exact time and a cached now() that lags it, refreshing every 1010 ms.
	// Snapshot and cleanup intervals are captured and run on demand.
	let clock;
	beforeEach(() => { clock = installSteppedClock(); });
	afterEach(() => resetRuntimeEnv());
	const flush = async () => {
		for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
	};
	const run = async (ms) => {
		for (const h of clock.intervals) if (!h.cleared && h.ms === ms) h.cb();
		await flush();
	};
	const catalogOf = async (cursor, ws, platform) => {
		platform.reset();
		await cursor.snapshot(ws, 'board', platform);
		return (platform.sent.find((frame) => frame.event === 'catalog')?.data ?? []).map((entry) => entry.user);
	};

	it('keeps a peer cursor that moved moments ago, in snapshots and through cleanup', async () => {
		const client = mockRedisClient();
		const a = createCursor(client, { ttl: 1, throttle: 0, topicThrottle: 0 });
		const b = createCursor(client, { ttl: 1, throttle: 0, topicThrottle: 0 });
		trackers.push(a, b);
		const platformA = mockPlatform();
		const platformB = mockPlatform();
		const watcher = mockWs({ id: 'watcher' });
		await b.attach(watcher, 'board', platformB);
		const mover = mockWs({ id: 'mover' });

		// The mover's last move and snapshot write land 20 ms before the
		// cached clock refreshes at 1010 ms.
		clock.advance(990);
		a.update(mover, 'board', { x: 1 }, platformA);
		await flush();
		await run(100);
		clock.advance(30);

		expect(await catalogOf(b, watcher, platformB)).toEqual([{ id: 'mover' }]);
		await run(10000);
		const hash = await client.redis.hgetall(client.key('cursor:{board}'));
		expect(Object.keys(hash)).toHaveLength(1);

		// With no further move it expires one TTL after its write.
		clock.advance(970);
		expect(await catalogOf(b, watcher, platformB)).toEqual([{ id: 'mover' }]);
		clock.advance(1);
		expect(await catalogOf(b, watcher, platformB)).toEqual([]);
	});
	it('stamps a direct write with the time it was made when snapshotIntervalMs is 0', async () => {
		const client = mockRedisClient();
		const a = createCursor(client, { ttl: 1, throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 });
		const b = createCursor(client, { ttl: 1, throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 });
		trackers.push(a, b);
		clock.advance(990);
		a.update(mockWs({ id: 'mover' }), 'board', { x: 1 }, mockPlatform());
		await flush();
		// The cached clock refreshes 20 ms after the write.
		clock.advance(30);
		expect((await b.list('board')).map((entry) => entry.user)).toEqual([{ id: 'mover' }]);
	});
	it('sweeps a persisted cursor one TTL after its write, before the cached clock steps', async () => {
		const client = mockRedisClient();
		const a = createCursor(client, { ttl: 1, throttle: 0, topicThrottle: 0 });
		trackers.push(a);
		const platform = mockPlatform();
		clock.advance(5);
		a.update(mockWs({ id: 'early' }), 'board', { x: 1 }, platform);
		await flush();
		await run(100);
		// A later mover keeps the hash itself from expiring.
		clock.advance(895);
		a.update(mockWs({ id: 'late' }), 'board', { x: 2 }, platform);
		await flush();
		await run(100);
		// The early write is 1004 ms old; the cached clock still reads the start.
		clock.advance(109);
		await run(10000);
		const hash = await client.redis.hgetall(client.key('cursor:{board}'));
		expect(Object.values(hash).map((value) => JSON.parse(value).user)).toEqual([{ id: 'late' }]);
	});
	it('releases relay entries one TTL after they arrived and stops pruning', () => {
		const timers = [];
		setRuntimeEnv({
			clock: { now: () => 1_000_000, monotonic: () => clock.t, wallEpoch: () => clock.t },
			timers: { set: (cb, ms) => { const h = { cb, ms, unref() {} }; timers.push(h); return h; }, clear() {} }
		});
		const roster = createRoster({ topics: new Map(), instanceId: 'self', cursorTtl: 1, snapshotIntervalMs: 50, revalidate: async () => true, removeMissing() {} });
		try {
			roster.record('board', 'update', { key: 'peer:1', data: { x: 1 } });
			expect(timers).toHaveLength(1);
			clock.advance(1001);
			timers.shift().cb();
			// Nothing is left to age out, so no prune is pending.
			expect(timers).toHaveLength(0);
		} finally { roster.dispose(); }
	});
	it('keeps a relayed peer join that has no persisted entry yet', async () => {
		const { cursor, platform, ws, relay } = setup({ ttl: 1 });
		await cursor.attach(ws, 'board', platform);
		clock.advance(990);
		await relay('join', { key: 'peer:1', user: { id: 'peer' } });
		await relay('update', { key: 'peer:1', data: { x: 7 } });
		// The cached clock refreshes 20 ms after the relay arrived.
		clock.advance(30);
		expect(await catalogOf(cursor, ws, platform)).toEqual([{ id: 'peer' }]);
	});
	it('stamps each relay message separately within one synchronous run', async () => {
		const { cursor, platform, ws, relay } = setup({ ttl: 1 });
		await cursor.attach(ws, 'board', platform);
		const joining = relay('join', { key: 'peer:1', user: { id: 'peer' } });
		clock.advance(1000);
		const updating = relay('update', { key: 'peer:1', data: { x: 7 } });
		await Promise.all([joining, updating]);
		clock.advance(1);
		expect(await catalogOf(cursor, ws, platform)).toEqual([{ id: 'peer' }]);
	});
});
