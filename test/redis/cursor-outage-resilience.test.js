import { describe, it, expect, afterEach } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createCursor } from '../../src/redis/cursor.js';
import { WS_SUBSCRIPTIONS } from '../../src/shared/ws-subscriptions.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const opts = { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (u) => ({ id: u.id }) };

/** A breaker whose circuit can be opened partway through a scenario. */
function toggleBreaker() {
	const b = {
		open: false,
		guard() { if (b.open) throw new Error('breaker open'); },
		success() {},
		failure() {}
	};
	return b;
}

describe('cursor snapshot during a Redis outage', () => {
	const rejections = [];
	const onRejection = (err) => rejections.push(err);

	afterEach(() => {
		process.off('unhandledRejection', onRejection);
		rejections.length = 0;
	});

	it('does not turn a snapshot frame into an unhandled rejection', async () => {
		process.on('unhandledRejection', onRejection);

		const client = mockRedisClient('out1:');
		const cursors = createCursor(client, opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);

		// Redis goes away. `list()` rethrows, and hooks.message dispatches the
		// snapshot fire-and-forget - so without an owner for the rejection the
		// worker exits. The shipped client sends this frame on every reconnect,
		// which makes the crash self-amplifying: the reconnect storm re-fires
		// it on each replacement worker while Redis is still down.
		client.redis.hgetall = async () => { throw new Error('redis down'); };

		cursors.hooks.message(ws, { data: { type: 'cursor-snapshot', topic: 'board' }, platform });
		await sleep(30);

		expect(rejections).toHaveLength(0);
		cursors.destroy();
	});

	it('still serves a snapshot once Redis recovers', async () => {
		const client = mockRedisClient('out2:');
		const cursors = createCursor(client, opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 1 } }, platform });
		await sleep(20);

		platform.reset();
		cursors.hooks.message(ws, { data: { type: 'cursor-snapshot', topic: 'board' }, platform });
		await sleep(20);
		// Swallowing the outage must not have swallowed the healthy path.
		expect(platform.sent.some((s) => s.topic === '__cursor:board')).toBe(true);
		cursors.destroy();
	});
});

describe('cursor close during a Redis outage', () => {
	it('drops the per-ws record even when the close path bails early', async () => {
		const breaker = toggleBreaker();
		const cursors = createCursor(mockRedisClient('out3:'), { ...opts, breaker });
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });

		await cursors.attach(ws, 'board', platform);
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 1 } }, platform });
		await sleep(20);

		// The socket disconnects while the circuit is open, so remove() takes
		// its early return. The record is keyed by the ws object in a plain
		// Map, so a skipped delete pins the closed socket and its user data for
		// the life of the process - once per disconnect for as long as the
		// outage lasts - and leaves the `member` authorization set alive after
		// the connection it authorized.
		expect(ws.getUserData()[WS_SUBSCRIPTIONS]?.has('__cursor:board')).toBe(true);

		breaker.open = true;
		await cursors.hooks.close(ws, { platform });
		breaker.open = false;

		platform.reset();
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 99 } }, platform });
		await sleep(20);
		expect(platform.published.filter((p) => p.event === 'update')).toHaveLength(0);

		// Membership has TWO halves and "nothing was published" only pins one
		// of them - the per-ws record, whose `member` set `isMember` reads. The
		// subscription registry is the other half, and it is what anything
		// still holding this socket reads to decide the socket is in the room.
		// Asserted on its own so deleting either half fails a test.
		expect(ws.getUserData()[WS_SUBSCRIPTIONS]?.has('__cursor:board')).toBe(false);
		cursors.destroy();
	});

	it('drops it on a healthy close too', async () => {
		const cursors = createCursor(mockRedisClient('out4:'), opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 1 } }, platform });
		await sleep(20);
		expect(ws.getUserData()[WS_SUBSCRIPTIONS]?.has('__cursor:board')).toBe(true);

		await cursors.hooks.close(ws, { platform });
		platform.reset();
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 99 } }, platform });
		await sleep(20);
		expect(platform.published.filter((p) => p.event === 'update')).toHaveLength(0);
		expect(ws.getUserData()[WS_SUBSCRIPTIONS]?.has('__cursor:board')).toBe(false);
		cursors.destroy();
	});
});
