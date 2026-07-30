import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createCursor, SubscribeDeniedError } from '../../src/redis/cursor.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const opts = { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (u) => ({ id: u.id }) };

describe('cursor room membership', () => {
	it('ignores a cursor frame from a socket that never attached', async () => {
		const cursors = createCursor(mockRedisClient('t1:'), opts);
		const platform = mockPlatform();
		const outsider = mockWs({ id: 'mallory' });

		cursors.hooks.message(outsider, { data: { type: 'cursor', topic: 'private-board', data: { x: 1 } }, platform });
		await sleep(20);

		// No room state allocated at all: an ungated frame used to create the
		// topic, seed the attacker's position, and show up in a victim's
		// snapshot.
		expect(cursors.stats().activeTopicsTotal).toBe(0);
		cursors.destroy();
	});

	it('accepts frames once the real attach handshake has granted membership', async () => {
		const cursors = createCursor(mockRedisClient('t2:'), opts);
		const platform = mockPlatform();
		const member = mockWs({ id: 'alice' });

		// Drive the shipped handshake rather than stamping membership by
		// hand: hand-stamping passes even when the grant inside attach() is
		// deleted outright, which is a wiring break that drops every
		// legitimate cursor frame in production.
		await cursors.attach(member, 'private-board', platform);
		cursors.hooks.message(member, { data: { type: 'cursor', topic: 'private-board', data: { x: 1 } }, platform });
		await sleep(20);

		expect(cursors.stats().activeTopicsTotal).toBe(1);
		const updates = platform.published.filter((p) => p.event === 'update');
		expect(updates.length).toBeGreaterThan(0);
		cursors.destroy();
	});

	it('keeps delivering after a reconnect that re-subscribes without re-attaching', async () => {
		const cursors = createCursor(mockRedisClient('t5:'), opts);
		const platform = mockPlatform();
		const member = mockWs({ id: 'alice' });

		// A reconnecting tab arrives on a FRESH socket. The adapter resubscribes
		// it to the cursor channel through the authorized server path, and the
		// shipped client then sends `cursor-snapshot` on every status==='open'.
		// That lane is an OBSERVER lane and must not mint a grant of its own -
		// so if membership is only ever granted by attach(), the reconnected
		// socket holds none and every cursor frame it sends is dropped by a
		// bare `return`: no throw, no metric, cursors simply stop working until
		// the tab is reloaded.
		await cursors.hooks.subscribe(member, '__cursor:board', { platform });
		cursors.hooks.message(member, { data: { type: 'cursor-snapshot', topic: 'board' }, platform });
		await sleep(20);

		cursors.hooks.message(member, { data: { type: 'cursor', topic: 'board', data: { x: 5 } }, platform });
		await sleep(20);

		expect(cursors.stats().activeTopicsTotal).toBe(1);
		expect(platform.published.filter((p) => p.event === 'update').length).toBeGreaterThan(0);
		cursors.destroy();
	});

	it('gates viewport frames the same way', async () => {
		const cursors = createCursor(mockRedisClient('t3:'), { ...opts, viewport: { enabled: true } });
		const platform = mockPlatform();
		const outsider = mockWs({ id: 'mallory' });

		cursors.hooks.message(outsider, { data: { type: 'cursor-viewport', topic: 'board', rect: { x: 0, y: 0, w: 1, h: 1 } }, platform });
		await sleep(10);
		expect(cursors.stats().viewportsReported).toBe(0);
		expect(cursors.viewportFor(outsider, 'board')).toBeNull();

		const member = mockWs({ id: 'alice' });
		await cursors.attach(member, 'board', platform);
		cursors.hooks.message(member, { data: { type: 'cursor-viewport', topic: 'board', rect: { x: 0, y: 0, w: 640, h: 480 } }, platform });
		expect(cursors.viewportFor(member, 'board')).toEqual({ x: 0, y: 0, w: 640, h: 480, zoom: 1 });
		cursors.destroy();
	});

	it('denies attach without subscribing, so a refused client cannot write either', async () => {
		const cursors = createCursor(mockRedisClient('t4:'), opts);
		const platform = mockPlatform();
		platform.checkSubscribe = async (_ws, topic) => (topic === 'private-board' ? 'FORBIDDEN' : null);
		const mallory = mockWs({ id: 'mallory' });

		await expect(cursors.attach(mallory, 'private-board', platform)).rejects.toThrow(SubscribeDeniedError);

		// The denial has to roll all the way back. Subscribing first and
		// authorizing afterwards would leave the socket receiving every
		// broadcast for the room AND holding the membership the frame gate
		// checks, so the denial would withhold nothing but the snapshot.
		expect(mallory.isSubscribed('__cursor:private-board')).toBe(false);
		cursors.hooks.message(mallory, { data: { type: 'cursor', topic: 'private-board', data: { x: 1 } }, platform });
		await sleep(20);
		expect(cursors.stats().activeTopicsTotal).toBe(0);
		cursors.destroy();
	});

	it('carries the platform denial reason on the thrown error', async () => {
		const cursors = createCursor(mockRedisClient('t5:'), opts);
		const platform = mockPlatform();
		platform.checkSubscribe = async () => 'NOT_A_MEMBER';
		await expect(cursors.attach(mockWs({ id: 'm' }), 'room', platform)).rejects.toMatchObject({
			code: 'SUBSCRIBE_DENIED',
			reason: 'NOT_A_MEMBER',
			topic: 'room'
		});
		cursors.destroy();
	});

	it('revokes membership on detach', async () => {
		const cursors = createCursor(mockRedisClient('t6:'), opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);
		cursors.detach(ws, 'board', platform);

		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 5 } }, platform });
		await sleep(20);
		const updates = platform.published.filter((p) => p.event === 'update');
		expect(updates).toHaveLength(0);
		cursors.destroy();
	});
	it('keeps membership for other rooms when one room is removed', async () => {
		const cursors = createCursor(mockRedisClient('t7:'), opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'roomA', platform);
		await cursors.attach(ws, 'roomB', platform);

		// remove() drops the cursor entry for one room. Membership lives on
		// the same per-ws record, so discarding that record when the
		// announced-topics set happens to be empty (which it is until the
		// socket first MOVES) would silently revoke every other room too: the
		// socket stays subscribed and keeps receiving, but its own frames are
		// dropped forever.
		await cursors.remove(ws, platform, 'roomA');

		platform.reset();
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'roomB', data: { x: 1 } }, platform });
		await sleep(20);
		expect(platform.published.filter((p) => p.event === 'update').length).toBeGreaterThan(0);
		cursors.destroy();
	});

	it('keeps membership when an unrelated room is removed', async () => {
		const cursors = createCursor(mockRedisClient('t8:'), opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);
		await cursors.remove(ws, platform, 'never-joined');

		platform.reset();
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 1 } }, platform });
		await sleep(20);
		expect(platform.published.filter((p) => p.event === 'update').length).toBeGreaterThan(0);
		cursors.destroy();
	});

	it('denies the wire subscribe itself, not just the snapshot', async () => {
		const cursors = createCursor(mockRedisClient('t9:'), opts);
		const platform = mockPlatform();
		platform.checkSubscribe = async () => 'FORBIDDEN';
		const ws = mockWs({ id: 'mallory' });
		// The adapter treats anything that is not `false` or a string as
		// ALLOW, so returning undefined here would leave the socket
		// subscribed to the broadcast channel and fed every later frame -
		// the larger half of what the gate exists to prevent.
		const verdict = await cursors.hooks.subscribe(ws, '__cursor:secret', { platform });
		expect(typeof verdict).toBe('string');
		expect(verdict).toBe('FORBIDDEN');
		cursors.destroy();
	});
});
