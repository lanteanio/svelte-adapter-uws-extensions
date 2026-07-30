import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createReplay } from '../../src/redis/replay.js';
import { OBSERVER_LANE } from '../../src/shared/ws-subscriptions.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Two questions reach `platform.checkSubscribe`, and they are not the same one:
//
//   "may this connection BE GRANTED this topic?"  - attach, join, resume
//   "may this connection SEE this topic?"         - sync, snapshot
//
// The second is the OBSERVER lane, marked with `{ requireGrant: true }`. It
// exists because in a pure-grant deployment - wire-subscribe authorization
// armed, no app subscribe hook - `checkSubscribe` has no hook to consult and
// therefore allows every topic name, so a read-only lane gated on it alone
// hands any connected socket any room's roster.
//
// No released adapter reads the flag yet, so these assert the QUESTION ASKED,
// not the answer. That is the part that has to be right before the adapter
// makes it load-bearing: once it does, a lane marked wrong is either a hole
// (missing the flag) or an outage (denying every legitimate join).
describe('observer lane: which lanes ask to SEE rather than to be granted', () => {
	const laneOf = (platform, topic) =>
		platform.checkedSubscribe.filter((c) => c.topic === topic).map((c) => c.options);

	it('presence.sync asks the observer question', async () => {
		const platform = mockPlatform();
		const presence = createPresence(mockRedisClient('ol1:'), { key: 'id', select: (u) => ({ id: u.id }) });
		await presence.sync(mockWs({ id: 'watcher' }), 'room', platform);

		expect(laneOf(platform, 'room')).toEqual([OBSERVER_LANE]);
		expect(laneOf(platform, 'room')[0]).toEqual({ requireGrant: true });
		presence.destroy();
	});

	it('presence.join does NOT, because it is the lane that establishes the grant', async () => {
		const platform = mockPlatform();
		const presence = createPresence(mockRedisClient('ol2:'), { key: 'id', select: (u) => ({ id: u.id }) });
		await presence.join(mockWs({ id: 'alice' }), 'room', platform);

		// Requiring a grant that this call is in the middle of establishing
		// would deny every legitimate join.
		for (const opts of laneOf(platform, 'room')) expect(opts?.requireGrant).not.toBe(true);
		presence.destroy();
	});

	it('cursor.snapshot asks the observer question', async () => {
		const platform = mockPlatform();
		const cursors = createCursor(mockRedisClient('ol3:'), {
			throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (u) => ({ id: u.id })
		});
		await cursors.snapshot(mockWs({ id: 'watcher' }), 'board', platform);

		expect(laneOf(platform, 'board')).toEqual([OBSERVER_LANE]);
		cursors.destroy();
	});

	it('cursor.attach does NOT, for the same reason as join', async () => {
		const platform = mockPlatform();
		const cursors = createCursor(mockRedisClient('ol4:'), {
			throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (u) => ({ id: u.id })
		});
		await cursors.attach(mockWs({ id: 'alice' }), 'board', platform);

		for (const opts of laneOf(platform, 'board')) expect(opts?.requireGrant).not.toBe(true);
		cursors.destroy();
	});

	it('the replay gate does NOT, because resume runs before the grant lands', async () => {
		// This is the one that reads like an observer lane and is not. Resume
		// runs INSIDE the subscribe handshake: the adapter gates the topic,
		// then calls the resume hook, then calls ws.subscribe. Requiring a
		// grant here would deny every legitimate resume, not close a hole.
		const platform = mockPlatform();
		const store = createReplay(mockRedisClient('ol5:'), { cleanupInterval: 0 });
		await store.publish(platform, 'room', 'msg', { n: 1 });
		platform.reset();

		await store.replay(mockWs({ id: 'alice' }), 'room', 0, platform);
		await sleep(10);

		expect(laneOf(platform, 'room').length).toBeGreaterThan(0);
		for (const opts of laneOf(platform, 'room')) expect(opts?.requireGrant).not.toBe(true);
		store.destroy?.();
	});

	it('a denial still closes the observer lanes, whatever the flag does', async () => {
		// The flag changes which deployments deny; it must not be the only
		// thing standing between a refused socket and the roster.
		const platform = mockPlatform();
		platform.checkSubscribeDenial = 'FORBIDDEN';
		const presence = createPresence(mockRedisClient('ol6:'), { key: 'id', select: (u) => ({ id: u.id }) });

		const denial = await presence.sync(mockWs({ id: 'watcher' }), 'room', platform);

		expect(denial).toBe('FORBIDDEN');
		expect(platform.sent.filter((s) => s.topic === '__presence:room')).toHaveLength(0);
		presence.destroy();
	});
});
