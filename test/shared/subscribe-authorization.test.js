import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor, SubscribeDeniedError } from '../../src/redis/cursor.js';
import { checkReplayAccess } from '../../src/shared/replay-gate.js';

// These three gates are the ONLY authorization on the presence-snapshot,
// cursor-snapshot and replay-resume read paths. A missing authorizer has to
// deny: degrade availability, never authorization.

describe('presence.sync', () => {
	it('denies when the platform cannot authorize', async () => {
		const client = mockRedisClient('p1:');
		const platform = mockPlatform();
		delete platform.checkSubscribe;
		const presence = createPresence(client, { select: (u) => ({ id: u.id }) });
		await presence.join(mockWs({ id: 'alice' }), 'tenant-b-secret', platform);
		platform.reset();

		await presence.sync(mockWs({ id: 'mallory' }), 'tenant-b-secret', platform);
		expect(JSON.stringify(platform.sent)).not.toContain('alice');
		await presence.destroy?.();
	});

	it('still serves an authorized socket', async () => {
		const client = mockRedisClient('p2:');
		const platform = mockPlatform();
		const presence = createPresence(client, { select: (u) => ({ id: u.id }) });
		await presence.join(mockWs({ id: 'alice' }), 'room', platform);
		platform.reset();
		await presence.sync(mockWs({ id: 'bob' }), 'room', platform);
		expect(JSON.stringify(platform.sent)).toContain('alice');
		await presence.destroy?.();
	});
});

describe('cursor.snapshot', () => {
	it('denies when the platform cannot authorize', async () => {
		const platform = mockPlatform();
		delete platform.checkSubscribe;
		const cursors = createCursor(mockRedisClient('c1:'), { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 });
		await cursors.snapshot(mockWs({ id: 'mallory' }), 'secret', platform);
		expect(platform.sent.length).toBe(0);
		cursors.destroy();
	});

	it('refuses attach so no membership is granted either', async () => {
		const platform = mockPlatform();
		delete platform.checkSubscribe;
		const cursors = createCursor(mockRedisClient('c2:'), { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 });
		const ws = mockWs({ id: 'mallory' });
		// Failing closed on the READ while attach still subscribed and
		// granted write membership would be the worst of both: no snapshot,
		// but a socket receiving every broadcast and able to write.
		await expect(cursors.attach(ws, 'secret', platform)).rejects.toThrow(SubscribeDeniedError);
		expect(ws.isSubscribed('__cursor:secret')).toBe(false);
		cursors.destroy();
	});
});

describe('checkReplayAccess', () => {
	it('denies when the platform cannot authorize', async () => {
		const platform = mockPlatform();
		delete platform.checkSubscribe;
		const ws = mockWs({ id: 'mallory' });
		expect(await checkReplayAccess(ws, 'secret', platform)).toBe(false);
		expect(platform.sent.some((s) => s.event === 'denied')).toBe(true);
	});

	it('denies when the authorizer throws', async () => {
		const platform = mockPlatform();
		platform.checkSubscribe = async () => { throw new Error('auth backend down'); };
		expect(await checkReplayAccess(mockWs({ id: 'm' }), 'secret', platform)).toBe(false);
	});

	it('denies without throwing when the platform has no send', async () => {
		// The platform missing checkSubscribe is by construction the
		// non-standard one, so it is also the likeliest to lack send. A
		// throw here would reject the whole resume through its Promise.all
		// and every OTHER topic would lose its gap-fill too.
		const platform = { checkSubscribe: undefined };
		expect(await checkReplayAccess(mockWs({ id: 'm' }), 'secret', platform)).toBe(false);
	});

	it('denies without throwing when the platform is missing entirely', async () => {
		expect(await checkReplayAccess(mockWs({ id: 'm' }), 'secret', undefined)).toBe(false);
	});

	it('allows an authorized topic', async () => {
		const platform = mockPlatform();
		expect(await checkReplayAccess(mockWs({ id: 'bob' }), 'room', platform)).toBe(true);
	});
});
