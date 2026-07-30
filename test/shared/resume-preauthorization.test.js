import { describe, it, expect } from 'vitest';
import { createReplay } from '../../src/redis/replay.js';
import { createStreamReplay } from '../../src/redis/replay-stream.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

/**
 * Drives each store's OWN `resumeHook()`, not a hand-wired `createResumeHook`
 * with a spy for `replay`. The forwarders the stores install are the thing
 * under test: a 4-arity `(ws, topic, seq, platform) => tracker.replay(...)`
 * silently drops the pre-authorization token, and a spy-based test cannot see
 * that because the spy IS the forwarder.
 */
const STORES = [
	{ name: 'redis replay', make: (c) => createReplay(c, {}) },
	{ name: 'redis replay-stream', make: (c) => createStreamReplay(c, {}) }
];

for (const store of STORES) {
	describe(`${store.name} resume pre-authorization`, () => {
		it('runs the app subscribe gate exactly once per resumed topic', async () => {
			const client = mockRedisClient('pa1:');
			const tracker = store.make(client);
			const platform = mockPlatform();
			const ws = mockWs({ id: 'u' });

			const topics = ['room:1', 'room:2', 'room:3'];
			for (const t of topics) await tracker.publish(platform, t, 'msg', { a: 1 });

			const seen = [];
			platform.checkSubscribe = async (_ws, topic) => { seen.push(topic); return null; };

			const lastSeenSeqs = {};
			for (const t of topics) lastSeenSeqs[t] = 0;
			await tracker.resumeHook()(ws, { lastSeenSeqs, platform });

			// Twice per topic means the app's subscribe hook - commonly
			// DB-backed, often side-effecting (audit rows, rate-limit tokens) -
			// runs a second time for every topic the hook just authorized.
			expect(seen).toHaveLength(topics.length);
			expect([...seen].sort()).toEqual([...topics].sort());
			tracker.destroy?.();
		});

		it('keeps the second gate off the unbounded path', async () => {
			const client = mockRedisClient('pa2:');
			const tracker = store.make(client);
			const platform = mockPlatform();
			const ws = mockWs({ id: 'u' });

			const topics = [];
			for (let i = 0; i < 200; i++) topics.push('room:' + i);
			for (const t of topics) await tracker.publish(platform, t, 'msg', { a: 1 });

			let inFlight = 0;
			let peak = 0;
			platform.checkSubscribe = async () => {
				inFlight++;
				peak = Math.max(peak, inFlight);
				await new Promise((r) => setTimeout(r, 0));
				inFlight--;
				return null;
			};

			const lastSeenSeqs = {};
			for (const t of topics) lastSeenSeqs[t] = 0;
			await tracker.resumeHook()(ws, { lastSeenSeqs, platform });

			// The hook authorizes at a bounded concurrency on purpose. A second
			// gate firing from inside the gap-fill Promise.all has no bound at
			// all, so it opens one auth query per resumed topic at once.
			expect(peak).toBeLessThanOrEqual(32);
			tracker.destroy?.();
		});

		it('cannot be skipped by a caller passing a truthy pre-authorized flag', async () => {
			const client = mockRedisClient('pa3:');
			const tracker = store.make(client);
			const platform = mockPlatform();
			await tracker.publish(platform, 'secret', 'msg', { a: 1 });

			platform.reset();
			platform.checkSubscribe = async () => 'FORBIDDEN';

			// `replay` is public. If the "already authorized" argument were a
			// plain boolean, this call would read the buffer of a topic the
			// gate just refused.
			const ws = mockWs({ id: 'mallory' });
			await tracker.replay(ws, 'secret', 0, platform, 'req-1', true);

			expect(platform.sent.some((s) => s.event === 'msg')).toBe(false);
			expect(platform.sent.some((s) => s.event === 'denied')).toBe(true);
			tracker.destroy?.();
		});
	});
}
