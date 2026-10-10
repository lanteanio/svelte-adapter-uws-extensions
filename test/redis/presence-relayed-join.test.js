import { describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

describe('presence relayed joins', () => {
	it('keeps transient fields out of same-turn join rosters on both instances', async () => {
		const client = mockRedisClient();
		const options = { binary: false, transient: ['typing'] };
		const a = createPresence(client, options);
		const b = createPresence(client, options);
		try {
			const pa = mockPlatform();
			const pb = mockPlatform();
			await b.join(mockWs({ id: 'bob' }), 'room', pb);
			b.flushDiffs();
			pb.reset();
			const alice = mockWs({ id: 'alice' });
			await a.join(alice, 'room', pa);
			await a.update(alice, 'room', { typing: true, color: 'blue' }, pa);
			a.flushDiffs();
			b.flushDiffs();
			for (const platform of [pa, pb]) {
				const join = platform.published.find((f) => f.event === 'diff' && f.data.joins.alice);
				expect(join.data.joins.alice).toEqual({ id: 'alice', color: 'blue' });
			}
			expect(await b.list('room')).toContainEqual({ id: 'alice', color: 'blue' });
		} finally { a.destroy(); b.destroy(); }
	});
	it('refreshes an existing identity and its durable fields on every instance', async () => {
		vi.useFakeTimers();
		const client = mockRedisClient();
		const options = { binary: false, select: (u) => ({ id: u.id, name: u.name }), heartbeat: 100 };
		const a = createPresence(client, options);
		const b = createPresence(client, options);
		try {
			const pa = mockPlatform();
			const pb = mockPlatform();
			const alice = mockWs({ id: 'alice', name: 'Old' });
			await a.join(alice, 'room', pa);
			await a.update(alice, 'room', { color: 'blue' }, pa);
			a.flushDiffs();
			pa.reset();
			vi.setSystemTime(Date.now() + 10);
			await b.join(mockWs({ id: 'alice', name: 'New' }), 'room', pb);
			a.flushDiffs();
			b.flushDiffs();
			const expected = { id: 'alice', name: 'New', color: 'blue' };
			for (const platform of [pa, pb]) {
				expect(platform.published.find((f) => f.event === 'diff').data.joins.alice).toEqual(expected);
				platform.reset();
			}
			await vi.advanceTimersByTimeAsync(100);
			for (const platform of [pa, pb]) {
				expect(platform.published.find((f) => f.event === 'heartbeat').data.alice).toEqual(expected);
			}
			expect(await b.list('room')).toEqual([expected]);
		} finally { a.destroy(); b.destroy(); vi.useRealTimers(); }
	});

});
