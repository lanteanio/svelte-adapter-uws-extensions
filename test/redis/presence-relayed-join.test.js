import { describe, expect, it } from 'vitest';
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
});
