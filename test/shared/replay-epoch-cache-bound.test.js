import { describe, it, expect, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPgClient } from '../helpers/mock-pg.js';

// The production bound is 50,000 topics. Lowering it here runs each store's
// real epoch cache up to its bound, so a store that went back to an unbounded
// Map would keep the evicted topic's epoch.
vi.mock('../../src/shared/caps.js', async (importOriginal) => ({
	...(await importOriginal()),
	MAX_REPLAY_EPOCH_CACHE_TOPICS: 3
}));

const { createReplay: createRedisReplay } = await import('../../src/redis/replay.js');
const { createReplay: createPgReplay } = await import('../../src/postgres/replay.js');

const STORES = [
	['redis sorted-set replay', () => createRedisReplay(mockRedisClient('epoch-zset:'), { size: 10 })],
	['redis stream replay', () => createRedisReplay(mockRedisClient('epoch-stream:'), { size: 10, storage: 'stream' })],
	['postgres replay', () => createPgReplay(mockPgClient(), { size: 10, cleanupInterval: 0 })]
];

describe('replay epoch cache is bounded in every store', () => {
	for (const [name, create] of STORES) {
		it(`${name}: the cache holds the bound and evicts the least recent past it`, async () => {
			const replay = create();
			try {
				// clearTopic starts a new generation and caches it, in the order
				// a, b, c. Reading 'a' makes 'b' the least recently used.
				for (const topic of ['a', 'b', 'c']) await replay.clearTopic(topic);
				expect(replay.cachedEpoch('a')).toBe(1);

				// One more topic, as a resume presenting many does.
				await replay.currentEpoch('spray');

				// 'b' left the cache, not 'a', the first one in: the synchronous
				// read falls back to the baseline while the store keeps its real
				// generation.
				expect(replay.cachedEpoch('b')).toBe(0);
				expect(await replay.currentEpoch('b')).toBe(1);
				expect(replay.cachedEpoch('a')).toBe(1);
			} finally {
				replay.destroy?.();
			}
		});
	}
});
