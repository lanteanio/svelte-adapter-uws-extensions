import type { RedisClient } from './index.js';
import type { RedisReplayOptions, RedisReplayBuffer } from './replay.js';

/**
 * Create a Redis Streams-backed replay buffer.
 *
 * Same external contract as the sorted-set backend (`createReplay`) but stores
 * entries in a Redis Stream (`XADD`/`XRANGE`) with `<seq>-0` IDs instead of a
 * sorted set. Listpack encoding is more compact for the typical message shape,
 * and `XRANGE` against `<seq>-0` IDs filters natively by sequence number with
 * no app-side scan.
 *
 * Reached through `createReplay(client, { storage: 'stream' })`; this direct
 * entry point is internal to the package and is not a published subpath. Unlike
 * the sorted-set backend, the buffer returned here always implements the
 * optional `publishIdempotent` method.
 *
 * Requires Redis 7+ for the listpack encoding wins; works on Redis 5+
 * functionally.
 */
export function createStreamReplay(client: RedisClient, options?: RedisReplayOptions): RedisReplayBuffer;
