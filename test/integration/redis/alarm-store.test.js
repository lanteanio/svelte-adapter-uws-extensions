/**
 * Integration test for the Redis durable alarm store's single-fire claim
 * against a real Redis server.
 *
 * The unit suite proves the delete() contract sequentially on the mock; only a
 * real server proves the property the realtime layer actually leans on: one
 * instance's precise in-memory timer and another instance's recovery poll can
 * call delete(topic) at the same wall moment on DIFFERENT connections, and
 * exactly one of them may claim the alarm. Redis serializes the MULTI
 * (ZREM + HDEL) per node, so the ZREM count is the claim; this suite races two
 * real connections to confirm no interleaving yields zero or two claims. Runs
 * against standalone and, via the mirror config, a real Redis Cluster (the
 * `{alarms}` hash tag keeps the claim single-slot).
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createAlarmStore } from '../../../src/redis/alarm-store.js';

/** Rounds of the two-way race; each round is one independent claim window. */
const ROUNDS = 40;

describe('redis alarm store single-fire claim (integration)', () => {
	let clientA;
	let clientB;
	let storeA;
	let storeB;

	beforeAll(() => {
		// Two real connections standing in for two cluster instances.
		clientA = createBackendClient({ keyPrefix: 'inttest-alarm:' });
		clientB = createBackendClient({ keyPrefix: 'inttest-alarm:' });
		storeA = createAlarmStore(clientA);
		storeB = createAlarmStore(clientB);
	});

	beforeEach(async () => {
		await resetBackendKeys(clientA);
	});

	afterAll(async () => {
		if (clientA) await clientA.quit?.();
		if (clientB) await clientB.quit?.();
	});

	it('two instances racing delete(): exactly one claims, every round', async () => {
		for (let round = 0; round < ROUNDS; round++) {
			const topic = 'room:race-' + round;
			await storeA.set(topic, 1000 + round, { path: 'rooms/x' });
			const results = await Promise.all([storeA.delete(topic), storeB.delete(topic)]);
			expect(results.filter(Boolean)).toHaveLength(1);
		}
		// Nothing left behind: the due index and the meta hash are both empty.
		expect(await storeA.due(Number.MAX_SAFE_INTEGER)).toEqual([]);
		expect(await clientA.redis.hlen(clientA.key('alarm:{alarms}:meta'))).toBe(0);
	});

	it('a wider pile-up (precise timer + poll retries) still claims exactly once', async () => {
		const topic = 'room:pileup';
		await storeA.set(topic, 500, { path: 'rooms/p' });
		const callers = [];
		for (let i = 0; i < 8; i++) {
			callers.push((i % 2 === 0 ? storeA : storeB).delete(topic));
		}
		const results = await Promise.all(callers);
		expect(results.filter(Boolean)).toHaveLength(1);
		expect(await storeA.due(Number.MAX_SAFE_INTEGER)).toEqual([]);
	});
});
