/**
 * Integration test for the Postgres durable alarm store's single-fire claim
 * against a real Postgres server.
 *
 * The unit suite proves the delete() contract sequentially on the mock; only a
 * real server proves the property the realtime layer actually leans on: one
 * instance's precise in-memory timer and another instance's recovery poll can
 * call delete(topic) at the same wall moment on DIFFERENT connections, and
 * exactly one of them may claim the alarm. `DELETE ... WHERE topic = $1
 * RETURNING topic` makes the row lock the claim - the second deleter blocks on
 * the first's lock and then sees zero rows - and this suite races two real
 * pools to confirm no interleaving yields zero or two claims.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createAlarmStore } from '../../../src/postgres/alarm-store.js';

const TABLE = 'svti_alarms_race_it';

/** Rounds of the two-way race; each round is one independent claim window. */
const ROUNDS = 25;

describe('postgres alarm store single-fire claim (integration)', () => {
	let clientA;
	let clientB;
	let storeA;
	let storeB;

	beforeAll(async () => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		// Two real pools standing in for two cluster instances.
		clientA = createPgClient({ connectionString: url, autoShutdown: false });
		clientB = createPgClient({ connectionString: url, autoShutdown: false });
		await clientA.query(`DROP TABLE IF EXISTS ${TABLE}`);
		storeA = createAlarmStore(clientA, { table: TABLE });
		storeB = createAlarmStore(clientB, { table: TABLE });
		await storeA.ready();
		await storeB.ready();
	});

	beforeEach(async () => {
		await storeA.clear();
	});

	afterAll(async () => {
		if (clientA) {
			await clientA.query(`DROP TABLE IF EXISTS ${TABLE}`);
			await clientA.end();
		}
		if (clientB) await clientB.end();
	});

	it('two instances racing delete(): exactly one claims, every round', async () => {
		for (let round = 0; round < ROUNDS; round++) {
			const topic = 'room:race-' + round;
			await storeA.set(topic, 1000 + round, { path: 'rooms/x' });
			const results = await Promise.all([storeA.delete(topic), storeB.delete(topic)]);
			expect(results.filter(Boolean)).toHaveLength(1);
		}
		expect(await storeA.due(Number.MAX_SAFE_INTEGER)).toEqual([]);
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
