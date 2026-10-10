/**
 * Integration test for the Postgres durable alarm store's single-fire claim
 * against a real Postgres server.
 *
 * The unit suite proves the delete() contract sequentially on the mock; only a
 * real server proves the property the realtime layer actually leans on: one
 * instance's precise in-memory timer and another instance's recovery poll can
 * call delete(topic, generation) at the same wall moment on DIFFERENT connections, and
 * exactly one of them may claim the alarm. Each delete locks the topic's
 * retained authority and removes only the matching current generation. This
 * suite races two real pools to confirm one claimant for an unchanged alarm.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createAlarmStore } from '../../../src/postgres/alarm-store.js';

const TABLE = 'svti_alarms_race_it';

/** Rounds of the two-way race; each round is one independent claim window. */
const ROUNDS = 25;

describe('postgres alarm store single-fire claim (integration)', { retry: 0 }, () => {
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
		await clientA.query(`DROP TABLE IF EXISTS ${TABLE}, ${TABLE}_generations`);
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
			await clientA.query(`DROP TABLE IF EXISTS ${TABLE}, ${TABLE}_generations`);
			await clientA.end();
		}
		if (clientB) await clientB.end();
	});

	it('two instances racing delete(): exactly one claims, every round', async () => {
		for (let round = 0; round < ROUNDS; round++) {
			const topic = 'room:race-' + round;
			const generation = await storeA.nextGeneration(topic);
			await storeA.set(topic, 1000 + round, { path: 'rooms/x' }, generation);
			const results = await Promise.all([storeA.delete(topic, generation), storeB.delete(topic, generation)]);
			expect(results.filter(Boolean)).toHaveLength(1);
		}
		expect(await storeA.due(Number.MAX_SAFE_INTEGER)).toEqual([]);
	});

	it('a wider pile-up (precise timer + poll retries) still claims exactly once', async () => {
		const topic = 'room:pileup';
		const generation = await storeA.nextGeneration(topic);
		await storeA.set(topic, 500, { path: 'rooms/p' }, generation);
		const callers = [];
		for (let i = 0; i < 8; i++) {
			callers.push((i % 2 === 0 ? storeA : storeB).delete(topic, generation));
		}
		const results = await Promise.all(callers);
		expect(results.filter(Boolean)).toHaveLength(1);
		expect(await storeA.due(Number.MAX_SAFE_INTEGER)).toEqual([]);
	});
});
