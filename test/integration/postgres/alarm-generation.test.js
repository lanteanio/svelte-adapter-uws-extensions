import pg from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createPgClient, migrate, postgresMigrations, validateSchema } from '../../../src/postgres/index.js';
import { createAlarmStore } from '../../../src/postgres/alarm-store.js';
import { MAX_ALARM_GENERATION } from '../../../src/shared/alarm-generation.js';

const schema = 'alarm_generations_it';
let admin, a, b, first, second;
const connect = (options = {}) => createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, options: { options: `-c search_path=${schema}`, ...options } });
beforeEach(async () => {
	admin = connect();
	await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
	await admin.query(`CREATE SCHEMA ${schema}`);
	a = connect(); b = connect();
});
afterEach(async () => {
	await Promise.all([a?.end(), b?.end()]);
	await admin?.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
	await admin?.end();
});
async function ready() {
	first = createAlarmStore(a); second = createAlarmStore(b);
	await Promise.all([first.ready(), second.ready()]);
}
async function schedule(store, topic, at = 1, meta = null) {
	const generation = await store.nextGeneration(topic);
	expect(await store.set(topic, at, meta, generation)).toBe(true);
	return generation;
}

describe('PostgreSQL ordered alarm generations', () => {
	it('reserves durably without changing the current row and accepts only the latest reservation', async () => {
		await ready();
		expect(await first.get('room')).toBeNull();
		const old = await schedule(first, 'room', 1, { path: 'old' });
		const slow = await first.nextGeneration('room'), fast = await second.nextGeneration('room');
		expect(await first.get('room')).toEqual({ topic: 'room', at: 1, meta: { path: 'old' }, generation: old });
		expect(await second.set('room', 2, { path: 'fast' }, fast)).toBe(true);
		expect(await first.set('room', 3, { path: 'slow' }, slow)).toBe(false);
		expect(await first.set('room', 4, null, fast)).toBe(false);
		expect(await first.due(2)).toEqual([{ topic: 'room', at: 2, meta: { path: 'fast' }, generation: fast }]);
	});
	it('allocates unique ordered tokens across independent pools', async () => {
		await ready();
		const generations = await Promise.all(Array.from({ length: 30 }, (_, index) => (index % 2 ? first : second).nextGeneration('race')));
		expect(new Set(generations).size).toBe(30);
		expect(generations.map(BigInt).sort((x, y) => x < y ? -1 : 1)).toEqual(Array.from({ length: 30 }, (_, index) => BigInt(index + 1)));
		const commits = await Promise.all(generations.map(generation => first.set('race', 1, null, generation)));
		expect(commits.filter(Boolean)).toHaveLength(1);
		expect(await second.get('race')).toMatchObject({ generation: '30' });
	});
	it('fences stale due claims, preserves newer reservations and prevents resurrection after delete', async () => {
		await ready();
		const old = await schedule(first, 'room');
		const observed = (await second.due(1))[0].generation;
		const current = await schedule(second, 'room', 2);
		expect(await first.delete('room', observed)).toBe(false);
		const pending = await first.nextGeneration('room');
		expect(await second.delete('room', current)).toBe(true);
		expect(await first.set('room', 3, null, old)).toBe(false);
		expect(await first.set('room', 3, null, current)).toBe(false);
		expect(await first.set('room', 3, null, pending)).toBe(true);
		const claims = await Promise.all([first.delete('room', pending), second.delete('room', pending)]);
		expect(claims.filter(Boolean)).toHaveLength(1);
		expect(await createAlarmStore(b, { autoMigrate: false }).nextGeneration('room')).toBe('4');
	});
	it('rolls back the commit watermark when the alarm write is rejected', async () => {
		await ready();
		const generation = await first.nextGeneration('room');
		await expect(first.set('room', 1e20, null, generation)).rejects.toThrow();
		expect((await a.query("SELECT committed::text FROM svti_alarms_generations WHERE topic = 'room'")).rows).toEqual([{ committed: '0' }]);
		expect(await first.set('room', 1, null, generation)).toBe(true);
	});
	it('clears current rows and pending reservations while retaining allocation authority', async () => {
		await ready();
		await schedule(first, 'room');
		const pending = await second.nextGeneration('room');
		await first.clear();
		expect(await second.get('room')).toBeNull();
		expect(await second.set('room', 2, null, pending)).toBe(false);
		expect(await second.nextGeneration('room')).toBe('3');
	});
	it('rolls back a failed clear and discards its checkout', async () => {
		await ready();
		const current = await schedule(first, 'room'), pending = await first.nextGeneration('room');
		const original = a.connect, releases = [];
		a.connect = async options => {
			const connection = await original(options), query = connection.query.bind(connection), release = connection.release.bind(connection);
			connection.query = statement => statement.text === 'DELETE FROM svti_alarms' ? Promise.reject(new Error('injected clear failure')) : query(statement);
			connection.release = discard => { releases.push(discard); release(discard); };
			return connection;
		};
		try { await expect(first.clear()).rejects.toThrow('injected clear failure'); } finally { a.connect = original; }
		expect(releases).toEqual([true]);
		expect(await second.get('room')).toMatchObject({ generation: current });
		expect(await second.set('room', 2, null, pending)).toBe(true);
	});
	it('clears without rewriting settled generation history at scale', async () => {
		await ready();
		await a.query("INSERT INTO svti_alarms_generations (topic, reserved, committed) SELECT 'history:' || n, 1, 1 FROM generate_series(1, 100000) n");
		await a.query('CREATE TABLE history_versions AS SELECT topic, xmin::text AS version FROM svti_alarms_generations');
		const pending = await first.nextGeneration('pending');
		await first.clear();
		await first.clear();
		expect(await first.set('pending', 1, null, pending)).toBe(false);
		expect((await a.query('SELECT count(*)::int AS changed FROM svti_alarms_generations g JOIN history_versions h USING (topic) WHERE g.xmin::text <> h.version')).rows[0].changed).toBe(0);
	});
	it('clear uses READ COMMITTED even when the checkout defaults to REPEATABLE READ', async () => {
		await ready();
		const generation = await first.nextGeneration('room'), writer = await b.connect();
		const original = a.connect;
		let clearing, isolation;
		a.connect = async options => {
			const connection = await original(options), query = connection.query.bind(connection);
			await query("SET default_transaction_isolation = 'repeatable read'");
			connection.query = async statement => {
				const result = await query(statement);
				if (statement.text.startsWith('BEGIN')) isolation = (await query('SHOW transaction_isolation')).rows[0].transaction_isolation;
				return result;
			};
			return connection;
		};
		try {
			await writer.query('BEGIN');
			await writer.query('UPDATE svti_alarms_generations SET committed = $1 WHERE topic = $2', [generation, 'room']);
			await writer.query('INSERT INTO svti_alarms (topic, fire_at, generation) VALUES ($1, 1, $2)', ['room', generation]);
			clearing = first.clear(); clearing.catch(() => {});
			const deadline = Date.now() + 3000;
			let waiting = false;
			while (!waiting && Date.now() < deadline) {
				waiting = (await admin.query("SELECT 1 FROM pg_stat_activity WHERE query = 'LOCK TABLE svti_alarms_generations IN EXCLUSIVE MODE' AND wait_event_type = 'Lock'")).rows.length > 0;
				if (!waiting) await new Promise(resolve => setTimeout(resolve, 10));
			}
			expect(waiting).toBe(true);
			await writer.query('COMMIT');
			await clearing;
		} finally { a.connect = original; await writer.query('ROLLBACK').catch(() => {}); writer.release(); await clearing?.catch(() => {}); }
		expect(isolation).toBe('read committed');
		expect(await second.get('room')).toBeNull();
		expect(await second.set('room', 2, null, generation)).toBe(false);
	});
	it.each([0, 2])('migrates %s legacy rows exactly once under concurrent initialization', async count => {
		for (const version of postgresMigrations({ store: 'alarm-store' }).slice(0, 2)) for (const statement of version.statements) await a.query(statement);
		for (let index = 0; index < count; index++) await a.query('INSERT INTO svti_alarms (topic, fire_at, meta) VALUES ($1, 1, $2)', [`legacy${index}`, { kept: true }]);
		await ready();
		await validateSchema(a, { store: 'alarm-store' });
		for (let index = 0; index < count; index++) {
			expect(await first.get(`legacy${index}`)).toMatchObject({ generation: '1', meta: { kept: true } });
			expect(await second.nextGeneration(`legacy${index}`)).toBe('2');
		}
		expect(await first.nextGeneration('new')).toBe('1');
		expect((await a.query("SELECT version FROM svti_schema_migrations WHERE table_name = 'svti_alarms' ORDER BY version")).rows).toEqual([{ version: 1 }, { version: 2 }, { version: 3 }]);
	});
	it('adopts an already provisioned generation schema without a migration ledger', async () => {
		for (const version of postgresMigrations({ store: 'alarm-store' })) for (const statement of version.statements) await a.query(statement);
		await a.query("INSERT INTO svti_alarms_generations (topic, reserved, committed) VALUES ('retained', 3, 2)");
		await a.query("INSERT INTO svti_alarms (topic, fire_at, generation) VALUES ('retained', 1, 2)");
		await ready();
		expect(await first.get('retained')).toMatchObject({ generation: '2' });
		expect(await second.set('retained', 2, null, '3')).toBe(true);
		expect(await first.nextGeneration('retained')).toBe('4');
		expect((await a.query("SELECT version FROM svti_schema_migrations WHERE table_name = 'svti_alarms' ORDER BY version")).rows).toEqual([{ version: 1 }, { version: 2 }, { version: 3 }]);
	});
	it('refuses incompatible legacy authority without changing original rows', async () => {
		const versions = postgresMigrations({ store: 'alarm-store' });
		for (const version of versions.slice(0, 2)) for (const statement of version.statements) await a.query(statement);
		await a.query("INSERT INTO svti_alarms (topic, fire_at, meta) VALUES ('legacy', 1, '{\"kept\":true}')");
		await a.query(versions[2].statements[0]);
		await a.query("INSERT INTO svti_alarms_generations (topic, reserved, committed) VALUES ('legacy', 2, 0)");
		await expect(migrate(a, { store: 'alarm-store' })).rejects.toThrow();
		expect((await a.query('SELECT topic, fire_at, meta FROM svti_alarms')).rows).toEqual([{ topic: 'legacy', fire_at: '1', meta: { kept: true } }]);
		expect((await a.query('SELECT reserved::text, committed::text FROM svti_alarms_generations')).rows).toEqual([{ reserved: '2', committed: '0' }]);
	});
	it('retains exact tokens beyond safe integers and rejects exhausted allocation after restart', async () => {
		await ready();
		await first.nextGeneration('room');
		await a.query("UPDATE svti_alarms_generations SET reserved = 9007199254740992 WHERE topic = 'room'");
		const generation = await first.nextGeneration('room');
		expect(generation).toBe('9007199254740993');
		expect(await first.set('room', 1, null, generation)).toBe(true);
		const numeric = connect({ types: { getTypeParser: (oid, format) => oid === 20 ? Number : pg.types.getTypeParser(oid, format) } });
		try {
			const reader = createAlarmStore(numeric, { autoMigrate: false });
			expect(await reader.get('room')).toMatchObject({ generation });
			expect((await reader.due(1))[0].generation).toBe(generation);
		} finally { await numeric.end(); }
		await a.query('UPDATE svti_alarms_generations SET reserved = $1 WHERE topic = $2', [(BigInt(MAX_ALARM_GENERATION) - 1n).toString(), 'room']);
		expect(await second.nextGeneration('room')).toBe(MAX_ALARM_GENERATION);
		expect(await second.set('room', 1, null, MAX_ALARM_GENERATION)).toBe(true);
		expect(await second.delete('room', MAX_ALARM_GENERATION)).toBe(true);
		await expect(createAlarmStore(a, { autoMigrate: false }).nextGeneration('room')).rejects.toThrow('generation counter exhausted');
		expect(await first.get('room')).toBeNull();
	});
	it('does not reset authority when an active row lost its companion record', async () => {
		await ready();
		const generation = await schedule(first, 'room');
		await a.query("DELETE FROM svti_alarms_generations WHERE topic = 'room'");
		await expect(second.nextGeneration('room')).rejects.toThrow('generation authority');
		expect(await first.get('room')).toMatchObject({ generation });
		expect(await first.delete('room', generation)).toBe(false);
	});
	it('preserves a partially restored row whose generation disagrees with retained authority', async () => {
		await ready();
		await schedule(first, 'room');
		const pending = await first.nextGeneration('room');
		await a.query("UPDATE svti_alarms SET generation = 5 WHERE topic = 'room'");
		await expect(second.nextGeneration('room')).rejects.toThrow('generation authority');
		expect(await second.set('room', 2, null, pending)).toBe(false);
		expect(await second.delete('room', '5')).toBe(false);
		await expect(first.clear()).rejects.toThrow('generation authority');
		expect(await first.get('room')).toMatchObject({ generation: '5' });
		expect((await a.query('SELECT reserved::text, committed::text FROM svti_alarms_generations')).rows).toEqual([{ reserved: '2', committed: '1' }]);
	});
	it('reserves after a held set commits and rejects the stale concurrent claim', async () => {
		await ready();
		const old = await schedule(first, 'room'), replacing = await first.nextGeneration('room'), writer = await a.connect();
		let reserving, claiming;
		try {
			await writer.query('BEGIN');
			await writer.query("UPDATE svti_alarms_generations SET committed = $1 WHERE topic = 'room'", [replacing]);
			await writer.query("UPDATE svti_alarms SET generation = $1 WHERE topic = 'room'", [replacing]);
			reserving = second.nextGeneration('room'); reserving.catch(() => {});
			claiming = first.delete('room', old); claiming.catch(() => {});
			const deadline = Date.now() + 3000;
			let waiting = 0;
			while (waiting < 2 && Date.now() < deadline) {
				waiting = (await admin.query("SELECT 1 FROM pg_stat_activity WHERE query LIKE '%svti_alarms_generations%' AND wait_event_type = 'Lock'")).rows.length;
				if (waiting < 2) await new Promise(resolve => setTimeout(resolve, 10));
			}
			expect(waiting).toBeGreaterThanOrEqual(2);
			await writer.query('COMMIT');
			expect(await reserving).toBe('3');
			expect(await claiming).toBe(false);
		} finally { await writer.query('ROLLBACK').catch(() => {}); writer.release(); await Promise.allSettled([reserving, claiming]); }
		expect(await second.set('room', 2, null, '3')).toBe(true);
		expect(await first.get('room')).toMatchObject({ generation: '3' });
	});
});
