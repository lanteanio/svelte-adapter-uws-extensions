import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { createPgClient, migrate } from '../../../src/postgres/index.js';
import { createReplay } from '../../../src/postgres/replay.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';

const fixture = JSON.parse(readFileSync(new URL('../fixtures/migration-0.5.10.json', import.meta.url), 'utf8'));
const guide = readFileSync(new URL('../../../docs/postgres-support.md', import.meta.url), 'utf8').replaceAll('\r\n', '\n');
const sqlBlocks = [...guide.matchAll(/```sql\n([\s\S]+?)\n```/g)].map(match => match[1]);
async function migrateManually(client, base, table) {
	const statements = sqlBlocks.flatMap(sql => sql.split(';')).filter(sql => sql.includes(base));
	expect(statements.length).toBeGreaterThan(0);
	for (let repeat = 0; repeat < 2; repeat++) {
		for (const sql of statements) await client.query(sql.replaceAll(base, table));
		await migrate(client, { store: base === 'svti_replay' ? 'replay' : 'idempotency', table });
	}
}
function connect() { return createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); }

describe('PostgreSQL migration readiness', () => {
	it.each([true, false])('preserves old replay authority through migration (automatic=%s)', async (autoMigrate) => {
		const client = connect();
		const table = 'migration_replay', seqTable = table + '_seq';
		const replay = createReplay(client, { table, cleanupInterval: 0, autoMigrate });
		try {
			await client.query(fixture.replay.replaceAll('${table}', table).replaceAll('${pkCol}', table + '_id'));
			await client.query(fixture.replaySequence.replaceAll('${seqTable}', seqTable));
			await client.query(`INSERT INTO ${table} (topic, seq, event, data) VALUES ('room', 7, 'old', '{"kept":true}')`);
			await client.query(`INSERT INTO ${seqTable} (topic, seq) VALUES ('room', 7)`);
			if (!autoMigrate) await migrateManually(client, 'svti_replay', table);
			expect(await replay.seq('room')).toBe(7);
			expect(await replay.since('room', 0)).toEqual([{ topic: 'room', seq: 7, event: 'old', data: { kept: true } }]);
			expect((await client.query(`SELECT epoch FROM ${seqTable} WHERE topic = 'room'`)).rows[0].epoch).toBe('0');
			const deliveries = [];
			await replay.publish({ publish: (...args) => { deliveries.push(args); return true; } }, 'room', 'new', { fresh: true });
			expect(await replay.seq('room')).toBe(8);
			expect(deliveries).toHaveLength(1);
		} finally {
			replay.destroy();
			await client.query(`DROP TABLE IF EXISTS ${table}, ${seqTable}`);
			await client.end();
		}
	});

	it.each([true, false])('preserves old idempotency results through migration (automatic=%s)', async (autoMigrate) => {
		const client = connect(), table = 'migration_idempotency';
		let store;
		try {
			await client.query(fixture.idempotency.replaceAll('${table}', table));
			await client.query(`INSERT INTO ${table} (svti_idempotency_key, status, result, expires_at) VALUES ('old', 'committed', '{"kept":true}', now() + interval '1 hour')`);
			if (!autoMigrate) await migrateManually(client, 'svti_idempotency', table);
			store = createIdempotencyStore(client, { table, cleanupInterval: 0, autoMigrate });
			await store.ready();
			expect(await store.acquire('old')).toMatchObject({ acquired: false, result: { kept: true } });
			const fresh = await store.acquire('new', 60, { user: 'u', tenant: 't' });
			expect(fresh.acquired).toBe(true); await fresh.commit({ fresh: true });
			expect(await store.purgeUser('t', 'u')).toBe(1);
			expect(await store.acquire('old')).toMatchObject({ acquired: false, result: { kept: true } });
		} finally {
			store?.destroy(); await client.query(`DROP TABLE IF EXISTS ${table}`); await client.end();
		}
	});

	it('rejects incomplete schemas with automatic migration enabled or disabled', async () => {
		const client = connect(), table = 'migration_incomplete_tasks';
		const options = { table, dispatchInterval: 0, recoveryInterval: 0, cleanupInterval: 0 };
		let automatic, manual;
		try {
			await client.query(`CREATE TABLE ${table} (svti_tasks_id UUID PRIMARY KEY)`);
			automatic = createTaskRunner(client, options);
			await expect(automatic.ready()).rejects.toThrow('missing expected column');
			manual = createTaskRunner(client, { ...options, autoMigrate: false });
			await expect(manual.ready()).rejects.toThrow('missing expected column');
			await expect(manual.list()).rejects.toThrow();
		} finally {
			automatic?.destroy(); manual?.destroy();
			await client.query(`DROP TABLE IF EXISTS ${table}`); await client.end();
		}
	});
});
