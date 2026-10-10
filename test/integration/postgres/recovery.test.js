import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { expect, it } from 'vitest';
import pg from 'pg';
import { createPgClient } from '../../../src/postgres/index.js';
import { createReplay } from '../../../src/postgres/replay.js';
import { createJobQueue } from '../../../src/postgres/jobs.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';
import { createAlarmStore } from '../../../src/postgres/alarm-store.js';
import { manifest, runVerification, postgresRecoveryConfig, connectPostgresRecovery } from '../../../docs/tools/recovery-verify.mjs';

it('restores a real PostgreSQL archive and detects partial replay and sequence restoration read-only', async () => {
	const admin = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
	const connection = database => { const url = new URL(process.env.INTEGRATION_POSTGRES_URL); url.pathname = '/' + database; return url.href; };
	const project = process.env.INTEGRATION_COMPOSE_PROJECT || 'svelte-adapter-uws-ext-int-postgres';
	const container = execFileSync('docker', ['compose', '-p', project, '-f', 'test/integration/docker-compose.yml', 'ps', '-q', 'postgres'], { encoding: 'utf8', windowsHide: true }).trim();
	const config = { schemaVersion: 1, postgres: { stores: Object.fromEntries(manifest.postgres.map(item => [item.id, item.table])) } };
	let source, reader, replay, tasks, cache, dlq;
	try {
		await admin.query('CREATE DATABASE recovery_source');
		await admin.query('CREATE DATABASE recovery_restored');
		source = createPgClient({ connectionString: connection('recovery_source'), autoShutdown: false });
		replay = createReplay(source, { cleanupInterval: 0 });
		await replay.publish({ publish() { return true; } }, 'private-room', 'message', { hidden: 'private-payload' });
		await createJobQueue(source).enqueue('mail', { value: 1 });
		tasks = createTaskRunner(source, { dispatchInterval: 0, recoveryInterval: 0, cleanupInterval: 0 });
		await tasks.ready(); await tasks.enqueue('work', { value: 2 });
		cache = createIdempotencyStore(source, { cleanupInterval: 0 });
		await cache.ready(); await (await cache.acquire('private-key', 600)).commit({ value: 3 });
		dlq = createDeadLetter(source, { cleanupInterval: 0 });
		await dlq.add({ webhookId: 'delivery', topic: 'private-room', event: 'message', data: { value: 4 }, attempts: 1, error: 'failure' });
		const alarms = createAlarmStore(source); await alarms.ready();
		await alarms.set('private-room', Date.now() + 60000, { alarmId: 'notify' }, await alarms.nextGeneration('private-room'));
		const deletedGeneration = await alarms.nextGeneration('private-deleted');
		await alarms.set('private-deleted', 1, null, deletedGeneration);
		await alarms.delete('private-deleted', deletedGeneration);
		const pendingGeneration = await alarms.nextGeneration('private-deleted');
		const dump = execFileSync('docker', ['exec', container, 'pg_dump', '-U', 'test', '-d', 'recovery_source', '-Fc'], { windowsHide: true, timeout: 30000 });
		execFileSync('docker', ['exec', '-i', container, 'pg_restore', '-U', 'test', '-d', 'recovery_restored', '--exit-on-error', '--no-owner', '--no-acl'], { input: dump, windowsHide: true, timeout: 30000 });
		const readerUrl = new URL(connection('recovery_restored'));
		readerUrl.searchParams.set('query_timeout', '999999999');
		readerUrl.searchParams.set('statement_timeout', '0');
		readerUrl.searchParams.set('options', '-c search_path=public -c default_transaction_read_only=off');
		reader = new pg.Client(postgresRecoveryConfig(readerUrl.href));
		await connectPostgresRecovery(reader);
		expect((await reader.query('SHOW default_transaction_read_only')).rows[0].default_transaction_read_only).toBe('on');
		expect((await reader.query('SHOW search_path')).rows[0].search_path).toBe('public');
		mkdirSync('source/recovery-drill', { recursive: true });
		writeFileSync('source/recovery-drill/postgres-scope.json', JSON.stringify(config));
		const report = JSON.parse(execFileSync(process.execPath, ['docs/tools/recovery-verify.mjs', 'source/recovery-drill/postgres-scope.json'], {
			encoding: 'utf8', windowsHide: true, timeout: 30000,
			env: { ...process.env, RECOVERY_POSTGRES_URL: readerUrl.href }
		}));
		expect(report.ok, JSON.stringify(report)).toBe(true);
		expect(Object.keys(report.postgres.observations)).toHaveLength(9);
		expect(report.postgres.observations['schema-migrations'].rows).toBe('13');
		expect(JSON.stringify(report)).not.toMatch(/private-room|private-payload|private-key/);
		await expect(reader.query('DELETE FROM svti_tasks')).rejects.toMatchObject({ code: '25006' });
		const restored = createPgClient({ connectionString: connection('recovery_restored'), autoShutdown: false });
		try {
			const restoredAlarms = createAlarmStore(restored, { autoMigrate: false });
			expect(await restoredAlarms.get('private-deleted')).toBeNull();
			expect(await restoredAlarms.nextGeneration('private-deleted')).toBe((BigInt(pendingGeneration) + 1n).toString());
			expect(await restoredAlarms.set('private-deleted', 1, null, pendingGeneration)).toBe(false);
			await restored.query("UPDATE svti_alarms_generations SET committed = 0 WHERE topic = 'private-room'");
			const corruptAlarm = await runVerification(config, { postgres: reader });
			expect(corruptAlarm.ok).toBe(false);
			expect(corruptAlarm.postgres.checks).toContainEqual({ scope: 'alarms', name: 'generation_authority', ok: false });
			await restored.query("UPDATE svti_alarms_generations SET committed = 1 WHERE topic = 'private-room'");
			await restored.query("DELETE FROM svti_alarms_generations WHERE topic = 'private-room'");
			const missingAuthority = await runVerification(config, { postgres: reader });
			expect(missingAuthority.postgres.checks).toContainEqual({ scope: 'alarms', name: 'generation_authority', ok: false });
			await restored.query('DELETE FROM svti_replay_seq');
			const partial = await runVerification(config, { postgres: reader });
			expect(partial.ok).toBe(false);
			expect(partial.postgres.checks).toContainEqual({ scope: 'replay', name: 'sequence_authority', ok: false });
			await restored.query("SELECT setval(pg_get_serial_sequence('svti_jobs','svti_jobs_id'),1,false)");
			const stale = await runVerification(config, { postgres: reader });
			expect(stale.postgres.checks).toContainEqual({ scope: 'jobs', name: 'serial_sequence_authority', ok: false });
			mkdirSync('source/recovery-drill', { recursive: true });
			writeFileSync('source/recovery-drill/postgres.dump', dump);
			writeFileSync('source/recovery-drill/postgres-report.json', JSON.stringify({ archiveSha256: createHash('sha256').update(dump).digest('hex'), restored: report, corruptAlarm, missingAuthority, partial, stale }, null, 2) + '\n');
		} finally { await restored.end(); }
	} finally {
		replay?.destroy(); tasks?.destroy(); cache?.destroy(); dlq?.destroy?.();
		await reader?.end(); await source?.end();
		await admin.query('DROP DATABASE IF EXISTS recovery_source WITH (FORCE)');
		await admin.query('DROP DATABASE IF EXISTS recovery_restored WITH (FORCE)');
		await admin.end();
	}
}, 120000);
