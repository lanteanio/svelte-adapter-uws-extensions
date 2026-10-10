import { expect, it } from 'vitest';
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateConfig, runVerification, postgresRecoveryConfig, connectPostgresRecovery } from '../docs/tools/recovery-verify.mjs';

it('pins PostgreSQL driver deadlines despite URL overrides and retains connection options', async () => {
	const { default: pg } = await import('pg');
	const options = '-c search_path=restored,public -c default_transaction_read_only=off';
	const url = new URL('postgres://reader:secret@localhost/recovery?query_timeout=999999999&statement_timeout=0&sslmode=disable');
	url.searchParams.set('options', options);
	const client = new pg.Client(postgresRecoveryConfig(url.href));
	expect(Number(client.connectionParameters.query_timeout)).toBe(6000);
	expect(Number(client.connectionParameters.statement_timeout)).toBe(5000);
	expect(client.connectionParameters.options).toBe(options);
	expect(client.connectionParameters.user).toBe('reader');
	expect(client.connectionParameters.database).toBe('recovery');
	const calls = [];
	await connectPostgresRecovery({ connect: async () => calls.push('connected'), query: async config => calls.push(config) });
	expect(calls).toEqual(['connected', { text: 'SET SESSION default_transaction_read_only = on', query_timeout: 6000 }]);
});

it('runs the command without the optional PostgreSQL dependency installed', () => {
	const root = dirname(dirname(fileURLToPath(import.meta.url)));
	const fixture = mkdtempSync(join(tmpdir(), 'recovery-cli-'));
	const dependency = join(fixture, 'node_modules', 'ioredis');
	let linked = false;
	try {
		mkdirSync(join(fixture, 'docs', 'tools'), { recursive: true });
		mkdirSync(join(fixture, 'node_modules'));
		copyFileSync(join(root, 'docs', 'tools', 'recovery-verify.mjs'), join(fixture, 'docs', 'tools', 'recovery-verify.mjs'));
		copyFileSync(join(root, 'docs', 'persistence.v1.json'), join(fixture, 'docs', 'persistence.v1.json'));
		writeFileSync(join(fixture, 'package.json'), JSON.stringify({ name: 'svelte-adapter-uws-extensions', version: '0.0.0', type: 'module' }));
		symlinkSync(join(root, 'node_modules', 'ioredis'), dependency, 'junction');
		linked = true;
		const result = spawnSync(process.execPath, ['docs/tools/recovery-verify.mjs'], {
			cwd: fixture, encoding: 'utf8', windowsHide: true, timeout: 10000
		});
		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(JSON.parse(result.stderr)).toEqual({ ok: false, complete: false, reason: 'usage_config_file_required' });
		writeFileSync(join(fixture, 'scope.json'), JSON.stringify({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'] } }));
		const redisOnly = spawnSync(process.execPath, ['docs/tools/recovery-verify.mjs', 'scope.json'], {
			cwd: fixture, encoding: 'utf8', windowsHide: true, timeout: 10000,
			env: { ...process.env, RECOVERY_REDIS_URL: '' }
		});
		expect(redisOnly.error).toBeUndefined();
		expect(redisOnly.status).toBe(1);
		expect(JSON.parse(redisOnly.stderr).reason).toBe('redis_url_required');
	} finally {
		if (linked) unlinkSync(dependency);
		rmSync(fixture, { recursive: true, force: true });
	}
});

it('rejects missing replay authority, unknown storage and unsafe configured identifiers', () => {
	for (const stores of [{ replay: 'events' }, { unknown: 'table' }, { tasks: 'tasks; DROP TABLE tasks' }, { tasks: 'x', jobs: 'x' }]) {
		expect(() => validateConfig({ schemaVersion: 1, postgres: { stores } })).toThrow();
	}
	expect(() => validateConfig({ schemaVersion: 1, redis: { keyPrefix: '', families: ['invented'] } })).toThrow();
	expect(() => validateConfig({ schemaVersion: 1, redis: { keyPrefix: '', families: ['replay'], maxKeys: Infinity } })).toThrow();
});
it('reports bounded scans as incomplete rather than successful empty verification', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: 'private:', families: ['sessions'], maxKeys: 1 } }, {
		redis: { scan: async () => ['0', ['private:other:one', 'private:other:two']] }
	});
	expect(report).toMatchObject({ ok: false, complete: false, redis: { reason: 'key_limit_reached', scannedKeys: 1 } });
	expect(JSON.stringify(report)).not.toContain('private:');
});
it('scans and deduplicates all Cluster masters and rejects a changed topology', async () => {
	let topologyCalls = 0;
	const calls = [];
	const redis = {
		nodes: () => [0,1,2].map(n => ({ scan: async () => { calls.push(n); return ['0', ['scope:sess:one']]; } })),
		cluster: async () => [[0, 16383, ['hidden-host', 6379, ++topologyCalls === 1 ? 'old' : 'new']]],
		type: async () => 'string', pttl: async () => 60000
	};
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: 'scope:', families: ['sessions'] } }, { redis });
	expect(calls).toEqual([0,1,2]);
	expect(report).toMatchObject({ ok: false, complete: false, redis: { reason: 'topology_changed', scannedKeys: 1 } });
	expect(JSON.stringify(report)).not.toMatch(/scope:|hidden-host/);
});
it('never includes backend errors, credentials or key values in the report', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['replay'] } }, {
		redis: { scan: async () => { throw new Error('credential=private-secret topic=customer'); } }
	});
	expect(report).toMatchObject({ ok: false, complete: false, redis: { reason: 'backend_read_failed' } });
	expect(JSON.stringify(report)).not.toMatch(/private-secret|customer/);
});
it('validates retained dead-letter counters even when no records remain', async () => {
	for (const sequence of ['not-an-integer', '-1', '0', '123']) {
		const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['dead-letter'] } }, {
			redis: {
				scan: async () => ['0', ['dlq:seq:{dlq}']], type: async () => 'string', pttl: async () => -1,
				get: async () => sequence
			}
		});
		expect(report.complete).toBe(true);
		expect(report.ok).toBe(sequence === '0' || sequence === '123');
		expect(report.redis.checks).toContainEqual({ scope: 'redis', name: 'paired_membership', ok: report.ok });
	}
});
it('does not call a missing configured namespace a successful expected restore', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'], minimumKeys: { sessions: 1 } } }, {
		redis: { scan: async () => ['0', []] }
	});
	expect(report).toMatchObject({ ok: false, complete: true });
	expect(report.redis.checks).toContainEqual({ scope: 'sessions', name: 'minimum_keys', ok: false });
});

it('treats configured prefix tokens and glob characters as literal text', async () => {
	const config = { schemaVersion: 1, redis: { keyPrefix: 'scope[*]:', families: ['sessions'], prefixes: { sessions: '<topic>:' }, minimumKeys: { sessions: 1 } } };
	const keys = ['scope[*]:<topic>:token\nline'];
	let match;
	const report = await runVerification(config, { redis: {
		scan: async (_cursor, _match, pattern) => { match = pattern; return ['0', keys]; },
		type: async () => 'string', pttl: async () => 60000
	} });
	expect(report.ok).toBe(true);
	expect(match).toBe('scope\\[\\*\\]:*');
});
it('fails observed overlapping family namespaces instead of choosing one silently', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions','registry'] } }, {
		redis: { scan: async () => ['0', ['sess:private']], type: async () => 'hash', pttl: async () => 60000 }
	});
	expect(report.ok).toBe(false);
	expect(report.redis.checks).toContainEqual({ scope: 'redis', name: 'unambiguous_namespaces', ok: false });
	expect(JSON.stringify(report)).not.toContain('private');
});
it('checks the deadline after an empty final scan completes', async () => {
	const report = await runVerification({ schemaVersion: 1, timeoutMs: 1, redis: { keyPrefix: '', families: ['sessions'] } }, {
		redis: { scan: async () => { await new Promise(resolve => setTimeout(resolve, 5)); return ['0', []]; } }
	});
	expect(report).toMatchObject({ ok: false, complete: false, redis: { reason: 'deadline_reached' } });
});

it('does not mistake the reserved emergency policy for a rate-window hash', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['rate-window'] } }, {
		redis: { scan: async () => ['0', ['v1:ratelimit:emergency']], type: async () => 'string', pttl: async () => -1 }
	});
	expect(report.ok).toBe(true);
	expect(report.redis.matchedKeys).toBe(0);
});
it('rejects unknown scope fields and non-boolean Cluster selection', () => {
	expect(() => validateConfig({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'], cluster: 'true' } })).toThrow();
	expect(() => validateConfig({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'], minKeys: 1 } })).toThrow();
});
