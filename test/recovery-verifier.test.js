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
		mkdirSync(join(fixture, 'src', 'shared'), { recursive: true });
		copyFileSync(join(root, 'src', 'shared', 'replay-envelope.js'), join(fixture, 'src', 'shared', 'replay-envelope.js'));
		writeFileSync(join(fixture, 'package.json'), JSON.stringify({ name: 'svelte-adapter-uws-extensions', version: '0.0.0', type: 'module' }));
		symlinkSync(join(root, 'node_modules', 'ioredis'), dependency, 'junction');
		linked = true;
		const result = spawnSync(process.execPath, ['docs/tools/recovery-verify.mjs'], {
			cwd: fixture, encoding: 'utf8', windowsHide: true, timeout: 30000
		});
		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(JSON.parse(result.stderr)).toEqual({ ok: false, complete: false, reason: 'usage_config_file_required' });
		writeFileSync(join(fixture, 'scope.json'), JSON.stringify({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'] } }));
		const redisOnly = spawnSync(process.execPath, ['docs/tools/recovery-verify.mjs', 'scope.json'], {
			cwd: fixture, encoding: 'utf8', windowsHide: true, timeout: 30000,
			env: { ...process.env, RECOVERY_REDIS_URL: '' }
		});
		expect(redisOnly.error).toBeUndefined();
		expect(redisOnly.status).toBe(1);
		expect(JSON.parse(redisOnly.stderr).reason).toBe('redis_url_required');
	} finally {
		if (linked) unlinkSync(dependency);
		rmSync(fixture, { recursive: true, force: true });
	}
}, 70000);

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
it('requires every lock fence counter to be a persistent positive safe integer', async () => {
	const cases = [['1', -1, true], ['9007199254740991', -1, true], ['7', 60000, false], ['0', -1, false], ['007', -1, false], ['abc', -1, false], ['9007199254740992', -1, false]];
	for (const [value, ttl, ok] of cases) {
		const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['lock-fences'] } }, {
			redis: {
				scan: async () => ['0', ['lock-fences:b3RoZXI', 'lock-fences:bG9jazpvcmRlcg']], type: async () => 'string',
				pttl: async key => key.endsWith('b3RoZXI') ? -1 : ttl,
				get: async key => key.endsWith('b3RoZXI') ? '3' : value
			}
		});
		expect(report.complete).toBe(true);
		expect(report.ok).toBe(ok);
		expect(report.redis.checks).toContainEqual({ scope: 'lock-fences', name: 'counter_authority', ok });
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
it('fails overlapping same-type namespaces instead of choosing one silently', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions','idempotency'], prefixes: { sessions: 'shared:', idempotency: 'shared:' } } }, {
		redis: { scan: async () => ['0', ['shared:byuser:private']], type: async () => 'hash', pttl: async () => 60000 }
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
it('classifies the emergency policy keys by name and type', async () => {
	const keys = { 'v1:ratelimit-control:emergency': 'string', 'v1:ratelimit:emergency': 'hash' };
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['rate-window', 'emergency-scale'] } }, {
		redis: { scan: async () => ['0', Object.keys(keys)], type: async key => keys[key], pttl: async () => -1 }
	});
	expect(report.redis.matchedKeys).toBe(2);
	expect(report.redis.observations['rate-window']).toMatchObject({ keys: 1, wrongTypes: 0 });
	expect(report.redis.observations['emergency-scale']).toMatchObject({ keys: 1, wrongTypes: 0 });
});
it('classifies tenant-budget rate-limit buckets apart from principal buckets', async () => {
	const keys = { 'v1:ratelimit-tenant:acme': 'hash', 'v1:ratelimits-tenant:acme': 'hash', 'v1:ratelimitg-tenant:acme': 'hash', 'v1:ratelimit:acme': 'hash' };
	const families = ['rate-window', 'rate-window-tenant', 'rate-sliding-tenant', 'rate-gcra-tenant'];
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families } }, {
		redis: { scan: async () => ['0', Object.keys(keys)], type: async key => keys[key], pttl: async () => -1 }
	});
	expect(report.redis.matchedKeys).toBe(4);
	for (const id of families) expect(report.redis.observations[id]).toMatchObject({ keys: 1, wrongTypes: 0 });
});
it('rejects unknown scope fields and non-boolean Cluster selection', () => {
	expect(() => validateConfig({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'], cluster: 'true' } })).toThrow();
	expect(() => validateConfig({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'], minKeys: 1 } })).toThrow();
});

it('requires PostgreSQL alarm rows, retained generation authority and executions together', () => {
	const all = { alarms: 'svti_alarms', 'alarm-generations': 'svti_alarms_generations', 'alarm-executions': 'svti_alarms_executions' };
	expect(() => validateConfig({ schemaVersion: 1, postgres: { stores: all } })).not.toThrow();
	for (const stores of [{ alarms: 'svti_alarms' }, { 'alarm-generations': 'svti_alarms_generations' }, { alarms: all.alarms, 'alarm-generations': all['alarm-generations'] }, { 'alarm-executions': all['alarm-executions'] }]) {
		expect(() => validateConfig({ schemaVersion: 1, postgres: { stores } })).toThrow('incomplete_restore_group');
	}
});

function alarmFixture({ due = [], metadata = [], reserved = [], committed = [], current = [], attempt = [], executions = [], order = executions.map(([id]) => id) } = {}) {
	const hashes = new Map([['meta', metadata.map(topic => [topic, '{}'])], ['reserved', reserved], ['committed', committed], ['current', current], ['attempt', attempt], ['executions', executions]].map(([role, entries]) => ['alarm:{alarms}:' + role, new Map(entries)]).filter(([, entries]) => entries.size));
	const sorted = new Map([['due', due], ['execution-order', order]].map(([role, members]) => ['alarm:{alarms}:' + role, members]).filter(([, members]) => members.length));
	return {
		scan: async () => ['0', [...hashes.keys(), ...sorted.keys()]],
		type: async key => hashes.has(key) ? 'hash' : 'zset', pttl: async () => -1,
		hlen: async key => hashes.get(key).size, hkeys: async key => [...hashes.get(key).keys()],
		hmget: async (key, ...fields) => fields.map(field => hashes.get(key)?.get(field) ?? null),
		zcard: async key => sorted.get(key)?.length ?? 0, zrange: async key => sorted.get(key) ?? []
	};
}

const retained = (topic, generation, fields = {}) => [topic.replaceAll('\0', '\0\x01') + '\0\0' + generation, JSON.stringify({ topic, generation, score: '1', payload: '', attempt: '1', status: 'claimed', claimedAt: '5', ...fields })];
it.each([
	[{ reserved: [['room', '2']], committed: [['room', '2']], executions: [retained('room', '1'), retained('room', '2', { status: 'failed', failedAt: '6', errorClass: 'handler', attempt: '3' })] }, true],
	[{ reserved: [['a\0b', '1']], committed: [['a\0b', '1']], executions: [retained('a\0b', '1')] }, true],
	[{ due: ['room'], metadata: ['room'], reserved: [['room', '1']], committed: [['room', '1']], current: [['room', '1']], attempt: [['room', '1:2']] }, true],
	[{ reserved: [['room', '2']], committed: [['room', '1']], executions: [retained('room', '2')] }, false],
	[{ executions: [retained('room', '1')] }, false],
	[{ reserved: [['room', '1']], committed: [['room', '1']], executions: [retained('room', '1')], order: [] }, false],
	[{ reserved: [['room', '1']], committed: [['room', '1']], executions: [[retained('other', '1')[0], retained('room', '1')[1]]] }, false],
	[{ reserved: [['room', '1']], committed: [['room', '1']], executions: [retained('room', '1', { status: 'failed' })] }, false],
	[{ reserved: [['room', '1']], committed: [['room', '1']], executions: [retained('room', '1', { errorClass: 'handler' })] }, false],
	[{ reserved: [['room', '1']], committed: [['room', '1']], executions: [retained('room', '1', { attempt: '0' })] }, false],
	[{ reserved: [['room', '1']], committed: [['room', '1']], executions: [['bogus-id', 'not json']] }, false],
	// A counter left by a cancelled or replaced requeued schedule is inert.
	[{ due: ['room'], metadata: ['room'], reserved: [['room', '2']], committed: [['room', '2']], current: [['room', '2']], attempt: [['room', '1:2']] }, true],
	[{ reserved: [['room', '1']], committed: [['room', '1']], attempt: [['room', '1:2']] }, true],
	[{ reserved: [['room', '3']], committed: [['room', '2']], attempt: [['room', '3:2']] }, false],
	[{ attempt: [['room', '1:2']] }, false],
	[{ reserved: [['room', '1']], committed: [['room', '1']], attempt: [['room', '1:2:3']] }, false],
	[{ due: ['room'], metadata: ['room'], reserved: [['room', '1']], committed: [['room', '1']], current: [['room', '1']], attempt: [['room', '1:1']] }, false]
])('verifies retained Redis alarm executions %#', async (state, ok) => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['alarms'] } }, { redis: alarmFixture(state) });
	expect(report.complete).toBe(true);
	expect(report.redis.checks).toContainEqual({ scope: 'alarms', name: 'execution_authority', ok });
	expect(report.redis.checks).toContainEqual({ scope: 'alarms', name: 'generation_authority', ok: true });
});

it.each([
	[{ reserved: [['deleted', '9007199254740993']], committed: [['deleted', '9007199254740992']] }, true],
	[{ reserved: [['pending', '1']], committed: [['pending', '0']] }, true],
	[{ due: ['active'], metadata: ['active'], reserved: [['active', '4']], committed: [['active', '3']], current: [['active', '3']] }, true],
	[{ due: ['legacy'], metadata: ['legacy'] }, false],
	[{ reserved: [['deleted', '5']] }, false],
	[{ reserved: [['deleted', '2']], committed: [['deleted', '3']] }, false],
	[{ reserved: [['deleted', '01']], committed: [['deleted', '0']] }, false],
	[{ reserved: [['deleted', '9223372036854775808']], committed: [['deleted', '0']] }, false],
	[{ due: ['active'], metadata: ['active'], reserved: [['active', '4']], committed: [['active', '3']], current: [['active', '2']] }, false],
	[{ reserved: [['deleted', '3']], committed: [['deleted', '3']], current: [['deleted', '3']] }, false]
])('verifies exact retained Redis alarm authority %#', async (state, ok) => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['alarms'] } }, { redis: alarmFixture(state) });
	expect(report.complete).toBe(true);
	expect(report.ok).toBe(ok);
	expect(report.redis.checks).toContainEqual({ scope: 'alarms', name: 'generation_authority', ok });
});

it('bounds retained alarm authority even when no schedule remains', async () => {
	const state = { reserved: [['one', '1'], ['two', '1']], committed: [['one', '0'], ['two', '0']] };
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['alarms'], maxMembers: 1 } }, { redis: alarmFixture(state) });
	expect(report).toMatchObject({ ok: false, complete: false, redis: { reason: 'member_limit_reached' } });
});

it('rejects expiring alarm authority after schedules are removed', async () => {
	const redis = alarmFixture({ reserved: [['deleted', '2']], committed: [['deleted', '1']] });
	redis.pttl = async () => 1000;
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['alarms'] } }, { redis });
	expect(report.complete).toBe(true);
	expect(report.redis.checks).toContainEqual({ scope: 'alarms', name: 'generation_authority', ok: false });
});

it('checks the last bounded generation batch without object-key aliasing', async () => {
	const topics = Array.from({ length: 600 }, (_, index) => index === 0 ? '__proto__' : 'retained-' + index);
	const state = { reserved: topics.map(topic => [topic, '4']), committed: topics.map(topic => [topic, '3']) };
	state.committed[599][1] = '5';
	const redis = alarmFixture(state), hmget = redis.hmget, batches = [];
	redis.hmget = async (key, ...fields) => { batches.push(fields.length); return hmget(key, ...fields); };
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['alarms'] } }, { redis });
	expect(report.complete).toBe(true);
	expect(report.redis.checks).toContainEqual({ scope: 'alarms', name: 'generation_authority', ok: false });
	expect(batches).toEqual([256, 256, 88, 256, 256, 88]);
});

for (const families of [['sessions'], ['registry'], ['sessions', 'registry']]) {
	it(`classifies default session records and registry routing in their own namespaces for ${families.join(', ')}`, async () => {
		const keys = new Map([['sess:token', 'string'], ['sess:byuser:user', 'hash'], ['regsess:route', 'hash'], ['conns:user', 'hash']]);
		const commands = [];
		const redis = {
			scan: async () => { commands.push('scan'); return ['0', [...keys.keys()]]; },
			type: async key => { commands.push('type'); return keys.get(key); },
			pttl: async () => { commands.push('pttl'); return 60000; }
		};
		const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families, minimumKeys: Object.fromEntries(families.map(id => [id, 1])) } }, { redis });
		expect(report.ok).toBe(true);
		if (families.includes('sessions')) expect(report.redis.observations.sessions.keys).toBe(2);
		if (families.includes('registry')) expect(report.redis.observations.registry.keys).toBe(2);
		expect(commands.every(command => ['scan', 'type', 'pttl'].includes(command))).toBe(true);
	});
}

it('flags a hash in the session namespace as a wrong type', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'] } }, {
		redis: { scan: async () => ['0', ['sess:route']], type: async () => 'hash', pttl: async () => 60000 }
	});
	expect(report.ok).toBe(false);
	expect(report.redis.checks).toContainEqual({ scope: 'sessions', name: 'key_types', ok: false });
});

it('flags a string in the registry routing namespace as a wrong type', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['registry'] } }, {
		redis: { scan: async () => ['0', ['regsess:token']], type: async () => 'string', pttl: async () => 60000 }
	});
	expect(report.ok).toBe(false);
	expect(report.redis.checks).toContainEqual({ scope: 'registry', name: 'key_types', ok: false });
});

it('still rejects a wrong type under a configured session prefix', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions'], prefixes: { sessions: 'auth:' } } }, {
		redis: { scan: async () => ['0', ['auth:token']], type: async () => 'hash', pttl: async () => 60000 }
	});
	expect(report.ok).toBe(false);
	expect(report.redis.checks).toContainEqual({ scope: 'sessions', name: 'key_types', ok: false });
});

it('rejects a key that configured prefixes place in both families', async () => {
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['sessions', 'registry'], prefixes: { sessions: 'regsess:' } } }, {
		redis: { scan: async () => ['0', ['regsess:shared']], type: async () => 'hash', pttl: async () => 60000 }
	});
	expect(report.ok).toBe(false);
	expect(report.redis.checks).toContainEqual({ scope: 'redis', name: 'unambiguous_namespaces', ok: false });
});

it('counts undecodable and missing replay entries without reporting topics or payloads', async () => {
	const member = (seq, data = { secret: 'private-payload' }) => JSON.stringify({ v: 1, seq, event: 'message', data });
	const buffer = [[member(3), '3'], ['{broken private-payload', '4'], [member(6), '6']];
	const stream = [['2-0', ['v', '1', 'event', 'message', 'data', '{}']], ['3-0', ['v', '99', 'event', 'message', 'data', '{}']]];
	const keys = ['replay:seq:{private-room}', 'replay:buf:{private-room}', 'replay:seq:{stream-room}', 'replay:streambuf:{stream-room}'];
	const report = await runVerification({ schemaVersion: 1, redis: { keyPrefix: '', families: ['replay'] } }, {
		redis: {
			scan: async () => ['0', keys],
			type: async key => key.includes(':seq:') ? 'string' : key.includes('streambuf') ? 'stream' : 'zset',
			pttl: async () => -1,
			get: async key => key.includes('private-room') ? '7' : '3',
			zrevrange: async () => buffer.at(-1),
			xrevrange: async () => [stream.at(-1)],
			zcard: async () => buffer.length, xlen: async () => stream.length,
			zrange: async () => buffer.flat(),
			xrange: async () => stream
		}
	});
	expect(report.complete).toBe(true);
	// Sorted set: seq 4 corrupt, 5 and 7 missing. Stream: seq 3 in an unknown version.
	expect(report.redis.observations.replayEntries).toEqual({ buffers: 2, entries: 5, corruptEntries: 2, missingEntries: 2 });
	expect(report.redis.checks).toContainEqual({ scope: 'replay', name: 'sequence_authority', ok: true });
	expect(report.redis.checks).toContainEqual({ scope: 'replay', name: 'decodable_entries', ok: false });
	expect(JSON.stringify(report)).not.toMatch(/private-room|stream-room|private-payload/);
});
