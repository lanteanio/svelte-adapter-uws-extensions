import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
import Redis from 'ioredis';

const bytes = readFileSync(new URL('../persistence.v1.json', import.meta.url));
export const manifest = JSON.parse(bytes);
const pkg = JSON.parse(readFileSync(new URL('../../package.json', import.meta.url)));
const manifestSha256 = createHash('sha256').update(bytes).digest('hex');
const quote = text => '"' + text.replaceAll('"', '""') + '"';
const sqlName = name => name.split('.').map(quote).join('.');
const escapeRegex = text => text.replace(/[.*+?^$()|[\]\\{}]/g, '\\$&');
const escapeGlob = text => text.replace(/[\\*?\[\]]/g, '\\$&');
const integer = text => typeof text === 'string' && /^\d+$/.test(text);
function fail(code) { throw Object.assign(new Error(code), { recoveryCode: code }); }
function bound(value, fallback, max) {
	if (value === undefined) return fallback;
	if (!Number.isSafeInteger(value) || value < 1 || value > max) fail('invalid_configuration');
	return value;
}
function keysOnly(value, allowed) {
	if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !allowed.includes(key))) fail('invalid_configuration');
}
export function validateConfig(config) {
	keysOnly(config, ['schemaVersion','timeoutMs','postgres','redis']);
	if (!config || config.schemaVersion !== 1 || (!config.postgres && !config.redis)) fail('invalid_configuration');
	const selected = { timeoutMs: bound(config.timeoutMs, 60000, 3600000) };
	if (config.postgres) {
		keysOnly(config.postgres, ['stores']);
		const stores = config.postgres.stores;
		if (!stores || Array.isArray(stores) || !Object.keys(stores).length) fail('invalid_configuration');
		selected.postgres = Object.entries(stores).map(([id, table]) => {
			const spec = manifest.postgres.find(item => item.id === id);
			if (!spec || typeof table !== 'string' || !/^[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?$/.test(table)) fail('invalid_configuration');
			return { ...spec, table };
		});
		if (Boolean(stores.replay) !== Boolean(stores['replay-sequence'])) fail('incomplete_restore_group');
		if (new Set(Object.values(stores)).size !== Object.keys(stores).length) fail('overlapping_tables');
	}
	if (config.redis) {
		keysOnly(config.redis, ['keyPrefix','families','prefixes','minimumKeys','maxKeys','maxScanCalls','maxMembers','cluster']);
		const { keyPrefix, families, prefixes = {}, minimumKeys = {} } = config.redis;
		if (config.redis.cluster !== undefined && typeof config.redis.cluster !== 'boolean') fail('invalid_configuration');
		if (typeof keyPrefix !== 'string' || !Array.isArray(families) || !families.length || new Set(families).size !== families.length) fail('invalid_configuration');
		const specs = families.map(id => manifest.redis.find(item => item.id === id));
		if (specs.some(item => !item)) fail('invalid_configuration');
		for (const [id, prefix] of Object.entries(prefixes)) {
			if (!specs.some(item => item.id === id && item.defaultPrefix !== undefined) || typeof prefix !== 'string') fail('invalid_configuration');
		}
		for (const [id, count] of Object.entries(minimumKeys)) {
			if (!families.includes(id) || !Number.isSafeInteger(count) || count < 0) fail('invalid_configuration');
		}
		selected.redis = { ...config.redis, specs, prefixes, minimumKeys,
			maxKeys: bound(config.redis.maxKeys, 10000, 1000000),
			maxScanCalls: bound(config.redis.maxScanCalls, 10000, 1000000),
			maxMembers: bound(config.redis.maxMembers, 10000, 1000000) };
	}
	return selected;
}
const section = () => ({ complete: true, checks: [], observations: {} });
const check = (result, scope, name, ok) => result.checks.push({ scope, name, ok: Boolean(ok) });

/** Catalog and selected data checks in one read-only repeatable-read transaction. */
export async function verifyPostgres(client, specs, remaining = () => {}) {
	const result = section();
	const query = async (...args) => { remaining(); const value = await client.query(...args); remaining(); return value; };
	try {
		await query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
		await query("SET LOCAL statement_timeout = '5s'");
		await query("SET LOCAL lock_timeout = '1s'");
		await query("SET LOCAL idle_in_transaction_session_timeout = '10s'");
		const version = (await query('SHOW server_version_num')).rows[0].server_version_num;
		check(result, 'postgres', 'server_version', Number(version) >= 160000);
		const valid = new Set();
		for (const spec of specs) {
			remaining();
			const oid = (await query('SELECT to_regclass($1)::oid AS oid', [spec.table])).rows[0].oid;
			check(result, spec.id, 'table_present', oid != null);
			if (oid == null) continue;
			const base = spec.table.split('.').at(-1);
			const resolve = name => name === '$id' ? base + '_id' : name;
			const columns = (await query('SELECT attname, format_type(atttypid, atttypmod) AS type FROM pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped', [oid])).rows;
			const columnsOk = Object.entries(spec.columns).every(([name, type]) => columns.some(column => column.attname === resolve(name) && column.type === type));
			check(result, spec.id, 'column_types', columnsOk);
			const indexes = (await query('SELECT i.indexrelid, array_agg(a.attname::text ORDER BY k.position) AS columns FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,position) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE i.indrelid=$1 AND i.indisvalid AND i.indisready AND k.position<=i.indnkeyatts GROUP BY i.indexrelid', [oid])).rows;
			check(result, spec.id, 'index_columns', spec.indexColumns.every(required => indexes.some(index => required.every((name, i) => index.columns[i] === resolve(name)))));
			if (!columnsOk) continue;
			valid.add(spec.id);
			result.observations[spec.id] = { rows: (await query('SELECT count(*)::text AS count FROM ' + sqlName(spec.table))).rows[0].count };
			if (spec.serialColumn) {
				const column = resolve(spec.serialColumn);
				const sequence = (await query('SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=pg_get_serial_sequence($1,$2)::regclass', [spec.table, column])).rows[0];
				check(result, spec.id, 'serial_sequence_present', Boolean(sequence));
				if (sequence) {
					const state = (await query('SELECT last_value::text, is_called FROM ' + quote(sequence.nspname) + '.' + quote(sequence.relname))).rows[0];
					const max = (await query('SELECT max(' + quote(column) + ')::text AS value FROM ' + sqlName(spec.table))).rows[0].value;
					check(result, spec.id, 'serial_sequence_authority', max === null || (BigInt(state.last_value) + (state.is_called ? 1n : 0n)) > BigInt(max));
				}
			}
		}
		if (valid.has('replay') && valid.has('replay-sequence')) {
			remaining();
			const rows = sqlName(specs.find(item => item.id === 'replay').table);
			const authority = sqlName(specs.find(item => item.id === 'replay-sequence').table);
			const broken = (await query('SELECT EXISTS(SELECT 1 FROM ' + rows + ' r LEFT JOIN ' + authority + ' s USING(topic) WHERE s.topic IS NULL OR s.seq<r.seq OR s.epoch<0) AS broken')).rows[0].broken;
			check(result, 'replay', 'sequence_authority', !broken);
		}
		await query('ROLLBACK');
	} catch (error) {
		result.complete = false;
		result.reason = error.recoveryCode || (error.code === '57014' ? 'deadline_reached' : 'backend_read_failed');
		try { await client.query('ROLLBACK'); } catch { /* caller closes the connection */ }
	}
	return result;
}
function matchers(config) {
	return config.specs.flatMap(spec => spec.roles.map(role => {
		const template = role.template;
		const names = [];
		let cursor = 0, regex = '^' + escapeRegex(config.keyPrefix), literal = 0;
		for (const match of template.matchAll(/<([^>]+)>/g)) {
			const part = template.slice(cursor, match.index);
			regex += escapeRegex(part); literal += part.length;
			if (match[1] === 'prefix') {
				const prefix = config.prefixes[spec.id] ?? spec.defaultPrefix ?? '';
				regex += escapeRegex(prefix); literal += prefix.length;
			} else { regex += '([\\s\\S]*)'; names.push(match[1]); }
			cursor = match.index + match[0].length;
		}
		regex += escapeRegex(template.slice(cursor)) + '$'; literal += template.length - cursor;
		return { spec, role, names, regex: new RegExp(regex), literal };
	})).sort((a, b) => b.literal - a.literal);
}
async function topology(redis) {
	if (typeof redis.nodes !== 'function') return null;
	return JSON.stringify((await redis.cluster('slots')).map(row => [row[0], row[1], row[2][2]]).sort((a,b) => a[0]-b[0]));
}
/** Scan every Cluster master. Key metadata and member checks have explicit caps. */
export async function verifyRedis(redis, config, remaining = () => {}) {
	const result = section(), groups = new Map(), seen = new Set(), patterns = matchers(config);
	let calls = 0, matched = 0, ambiguous = 0;
	const read = async (command, ...args) => { remaining(); const value = await redis[command](...args); remaining(); return value; };
	try {
		remaining(); const before = await topology(redis); remaining();
		const masters = typeof redis.nodes === 'function' ? redis.nodes('master') : [redis];
		if (!masters.length) fail('incomplete_topology');
		for (const node of masters) {
			let cursor = '0';
			do {
				remaining(); if (++calls > config.maxScanCalls) fail('scan_limit_reached');
				const page = await node.scan(cursor, 'MATCH', escapeGlob(config.keyPrefix) + '*', 'COUNT', 100);
				remaining(); cursor = page[0];
				for (const key of page[1]) {
					if (seen.has(key)) continue;
					if (seen.size >= config.maxKeys) fail('key_limit_reached');
					seen.add(key);
					let matches = patterns.filter(item => item.regex.test(key));
					// The exact operator policy key intentionally shares the rate-window prefix.
					if (key === config.keyPrefix + 'v1:ratelimit:emergency') matches = matches.filter(item => item.spec.id !== 'rate-window');
					if (new Set(matches.map(item => item.spec.id)).size > 1) { ambiguous++; continue; }
					const pattern = matches[0];
					if (!pattern) continue;
					const type = await read('type', key);
					if (type === 'none') { result.complete = false; result.reason = 'keys_changed'; continue; }
					const { spec, role } = pattern;
					matched++;
					const stats = result.observations[spec.id] ??= { keys: 0, expiringKeys: 0, persistentKeys: 0, wrongTypes: 0 };
					stats.keys++;
					const ttl = await read('pttl', key);
					if (ttl === -2) { result.complete = false; result.reason = 'keys_changed'; }
					else if (ttl === -1) stats.persistentKeys++; else stats.expiringKeys++;
					if (type !== role.type) { stats.wrongTypes++; continue; }
					const values = pattern.regex.exec(key).slice(1);
					const topic = values[pattern.names.indexOf('topic')] ?? '';
					const groupId = spec.id + '\0' + topic;
					let group = groups.get(groupId);
					if (!group) { group = { family: spec.id, roles: {} }; groups.set(groupId, group); }
					group.roles[role.id] = key;
				}
			} while (cursor !== '0');
		}
		for (const spec of config.specs) {
			const stats = result.observations[spec.id] ??= { keys: 0, expiringKeys: 0, persistentKeys: 0, wrongTypes: 0 };
			check(result, spec.id, 'key_types', stats.wrongTypes === 0);
			check(result, spec.id, 'minimum_keys', stats.keys >= (config.minimumKeys[spec.id] ?? 0));
		}
		let replayOk = true, pairOk = true;
		for (const { family, roles } of groups.values()) {
			remaining();
			if (family === 'replay') {
				const seq = roles.sequence ? await read('get', roles.sequence) : null;
				if (roles.sequence) replayOk &&= integer(seq);
				if (roles.epoch) replayOk &&= integer(await read('get', roles.epoch));
				if (roles.buffer || roles.stream) {
					replayOk &&= integer(seq);
					if (roles.buffer) {
						const last = await read('zrevrange', roles.buffer, 0, 0, 'WITHSCORES');
						if (last.length) replayOk &&= integer(last[1]) && integer(seq) && BigInt(seq) >= BigInt(last[1]);
					}
					if (roles.stream) {
						const last = await read('xrevrange', roles.stream, '+', '-', 'COUNT', 1);
						if (last.length) replayOk &&= integer(seq) && BigInt(seq) >= BigInt(last[0][0].split('-')[0]);
					}
				}
			}
			if (family === 'alarms' || family === 'dead-letter') {
				const hash = roles[family === 'alarms' ? 'metadata' : 'records'];
				const sorted = roles[family === 'alarms' ? 'due' : 'order'];
				const hashCount = hash ? Number(await read('hlen', hash)) : 0;
				const sortedCount = sorted ? Number(await read('zcard', sorted)) : 0;
				pairOk &&= hashCount === sortedCount;
				if (Math.max(hashCount, sortedCount) > config.maxMembers) fail('member_limit_reached');
				const fields = new Set(hash ? await read('hkeys', hash) : []);
				const members = sorted ? await read('zrange', sorted, 0, -1) : [];
				pairOk &&= members.every(member => fields.has(member));
				if (family === 'dead-letter' && (roles.sequence || hashCount)) {
					const seq = roles.sequence ? await read('get', roles.sequence) : null;
					pairOk &&= integer(seq) && members.every(member => integer(member) && BigInt(member) <= BigInt(seq));
				}
			}
		}
		if (config.specs.some(spec => spec.id === 'replay')) check(result, 'replay', 'sequence_authority', replayOk);
		if (config.specs.some(spec => ['alarms','dead-letter'].includes(spec.id))) check(result, 'redis', 'paired_membership', pairOk);
		if (before !== await topology(redis)) fail('topology_changed');
		remaining(); check(result, 'redis', 'unambiguous_namespaces', ambiguous === 0);
	} catch (error) { result.complete = false; result.reason = error.recoveryCode || 'backend_read_failed'; }
	result.scannedKeys = seen.size; result.matchedKeys = matched; result.scanCalls = calls;
	return result;
}
export async function runVerification(config, clients) {
	const selected = validateConfig(config), deadline = performance.now() + selected.timeoutMs;
	const remaining = () => { if (performance.now() >= deadline) fail('deadline_reached'); };
	const report = { schemaVersion: 1, package: pkg.name, packageVersion: pkg.version, manifestSha256,
		claim: 'Selected storage invariants only; backup freshness, complete application recovery and RTO/RPO require a host drill.' };
	if (selected.postgres) report.postgres = await verifyPostgres(clients.postgres, selected.postgres, remaining);
	if (selected.redis) report.redis = await verifyRedis(clients.redis, selected.redis, remaining);
	const sections = [report.postgres, report.redis].filter(Boolean);
	report.complete = sections.every(part => part.complete);
	report.ok = report.complete && sections.every(part => part.checks.every(item => item.ok));
	return report;
}
export function postgresRecoveryConfig(connectionString) {
	const url = new URL(connectionString);
	if (!['postgres:', 'postgresql:'].includes(url.protocol)) fail('invalid_postgres_url');
	// node-pg merges URL parameters after its configuration object. Pin these
	// bounds in the URL while retaining search_path, authentication and TLS.
	url.searchParams.set('query_timeout', '6000');
	url.searchParams.set('statement_timeout', '5000');
	return { connectionString: url.href, connectionTimeoutMillis: 5000,
		query_timeout: 6000, statement_timeout: 5000, application_name: 'storage-recovery-verify' };
}
export async function connectPostgresRecovery(client) {
	await client.connect();
	await client.query({ text: 'SET SESSION default_transaction_read_only = on', query_timeout: 6000 });
}
async function main() {
	const args = process.argv.slice(2);
	if (args.length !== 1) fail('usage_config_file_required');
	const config = JSON.parse(readFileSync(args[0], 'utf8'));
	const selected = validateConfig(config), clients = {};
	try {
		if (selected.postgres) {
			if (!process.env.RECOVERY_POSTGRES_URL) fail('postgres_url_required');
			const { default: pg } = await import('pg');
			clients.postgres = new pg.Client(postgresRecoveryConfig(process.env.RECOVERY_POSTGRES_URL));
			clients.postgres.on('error', () => {});
			await connectPostgresRecovery(clients.postgres);
		}
		if (selected.redis) {
			if (!process.env.RECOVERY_REDIS_URL) fail('redis_url_required');
			const url = new URL(process.env.RECOVERY_REDIS_URL);
			if (!['redis:', 'rediss:'].includes(url.protocol)) fail('invalid_redis_url');
			const options = { lazyConnect: true, connectTimeout: 5000, commandTimeout: 5000, maxRetriesPerRequest: 0,
				enableOfflineQueue: false, retryStrategy: () => null };
			if (config.redis.cluster === true) {
				if (!['','/','/0'].includes(url.pathname)) fail('cluster_database_must_be_zero');
				clients.redis = new Redis.Cluster([{ host: url.hostname, port: Number(url.port || 6379) }], {
					lazyConnect: true, enableOfflineQueue: false, clusterRetryStrategy: () => null,
					redisOptions: { ...options, lazyConnect: false, username: decodeURIComponent(url.username) || undefined,
						password: decodeURIComponent(url.password) || undefined, ...(url.protocol === 'rediss:' ? { tls: {} } : {}) }
				});
			} else clients.redis = new Redis(process.env.RECOVERY_REDIS_URL, options);
			clients.redis.on('error', () => {});
			await clients.redis.connect();
		}
		const report = await runVerification(config, clients);
		process.stdout.write(JSON.stringify(report, null, 2) + '\n');
		if (!report.ok) process.exitCode = 1;
	} finally {
		clients.redis?.disconnect();
		await clients.postgres?.end().catch(() => {});
	}
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	main().catch(error => { process.stderr.write(JSON.stringify({ ok: false, complete: false, reason: error.recoveryCode || 'verification_failed' }) + '\n'); process.exitCode = 1; });
}
