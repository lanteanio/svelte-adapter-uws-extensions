import { createHash } from 'node:crypto';
import { assertSafeTableName } from '../shared/pg-migrate.js';

const defaults = { replay: 'svti_replay', idempotency: 'svti_idempotency', jobs: 'svti_jobs', tasks: 'svti_tasks', 'dead-letter': 'svti_dead_letter', 'alarm-store': 'svti_alarms' };
const text = (name, required = false) => [name, 'text', required];
const timestamp = (name, required = false, fallback) => [name, 'timestamp with time zone', required, fallback];
const json = (name) => [name, 'jsonb', false];
const integer = (name, fallback = '0') => [name, 'integer', true, fallback];

/** The first two versions are immutable; schema changes add another version. */
export function schemaFor({ store, table = defaults[store] } = {}) {
	if (!Object.hasOwn(defaults, store)) throw new TypeError('postgres migrations: unknown store');
	assertSafeTableName(table, 'postgres migrations');
	table = table.toLowerCase();
	if (table === 'svti_schema_migrations') throw new TypeError('postgres migrations: the ledger table name is reserved');
	const primary = store === 'jobs' ? 'svti_jobs_id' : store === 'tasks' ? 'svti_tasks_id' : `${table}_id`;
	const schema = { store, table, tables: [], versions: [] };
	const main = { name: table, columns: [], primary: [], indexes: [], checks: [], additions: [] };
	main.privileges = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'];
	schema.tables.push(main);
	const index = (suffix, columns, predicate = null, unique = false) => ({ name: `idx_${table}_${suffix}`, columns, predicate, unique });
	const check = (suffix, expression) => ({ name: `chk_${table}_${suffix}`, expression });
	if (store === 'replay') {
		main.columns = [[primary, 'bigint', true, 'serial'], text('topic', true), ['seq', 'bigint', true], text('event', true), json('data'), timestamp('created_at', false, 'now()'), text('user_id')];
		main.primary = [primary]; main.additions = ['user_id'];
		main.indexes = [index('topic_seq', ['topic', 'seq']), index('created_at', ['created_at']), index('user', ['user_id'])];
		schema.tables.push({ name: `${table}_seq`, columns: [text('topic', true), ['seq', 'bigint', true, '0'], ['epoch', 'bigint', true, '0']], primary: ['topic'], indexes: [], checks: [], additions: ['epoch'], privileges: ['SELECT', 'INSERT', 'UPDATE', 'DELETE'] });
	} else if (store === 'idempotency') {
		main.columns = [text('svti_idempotency_key', true), text('status', true), json('result'), timestamp('expires_at', true), text('user_id'), text('tenant_id'), text('owner_token')];
		main.primary = ['svti_idempotency_key']; main.additions = ['user_id', 'tenant_id', 'owner_token'];
		main.indexes = [index('expires_at', ['expires_at']), index('user', ['tenant_id', 'user_id'])];
		main.checks = [check('status', "status = 'pending' OR status = 'committed'")];
	} else if (store === 'jobs') {
		main.columns = [[primary, 'bigint', true, 'serial'], text('queue', true), json('payload'), text('request_id'), text('traceparent'), text('tracestate'), timestamp('claimed_at'), timestamp('claimed_until'), integer('attempts'), timestamp('created_at', false, 'now()')];
		main.primary = [primary]; main.additions = ['request_id', 'traceparent', 'tracestate'];
		main.indexes = [index('queue_pending', ['queue', primary], 'claimed_at IS NULL'), index('visibility', ['claimed_until'], 'claimed_at IS NOT NULL')];
		main.checks = [check('attempts', 'attempts >= 0'), check('claim', '(claimed_at IS NULL AND claimed_until IS NULL) OR (claimed_at IS NOT NULL AND claimed_until IS NOT NULL AND claimed_until >= claimed_at)')];
	} else if (store === 'tasks') {
		main.columns = [[primary, 'uuid', true], text('name', true), json('input'), text('svti_idempotency_key'), text('request_id'), text('traceparent'), text('tracestate'), text('status', true), json('result'), json('error'), ['fence', 'uuid', true], timestamp('fence_expires_at', true), integer('attempts', '1'), timestamp('created_at', true, 'now()'), timestamp('updated_at', true, 'now()'), text('user_id')];
		main.primary = [primary]; main.additions = ['request_id', 'traceparent', 'tracestate', 'user_id'];
		main.indexes = [index('user', ['user_id']), index('running_fence', ['fence_expires_at'], "status = 'running'"), index('terminal_updated', ['updated_at'], "status IN ('committed', 'failed')")];
		main.checks = [check('status', "status = 'pending' OR status = 'running' OR status = 'committed' OR status = 'failed'"), check('attempts', 'attempts >= 0')];
	} else if (store === 'dead-letter') {
		main.columns = [[primary, 'bigint', true, 'serial'], text('webhook_id'), text('topic', true), text('event'), json('data'), integer('attempts'), text('error'), ['failed_at', 'bigint', true], text('user_id')];
		main.primary = [primary]; main.additions = ['user_id'];
		main.indexes = [index('topic', ['topic']), index('failed_at', ['failed_at']), index('user', ['user_id'])];
	} else {
		main.columns = [text('topic', true), ['fire_at', 'bigint', true], json('meta'), text('tenant')];
		main.primary = ['topic']; main.indexes = [index('fire_at', ['fire_at'])];
	}
	const baseline = [];
	for (const relation of schema.tables) {
		baseline.push(`CREATE TABLE IF NOT EXISTS ${relation.name} (${relation.columns.map(columnSql).join(', ')}, PRIMARY KEY (${relation.primary.join(', ')}))`);
		for (const name of relation.additions) baseline.push(`ALTER TABLE ${relation.name} ADD COLUMN IF NOT EXISTS ${columnSql(relation.columns.find(column => column[0] === name))}`);
		for (const entry of relation.indexes) baseline.push(indexSql(relation.name, entry));
	}
	const constraints = [];
	schema.baseline = schema.tables.map(relation => ({ ...relation, indexes: [...relation.indexes], checks: [] }));
	schema.retiredIndexes = [];
	for (const relation of schema.tables) for (const entry of relation.checks) {
		constraints.push(`DO $migration$ BEGIN ALTER TABLE ${relation.name} ADD CONSTRAINT ${entry.name} CHECK (${entry.expression}) NOT VALID; EXCEPTION WHEN duplicate_object THEN NULL; END $migration$`);
		constraints.push(`ALTER TABLE ${relation.name} VALIDATE CONSTRAINT ${entry.name}`);
	}
	if (store === 'replay') {
		const identity = index('identity', ['topic', 'seq'], null, true);
		const obsolete = main.indexes.shift();
		schema.retiredIndexes.push({ ...obsolete, table });
		main.indexes.push(identity); constraints.push(indexSql(table, identity));
		constraints.push(`DROP INDEX CONCURRENTLY IF EXISTS ${obsolete.name}`);
	}
	schema.checkpoints = { 1: schema.baseline, 2: schema.tables.map(relation => ({ ...relation, columns: [...relation.columns], indexes: [...relation.indexes], checks: [...relation.checks] })) };
	const versions = [baseline, constraints];
	if (store === 'alarm-store') {
		const authority = { name: `${table}_generations`, columns: [text('topic', true), ['reserved', 'bigint', true, '0'], ['committed', 'bigint', true, '0']], primary: ['topic'], indexes: [], checks: [check('authority', 'reserved >= 0 AND committed >= 0 AND reserved >= committed')], privileges: ['SELECT', 'INSERT', 'UPDATE'] };
		main.columns = [...main.columns, ['generation', 'bigint', true]];
		main.checks = [check('generation', 'generation > 0')];
		schema.tables.push(authority);
		const generations = [
			`CREATE TABLE IF NOT EXISTS ${authority.name} (${authority.columns.map(columnSql).join(', ')}, PRIMARY KEY (topic))`,
			`DO $migration$ BEGIN LOCK TABLE ${authority.name} IN EXCLUSIVE MODE; LOCK TABLE ${table} IN SHARE ROW EXCLUSIVE MODE; ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS generation BIGINT; INSERT INTO ${authority.name} (topic, reserved, committed) SELECT topic, 1, 1 FROM ${table} WHERE generation IS NULL ON CONFLICT (topic) DO NOTHING; UPDATE ${table} a SET generation = 1 FROM ${authority.name} g WHERE a.topic = g.topic AND a.generation IS NULL AND g.reserved = 1 AND g.committed = 1; ALTER TABLE ${table} ALTER COLUMN generation SET NOT NULL; IF EXISTS (SELECT 1 FROM ${table} a LEFT JOIN ${authority.name} g ON g.topic = a.topic WHERE g.topic IS NULL OR a.generation <> g.committed OR g.reserved < g.committed) THEN RAISE EXCEPTION 'postgres alarm store: incompatible generation authority'; END IF; END $migration$`
		];
		for (const relation of [main, authority]) for (const entry of relation.checks) {
			generations.push(`DO $migration$ BEGIN ALTER TABLE ${relation.name} ADD CONSTRAINT ${entry.name} CHECK (${entry.expression}) NOT VALID; EXCEPTION WHEN duplicate_object THEN NULL; END $migration$`);
			generations.push(`ALTER TABLE ${relation.name} VALIDATE CONSTRAINT ${entry.name}`);
		}
		versions.push(generations);
		schema.checkpoints[3] = schema.tables;
	}
	for (const [offset, statements] of versions.entries()) {
		const version = offset + 1;
		const checksum = createHash('sha256').update(JSON.stringify({ store, version, statements })).digest('hex');
		schema.versions.push({ version, checksum, statements });
	}
	return schema;
}

function columnSql([name, type, required, fallback]) {
	return `${name} ${fallback === 'serial' ? 'BIGSERIAL' : type.toUpperCase()}${required ? ' NOT NULL' : ''}${fallback && fallback !== 'serial' ? ` DEFAULT ${fallback}` : ''}`;
}

function indexSql(table, entry) {
	return `CREATE ${entry.unique ? 'UNIQUE ' : ''}INDEX CONCURRENTLY IF NOT EXISTS ${entry.name} ON ${table} (${entry.columns.join(', ')})${entry.predicate ? ` WHERE ${entry.predicate}` : ''}`;
}
