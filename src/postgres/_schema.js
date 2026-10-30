import { createHash } from 'node:crypto';
import { assertSafeTableName } from '../shared/pg-migrate.js';
import { topicTenantScopeSql } from '../shared/tenant-topic.js';

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
	if (store === 'replay') {
		// Version 3: each topic's last use, for maxTopics eviction and idle
		// metadata expiry, and an epoch default drawn from one sequence so a
		// deleted topic row can never hand its epoch out again. The sequence
		// starts above every epoch already stored, and an existing topic's last
		// use starts at its newest event's created_at (the epoch of 1970 for a
		// topic with no event left), so eviction ranks existing topics by when
		// they were last published rather than all at the migration time.
		const sequenceTable = schema.tables[1];
		const lastUsed = timestamp('last_used_at', true, 'now()');
		const sequence = `${sequenceTable.name}_epoch_seq`;
		sequenceTable.columns = [...sequenceTable.columns.map(entry => entry[0] === 'epoch' ? ['epoch', 'bigint', true, 'serial'] : entry), lastUsed];
		versions.push([
			`ALTER TABLE ${sequenceTable.name} ADD COLUMN IF NOT EXISTS ${columnSql(lastUsed)}`,
			`DO $migration$ DECLARE top BIGINT; BEGIN LOCK TABLE ${sequenceTable.name} IN SHARE ROW EXCLUSIVE MODE; CREATE SEQUENCE IF NOT EXISTS ${sequence} AS BIGINT OWNED BY ${sequenceTable.name}.epoch; SELECT COALESCE(MAX(epoch), 0) INTO top FROM ${sequenceTable.name}; IF top > 0 THEN PERFORM setval('${sequence}', GREATEST(top, (SELECT last_value FROM ${sequence}))); END IF; ALTER TABLE ${sequenceTable.name} ALTER COLUMN epoch SET DEFAULT nextval('${sequence}'::regclass); UPDATE ${sequenceTable.name} s SET last_used_at = COALESCE((SELECT r.created_at FROM ${table} r WHERE r.topic = s.topic ORDER BY r.seq DESC LIMIT 1), to_timestamp(0)); END $migration$`
		]);
		schema.checkpoints[3] = schema.tables;
	}
	if (store === 'tasks') {
		const pending = index('pending_created', ['created_at'], "status = 'pending'");
		main.indexes.push(pending);
		versions.push([indexSql(table, pending)]);
		schema.checkpoints[3] = schema.tables.map(relation => ({ ...relation, columns: [...relation.columns], indexes: [...relation.indexes], checks: [...relation.checks] }));
		// Tenant-qualified identity: identity_version 1 marks a row stamped by a
		// forgetIdentity runner (tenant_id NULL is then the untenanted scope);
		// NULL marks a row written without one, whose tenant is unknown. The
		// pending-scope index serves per-tenant dispatch with equality on the
		// task name, and replaces the creation-time index, which no claim reads
		// any more.
		const identity = index('identity', ['identity_version', 'tenant_id', 'user_id']);
		const scope = index('pending_scope', ['identity_version', 'name', 'tenant_id', 'created_at'], "status = 'pending'");
		// IS NOT NULL keeps the expression false, not unknown, for an unstamped
		// row with a tenant: a CHECK passes on unknown.
		const stamp = check('identity', '(identity_version IS NULL AND tenant_id IS NULL) OR (identity_version IS NOT NULL AND identity_version = 1)');
		main.columns = [...main.columns, ['identity_version', 'integer', false], text('tenant_id')];
		main.indexes = [...main.indexes.filter(entry => entry !== pending), identity, scope];
		main.checks = [...main.checks, stamp];
		schema.retiredIndexes.push({ ...pending, table });
		versions.push([
			`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS identity_version INTEGER`,
			`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS tenant_id TEXT`,
			`DO $migration$ BEGIN ALTER TABLE ${table} ADD CONSTRAINT ${stamp.name} CHECK (${stamp.expression}) NOT VALID; EXCEPTION WHEN duplicate_object THEN NULL; END $migration$`,
			`ALTER TABLE ${table} VALIDATE CONSTRAINT ${stamp.name}`,
			indexSql(table, identity),
			indexSql(table, scope),
			`DROP INDEX CONCURRENTLY IF EXISTS ${pending.name}`
		]);
		schema.checkpoints[4] = schema.tables;
	}
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
		schema.checkpoints[3] = schema.tables.map(relation => ({ ...relation, columns: [...relation.columns], indexes: [...relation.indexes], checks: [...relation.checks] }));
		// Retained executions: a claim moves the schedule row here before the
		// handler runs. The schedule carries the attempt its next claim receives.
		const attempt = check('attempt', "attempt >= 1 AND attempt <= '9007199254740991'::bigint");
		const executions = {
			name: `${table}_executions`,
			columns: [text('topic', true), ['generation', 'bigint', true], ['fire_at', 'bigint', true], json('meta'), text('tenant'), ['attempt', 'bigint', true], text('status', true), ['claimed_at', 'bigint', true], ['failed_at', 'bigint', false], text('error_class')],
			// Byte-order topic index: executions() pages by topic, then sorts the
			// few generations of each topic incrementally.
			primary: ['topic', 'generation'], indexes: [index('execution_order', ['topic COLLATE "C"'])],
			checks: [
				check('exec_generation', 'generation > 0'),
				check('exec_attempt', "attempt >= 1 AND attempt <= '9007199254740991'::bigint"),
				check('exec_status', "(status = 'claimed' AND failed_at IS NULL AND error_class IS NULL) OR (status = 'failed' AND failed_at IS NOT NULL AND error_class = 'handler')")
			],
			privileges: ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
		};
		main.columns = [...main.columns, ['attempt', 'bigint', true, '1']];
		main.checks = [...main.checks, attempt];
		schema.tables.push(executions);
		const retained = [
			`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS attempt BIGINT NOT NULL DEFAULT 1`,
			`CREATE TABLE IF NOT EXISTS ${executions.name} (${executions.columns.map(columnSql).join(', ')}, PRIMARY KEY (topic, generation))`
		];
		for (const [relation, entries] of [[main, [attempt]], [executions, executions.checks]]) for (const entry of entries) {
			retained.push(`DO $migration$ BEGIN ALTER TABLE ${relation.name} ADD CONSTRAINT ${entry.name} CHECK (${entry.expression}) NOT VALID; EXCEPTION WHEN duplicate_object THEN NULL; END $migration$`);
			retained.push(`ALTER TABLE ${relation.name} VALIDATE CONSTRAINT ${entry.name}`);
		}
		for (const entry of executions.indexes) retained.push(indexSql(executions.name, entry));
		versions.push(retained);
		schema.checkpoints[4] = schema.tables;
	}
	if (store === 'dead-letter') {
		const erasure = {
			name: `${table}_purges`,
			columns: [text('scope', true), text('user_id', true), ['purged_at', 'bigint', true]],
			primary: ['scope', 'user_id'],
			indexes: [index('purges_expiry', ['purged_at'])],
			checks: [], additions: [],
			privileges: ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
		};
		schema.tables.push(erasure);
		versions.push([
			`CREATE TABLE IF NOT EXISTS ${erasure.name} (${erasure.columns.map(columnSql).join(', ')}, PRIMARY KEY (scope, user_id))`,
			indexSql(erasure.name, erasure.indexes[0])
		]);
		schema.checkpoints[3] = schema.tables.map(relation => ({ ...relation, columns: [...relation.columns], indexes: [...relation.indexes], checks: [...relation.checks] }));
		// Fair-share eviction: each record carries its tenant scope, statement
		// triggers keep the record count and oldest id per scope in the
		// companion table for every writer, and the (scope, id) index yields a
		// scope's oldest records. Under a lock that holds writers off, the
		// migration derives the scope of the rows already present from their
		// topics and counts them. The trigger function names its tables with the
		// schema of the record table and runs with a fixed search_path, so it
		// updates the same counts whatever the writer's search_path is.
		const counts = {
			name: `${table}_scopes`,
			columns: [text('scope', true), ['records', 'bigint', true], ['oldest', 'bigint', true]],
			primary: ['scope'], indexes: [], checks: [], additions: [],
			privileges: ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
		};
		const scopeColumn = ['scope', 'text', true, "''::text"];
		const byScope = index('scope', ['scope', primary]);
		const sync = `${table}_scopes_sync`;
		const source = scopeSyncSource(table, counts.name, primary);
		const config = 'search_path=pg_catalog, pg_temp';
		main.columns = [...main.columns, scopeColumn];
		main.indexes = [...main.indexes, byScope];
		main.triggers = [
			{ name: `${table}_scopes_insert`, event: 'INSERT', type: 4, oldTable: null, newTable: 'added', function: sync, source, config },
			{ name: `${table}_scopes_update`, event: 'UPDATE', type: 16, oldTable: 'removed', newTable: 'added', function: sync, source, config },
			{ name: `${table}_scopes_delete`, event: 'DELETE', type: 8, oldTable: 'removed', newTable: null, function: sync, source, config },
			{ name: `${table}_scopes_truncate`, event: 'TRUNCATE', type: 32, oldTable: null, newTable: null, function: sync, source, config }
		];
		schema.tables.push(counts);
		const triggers = main.triggers.map(entry => {
			const referencing = [entry.oldTable && `OLD TABLE AS ${entry.oldTable}`, entry.newTable && `NEW TABLE AS ${entry.newTable}`].filter(Boolean).join(' ');
			return `EXECUTE format('CREATE OR REPLACE TRIGGER %I AFTER ${entry.event} ON %s.%I ${referencing ? `REFERENCING ${referencing} ` : ''}FOR EACH STATEMENT EXECUTE FUNCTION %s.%I()', '${entry.name}', target, '${table}', target, '${sync}');`;
		});
		const derived = topicTenantScopeSql('topic');
		versions.push([
			`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${columnSql(scopeColumn)}`,
			`CREATE TABLE IF NOT EXISTS ${counts.name} (${counts.columns.map(columnSql).join(', ')}, PRIMARY KEY (scope))`,
			`DO $migration$ DECLARE target text; BEGIN LOCK TABLE ${table} IN SHARE ROW EXCLUSIVE MODE;`
				+ ` SELECT '"' || replace(n.nspname, '"', '""') || '"' INTO target FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = '${table}'::regclass;`
				+ ` UPDATE ${table} SET scope = ${derived} WHERE scope = '' AND ${derived} <> '';`
				+ ` EXECUTE format('CREATE OR REPLACE FUNCTION %s.%I() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS %L', target, '${sync}', replace($sync$${source}$sync$, '@S@', target));`
				+ ` ${triggers.join(' ')}`
				+ ` DELETE FROM ${counts.name}; INSERT INTO ${counts.name} (scope, records, oldest) SELECT scope, count(*), min(${primary}) FROM ${table} GROUP BY scope; END $migration$`,
			indexSql(table, byScope)
		]);
		schema.checkpoints[4] = schema.tables;
	}
	for (const [offset, statements] of versions.entries()) {
		const version = offset + 1;
		const checksum = createHash('sha256').update(JSON.stringify({ store, version, statements })).digest('hex');
		schema.versions.push({ version, checksum, statements });
	}
	return schema;
}

// The dead-letter scope-count trigger body, kept on one line so the stored
// source compares exactly. `@S@` stands for the quoted schema of the record
// table; the migration substitutes it, and validation compares against the
// same substitution. Each statement applies its change per scope in scope
// order, so concurrent multi-scope statements take the count rows in one
// order. A count that reaches zero is deleted; a scope whose oldest record was
// deleted reads its next oldest id from the (scope, id) index.
function scopeSyncSource(table, counts, primary) {
	const rows = `@S@."${table}"`;
	const scopes = `@S@."${counts}"`;
	return [
		' DECLARE delta record;',
		'BEGIN',
		`IF TG_OP = 'TRUNCATE' THEN DELETE FROM ${scopes}; RETURN NULL; END IF;`,
		`IF TG_OP <> 'INSERT' THEN`,
		`FOR delta IN SELECT scope, count(*) AS n, min(${primary}) AS lowest FROM removed GROUP BY scope ORDER BY scope LOOP`,
		`DELETE FROM ${scopes} WHERE scope = delta.scope AND records <= delta.n;`,
		`IF NOT FOUND THEN UPDATE ${scopes} AS c SET records = c.records - delta.n,`,
		`oldest = CASE WHEN c.oldest >= delta.lowest THEN (SELECT min(r.${primary}) FROM ${rows} AS r WHERE r.scope = delta.scope) ELSE c.oldest END`,
		'WHERE c.scope = delta.scope; END IF;',
		'END LOOP;',
		'END IF;',
		`IF TG_OP <> 'DELETE' THEN`,
		`FOR delta IN SELECT scope, count(*) AS n, min(${primary}) AS lowest FROM added GROUP BY scope ORDER BY scope LOOP`,
		`INSERT INTO ${scopes} AS c (scope, records, oldest) VALUES (delta.scope, delta.n, delta.lowest)`,
		'ON CONFLICT (scope) DO UPDATE SET records = c.records + EXCLUDED.records, oldest = LEAST(c.oldest, EXCLUDED.oldest);',
		'END LOOP;',
		'END IF;',
		'RETURN NULL;',
		'END '
	].join(' ');
}

function columnSql([name, type, required, fallback]) {
	return `${name} ${fallback === 'serial' ? 'BIGSERIAL' : type.toUpperCase()}${required ? ' NOT NULL' : ''}${fallback && fallback !== 'serial' ? ` DEFAULT ${fallback}` : ''}`;
}

function indexSql(table, entry) {
	return `CREATE ${entry.unique ? 'UNIQUE ' : ''}INDEX CONCURRENTLY IF NOT EXISTS ${entry.name} ON ${table} (${entry.columns.join(', ')})${entry.predicate ? ` WHERE ${entry.predicate}` : ''}`;
}
