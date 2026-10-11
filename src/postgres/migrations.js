import { schemaFor } from './_schema.js';
import { schemaExpression } from './_schema-expression.js';
import { monotonicNow, setTimer } from '../shared/runtime.js';

const LEDGER = 'svti_schema_migrations';
const LOCK = "hashtextextended(current_database() || ':' || current_schema() || ':svti_schema_migrations', 0)";

/** Inspect the ordered SQL and checksums without connecting to a database. */
export function postgresMigrations(options) {
	return schemaFor(options).versions;
}

/** Apply resumable migrations using a schema-owner connection. Existing data is never repaired or discarded. */
export async function migrate(client, options) {
	const schema = schemaFor(options);
	const statementTimeoutMs = options.statementTimeoutMs ?? 30000;
	const lockTimeoutMs = options.lockTimeoutMs ?? 5000;
	for (const [name, value] of [['statementTimeoutMs', statementTimeoutMs], ['lockTimeoutMs', lockTimeoutMs]]) {
		if (!Number.isSafeInteger(value) || value <= 0 || value > 2147482647) throw new TypeError(`postgres migrations: ${name} must be an integer between 1 and 2147482647`);
	}
	const connection = await client.connect({ signal: AbortSignal.timeout(10000) });
	let disconnected = null;
	const onError = error => { disconnected = error; };
	connection.on?.('error', onError);
	const query = (text, values, timeoutMs = statementTimeoutMs + 1000) => {
		if (disconnected) return Promise.reject(disconnected);
		return connection.query({ text, values, query_timeout: timeoutMs });
	};
	try {
		await query("SELECT set_config('lock_timeout', $1, false), set_config('statement_timeout', $2, false)", [String(lockTimeoutMs), String(statementTimeoutMs)]);
		const lockDeadline = monotonicNow() + lockTimeoutMs;
		while (true) {
			const remaining = Math.max(1, Math.ceil(lockDeadline - monotonicNow()));
			const lock = await query(`SELECT pg_try_advisory_lock(${LOCK}) AS acquired`, undefined, Math.min(remaining, statementTimeoutMs + 1000));
			if (lock.rows[0]?.acquired) break;
			if (monotonicNow() >= lockDeadline) {
				const error = new Error('postgres migrations: advisory lock deadline exceeded');
				error.code = '55P03'; throw error;
			}
			// A blocking lock query retains a snapshot that concurrent index
			// creation may need to finish. Poll only between completed queries.
			await new Promise(resolve => setTimer(resolve, Math.min(10, remaining)));
		}
		await query(`CREATE TABLE IF NOT EXISTS ${LEDGER} (table_name TEXT NOT NULL, store TEXT NOT NULL, version INTEGER NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (table_name, version))`);
		const { rows } = await query(`SELECT store, version, checksum FROM ${LEDGER} WHERE table_name = $1 ORDER BY version`, [schema.table]);
		validateLedger(rows, schema);
		for (const version of schema.versions) {
			if (rows.some(row => row.version === version.version)) continue;
			const checkpoint = { tables: schema.checkpoints[version.version] };
			for (const statement of version.statements) {
				if (statement.startsWith('DROP INDEX CONCURRENTLY')) {
					const name = statement.split(' ').at(-1);
					const expected = schema.retiredIndexes.find(entry => entry.name === name);
					const owner = await query('SELECT i.indisvalid, i.indrelid = to_regclass($2) AS owned FROM pg_index i WHERE i.indexrelid = to_regclass($1)', [name, expected.table]);
					if (owner.rows[0]?.owned === false) throw new Error(`postgres migrations: conflicting index ${name}`);
					const candidates = await readIndexes({ query }, expected.table);
					const actual = candidates.find(entry => entry.name === name);
					if (actual && !compatibleIndex(actual, expected)) throw new Error(`postgres migrations: conflicting index ${name}`);
				}
				if (statement.startsWith('CREATE') && statement.includes('INDEX CONCURRENTLY')) {
					const table = / ON (\w+) /.exec(statement)[1];
					await validateColumns({ query }, { tables: checkpoint.tables.filter(relation => relation.name === table) }, schema);
					const name = /INDEX CONCURRENTLY IF NOT EXISTS (\w+)/.exec(statement)[1];
					const invalid = await query('SELECT i.indisvalid, i.indrelid = to_regclass($2) AS owned FROM pg_index i WHERE i.indexrelid = to_regclass($1)', [name, table]);
					// A failed concurrent build leaves an invalid index. Only its own
					// deterministic index is removed before retrying the same migration.
					if (invalid.rows[0]?.owned === false) throw new Error(`postgres migrations: conflicting index ${name}`);
					if (invalid.rows[0]?.indisvalid === false) {
						const expected = schema.tables.find(relation => relation.name === table).indexes.find(entry => entry.name === name)
							?? schema.baseline.find(relation => relation.name === table).indexes.find(entry => entry.name === name);
						const actual = (await readIndexes({ query }, table)).find(entry => entry.name === name);
						if (!compatibleIndex({ ...actual, valid: true }, expected)) throw new Error(`postgres migrations: conflicting index ${name}`);
						await query(`DROP INDEX CONCURRENTLY ${name}`);
					}
				}
				await query(statement);
			}
			await validateCatalog({ query }, checkpoint, schema);
			await query(`INSERT INTO ${LEDGER} (table_name, store, version, checksum) VALUES ($1, $2, $3, $4)`, [schema.table, schema.store, version.version, version.checksum]);
		}
		await validateCatalog({ query }, schema);
	} finally {
		// Discard this checkout, including its timeout settings and session lock,
		// on success or any failure. No session state can leak to runtime queries.
		connection.release(true);
		connection.removeListener?.('error', onError);
	}
}

async function validateColumns(client, schema, known = schema) {
	for (const relation of schema.tables) {
		const { rows } = await client.query(`SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull AS not_null, a.attgenerated AS generated, a.attidentity AS identity, pg_get_expr(d.adbin, d.adrelid) AS column_default
			FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`, [relation.name]);
		const permitted = known.tables.find(entry => entry.name === relation.name) ?? relation;
		const columns = [...relation.columns, ...permitted.columns.filter(column => !relation.columns.some(entry => entry[0] === column[0]) && rows.some(entry => entry.column_name === column[0]))];
		for (const actual of rows) {
			if (actual.not_null && !columns.some(column => column[0] === actual.column_name)) throw new Error(`postgres schema: unexpected required column ${relation.name}.${actual.column_name}`);
			if (!columns.some(column => column[0] === actual.column_name) && (actual.column_default != null || actual.generated || actual.identity)) throw new Error(`postgres schema: unexpected computed column ${relation.name}.${actual.column_name}`);
		}
		for (const [name, type, required, fallback] of columns) {
			const actual = rows.find(column => column.column_name === name);
			if (!actual) throw new Error(`postgres schema: ${relation.name} is missing expected column ${name}`);
			if (actual.generated || actual.identity) throw new Error(`postgres schema: incompatible generated or identity column ${relation.name}.${name}`);
			if (actual.data_type !== type || actual.not_null !== required) throw new Error(`postgres schema: incompatible type or nullability for ${relation.name}.${name}`);
			// PostgreSQL renders these literal numeric/now() defaults canonically.
			// Never lowercase SQL literals or discard grouping when comparing them.
			const defaultMatches = fallback === 'serial' ? /^nextval\(/.test(actual.column_default ?? '') : (actual.column_default ?? null) === (fallback ?? null);
			if (!defaultMatches) throw new Error(`postgres schema: incompatible default for ${relation.name}.${name}`);
		}
	}
}

/** Inspect only catalogs; safe for a runtime role with no schema-creation privileges. */
export async function validateSchema(client, options) {
	const schema = schemaFor(options);
	const query = (text, values) => client.query({ text, values, query_timeout: 10000 }, undefined, { signal: AbortSignal.timeout(10000) });
	const ledger = await query('SELECT to_regclass($1) IS NOT NULL AS present', [LEDGER]);
	if (ledger.rows[0]?.present) {
		const { rows } = await query(`SELECT store, version, checksum FROM ${LEDGER} WHERE table_name = $1 ORDER BY version`, [schema.table]);
		validateLedger(rows, schema);
		if (rows.length && rows.length !== schema.versions.length) throw new Error(`postgres migrations: incomplete ledger for ${schema.table}`);
	}
	await validateCatalog({ query }, schema);
}

function validateLedger(rows, schema) {
	for (const [index, row] of rows.entries()) {
		const expected = schema.versions.find(entry => entry.version === row.version);
		if (!expected || row.version !== index + 1 || row.store !== schema.store || row.checksum !== expected.checksum) throw new Error(`postgres migrations: incompatible ledger for ${schema.table} version ${row.version}`);
	}
}

async function readIndexes(client, table) {
	const { rows } = await client.query(`SELECT c.relname AS name, i.indisvalid AS valid, i.indisunique AS unique, i.indisprimary AS primary, am.amname AS method,
		ARRAY(SELECT pg_get_indexdef(i.indexrelid, n, true) FROM generate_series(1, i.indnkeyatts) n) AS columns,
		pg_get_expr(i.indpred, i.indrelid) AS predicate
		FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am am ON am.oid = c.relam WHERE i.indrelid = to_regclass($1)`, [table]);
	return rows;
}

function compatibleIndex(actual, expected) {
	const predicate = schemaExpression(actual?.predicate);
	return actual?.valid && actual.method === 'btree' && actual.unique === expected.unique && JSON.stringify(actual.columns) === JSON.stringify(expected.columns) && predicate !== null && predicate === schemaExpression(expected.predicate);
}

async function validateCatalog(client, schema, known = schema) {
	await validateColumns(client, schema, known);
	for (const relation of schema.tables) {
		const permitted = known.tables.find(entry => entry.name === relation.name) ?? relation;
		const privileges = await client.query('SELECT privilege, has_table_privilege(to_regclass($1), privilege) AS granted FROM unnest($2::text[]) privilege', [relation.name, relation.privileges]);
		for (const privilege of privileges.rows) if (!privilege.granted) throw new Error(`postgres schema: missing ${privilege.privilege} privilege on ${relation.name}`);
		for (const [name, , , fallback] of relation.columns) if (fallback === 'serial') {
			const sequence = await client.query(`SELECT has_sequence_privilege(pg_get_serial_sequence($1, $2), 'USAGE') OR has_sequence_privilege(pg_get_serial_sequence($1, $2), 'UPDATE') AS allowed,
				pg_get_expr(d.adbin, d.adrelid) = format('nextval(%L::regclass)', pg_get_serial_sequence($1, $2)::regclass::text) AS default_matches
				FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
				WHERE a.attrelid = to_regclass($1) AND a.attname = $2 AND NOT a.attisdropped`, [relation.name, name]);
			if (!sequence.rows[0]?.default_matches) throw new Error(`postgres schema: incompatible serial default for ${relation.name}.${name}`);
			if (!sequence.rows[0]?.allowed) throw new Error(`postgres schema: missing sequence privilege for ${relation.name}.${name}`);
		}
		const indexes = await readIndexes(client, relation.name);
		if (!indexes.some(entry => entry.primary && entry.valid && JSON.stringify(entry.columns) === JSON.stringify(relation.primary))) throw new Error(`postgres schema: missing primary key on ${relation.name}`);
		for (const expected of relation.indexes) {
			if (!compatibleIndex(indexes.find(entry => entry.name === expected.name), expected)) throw new Error(`postgres schema: missing or incompatible index ${expected.name}`);
		}
		for (const actual of indexes) {
			if (actual.unique && !actual.primary && !permitted.indexes.some(expected => expected.name === actual.name && compatibleIndex({ ...actual, valid: true }, expected))) throw new Error(`postgres schema: unexpected unique index ${actual.name}`);
		}
		const restrictions = await client.query("SELECT conname AS name FROM pg_constraint WHERE conrelid = to_regclass($1) AND contype IN ('f', 'x')", [relation.name]);
		if (restrictions.rows.length) throw new Error(`postgres schema: unexpected foreign-key or exclusion constraint ${restrictions.rows[0].name}`);
		const checks = await client.query(`SELECT conname AS name, convalidated AS valid, pg_get_expr(conbin, conrelid) AS expression FROM pg_constraint WHERE conrelid = to_regclass($1) AND contype = 'c'`, [relation.name]);
		for (const expected of relation.checks) {
			const actual = checks.rows.find(entry => entry.name === expected.name);
			const expression = schemaExpression(actual?.expression);
			if (!actual?.valid || expression === null || expression !== schemaExpression(expected.expression)) throw new Error(`postgres schema: missing or incompatible check ${expected.name}`);
		}
		for (const actual of checks.rows) {
			const expected = permitted.checks.find(entry => entry.name === actual.name);
			const expression = schemaExpression(actual.expression);
			if (!expected || expression === null || expression !== schemaExpression(expected.expression)) throw new Error(`postgres schema: unexpected or incompatible check ${actual.name}`);
		}
	}
}

/** Await connectivity and every explicitly selected component before serving traffic. */
export async function postgresReadiness(client, components = []) {
	await client.check();
	const results = await Promise.allSettled(components.map(component => Promise.resolve().then(() => component.ready())));
	const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
	if (errors.length) throw new AggregateError(errors, 'PostgreSQL components are not ready');
}

export function createStoreSchema(client, store, table, automatic) {
	const retryable = error => error?.code === 'PG_CAPACITY_EXCEEDED'
		|| error?.name === 'TimeoutError'
		|| ['Query read timeout', 'Connection terminated unexpectedly', 'Connection terminated', 'Connection terminated due to connection timeout', 'timeout exceeded when trying to connect'].includes(error?.message)
		|| /^(08|40|53)/.test(error?.code ?? '')
		|| ['55P03', '57014', '57P01', '57P02', '57P03', 'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'ENETUNREACH', 'EHOSTUNREACH'].includes(error?.code);
	let state = 'idle', failure = null, pending = null, complete = false, retryAt = 0, retryDelay = 0;
	function ready() {
		if (complete) return Promise.resolve();
		if (pending) return pending;
		state = 'checking'; failure = null;
		pending = Promise.resolve().then(async () => {
			const delay = retryAt - monotonicNow();
			if (delay > 0) await new Promise(resolve => setTimer(resolve, delay));
			return automatic ? migrate(client, { store, table }) : validateSchema(client, { store, table });
		})
			.then(() => { complete = true; state = 'ready'; }, error => {
				state = retryable(error) ? 'idle' : 'failed'; failure = error;
				if (state === 'idle') { retryDelay = Math.min(retryDelay ? retryDelay * 2 : 100, 5000); retryAt = monotonicNow() + retryDelay; }
				throw error;
			})
			.finally(() => { pending = null; });
		return pending;
	}
	return {
		// Schema/data/privilege errors need explicit repair. Transient connection,
		// admission and lock failures retry on the next ordinary operation.
		ensure: automatic ? () => state === 'failed' ? Promise.reject(failure) : ready() : () => Promise.resolve(),
		ready,
		status: () => ({ state, error: failure })
	};
}
