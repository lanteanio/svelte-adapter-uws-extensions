const TABLE_NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

// Modules build index names as `idx_<table>_<suffix>`; the longest suffix in
// use is `_terminal_updated` (tasks), so `idx_` + name + `_terminal_updated`
// is name.length + 21 bytes. Postgres truncates identifiers at 63 bytes
// (NAMEDATALEN - 1) SILENTLY, so a table name past this ceiling can collapse
// two distinct index names to the same truncated string - the second
// `CREATE INDEX IF NOT EXISTS` then no-ops against the first and one index
// never exists, with no error. Cap the name so every generated identifier
// stays unique within the engine limit. The pattern above restricts names to
// ASCII, so `.length` is the byte length.
const MAX_TABLE_NAME_BYTES = 42; // 63 - 21 (the longest generated suffix)

/**
 * Validate a Postgres table-name option from a factory call. Rejects
 * SQL-injectable shapes (anything outside `[a-zA-Z_][a-zA-Z0-9_]*`)
 * and the `pg_*` / `information_schema*` reserved namespaces (which
 * map to internal catalogs the caller never intended to touch).
 *
 * @param {string} table - User-supplied table name.
 * @param {string} mod - Module label for the error prefix
 *   (e.g. `'postgres tasks'`).
 */
export function assertSafeTableName(table, mod) {
	if (!TABLE_NAME_PATTERN.test(table)) {
		throw new Error(`${mod}: invalid table name "${table}"`);
	}
	if (table.length > MAX_TABLE_NAME_BYTES) {
		throw new Error(
			`${mod}: table name "${table}" is ${table.length} bytes; the maximum is ` +
			`${MAX_TABLE_NAME_BYTES} so generated index names stay unique within ` +
			`Postgres's 63-byte identifier limit`
		);
	}
	const lower = table.toLowerCase();
	if (lower.startsWith('pg_') || lower.startsWith('information_schema')) {
		throw new Error(`${mod}: table name "${table}" is in a reserved Postgres schema`);
	}
}

/**
 * Run a `CREATE TABLE/INDEX IF NOT EXISTS` while tolerating the codes Postgres
 * raises when two concurrent connections both pass the existence check and
 * race on the create. Per-statement so a race on the first DDL does not skip
 * later ones.
 *
 * Optional `expectedColumns` guards against the schema-drift case where an
 * existing table has the same name but a different shape. When a guard is
 * supplied the function ALWAYS verifies the resolved table against
 * `information_schema.columns` after the DDL runs and throws if any expected
 * column is missing. Verifying only in the `42P07` catch was the original
 * bug: `CREATE TABLE IF NOT EXISTS` on a PRE-EXISTING table succeeds with a
 * notice and NEVER raises `42P07`, so the steady-state "table already there
 * with the wrong shape" case - the common one - skipped the check entirely.
 * Without this guard, schema-drift surfaces much later as a confusing
 * `42703` (undefined_column) error from a downstream query.
 *
 * Callers that ONLY pass `ddl` retain the original silent-swallow behavior
 * for backward compatibility. Callers that want drift detection pass
 * `{ table, columns }`.
 *
 * @param {{ query(text: string, values?: unknown[]): Promise<{ rows: { column_name: string }[] }> }} client
 * @param {string} ddl
 * @param {{ table: string, columns: string[] }} [expectedColumns]
 */
export async function safeCreate(client, ddl, expectedColumns) {
	try {
		await client.query(ddl);
	} catch (err) {
		if (err.code !== '23505' && err.code !== '42P07' && err.code !== '42710') {
			throw err;
		}
		// 23505 / 42710 are index / object-creation races with no table-shape
		// implication - nothing to verify, so return. A 42P07 (relation already
		// exists) DOES warrant a column check, so it falls through.
		if (err.code !== '42P07') return;
	}
	// Reached on a clean success OR a swallowed 42P07. Verify whenever the
	// caller supplied a drift guard: the clean-success branch is the case the
	// old catch-only guard missed entirely - `CREATE TABLE IF NOT EXISTS` on a
	// pre-existing (possibly wrong-shape) table succeeds with a notice and
	// never raises 42P07.
	if (expectedColumns) {
		await verifyTableColumns(client, expectedColumns);
	}
}

/**
 * Verify that a pre-existing table has all the columns the caller expected
 * the (swallowed) `CREATE TABLE` to create. Throws on missing columns
 * with a message that identifies the table and the missing columns by
 * name. Does NOT check column types or extra columns: extras are normal
 * for forward-migrated tables, and type-drift is a rarer / more complex
 * case this validator deliberately does not check.
 *
 * @param {{ query(text: string, values?: unknown[]): Promise<{ rows: { column_name: string }[] }> }} client
 * @param {{ table: string, columns: string[] }} expected
 */
async function verifyTableColumns(client, expected) {
	const { table, columns } = expected;
	// Parameterize the lookup. The caller's table value is also typically
	// vetted by `assertSafeTableName` upstream of this helper, but the
	// parameterized binding is the safer pattern regardless.
	//
	// Scope to `current_schema()` - the schema an unqualified `CREATE TABLE`
	// actually writes to (the first writable entry in search_path). Without
	// it, a same-named table in ANY other schema on the database satisfies
	// (or pollutes) the column set, so drift on the real table could be
	// masked by an unrelated namesake elsewhere.
	const result = await client.query(
		'SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND table_schema = current_schema()',
		[table]
	);
	const actual = new Set(result.rows.map((r) => r.column_name));
	const missing = [];
	for (let i = 0; i < columns.length; i++) {
		if (!actual.has(columns[i])) missing.push(columns[i]);
	}
	if (missing.length > 0) {
		throw new Error(
			`pg-migrate: existing table "${table}" is missing expected column(s): ${missing.join(', ')}. ` +
			'An object with this name pre-existed in the database with a different schema. ' +
			'Drop / rename the conflicting table, or change the `table` option this module was constructed with.'
		);
	}
}
