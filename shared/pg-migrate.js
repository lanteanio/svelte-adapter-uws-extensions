const TABLE_NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

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
 * existing table has the same name but a different shape. When the swallow
 * fires on `42P07` (relation already exists), the function queries
 * `information_schema.columns` for the named table and throws if any of
 * the expected columns is missing. Without this guard, schema-drift
 * surfaces much later as a confusing `42703` (undefined_column) error
 * from a downstream query.
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
		return;
	} catch (err) {
		if (err.code !== '23505' && err.code !== '42P07' && err.code !== '42710') {
			throw err;
		}
		// Swallowed an "already exists" race. If the caller supplied an
		// expected-columns guard AND the error is specifically the
		// table-already-exists code, verify the existing table has the
		// columns the DDL was supposed to create.
		if (err.code === '42P07' && expectedColumns) {
			await verifyTableColumns(client, expectedColumns);
		}
	}
}

/**
 * Verify that a pre-existing table has all the columns the caller expected
 * the (swallowed) `CREATE TABLE` to create. Throws on missing columns
 * with a message that identifies the table and the missing columns by
 * name. Does NOT check column types or extra columns: extras are normal
 * for forward-migrated tables, and type-drift is a rarer / more complex
 * scenario the audit explicitly scoped out.
 *
 * @param {{ query(text: string, values?: unknown[]): Promise<{ rows: { column_name: string }[] }> }} client
 * @param {{ table: string, columns: string[] }} expected
 */
async function verifyTableColumns(client, expected) {
	const { table, columns } = expected;
	// Parameterize the lookup. The caller's table value is also typically
	// vetted by `assertSafeTableName` upstream of this helper, but the
	// parameterized binding is the safer pattern regardless.
	const result = await client.query(
		'SELECT column_name FROM information_schema.columns WHERE table_name = $1',
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
