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
 * @param {{ query(text: string): Promise<unknown> }} client
 * @param {string} ddl
 */
export async function safeCreate(client, ddl) {
	try {
		await client.query(ddl);
	} catch (err) {
		if (err.code !== '23505' && err.code !== '42P07' && err.code !== '42710') {
			throw err;
		}
	}
}
