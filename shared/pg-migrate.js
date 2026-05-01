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
