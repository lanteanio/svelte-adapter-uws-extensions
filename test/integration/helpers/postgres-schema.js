/** Remove only the named disposable fixtures' migration history after dropping their tables. */
export async function forgetFixtureMigrations(client, tables) {
	const { rows } = await client.query("SELECT to_regclass('svti_schema_migrations') AS ledger");
	if (rows[0].ledger) await client.query('DELETE FROM svti_schema_migrations WHERE table_name = ANY($1::text[])', [tables]);
}
