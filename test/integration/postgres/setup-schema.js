import { afterEach } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';

// Test fixtures intentionally drop their store tables between cases. Remove
// only those fixtures' orphan ledger rows after each case has completed.
afterEach(async () => {
	const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
	try {
		const exists = await client.query("SELECT to_regclass('svti_schema_migrations') AS ledger");
		if (exists.rows[0].ledger) await client.query('DELETE FROM svti_schema_migrations WHERE to_regclass(table_name) IS NULL');
	} finally { await client.end(); }
});
