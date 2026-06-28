import { defineConfig, configDefaults } from 'vitest/config';

// Postgres tier: the Postgres-backed plugin suites against a standalone Postgres.
// One backend, one suite. There is no alternative Postgres backend to test parity
// against (unlike Redis <-> Valkey), so this is the single Postgres integration
// command - no solo/cluster/variant split.
export default defineConfig({
	test: {
		include: ['test/integration/postgres/**/*.test.js'],
		exclude: [...configDefaults.exclude],
		globalSetup: ['./test/integration/global-setup-postgres.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		testTimeout: 30000,
		hookTimeout: 60000
	}
});
