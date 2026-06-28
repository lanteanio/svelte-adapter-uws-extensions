import { defineConfig, configDefaults } from 'vitest/config';

// Redis solo tier: the Redis-backed plugin suites against a standalone Redis. One
// backend, one suite - the Postgres suites are an orthogonal axis (their own
// `test:integration:postgres` command), and the cluster/Valkey variants are
// separate tiers. keyslot-parity is cluster-only (it compares against CLUSTER
// KEYSLOT) and lives in redis/, so the cluster-mirror tier runs it but the solo
// tier scopes it OUT (not skips it) to stay zero-skip.
export default defineConfig({
	test: {
		include: ['test/integration/redis/**/*.test.js'],
		exclude: [...configDefaults.exclude, 'test/integration/redis/keyslot-parity.test.js'],
		globalSetup: ['./test/integration/global-setup-redis.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		testTimeout: 30000,
		hookTimeout: 60000
	}
});
