import { defineConfig, configDefaults } from 'vitest/config';

// Valkey integration tier: re-runs the Redis-backed integration suite
// (test/integration/redis/**) against a real Valkey server via
// global-setup-valkey.js. Same suites, swapped backend - the DRY mirror of
// the cluster-mirror tier. The Postgres suite is independent of the
// Redis/Valkey choice, so it is not re-run here. Requires Valkey 9.0+ to
// survive the whole suite (the presence HEXPIRE activation gate).
export default defineConfig({
	test: {
		include: ['test/integration/redis/**/*.test.js'],
		// keyslot-parity is cluster-only (compares against CLUSTER KEYSLOT); scope it
		// out of the standalone Valkey tier (not skip it) so this suite is zero-skip.
		exclude: [...configDefaults.exclude, 'test/integration/redis-cluster/**', 'test/integration/redis/keyslot-parity.test.js'],
		globalSetup: ['./test/integration/global-setup-valkey.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		testTimeout: 30000,
		hookTimeout: 60000
	}
});
