import { defineConfig, configDefaults } from 'vitest/config';

export default defineConfig({
	test: {
		include: ['test/integration/**/*.test.js'],
		// Cluster tests live under redis-cluster/ and need the Redis Cluster
		// stack from global-setup-cluster.js (run via the separate
		// `test:integration:cluster` script + vitest.config.integration.cluster.js).
		// Excluded here so the standalone tier does not load them without the
		// cluster env (they would throw in beforeAll on a missing cluster).
		exclude: [...configDefaults.exclude, 'test/integration/redis-cluster/**'],
		globalSetup: ['./test/integration/global-setup.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		testTimeout: 30000,
		hookTimeout: 60000
	}
});
