import { defineConfig } from 'vitest/config';

// Cluster integration tests live in test/integration/redis-cluster/ and run
// against a 3-master + 3-replica Redis Cluster brought up by
// `test/integration/global-setup-cluster.js`. Separate config + npm script
// from the standalone integration tier so devs only pay the cluster
// bootstrap cost when they actually need Cluster coverage.
export default defineConfig({
	test: {
		include: ['test/integration/redis-cluster/**/*.test.js'],
		globalSetup: ['./test/integration/global-setup-cluster.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		testTimeout: 30000,
		hookTimeout: 90000
	}
});
