import { defineConfig, configDefaults } from 'vitest/config';

// Valkey cluster-mirror tier: runs the SAME standalone redis/ integration
// suites against a real Valkey Cluster, with createBackendClient switched to
// the cluster backend (INTEGRATION_BACKEND=cluster, set by the global-setup).
// The Valkey analogue of vitest.config.integration.cluster-mirror.js - it
// completes the backend matrix (Redis solo, Valkey solo, Redis cluster, Valkey
// cluster). Requires Valkey 9.0+ to survive the whole suite (presence HEXPIRE).
export default defineConfig({
	test: {
		include: ['test/integration/redis/**/*.test.js'],
		exclude: [...configDefaults.exclude],
		globalSetup: ['./test/integration/global-setup-valkey-cluster-mirror.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		// Same inherent multi-node timing jitter as the Redis cluster-mirror tier:
		// a small retry budget self-heals real-time TTL/refill/lease band misses so
		// the mirror reports genuine cluster gaps, not latency noise.
		retry: 2,
		testTimeout: 30000,
		hookTimeout: 90000
	}
});
