import { defineConfig, configDefaults } from 'vitest/config';

// Cluster-mirror tier: runs the SAME standalone redis/ integration suites
// against a real Redis Cluster, with createBackendClient switched to the
// cluster backend (INTEGRATION_BACKEND=cluster, set by the global-setup). This
// is the DRY half of "mirror the solo integration suites against the cluster"
// - one set of suites, two backends - whose first pass maps which operations
// are cluster-safe and which need a hash-tag / per-node redesign (tracked as
// follow-ups). The redis-cluster/ tier (vitest.config.integration.cluster.js)
// keeps its own cluster-only suites; this config covers the mirrored ones.
export default defineConfig({
	test: {
		include: ['test/integration/redis/**/*.test.js'],
		exclude: [...configDefaults.exclude],
		globalSetup: ['./test/integration/global-setup-cluster-mirror.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		// A real 6-node cluster has higher, more variable per-command latency than
		// a single node, so tests that assert a real-time TTL / refill / lease
		// window can occasionally miss a tight band even though the plugin behavior
		// is correct (and is asserted deterministically on the solo tier). A small
		// retry budget self-heals that inherent timing jitter so the mirror reports
		// genuine cluster gaps (which are skip-documented), not latency noise. Does
		// not apply to the solo config.
		retry: 2,
		testTimeout: 30000,
		hookTimeout: 90000
	}
});
