import { defineConfig } from 'vitest/config';

// Valkey Cluster tier: the cluster-only suites (test/integration/redis-cluster/**)
// run against a 3-master + 3-replica Valkey Cluster brought up by
// global-setup-valkey-cluster.js - the Valkey analogue of
// vitest.config.integration.cluster.js. Valkey speaks the Redis cluster
// protocol, so the cluster-only suites (keyslot parity, reshard, scan/unlink)
// run unchanged.
export default defineConfig({
	test: {
		include: ['test/integration/redis-cluster/**/*.test.js'],
		globalSetup: ['./test/integration/global-setup-valkey-cluster.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		// Right after `cluster create`, slot-ownership gossip can still be
		// propagating to a node even though the probe node reports
		// cluster_state:ok, so a multi-master scan/unlink can transiently miss a
		// master or hit a stale-view CROSSSLOT on the first attempt and pass on a
		// retry once gossip settles. The same multi-node jitter the cluster-mirror
		// tier absorbs with retry:2; Valkey converges a touch slower than Redis, so
		// the cluster-only suite needs it here too. Self-heals the bootstrap race
		// without masking a genuine gap (a real failure fails all attempts).
		retry: 2,
		testTimeout: 30000,
		hookTimeout: 90000
	}
});
