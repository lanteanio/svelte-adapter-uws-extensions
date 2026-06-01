// Global setup for the cluster-mirror tier: brings up the same Redis Cluster
// stack as global-setup-cluster.js, and additionally flips INTEGRATION_BACKEND
// to 'cluster' so the standalone redis/ suites (run via
// vitest.config.integration.cluster-mirror.js) resolve their client through the
// cluster-backed createBackendClient instead of the standalone one.
//
// Setting the env here (the global-setup context) rather than in the config
// guarantees it is visible to the test workers the same way the cluster node
// list is - vitest propagates global-setup process.env mutations to workers.
import clusterSetup from './global-setup-cluster.js';

export default async function setup() {
	process.env.INTEGRATION_BACKEND = 'cluster';
	return clusterSetup();
}
