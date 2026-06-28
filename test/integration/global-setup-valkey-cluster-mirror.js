// Global setup for the Valkey cluster-mirror tier: brings up the same Valkey
// Cluster stack as global-setup-valkey-cluster.js, and additionally flips
// INTEGRATION_BACKEND to 'cluster' so the standalone redis/ suites (run via
// vitest.config.integration.valkey-cluster-mirror.js) resolve their client
// through the cluster-backed createBackendClient against the Valkey cluster.
// Mirrors global-setup-cluster-mirror.js exactly, swapping the backend server.
import valkeyClusterSetup from './global-setup-valkey-cluster.js';

export default async function setup() {
	process.env.INTEGRATION_BACKEND = 'cluster';
	return valkeyClusterSetup();
}
