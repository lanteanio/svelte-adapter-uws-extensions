// Cluster client factory for integration tests. Reads the node list and
// natMap that global-setup-cluster.js exported via process.env.
//
// natMap is essential here: the docker-compose stack puts each node on a
// static bridge-network IP (172.30.0.10 - 172.30.0.15) which the cluster
// announces in MOVED redirects. Without the natMap, the ioredis client
// running on the host cannot resolve those IPs and every redirect fails.
import Redis from 'ioredis';

function readEnv() {
	const nodesEnv = process.env.INTEGRATION_REDIS_CLUSTER_NODES;
	if (!nodesEnv) {
		throw new Error(
			'INTEGRATION_REDIS_CLUSTER_NODES not set; cluster integration global-setup did not run'
		);
	}
	const natMapEnv = process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP;
	if (!natMapEnv) {
		throw new Error('INTEGRATION_REDIS_CLUSTER_NAT_MAP not set');
	}
	const nodes = nodesEnv.split(',').map((s) => {
		const [host, port] = s.trim().split(':');
		return { host, port: Number(port) };
	});
	const natMap = JSON.parse(natMapEnv);
	return { nodes, natMap };
}

/**
 * Create a connected ioredis Cluster client. Caller owns lifecycle and must
 * `await client.quit()` (or `client.disconnect()`) at teardown.
 *
 * @param {import('ioredis').ClusterOptions} [overrides]
 */
export function clusterClient(overrides = {}) {
	const { nodes, natMap } = readEnv();
	return new Redis.Cluster(nodes, {
		natMap,
		// Test-friendly retry/redirect behavior: surface failures fast instead
		// of hanging the test runner.
		maxRedirections: 16,
		retryDelayOnFailover: 100,
		retryDelayOnClusterDown: 100,
		slotsRefreshTimeout: 5000,
		clusterRetryStrategy: (times) => (times >= 3 ? null : 100),
		...overrides
	});
}
