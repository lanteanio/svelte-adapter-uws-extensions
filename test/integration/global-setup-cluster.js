import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const composePath = path.join(__dirname, 'docker-compose.cluster.yml');

// Host ports default to a 6-port range starting at 57000. Override via env
// when running multiple cluster stacks in parallel (worktree agents, CI
// matrix slices, etc.):
//   INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE=58000 \
//   INTEGRATION_COMPOSE_PROJECT=my-slice-cluster \
//   npm run test:integration:cluster
const BASE_PORT = Number(process.env.INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE) || 57000;
const NODE_PORTS = [
	BASE_PORT,
	BASE_PORT + 1,
	BASE_PORT + 2,
	BASE_PORT + 3,
	BASE_PORT + 4,
	BASE_PORT + 5
];

const PROJECT_NAME = process.env.INTEGRATION_COMPOSE_PROJECT
	|| (BASE_PORT === 57000
		? 'svelte-adapter-uws-extensions-cluster'
		: `svelte-adapter-uws-ext-cluster-${BASE_PORT}`);

// The cluster nodes use static IPs (172.30.0.10 - 172.30.0.15) inside the
// bridge network. The natMap lets ioredis on the host translate those IPs
// back to 127.0.0.1:<host-port> when it follows MOVED redirects.
const NAT_MAP = {
	'172.30.0.10:6379': { host: '127.0.0.1', port: NODE_PORTS[0] },
	'172.30.0.11:6379': { host: '127.0.0.1', port: NODE_PORTS[1] },
	'172.30.0.12:6379': { host: '127.0.0.1', port: NODE_PORTS[2] },
	'172.30.0.13:6379': { host: '127.0.0.1', port: NODE_PORTS[3] },
	'172.30.0.14:6379': { host: '127.0.0.1', port: NODE_PORTS[4] },
	'172.30.0.15:6379': { host: '127.0.0.1', port: NODE_PORTS[5] }
};

function compose(args, opts = {}) {
	const result = spawnSync(
		'docker',
		['compose', '-p', PROJECT_NAME, '-f', composePath, ...args],
		{
			stdio: opts.silent ? 'pipe' : 'inherit',
			encoding: 'utf8',
			env: {
				...process.env,
				INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE: String(NODE_PORTS[0]),
				INTEGRATION_REDIS_CLUSTER_NODE_1_PORT: String(NODE_PORTS[1]),
				INTEGRATION_REDIS_CLUSTER_NODE_2_PORT: String(NODE_PORTS[2]),
				INTEGRATION_REDIS_CLUSTER_NODE_3_PORT: String(NODE_PORTS[3]),
				INTEGRATION_REDIS_CLUSTER_NODE_4_PORT: String(NODE_PORTS[4]),
				INTEGRATION_REDIS_CLUSTER_NODE_5_PORT: String(NODE_PORTS[5])
			}
		}
	);
	if (result.error) {
		throw new Error(
			`docker compose failed: ${result.error.message}\n` +
			'Cluster integration tests require Docker. Start Docker and re-run.'
		);
	}
	if (result.status !== 0) {
		throw new Error(`docker compose ${args.join(' ')} exited with code ${result.status}`);
	}
	return result;
}

// Bitnami / docker-compose `--wait` waits for healthchecks, but the cluster-
// init service is short-lived (one-shot create). We poll the cluster state
// directly until `cluster_state=ok` from any node, with backoff. Bootstrap
// is typically 5-15 seconds; allow up to 60.
async function waitForClusterReady() {
	const { default: Redis } = await import('ioredis');
	const probeUrl = `redis://localhost:${NODE_PORTS[0]}`;
	const deadlineMs = Date.now() + 60_000;
	let lastErr = null;
	while (Date.now() < deadlineMs) {
		const r = new Redis(probeUrl, { maxRetriesPerRequest: 0, lazyConnect: true });
		try {
			await r.connect();
			const info = await r.cluster('info');
			const state = /cluster_state:(\w+)/.exec(info)?.[1];
			const slotsOk = /cluster_slots_ok:(\d+)/.exec(info)?.[1];
			if (state === 'ok' && Number(slotsOk) === 16384) {
				return;
			}
			lastErr = new Error(`cluster_state=${state}, cluster_slots_ok=${slotsOk}`);
		} catch (err) {
			lastErr = err;
		} finally {
			r.disconnect();
		}
		await new Promise((res) => setTimeout(res, 500));
	}
	throw new Error('Cluster never reached cluster_state=ok within 60s. Last: ' + (lastErr?.message || 'unknown'));
}

export default async function setup() {
	// Vitest globalSetup runs in a separate context; tests read these via
	// process.env. Tests should use `clusterClient()` from helpers/ which
	// reads INTEGRATION_REDIS_CLUSTER_NODES + INTEGRATION_REDIS_CLUSTER_NAT_MAP.
	process.env.INTEGRATION_REDIS_CLUSTER_NODES = NODE_PORTS
		.map((p) => `127.0.0.1:${p}`)
		.join(',');
	process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP = JSON.stringify(NAT_MAP);

	console.log(`[integration-cluster] starting docker compose stack (project=${PROJECT_NAME})...`);
	compose(['up', '-d', '--wait']);
	console.log('[integration-cluster] containers healthy; waiting for cluster bootstrap...');
	await waitForClusterReady();
	console.log(
		`[integration-cluster] cluster ready (${NODE_PORTS.length} nodes at ` +
		`127.0.0.1:${NODE_PORTS[0]}-${NODE_PORTS[NODE_PORTS.length - 1]})`
	);

	return async () => {
		console.log('[integration-cluster] tearing down docker compose stack...');
		compose(['down', '-v']);
	};
}
