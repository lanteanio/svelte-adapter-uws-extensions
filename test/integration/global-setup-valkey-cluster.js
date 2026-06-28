import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Valkey Cluster global-setup: the Valkey analogue of global-setup-cluster.js.
// Brings up the 6-node Valkey cluster (docker-compose.valkey-cluster.yml) and
// exposes the node list + natMap the same way, so the cluster-aware suites run
// unchanged against Valkey. Distinct default ports (571xx) and bridge subnet
// (172.31.0.0/24) let it sit beside the Redis cluster.

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const composePath = path.join(__dirname, 'docker-compose.valkey-cluster.yml');

const BASE_PORT = Number(process.env.INTEGRATION_VALKEY_CLUSTER_HOST_PORT_BASE) || 57100;
const NODE_PORTS = [BASE_PORT, BASE_PORT + 1, BASE_PORT + 2, BASE_PORT + 3, BASE_PORT + 4, BASE_PORT + 5];

const PROJECT_NAME = process.env.INTEGRATION_COMPOSE_PROJECT
	|| (BASE_PORT === 57100
		? 'svelte-adapter-uws-extensions-valkey-cluster'
		: `svelte-adapter-uws-ext-valkey-cluster-${BASE_PORT}`);

// Static IPs (172.31.0.10 - 172.31.0.15) inside the bridge network; the natMap
// lets ioredis on the host translate them to 127.0.0.1:<host-port> on MOVED.
const NAT_MAP = {
	'172.31.0.10:6379': { host: '127.0.0.1', port: NODE_PORTS[0] },
	'172.31.0.11:6379': { host: '127.0.0.1', port: NODE_PORTS[1] },
	'172.31.0.12:6379': { host: '127.0.0.1', port: NODE_PORTS[2] },
	'172.31.0.13:6379': { host: '127.0.0.1', port: NODE_PORTS[3] },
	'172.31.0.14:6379': { host: '127.0.0.1', port: NODE_PORTS[4] },
	'172.31.0.15:6379': { host: '127.0.0.1', port: NODE_PORTS[5] }
};

function compose(args) {
	const result = spawnSync(
		'docker',
		['compose', '-p', PROJECT_NAME, '-f', composePath, ...args],
		{
			stdio: 'inherit',
			encoding: 'utf8',
			env: {
				...process.env,
				INTEGRATION_VALKEY_CLUSTER_HOST_PORT_BASE: String(NODE_PORTS[0]),
				INTEGRATION_VALKEY_CLUSTER_NODE_1_PORT: String(NODE_PORTS[1]),
				INTEGRATION_VALKEY_CLUSTER_NODE_2_PORT: String(NODE_PORTS[2]),
				INTEGRATION_VALKEY_CLUSTER_NODE_3_PORT: String(NODE_PORTS[3]),
				INTEGRATION_VALKEY_CLUSTER_NODE_4_PORT: String(NODE_PORTS[4]),
				INTEGRATION_VALKEY_CLUSTER_NODE_5_PORT: String(NODE_PORTS[5])
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
	throw new Error('Valkey cluster never reached cluster_state=ok within 60s. Last: ' + (lastErr?.message || 'unknown'));
}

export default async function setup() {
	process.env.INTEGRATION_REDIS_CLUSTER_NODES = NODE_PORTS.map((p) => `127.0.0.1:${p}`).join(',');
	process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP = JSON.stringify(NAT_MAP);

	console.log(`[integration-valkey-cluster] starting docker compose stack (project=${PROJECT_NAME})...`);
	compose(['up', '-d', '--wait']);
	console.log('[integration-valkey-cluster] containers healthy; waiting for cluster bootstrap...');
	await waitForClusterReady();
	console.log(
		`[integration-valkey-cluster] cluster ready (${NODE_PORTS.length} nodes at ` +
		`127.0.0.1:${NODE_PORTS[0]}-${NODE_PORTS[NODE_PORTS.length - 1]})`
	);

	return async () => {
		console.log('[integration-valkey-cluster] tearing down docker compose stack...');
		compose(['down', '-v']);
	};
}
