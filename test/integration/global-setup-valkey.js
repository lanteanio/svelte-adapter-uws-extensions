import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Valkey integration tier: brings up a real Valkey server and points
// INTEGRATION_REDIS_URL at it, so the Redis-backed integration suite
// (test/integration/redis/**) runs unchanged against Valkey - the same
// "swap the backend, re-run the suite" shape as the cluster-mirror tier.
// Valkey speaks the Redis protocol, so the URL scheme stays redis://.

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const composePath = path.join(__dirname, 'docker-compose.valkey.yml');

// A distinct default port (56381) from the Redis solo tier (56379) and the
// cluster tier so a Valkey run can sit beside them without a clash.
const VALKEY_HOST_PORT = Number(process.env.INTEGRATION_VALKEY_HOST_PORT) || 56381;
const PROJECT_NAME = process.env.INTEGRATION_COMPOSE_PROJECT
	|| `svelte-adapter-uws-ext-valkey-${VALKEY_HOST_PORT}`;
const REDIS_URL = `redis://localhost:${VALKEY_HOST_PORT}`;

function compose(args) {
	const result = spawnSync(
		'docker',
		['compose', '-p', PROJECT_NAME, '-f', composePath, ...args],
		{
			stdio: 'inherit',
			encoding: 'utf8',
			env: { ...process.env, INTEGRATION_VALKEY_HOST_PORT: String(VALKEY_HOST_PORT) }
		}
	);
	if (result.error) {
		throw new Error(
			`docker compose failed: ${result.error.message}\n` +
			'Integration tests require Docker Desktop. Start Docker and re-run.'
		);
	}
	if (result.status !== 0) {
		throw new Error(`docker compose ${args.join(' ')} exited with code ${result.status}`);
	}
}

export default async function setup() {
	process.env.INTEGRATION_REDIS_URL = REDIS_URL;

	console.log(`[integration:valkey] starting docker compose stack (project=${PROJECT_NAME})...`);
	compose(['up', '-d', '--wait']);
	console.log(`[integration:valkey] stack ready (valkey :${VALKEY_HOST_PORT})`);

	return async () => {
		console.log('[integration:valkey] tearing down docker compose stack...');
		compose(['down', '-v']);
	};
}
