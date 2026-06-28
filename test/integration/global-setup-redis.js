import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const composePath = path.join(__dirname, 'docker-compose.yml');

// Redis-only solo tier: brings up JUST the `redis` service from the shared
// compose file and points INTEGRATION_REDIS_URL at it. The Redis-backed plugin
// suites (test/integration/redis/**) never open a Postgres connection, so the
// postgres service is not started - this tier is one backend, one suite. Override
// the host port / project to run side-by-side with another stack:
//   INTEGRATION_REDIS_HOST_PORT=56390 INTEGRATION_COMPOSE_PROJECT=my-slice npm run test:integration:redis
const REDIS_HOST_PORT = Number(process.env.INTEGRATION_REDIS_HOST_PORT) || 56379;
const PROJECT_NAME = process.env.INTEGRATION_COMPOSE_PROJECT
	|| (REDIS_HOST_PORT === 56379
		? 'svelte-adapter-uws-ext-int-redis'
		: `svelte-adapter-uws-ext-int-redis-${REDIS_HOST_PORT}`);
const REDIS_URL = `redis://localhost:${REDIS_HOST_PORT}`;

function compose(args) {
	const result = spawnSync('docker', ['compose', '-p', PROJECT_NAME, '-f', composePath, ...args], {
		stdio: 'inherit',
		encoding: 'utf8',
		env: { ...process.env, INTEGRATION_REDIS_HOST_PORT: String(REDIS_HOST_PORT) }
	});
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

	console.log(`[integration:redis] starting redis service (project=${PROJECT_NAME})...`);
	compose(['up', '-d', '--wait', 'redis']);
	console.log(`[integration:redis] redis ready (:${REDIS_HOST_PORT})`);

	return async () => {
		console.log('[integration:redis] tearing down docker compose stack...');
		compose(['down', '-v']);
	};
}
