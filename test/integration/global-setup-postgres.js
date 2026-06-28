import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const composePath = path.join(__dirname, 'docker-compose.yml');

// Postgres-only tier: brings up JUST the `postgres` service from the shared
// compose file and points INTEGRATION_POSTGRES_URL at it. The Postgres-backed
// plugin suites (test/integration/postgres/**) never open a Redis connection, so
// the redis service is not started - this tier is one backend, one suite. There
// is no "Valkey of Postgres", so unlike the Redis suite this tier has no backend
// variants; it is the single Postgres integration command. Override host port /
// project to run side-by-side with another stack:
//   INTEGRATION_POSTGRES_HOST_PORT=55440 INTEGRATION_COMPOSE_PROJECT=my-slice npm run test:integration:postgres
const POSTGRES_HOST_PORT = Number(process.env.INTEGRATION_POSTGRES_HOST_PORT) || 55432;
const PROJECT_NAME = process.env.INTEGRATION_COMPOSE_PROJECT
	|| (POSTGRES_HOST_PORT === 55432
		? 'svelte-adapter-uws-ext-int-postgres'
		: `svelte-adapter-uws-ext-int-postgres-${POSTGRES_HOST_PORT}`);
const POSTGRES_URL = `postgres://test:test@localhost:${POSTGRES_HOST_PORT}/test`;

function compose(args) {
	const result = spawnSync('docker', ['compose', '-p', PROJECT_NAME, '-f', composePath, ...args], {
		stdio: 'inherit',
		encoding: 'utf8',
		env: { ...process.env, INTEGRATION_POSTGRES_HOST_PORT: String(POSTGRES_HOST_PORT) }
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
	process.env.INTEGRATION_POSTGRES_URL = POSTGRES_URL;

	console.log(`[integration:postgres] starting postgres service (project=${PROJECT_NAME})...`);
	compose(['up', '-d', '--wait', 'postgres']);
	console.log(`[integration:postgres] postgres ready (:${POSTGRES_HOST_PORT})`);

	return async () => {
		console.log('[integration:postgres] tearing down docker compose stack...');
		compose(['down', '-v']);
	};
}
