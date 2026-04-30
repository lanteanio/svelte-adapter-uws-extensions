import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const composePath = path.join(__dirname, 'docker-compose.yml');

// Host ports default to non-standard values that won't clash with a locally
// running Postgres (5432) or Redis (6379). Override via env vars when running
// multiple integration stacks side-by-side (e.g. parallel agents in worktrees):
//   INTEGRATION_REDIS_HOST_PORT=56380 \
//   INTEGRATION_POSTGRES_HOST_PORT=55433 \
//   INTEGRATION_COMPOSE_PROJECT=my-slice \
//   npm run test:integration
const REDIS_HOST_PORT = Number(process.env.INTEGRATION_REDIS_HOST_PORT) || 56379;
const POSTGRES_HOST_PORT = Number(process.env.INTEGRATION_POSTGRES_HOST_PORT) || 55432;

// Project name keys the docker compose stack and must be unique per concurrent
// stack so containers don't collide. Defaults derive from the port pair so
// custom-port runs auto-isolate from the default-port stack.
const PROJECT_NAME = process.env.INTEGRATION_COMPOSE_PROJECT
	|| (REDIS_HOST_PORT === 56379 && POSTGRES_HOST_PORT === 55432
		? 'svelte-adapter-uws-extensions-integration'
		: `svelte-adapter-uws-ext-int-${REDIS_HOST_PORT}-${POSTGRES_HOST_PORT}`);

const REDIS_URL = `redis://localhost:${REDIS_HOST_PORT}`;
const POSTGRES_URL = `postgres://test:test@localhost:${POSTGRES_HOST_PORT}/test`;

function compose(args) {
	const result = spawnSync(
		'docker',
		['compose', '-p', PROJECT_NAME, '-f', composePath, ...args],
		{
			stdio: 'inherit',
			encoding: 'utf8',
			env: {
				...process.env,
				INTEGRATION_REDIS_HOST_PORT: String(REDIS_HOST_PORT),
				INTEGRATION_POSTGRES_HOST_PORT: String(POSTGRES_HOST_PORT)
			}
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
	process.env.INTEGRATION_POSTGRES_URL = POSTGRES_URL;

	console.log(`[integration] starting docker compose stack (project=${PROJECT_NAME})...`);
	compose(['up', '-d', '--wait']);
	console.log(
		`[integration] stack ready (postgres :${POSTGRES_HOST_PORT}, redis :${REDIS_HOST_PORT})`
	);

	return async () => {
		console.log('[integration] tearing down docker compose stack...');
		compose(['down', '-v']);
	};
}
