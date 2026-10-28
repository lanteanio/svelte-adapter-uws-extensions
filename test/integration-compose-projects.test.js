import { afterEach, describe, expect, it, vi } from 'vitest';

const spawnSync = vi.fn(() => ({ status: 0 }));
vi.mock('node:child_process', () => ({ spawnSync }));

const previousProject = process.env.INTEGRATION_COMPOSE_PROJECT;
process.env.INTEGRATION_COMPOSE_PROJECT = 'shared-integration-project';
const standalone = await import('./integration/global-setup-redis.js');
const cluster = await import('./integration/global-setup-cluster.js');
if (previousProject === undefined) delete process.env.INTEGRATION_COMPOSE_PROJECT;
else process.env.INTEGRATION_COMPOSE_PROJECT = previousProject;

afterEach(() => {
	spawnSync.mockClear();
});

describe('integration Compose project isolation', () => {
	it('suffixes the cluster project when standalone and cluster share one environment value', () => {
		standalone.composeDown();
		cluster.composeDown();

		expect(spawnSync).toHaveBeenCalledTimes(2);
		const projects = spawnSync.mock.calls.map(([, args]) => args[args.indexOf('-p') + 1]);
		expect(projects).toEqual(['shared-integration-project', 'shared-integration-project-cluster']);
		expect(new Set(projects).size).toBe(2);
	});
});
