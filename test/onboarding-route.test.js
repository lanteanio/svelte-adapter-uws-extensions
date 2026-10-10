import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { describe, it, expect } from 'vitest';

const root = resolve(import.meta.dirname, '..');
const read = (path) => readFileSync(resolve(root, path), 'utf8');
const pkg = JSON.parse(read('package.json'));
const adapterRoot = resolve(root, 'node_modules/svelte-adapter-uws');
const adapter = JSON.parse(readFileSync(resolve(adapterRoot, 'package.json'), 'utf8'));
const preflightPath = resolve(adapterRoot, adapter.bin['svelte-adapter-uws-preflight']);
const { runPreflight } = await import(pathToFileURL(preflightPath).href);
const guide = read('docs/getting-started.md');
const workflow = read('.github/workflows/verify.yml');

function publishedCommands(text) {
	return [...text.matchAll(/```sh\n([\s\S]*?)```/g)]
		.flatMap((block) => block[1].trim().split('\n'))
		.filter((line) => line.startsWith('npm '));
}

describe('first-success route', () => {
	it('runs the literal documented post-clone commands in a clean minimum-profile CI job', () => {
		const job = workflow.slice(workflow.indexOf('  onboarding:'));
		const commands = [...job.matchAll(/- run: (.+)/g)].map((match) => match[1]);
		expect(commands).toEqual(publishedCommands(guide));
		expect(commands).toEqual(['npm ci', 'npm run preflight', 'npm run smoke -- --mock-only', 'npm run smoke']);
		expect(job).toContain('runs-on: ubuntu-24.04');
		expect(job).toContain("node-version: '22.0.0'");
		expect(job).not.toContain('cache:');
		expect(guide).toContain('continue only after `preflight OK`');
	});

	it('uses the installed lead preflight and makes it the first PR prerequisite', () => {
		expect(pkg.scripts.preflight).toBe('svelte-adapter-uws-preflight');
		expect(adapter.bin['svelte-adapter-uws-preflight']).toBeTruthy();
		expect(pkg.scripts['verify:pr']).toMatch(/^npm run preflight && /);
		expect(read('CONTRIBUTING.md')).toContain('npm ci\nnpm run preflight\nnpm run smoke');
		expect(read('README.md')).toContain('[First success](docs/getting-started.md)');
	});

	it('loads the actual native addon at the prerequisite boundary', () => {
		const result = spawnSync(process.execPath, [preflightPath], { encoding: 'utf8', timeout: 15_000 });
		expect(result.status, result.stdout + result.stderr).toBe(0);
		expect(result.stdout).toContain('preflight OK');
		expect(result.stdout).toContain('uWebSockets.js');
	});

	it.each([
		['old Node', { nodeVersion: '20.0.0' }],
		['unsupported OS', { platform: 'freebsd' }],
		['unsupported architecture', { arch: 'ia32' }],
		['musl libc', { platform: 'linux', libc: { name: 'musl', version: null } }],
		['unknown libc', { platform: 'linux', libc: null }],
		['old glibc', { platform: 'linux', libc: { name: 'glibc', version: '2.36' } }],
		['missing native addon', { native: { nativeVersion: null, nativeError: 'module not found' } }]
	])('fails before demonstration for %s', async (_name, fault) => {
		const output = [];
		const nativeVersion = /v(\d+\.\d+\.\d+)/.exec(adapter.optionalDependencies['uWebSockets.js'])[1];
		const result = await runPreflight({
			pkg: adapter, nodeVersion: '22.0.0', platform: 'linux', arch: 'x64',
			libc: { name: 'glibc', version: '2.39' }, native: { nativeVersion, nativeError: null },
			log: (line) => output.push(line), error: (line) => output.push(line), ...fault
		});
		expect(result.ok).toBe(false);
		expect(output.join('\n')).toContain('preflight FAILED at prerequisite boundary');
		expect(output.join('\n')).not.toContain('preflight OK');
	});
});
