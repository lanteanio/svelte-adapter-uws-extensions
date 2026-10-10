import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import { installedCompatibility } from '../scripts/check-compatibility.js';

const root = resolve(import.meta.dirname, '..');
const read = (path) => readFileSync(resolve(root, path), 'utf8');
const readme = read('README.md');
// The installed adapter owns the published compatibility declaration.
const adapter = JSON.parse(read('node_modules/svelte-adapter-uws/package.json'));
const current = installedCompatibility().rows.find(row => row.current === 'true');

describe('newcomer prerequisite route', () => {
	it('installs the declared extensions tuple and the installed adapter\'s own uWebSockets.js build', () => {
		expect(readme).toContain(`npm install svelte-adapter-uws@${adapter.version} svelte-adapter-uws-extensions@${current.extensions_version} ioredis`);
		expect(readme).toContain(`npm install ${adapter.optionalDependencies['uWebSockets.js']}`);
	});

	it('lists the same pair in the version table', () => {
		const row = readme.split('\n').find((line) => line.startsWith('| `' + adapter.version + '` |'));
		expect(row).toBeDefined();
		expect(row).toContain('| `' + current.extensions_version + '` |');
	});

	it('requires the public adapter preflight before backend setup', () => {
		const preflight = readme.indexOf('npm exec -- svelte-adapter-uws-preflight');
		const postgres = readme.indexOf('Postgres support is optional:');
		expect(preflight).toBeGreaterThan(0);
		expect(postgres).toBeGreaterThan(preflight);
		expect(readme).toContain('Continue only after `preflight OK`');
	});
});
