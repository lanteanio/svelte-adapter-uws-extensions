import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const root = resolve(import.meta.dirname, '..');
const read = (path) => readFileSync(resolve(root, path), 'utf8');
const readme = read('README.md');
// The adapter this suite runs against, and the release the README documents
// (the newest numbered CHANGELOG section; an Unreleased one is skipped).
const adapter = JSON.parse(read('node_modules/svelte-adapter-uws/package.json'));
const releaseOf = (changelog) => /^## \[(\d[^\]]*)\]/m.exec(changelog)[1];
const release = releaseOf(read('CHANGELOG.md'));

describe('newcomer prerequisite route', () => {
	it('installs this release with the adapter it is tested against and that adapter\'s own uWebSockets.js build', () => {
		expect(readme).toContain(`npm install svelte-adapter-uws@${adapter.version} svelte-adapter-uws-extensions@${release} ioredis`);
		expect(readme).toContain(`npm install ${adapter.optionalDependencies['uWebSockets.js']}`);
	});

	it('reads the release past an Unreleased section', () => {
		expect(releaseOf('## [Unreleased]\n\n### Fixed\n\n## [0.6.0-next.65] - 2026-10-01\n')).toBe('0.6.0-next.65');
	});

	it('lists the same pair in the version table', () => {
		const row = readme.split('\n').find((line) => line.startsWith('| `' + adapter.version + '` |'));
		expect(row).toBeDefined();
		expect(row).toContain('| `' + release + '` |');
	});

	it('requires the public adapter preflight before backend setup', () => {
		const preflight = readme.indexOf('npm exec -- svelte-adapter-uws-preflight');
		const postgres = readme.indexOf('Postgres support is optional:');
		expect(preflight).toBeGreaterThan(0);
		expect(postgres).toBeGreaterThan(preflight);
		expect(readme).toContain('Continue only after `preflight OK`');
	});
});
