import { readFileSync } from 'node:fs';
import { describe, it, expect } from 'vitest';
import { validateManifest, validateReadme, validatePackageScripts, governedNativeRefs, renderEntries, renderTopics, replaceBlock } from '../scripts/check-documentation-contract.js';
import { anchorsOf } from '../scripts/check-links.js';
const read = (path) => readFileSync(new URL('../' + path, import.meta.url), 'utf8');
const pkg = JSON.parse(read('package.json')), manifest = JSON.parse(read('docs/documentation.v2.json')), catalog = JSON.parse(read('docs/public-entries.v1.json')), readme = read('README.md');
describe('documentation entry contract', () => {
	it('pins the actual README length and fails when one line is added', () => {
		expect(validateManifest(manifest, pkg)).toEqual([]);
		expect(validateReadme(readme, manifest)).toEqual([]);
		expect(manifest.readmeMaxLines).toBe(readme.replaceAll('\r\n', '\n').split('\n').length);
		expect(validateReadme(readme + '\n', manifest)).toContain('README exceeds readmeMaxLines; move content into docs before adding more');
	});
	it('enforces the entry position independently of total size', () => {
		expect(validateReadme('\n'.repeat(150) + readme, { ...manifest, readmeMaxLines: 10000 })).toContain('Reader paths exceed entry budget');
	});
	it('rejects swapped, duplicated, wrongly owned or unsafe routes', () => {
		for (const mutate of [
			(m) => m.paths.reverse(),
			(m) => { m.paths[0].type = 'Banana'; },
			(m) => { m.compatibilitySource = 'elsewhere'; },
			(m) => { m.extra = true; },
			(m) => { m.schemaVersion = 1; },
			(m) => { m.paths[1].destination = m.paths[0].destination; },
			(m) => { m.paths[0].owner = 'elsewhere'; },
			(m) => { m.paths[3].destination = 'http://svelte-realtime.dev/tutorial'; },
			(m) => { m.paths[3].destination = 'https://svelte-realtime.dev/tutorial?secret=1'; }
		]) { const copy = structuredClone(manifest); mutate(copy); expect(validateManifest(copy, pkg).length).toBeGreaterThan(0); }
	});
	it('requires complete export metadata and resolved references', () => {
		expect(renderEntries(pkg, catalog, readme)).toBe(read('docs/public-entries.md'));
		const copy = structuredClone(catalog); copy.entries.pop();
		expect(() => renderEntries(pkg, copy, readme)).toThrow(/every package export/);
		const unknown = structuredClone(catalog); unknown.entries[0].heading = 'Absent heading';
		expect(() => renderEntries(pkg, unknown, readme)).toThrow(/Missing public entry/);
	});
	it('requires navigation to follow headings and ignores fenced headings', () => {
		const changed = readme + '\n## Extra extension\n';
		expect(validateReadme(changed, { ...manifest, readmeMaxLines: 10000 })).toContain('Stale topic index');
		expect(renderTopics(changed)).toContain('[Extra extension](#extra-extension)');
		expect(renderTopics(readme + '\n```md\n## Fake heading\n```')).not.toContain('Fake heading');
	});
	it('rejects missing, duplicate and reversed block markers', () => {
		for (const source of ['prefix <!-- x:start -->\n<!-- x:end -->', '<!-- x:start -->\n<!-- x:end --> trailing', '', '<!-- x:end -->\n<!-- x:start -->', '<!-- x:start -->\n<!-- x:start -->\n<!-- x:end -->']) expect(() => replaceBlock(source, 'x', '')).toThrow();
	});
	it('rejects native version facts outside every generated compatibility block', () => {
		const refs = governedNativeRefs([{ uwebsockets: 'https://github.com/uNetworking/uWebSockets.js/archive/refs/tags/v20.69.0.tar.gz' }]);
		expect(refs).toEqual(['v20.69.0']);
		for (const fact of ['Native v25.3.1', 'Use 20.69', 'Use 20.69.3', 'https://github.com/uNetworking/uWebSockets.js/archive/refs/tags/v40.1.2.tar.gz']) {
			expect(validateReadme(readme + '\n' + fact, { ...manifest, readmeMaxLines: 10000 }, refs)).toContain('Native compatibility facts must stay in generated compatibility blocks');
		}
		expect(validateReadme(readme, manifest, refs)).toEqual([]);
	});
	it('requires the standard scripts to execute the guard exactly once', () => {
		expect(validatePackageScripts(pkg)).toEqual([]);
		for (const key of ['check', 'verify:docs', 'check:documentation']) {
			for (const value of ['', pkg.scripts[key] + ' && ' + pkg.scripts[key]]) {
				expect(validatePackageScripts({ ...pkg, scripts: { ...pkg.scripts, [key]: value } }).length).toBeGreaterThan(0);
			}
		}
	});
	it('resolves collisions between suffix-bearing and repeated headings', () => {
		const source = '## Topic\n## Topic-1\n## Topic\n';
		expect([...anchorsOf(source)]).toEqual(['topic', 'topic-1', 'topic-2']);
		expect(renderTopics(source)).toContain('[Topic](#topic-2)');
	});
});
