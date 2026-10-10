import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { installedCompatibility, parseCompatibility, renderBlocks, replaceBlocks, validateCompatibility } from '../scripts/check-compatibility.js';

const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');
const { adapter, rows } = installedCompatibility();
const currentIndex = rows.findIndex(row => row.current === 'true');
const change = edit => { const next = structuredClone(rows); edit(next[currentIndex], next); return next; };

describe('installed adapter compatibility declaration', () => {
	it('pins the declared train, installation and peer floor before tests and publication', () => {
		expect(validateCompatibility(rows, pkg, adapter)).toEqual(rows[currentIndex]);
		expect(replaceBlocks(readme, renderBlocks(rows))).toBe(readme);
		expect(pkg.scripts.check.split(' && ')).toContain('node scripts/check-compatibility.js');
		expect(pkg.scripts.pretest).toBe('npm run check');
		expect(pkg.scripts.prepublishOnly).toBe('npm run check');
	});

	it.each(['table', 'install', 'floor'])('detects hand-edited %s content and refuses duplicate or missing ownership markers', name => {
		const marker = '<!-- compatibility-' + name + ':start -->';
		const drift = readme.replace(marker, marker + '\nIncorrect hand-maintained version');
		expect(replaceBlocks(drift, renderBlocks(rows))).not.toBe(drift);
		expect(() => replaceBlocks(readme + marker, renderBlocks(rows))).toThrow('one ordered');
		expect(() => replaceBlocks(readme.replace(marker, ''), renderBlocks(rows))).toThrow('one ordered');
	});

	it('refuses mismatched peer floor, development pin, installed version and native build', () => {
		for (const mutate of [
			value => { value.peerDependencies['svelte-adapter-uws'] = '>=0.6.0-next.1'; },
			value => { value.devDependencies['svelte-adapter-uws'] = '^0.6.0-next.95'; }
		]) {
			const next = structuredClone(pkg); mutate(next);
			expect(() => validateCompatibility(rows, next, adapter)).toThrow(/disagree/);
		}
		expect(() => validateCompatibility(rows, pkg, { ...adapter, version: '0.0.0' })).toThrow(/disagree/);
		expect(() => validateCompatibility(rows, pkg, { ...adapter, optionalDependencies: { 'uWebSockets.js': 'different' } })).toThrow(/disagree/);
	});

	it('refuses unowned, ambiguous or non-exact current declarations', () => {
		for (const mutate of [
			row => { row.schema_version = '2'; }, row => { row.owner = 'other'; },
			row => { row.current = 'false'; }, (_row, all) => { all[0].current = 'true'; },
			row => { row.extensions_version = '^0.6.0'; }, row => { row.realtime_version = ''; },
			row => { row.provenance = '<script>'; }
		]) expect(() => validateCompatibility(change(mutate), pkg, adapter)).toThrow();
	});

	it('derives future tuple and ecosystem release metadata without README edits', () => {
		const future = change(row => { row.extensions_version = '0.6.0-next.999'; row.realtime_version = '0.6.0-next.888'; row.ecosystem_release = 'train-0.6-candidate'; });
		const blocks = renderBlocks(future);
		expect(blocks.table).toContain('`0.6.0-next.888` | `0.6.0-next.999`');
		expect(blocks.table).toContain('`train-0.6-candidate`');
		expect(blocks.install).toContain('svelte-adapter-uws-extensions@0.6.0-next.999');
	});

	it('rejects duplicate headers and truncated CSV rows rather than guessing fields', () => {
		expect(() => parseCompatibility('owner,owner\na,b')).toThrow('headers');
		const headers = Object.keys(rows[0]);
		expect(() => parseCompatibility(headers.join(',') + '\n1,owner')).toThrow('row');
		const text = [headers.join(','), ...rows.map(row => headers.map(name => row[name]).join(','))].join('\n');
		expect(parseCompatibility(text)).toEqual(rows);
	});
});
