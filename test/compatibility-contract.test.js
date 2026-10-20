import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { NODE_FLOOR_STATEMENTS, installedCompatibility, nodeFloor, parseCompatibility, renderBlocks, renderMigrationBlocks, replaceBlocks, replaceNodeFloor, validateCompatibility } from '../scripts/check-compatibility.js';

const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');
const migration = readFileSync(new URL('../MIGRATION.md', import.meta.url), 'utf8');
const { adapter, rows } = installedCompatibility();
const currentIndex = rows.findIndex(row => row.current === 'true');
const floorFiles = [...new Set(NODE_FLOOR_STATEMENTS.map(statement => statement.file))];
const readRepo = name => readFileSync(new URL('../' + name, import.meta.url), 'utf8');
const change = edit => { const next = structuredClone(rows); edit(next[currentIndex], next); return next; };

describe('installed adapter compatibility declaration', () => {
	it('pins the declared train, installation and peer floor before tests and publication', () => {
		expect(validateCompatibility(rows, pkg, adapter)).toEqual(rows[currentIndex]);
		expect(replaceBlocks(readme, renderBlocks(rows))).toBe(readme);
		expect(replaceBlocks(migration, renderMigrationBlocks(rows))).toBe(migration);
		expect(pkg.scripts.check.split(' && ')).toContain('node scripts/check-compatibility.js');
		expect(pkg.scripts.pretest).toBe('npm run check');
		expect(pkg.scripts.prepublishOnly).toBe('node scripts/check-compatibility.js --release && npm run check');
	});

	it('updates both migration prerequisites when the adapter floor changes', () => {
		const future = change(row => { row.adapter_version = '0.6.0-next.999'; });
		const expected = replaceBlocks(migration, renderMigrationBlocks(future));
		expect(expected).toContain('### The adapter peer floor is `svelte-adapter-uws >= 0.6.0-next.999`');
		expect(expected).toContain('**Action.** Bump `svelte-adapter-uws` to `0.6.0-next.999`');
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

	it('checks the Node engine in development and publication, and the own version at publication', () => {
		const future = { ...pkg, version: '0.0.0' };
		expect(() => validateCompatibility(rows, future, adapter)).not.toThrow();
		expect(() => validateCompatibility(rows, future, adapter, { release: true })).toThrow('Publication version');
		const wrongNode = { ...pkg, engines: { ...pkg.engines, node: '>=24.0.0' } };
		for (const release of [false, true]) expect(() => validateCompatibility(rows, wrongNode, adapter, { release })).toThrow('Node engine');
	});

	it('states the manifest Node floor in every prose and workflow line that names it', () => {
		const floor = nodeFloor(rows[currentIndex].node);
		expect(floor).toEqual({ major: '22', minimum: '22.0.0' });
		for (const name of floorFiles) expect(replaceNodeFloor(name, readRepo(name), floor)).toBe(readRepo(name));
		// A manifest floor bump rewrites every statement, including the
		// minimum-profile CI job and the scheduled crawler's runtime.
		const bumped = nodeFloor('>=24.0.0');
		const statements = [];
		for (const name of floorFiles) {
			const next = replaceNodeFloor(name, readRepo(name), bumped);
			for (const statement of NODE_FLOOR_STATEMENTS.filter(entry => entry.file === name)) {
				statements.push([...next.matchAll(statement.pattern)].map(match => match[1]));
			}
		}
		expect(statements).toEqual(NODE_FLOOR_STATEMENTS.map(statement => [statement.value === 'major' ? '24' : '24.0.0']));
		expect(replaceNodeFloor('.github/workflows/verify.yml', readRepo('.github/workflows/verify.yml'), bumped)).toContain("node-version: '24.0.0'");
	});

	it('refuses a missing or duplicated Node floor statement', () => {
		const floor = nodeFloor(rows[currentIndex].node);
		const contributing = readRepo('CONTRIBUTING.md');
		expect(() => replaceNodeFloor('CONTRIBUTING.md', contributing.replace('Install Node 22 or newer', 'Install a recent Node'), floor)).toThrow('found 0');
		expect(() => replaceNodeFloor('CONTRIBUTING.md', contributing + '\nInstall Node 22 or newer\n', floor)).toThrow('found 2');
		expect(() => nodeFloor('not a range')).toThrow('Invalid Node prerequisite');
	});

	it('rejects a mismatched publication version through the actual release CLI', () => {
		const fixture = mkdtempSync(join(tmpdir(), 'extensions-compatibility-'));
		try {
			mkdirSync(join(fixture, 'scripts'));
			copyFileSync(new URL('../scripts/check-compatibility.js', import.meta.url), join(fixture, 'scripts/check-compatibility.js'));
			writeFileSync(join(fixture, 'README.md'), readme);
			writeFileSync(join(fixture, 'MIGRATION.md'), migration);
			for (const name of floorFiles) {
				mkdirSync(join(fixture, dirname(name)), { recursive: true });
				writeFileSync(join(fixture, name), readRepo(name));
			}
			writeFileSync(join(fixture, 'package.json'), JSON.stringify({ ...pkg, version: '0.0.0' }));
			symlinkSync(fileURLToPath(new URL('../node_modules', import.meta.url)), join(fixture, 'node_modules'), 'junction');
			const run = args => spawnSync(process.execPath, [join(fixture, 'scripts/check-compatibility.js'), ...args], { encoding: 'utf8', timeout: 15000, windowsHide: true });
			const development = run([]), release = run(['--release']);
			expect(development.status, development.stderr).toBe(0);
			expect(release.status).toBe(1);
			expect(release.stderr).toContain('Publication version disagrees');
			writeFileSync(join(fixture, 'package.json'), JSON.stringify({ ...pkg, version: rows[currentIndex].extensions_version }));
			const matched = run(['--release']);
			expect(matched.status, matched.stderr).toBe(0);
			writeFileSync(join(fixture, 'MIGRATION.md'), migration.replace(renderMigrationBlocks(rows)['migration-action'], 'Incorrect minimum version'));
			const drift = run([]);
			expect(drift.status).toBe(1);
			expect(drift.stderr).toContain('MIGRATION.md compatibility blocks or Node floor statements are stale');
			expect(run(['--write']).status).toBe(0);
			expect(readFileSync(join(fixture, 'MIGRATION.md'), 'utf8')).toBe(migration);
			const contributing = readRepo('CONTRIBUTING.md');
			writeFileSync(join(fixture, 'CONTRIBUTING.md'), contributing.replace('Install Node 22 or newer', 'Install Node 20 or newer'));
			const nodeDrift = run([]);
			expect(nodeDrift.status).toBe(1);
			expect(nodeDrift.stderr).toContain('CONTRIBUTING.md compatibility blocks or Node floor statements are stale');
			// A missing marker anywhere refuses the whole write: the stale
			// statement in another file stays as it was.
			writeFileSync(join(fixture, 'README.md'), readme.replace('<!-- compatibility-floor:start -->', ''));
			const partial = run(['--write']);
			expect(partial.status).toBe(1);
			expect(partial.stderr).toContain('one ordered compatibility floor block');
			expect(readFileSync(join(fixture, 'CONTRIBUTING.md'), 'utf8')).toContain('Install Node 20 or newer');
			writeFileSync(join(fixture, 'README.md'), readme);
			expect(run(['--write']).status).toBe(0);
			expect(readFileSync(join(fixture, 'CONTRIBUTING.md'), 'utf8')).toBe(contributing);
		} finally { rmSync(fixture, { recursive: true, force: true }); }
	}, 30000);

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
