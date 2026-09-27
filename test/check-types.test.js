import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const script = join(root, 'scripts', 'check-types.js');

/** @type {string[]} */
const fixtures = [];

afterEach(() => {
	for (const dir of fixtures.splice(0)) rmSync(dir, { recursive: true, force: true });
});

/**
 * A package directory holding the given files, with package.json built from
 * `exports`, and the gate's output when run over it.
 *
 * @param {Record<string, unknown>} exports
 * @param {Record<string, string>} files
 */
function check(exports, files) {
	const dir = mkdtempSync(join(tmpdir(), 'extensions-check-types-'));
	fixtures.push(dir);
	writeFileSync(join(dir, 'package.json'), JSON.stringify({ name: 'fixture-pkg', version: '0.0.0', files: ['src'], exports }));
	writeFileSync(join(dir, 'MIGRATION.md'), '# Migration\n');
	for (const [path, text] of Object.entries(files)) {
		mkdirSync(dirname(join(dir, path)), { recursive: true });
		writeFileSync(join(dir, path), text);
	}
	const result = spawnSync(process.execPath, [script, '--root', dir], { encoding: 'utf8' });
	return { status: result.status, output: (result.stdout || '') + (result.stderr || '') };
}

describe('check-types', () => {
	it('passes the real package', () => {
		const result = spawnSync(process.execPath, [script], { encoding: 'utf8' });
		expect(result.stdout + result.stderr).toContain('0 undeclared');
		expect(result.status).toBe(0);
	});

	it('passes a subpath whose every named export is declared', () => {
		const { status, output } = check(
			{ './widget': { types: './src/widget.d.ts', default: './src/widget.js' } },
			{
				'src/widget.js': 'export function make() {}\nexport const SIZE = 1;\n',
				'src/widget.d.ts': 'export function make(): void;\nexport const SIZE: number;\n'
			}
		);
		expect(output).toContain('0 undeclared');
		expect(status).toBe(0);
	});

	// A JavaScript subpath with no declaration target types as `any` for every
	// consumer, and there is nothing to pair it with.
	it('fails a JavaScript subpath with no types target, in either spelling', () => {
		for (const entry of ['./src/widget.js', { default: './src/widget.js' }]) {
			const { status, output } = check({ './widget': entry }, { 'src/widget.js': 'export function make() {}\n' });
			expect(output, JSON.stringify(entry)).toContain('./widget: ./src/widget.js has no types condition');
			expect(status).toBe(1);
		}
	});

	it('fails a named export its declaration does not declare', () => {
		const { status, output } = check(
			{ './widget': { types: './src/widget.d.ts', default: './src/widget.js' } },
			{
				'src/widget.js': 'export function make() {}\nexport function internalHelper() {}\n',
				// A commented-out declaration is absent, not present.
				'src/widget.d.ts': 'export function make(): void;\n// export function internalHelper(): void;\n'
			}
		);
		expect(output).toContain('exports `internalHelper` but ./src/widget.d.ts does not declare it');
		expect(output).not.toContain('`make`');
		expect(status).toBe(1);
	});

	// Re-exporting a whole module on both sides declares exactly what the
	// runtime exposes, whatever that module adds later.
	it('accepts a module re-exported whole on both sides', () => {
		const { status } = check(
			{ './widget': { types: './src/widget.d.ts', default: './src/widget.js' } },
			{
				'src/widget.js': "export * from './impl.js';\n",
				'src/widget.d.ts': "export * from './impl.js';\n",
				'src/impl.js': 'export function make() {}\n'
			}
		);
		expect(status).toBe(0);
	});
});
