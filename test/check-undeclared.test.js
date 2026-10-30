import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const script = join(root, 'scripts', 'check-undeclared.js');

/** @type {string[]} */
const fixtures = [];

afterEach(() => {
	for (const dir of fixtures.splice(0)) rmSync(dir, { recursive: true, force: true });
});

/**
 * A package directory holding the given src files, and the gate's output when
 * run over it.
 *
 * @param {Record<string, string>} files paths relative to src
 */
function check(files) {
	const dir = mkdtempSync(join(tmpdir(), 'extensions-check-undeclared-'));
	fixtures.push(dir);
	for (const [path, text] of Object.entries(files)) {
		mkdirSync(dirname(join(dir, 'src', path)), { recursive: true });
		writeFileSync(join(dir, 'src', path), text);
	}
	const result = spawnSync(process.execPath, [script, '--root', dir], { encoding: 'utf8' });
	return { status: result.status, output: (result.stdout || '') + (result.stderr || '') };
}

const clock = 'export function now() {\n\treturn Date.now();\n}\n';

describe('check-undeclared', () => {
	it('passes the real package', () => {
		const result = spawnSync(process.execPath, [script], { encoding: 'utf8' });
		expect(result.stdout + result.stderr).toContain('0 undeclared references');
		expect(result.status).toBe(0);
	});

	it('passes a module whose every name is a binding, an import, or a global', () => {
		const { status, output } = check({
			'shared/clock.js': clock,
			'redis/groups.js': [
				"import { now } from '../shared/clock.js';",
				"import { createHash } from 'node:crypto';",
				'/** @param {SomeUndeclaredType} x */',
				'export function touch(x) {',
				'\tconst at = now();',
				"\tconst id = createHash('sha1').update(String(x)).digest('hex');",
				'\tsetTimeout(() => {}, 0).unref();',
				"\treturn { at, id, pid: process.pid, buf: Buffer.from('a'), url: new URL('http://x'), copy: structuredClone({ at }) };",
				'}',
				''
			].join('\n')
		});
		expect(output).toContain('0 undeclared references');
		expect(status).toBe(0);
	});

	it('fails on a call whose import was dropped, even inside a try/catch', () => {
		const { status, output } = check({
			'shared/clock.js': clock,
			'redis/groups.js': [
				'export function touch() {',
				'\ttry {',
				'\t\treturn now();',
				'\t} catch {',
				'\t\treturn 0;',
				'\t}',
				'}',
				''
			].join('\n')
		});
		expect(output).toContain("src/redis/groups.js:3:10: 'now' is neither declared, imported, nor a global");
		expect(status).toBe(1);
	});

	it('fails on an object shorthand with no binding behind it', () => {
		const { status, output } = check({ 'a.js': 'export const stamp = () => ({ now });\n' });
		expect(output).toContain("'now' is neither declared");
		expect(status).toBe(1);
	});

	it('is not silenced by a type-checker directive on the line above', () => {
		const { status, output } = check({ 'a.js': 'export function f() {\n\t// @ts-ignore unrelated\n\treturn now();\n}\n' });
		expect(output).toContain("src/a.js:3:9: 'now' is neither declared");
		expect(status).toBe(1);
	});
});
