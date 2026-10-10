import { describe, it, expect } from 'vitest';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readFileSync, readdirSync } from 'node:fs';
import { checkText, parseEditorConfig, propertiesFor } from '../scripts/check-formatting.js';
import { checkMap, expandLanes, inventory, workflowLanes } from '../scripts/check-contributor-map.js';
import { exerciseSharedCursors } from '../scripts/smoke.js';
import { mockRedisClient } from '../src/testing/mock-redis.js';

const properties = { indent_style: 'tab', end_of_line: 'lf', insert_final_newline: 'true', trim_trailing_whitespace: 'true', charset: 'utf-8' };
describe('format conformance', () => {
	it('applies EditorConfig section precedence instead of duplicating styles', () => {
		const config = parseEditorConfig('[*]\nindent_style = tab\n[*.json]\nindent_style = space\nindent_size = 2\n');
		expect(propertiesFor(config, 'test/a.json')).toEqual({ indent_style: 'space', indent_size: '2' });
		expect(checkText('test/a.json', '   {}\n', propertiesFor(config, 'test/a.json'), 'lf')).toEqual([expect.stringContaining('multiple of 2')]);
	});
	it('checks committed EOL while permitting CRLF working copies', () => {
		expect(checkText('a.js', '\tlet a;\r\n', properties, 'lf')).toEqual([]);
		expect(checkText('a.js', '\tlet a;\n', properties, 'crlf')).toEqual([expect.stringContaining('committed end-of-line')]);
	});
	it('detects missing final newline, trailing whitespace, indentation and BOM', () => {
		expect(checkText('a.txt', 'text', properties, 'none')).toEqual([expect.stringContaining('no final newline')]);
		expect(checkText('a.js', ' let a; \n', properties, 'lf')).toHaveLength(2);
		expect(checkText('a.txt', String.fromCharCode(0xfeff) + 'text\n', properties, 'lf')).toEqual([expect.stringContaining('byte-order mark')]);
	});
	it('preserves indentation and whitespace inside literal data', () => {
		const source = 'const sql = `\n  select 1  \n`;\n';
		expect(checkText('a.js', source, properties, 'lf')).toEqual([]);
	});
});

describe('verification inventory', () => {
	const scripts = { 'verify:pr': 'npm run verify:unit && npm run smoke', 'verify:unit': 'npm test', test: 'vitest run', smoke: 'node scripts/smoke.js', 'verify:cluster': 'npm run backend', backend: 'vitest run --config cluster.js' };
	const workflow = 'jobs:\n  suite:\n    steps:\n      - run: npm run verify:pr\n';
	it('expands aggregate lanes and distinguishes the local cluster lane', () => {
		expect(expandLanes('verify:pr', scripts)).toEqual(['verify:pr', 'verify:unit', 'test', 'smoke']);
		const result = inventory(scripts, workflow);
		expect(result).toContain('| `npm run verify:cluster` | `backend` | Local |');
		expect(result).toContain('| `npm run smoke` | `node scripts/smoke.js` | Hosted |');
	});
	it('fails stale prose, unknown scripts and cycles', () => {
		const source = '<!-- verification-inventory:start -->\n<!-- verification-inventory:end -->';
		expect(checkMap(source, scripts, workflow).matches).toBe(false);
		expect(() => checkMap(source + '\nnpm run missing', scripts, workflow)).toThrow('does not exist');
		expect(() => expandLanes('a', { a: 'npm run b', b: 'npm run a' })).toThrow('Cyclic');
		expect(() => workflowLanes('run: |\n  npm run smoke')).toThrow('single-line');
	});
	it('extracts explicit npm lanes beside closed multiline artifact commands', () => {
		expect(workflowLanes('jobs:\n  release:\n    steps:\n      - run: npm run verify:pr\n      - run: |\n          npm pack --json\n          Get-FileHash artifact.tgz\n      - run: npm publish artifact.tgz --tag candidate\n')).toEqual(['verify:pr']);
	});
	it('matches the checked-in package and hosted workflow', () => {
		const load = (path) => readFileSync(new URL('../' + path, import.meta.url), 'utf8');
		expect(checkMap(load('CONTRIBUTING.md'), JSON.parse(load('package.json')).scripts, readdirSync(new URL('../.github/workflows/', import.meta.url)).filter((name) => /\.ya?ml$/.test(name)).sort().map((name) => load('.github/workflows/' + name)).join('\n---\n')).matches).toBe(true);
	});
});

it('the smoke asserts shared cursor delivery and removal through the public tracker', async () => {
	const client = mockRedisClient('smoke-test:');
	await exerciseSharedCursors(client, client);
});

it('a missing Docker executable fails the default smoke without claiming real coverage', () => {
	const result = spawnSync(process.execPath, [fileURLToPath(new URL('../scripts/smoke.js', import.meta.url))], {
		encoding: 'utf8', timeout: 10_000, env: { ...process.env, PATH: '', Path: '' }
	});
	expect(result.status).toBe(1);
	expect(result.stdout).toContain('PASS mock Redis');
	expect(result.stdout).not.toContain('PASS real Redis');
	expect(result.stderr).toContain('docker info failed');
});
