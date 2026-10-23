import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { minVersion, valid, validRange } from 'semver';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const requiredHeaders = ['schema_version', 'owner', 'current', 'channel', 'provenance', 'adapter_version', 'adapter', 'realtime', 'extensions', 'node', 'uwebsockets', 'realtime_version', 'extensions_version'];

export function parseCompatibility(source) {
	const lines = source.trim().split(/\r?\n/);
	const headers = lines.shift().split(',');
	if (new Set(headers).size !== headers.length || requiredHeaders.some(name => !headers.includes(name))) throw new Error('Invalid compatibility manifest headers');
	return lines.map((line, index) => {
		const values = line.split(',');
		if (values.length !== headers.length) throw new Error('Invalid compatibility row ' + (index + 2));
		return Object.fromEntries(headers.map((name, column) => [name, values[column]]));
	});
}

export function validateCompatibility(rows, pkg, adapter, { release = false } = {}) {
	if (!rows.length || rows.some(row => row.schema_version !== '1' || row.owner !== 'svelte-adapter-uws')) throw new Error('Unsupported compatibility schema or owner');
	if (rows.some(row => !['true', 'false'].includes(row.current)) || rows.filter(row => row.current === 'true').length !== 1) throw new Error('Expected exactly one current compatibility row');
	if (new Set(rows.map(row => row.channel)).size !== rows.length || rows.some(row => !['legacy', 'stable', 'prerelease'].includes(row.channel))) throw new Error('Invalid compatibility channels');
	for (const row of rows) {
		for (const key of ['adapter', 'realtime', 'extensions']) if (!row[key] || !validRange(row[key])) throw new Error('Invalid package train: ' + key);
		for (const value of Object.values(row)) if (/[\u0000-\u001f\u007f`|<]/.test(value)) throw new Error('Unsafe compatibility cell');
	}
	const current = rows.find(row => row.current === 'true');
	for (const key of ['adapter_version', 'realtime_version', 'extensions_version']) if (valid(current[key]) !== current[key]) throw new Error('Current train requires exact ' + key);
	if (!validRange(current.node)) throw new Error('Invalid Node prerequisite');
	if (pkg.engines?.node !== current.node) throw new Error('Node engine disagrees with the current manifest');
	if (release && pkg.version !== current.extensions_version) throw new Error('Publication version disagrees with the current manifest');
	if (adapter.version !== current.adapter_version || pkg.devDependencies['svelte-adapter-uws'] !== current.adapter_version) throw new Error('Installed adapter and development pin disagree with the current manifest');
	if (pkg.peerDependencies['svelte-adapter-uws'] !== '>=' + current.adapter_version) throw new Error('Adapter peer floor disagrees with the current manifest');
	if (current.uwebsockets !== adapter.optionalDependencies['uWebSockets.js']) throw new Error('Native addon pin disagrees with the installed adapter');
	if (!/^https:\/\/github\.com\/uNetworking\/uWebSockets\.js\/archive\/refs\/tags\/v\d+\.\d+\.\d+\.tar\.gz$/.test(current.uwebsockets)) throw new Error('Invalid native addon install source');
	return current;
}

export function renderBlocks(rows) {
	const current = rows.find(row => row.current === 'true');
	const table = ['| `svelte-adapter-uws` | `svelte-realtime` | `svelte-adapter-uws-extensions` | Notes |', '|---|---|---|---|'];
	for (const row of rows) {
		const versions = [row.adapter_version || row.adapter, row.realtime_version || row.realtime, row.extensions_version || row.extensions];
		const note = row.current === 'true' ? 'Declared current train; Node ' + row.node + '. Redis 7+ / Valkey 7.2+ for sharded pub/sub and functions; Redis 7.4+ / Valkey 9.0+ for presence.' : row.channel + (row.node ? '; Node ' + row.node : '');
		table.push('| ' + versions.map(value => '`' + value + '`').join(' | ') + ' | ' + note + ' |');
	}
	const release = current.ecosystem_release || current.release_id;
	table.push('', 'Generated from the installed adapter\'s `docs/compatibility.v1.csv`' + (release ? '; ecosystem release `' + release + '`' : '') + '. Provenance `' + current.provenance + '` describes the declared train, not proof of coordinated publication. Confirm the exact versions are published before installing.');
	return {
		table: table.join('\n'),
		install: ['```bash', 'npm install svelte-adapter-uws@' + current.adapter_version + ' svelte-adapter-uws-extensions@' + current.extensions_version + ' ioredis', 'npm install ' + current.uwebsockets, 'npm exec -- svelte-adapter-uws-preflight', '```'].join('\n'),
		floor: 'Requires `svelte-adapter-uws >= ' + current.adapter_version + '` as a peer dependency.'
	};
}

export function replaceBlocks(source, blocks) {
	for (const [name, body] of Object.entries(blocks)) {
		const start = '<!-- compatibility-' + name + ':start -->', end = '<!-- compatibility-' + name + ':end -->';
		if (source.split(start).length !== 2 || source.split(end).length !== 2 || source.indexOf(end) < source.indexOf(start)) throw new Error('Expected one ordered compatibility ' + name + ' block');
		const first = source.indexOf(start), last = source.indexOf(end);
		source = source.slice(0, first + start.length) + '\n' + body + '\n' + source.slice(last);
	}
	return source;
}

export function renderMigrationBlocks(rows) {
	const current = rows.find(row => row.current === 'true');
	return {
		'migration-floor': '### The adapter peer floor is `svelte-adapter-uws >= ' + current.adapter_version + '`',
		'migration-action': '**Action.** Bump `svelte-adapter-uws` to `' + current.adapter_version + '` or later in the same\nupgrade. The ecosystem packages move together (see the version table in the\nREADME).'
	};
}

/**
 * Prose and workflow lines that state the supported Node floor. Each pattern
 * must match exactly once in its file, with the stated version in group 1, so
 * a reworded or deleted statement fails the gate instead of silently escaping
 * it. `major` is the floor's major (`22`), `minimum` the exact lowest
 * version the manifest range admits (`22.0.0`).
 */
export const NODE_FLOOR_STATEMENTS = [
	{ file: 'MIGRATION.md', pattern: /Use Node\.js (\d+) or later/g, value: 'major' },
	{ file: 'docs/getting-started.md', pattern: /Use Node (\d+) or newer/g, value: 'major' },
	{ file: 'docs/getting-started.md', pattern: /minimum CI route uses Node (\d+\.\d+\.\d+) /g, value: 'minimum' },
	{ file: 'CONTRIBUTING.md', pattern: /Install Node (\d+) or newer/g, value: 'major' },
	{ file: 'CONTRIBUTING.md', pattern: /smoke gate on Linux with Node (\d+);/g, value: 'major' },
	{ file: 'CONTRIBUTING.md', pattern: /supported runtime minimum\s+remains Node (\d+)\./g, value: 'major' },
	{ file: '.github/workflows/verify.yml', pattern: /node-version: '(\d+)'/g, value: 'major' },
	{ file: '.github/workflows/verify.yml', pattern: /node-version: '(\d+\.\d+\.\d+)'/g, value: 'minimum' },
	{ file: '.github/workflows/docs-links.yml', pattern: /node-version: '(\d+)'/g, value: 'major' },
	{ file: '.github/workflows/integration.yml', pattern: /node: \['(\d+)'/g, value: 'major' }
];

export function nodeFloor(range) {
	const minimum = validRange(range) ? minVersion(range) : null;
	if (!minimum) throw new Error('Invalid Node prerequisite');
	return { major: String(minimum.major), minimum: minimum.version };
}

/** Rewrite every Node floor statement in one file to the manifest floor. */
export function replaceNodeFloor(file, source, floor) {
	for (const statement of NODE_FLOOR_STATEMENTS) {
		if (statement.file !== file) continue;
		const count = [...source.matchAll(statement.pattern)].length;
		if (count !== 1) throw new Error('Expected one Node floor statement matching ' + statement.pattern + ' in ' + file + ', found ' + count);
		source = source.replace(statement.pattern, (match, stated) => match.replace(stated, floor[statement.value]));
	}
	return source;
}

export function installedCompatibility() {
	let directory = dirname(fileURLToPath(import.meta.resolve('svelte-adapter-uws/testing')));
	for (let depth = 0; depth < 12; depth++) {
		const path = join(directory, 'package.json');
		if (existsSync(path)) {
			const adapter = JSON.parse(readFileSync(path, 'utf8'));
			if (adapter.name === 'svelte-adapter-uws') return { adapter, rows: parseCompatibility(readFileSync(join(directory, 'docs/compatibility.v1.csv'), 'utf8')) };
		}
		const parent = dirname(directory);
		if (parent === directory) break;
		directory = parent;
	}
	throw new Error('Cannot locate the installed adapter compatibility manifest');
}

function main() {
	const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
	const { adapter, rows } = installedCompatibility();
	const current = validateCompatibility(rows, pkg, adapter, { release: process.argv.includes('--release') });
	const floor = nodeFloor(current.node);
	const blocks = new Map([['README.md', renderBlocks(rows)], ['MIGRATION.md', renderMigrationBlocks(rows)]]);
	const files = new Set([...blocks.keys(), ...NODE_FLOOR_STATEMENTS.map(statement => statement.file)]);
	// Render every file before writing any, so a missing or duplicated marker
	// or statement leaves the tree untouched instead of half rewritten.
	const stale = [];
	for (const name of files) {
		const path = join(root, ...name.split('/')), source = readFileSync(path, 'utf8');
		let expected = blocks.has(name) ? replaceBlocks(source, blocks.get(name)) : source;
		expected = replaceNodeFloor(name, expected, floor);
		if (source !== expected) stale.push({ name, path, expected });
	}
	if (process.argv.includes('--write')) for (const { path, expected } of stale) writeFileSync(path, expected);
	else if (stale.length) throw new Error(stale.map(({ name }) => name).join(', ') + ' compatibility blocks or Node floor statements are stale; run node scripts/check-compatibility.js --write');
	console.log('check-compatibility: declared tuple, Node engine and stated Node floor, native pin and adapter peer floor match the installed manifest' + (process.argv.includes('--release') ? '; publication version matches' : ''));
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
