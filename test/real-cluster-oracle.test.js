import { spawnSync } from 'node:child_process';
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import Module from 'node:module';
import { tmpdir } from 'node:os';
import { delimiter, join, resolve } from 'node:path';
import { gzipSync } from 'node:zlib';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
	assertResolveHooks,
	describeSource,
	installedProblems,
	interrupt,
	pathSpellings,
	portablePath,
	scrubPaths,
	settleTeardown,
	shortPathName,
	stopCluster
} from '../scripts/real-cluster-oracle.js';

const SCENARIO = 'scripts/fixtures/real-cluster-oracle.mjs';
const ORACLE = 'scripts/real-cluster-oracle.js';
const dirs = [];
// The scenario confines module resolution with module.registerHooks, which
// Node has from 22.15; the runner refuses an older Node up front.
const hasHooks = typeof Module.registerHooks === 'function';

function run(script, args = [], env = {}, cwd = undefined) {
	const r = spawnSync(process.execPath, [script, ...args], {
		cwd,
		encoding: 'utf8',
		timeout: 20000,
		env: { ...process.env, ORACLE_CLUSTER_NODES: '', ORACLE_CLUSTER_NAT_MAP: '', ...env }
	});
	return { code: r.status, stdout: r.stdout, stderr: r.stderr };
}

function workDir() {
	const dir = mkdtempSync(join(tmpdir(), 'real-cluster-oracle-'));
	dirs.push(dir);
	return dir;
}

afterEach(() => {
	for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

// A minimal stand-in for each package the scenario loads. The cluster class
// throws, so setup stops right after the packages are identified.
const STUBS = {
	ioredis: { main: 'index.js', files: { 'index.js': "export default { Cluster: class { constructor() { throw new Error('stub cluster'); } } };" } },
	ws: { main: 'index.js', files: { 'index.js': 'export class WebSocket {}' } },
	'uWebSockets.js': { main: 'index.js', files: { 'index.js': 'export default {};' } },
	'svelte-adapter-uws': { exports: { './testing': './testing.js' }, files: { 'testing.js': 'export function createTestServer() {}' } },
	'svelte-adapter-uws-extensions': { exports: { './redis/pubsub': './pubsub.js' }, files: { 'pubsub.js': 'export function createPubSubBus() {}' } },
	'svelte-realtime': { exports: { './server': './server.js' }, files: { 'server.js': 'export function createMessage() {}\nexport function setBus() {}' } }
};

function stub(nodeModules, name) {
	const dir = join(nodeModules, name);
	mkdirSync(dir, { recursive: true });
	const { files, ...manifest } = STUBS[name];
	writeFileSync(join(dir, 'package.json'), JSON.stringify({ name, version: '0.0.0-stub', type: 'module', ...manifest }));
	for (const [file, body] of Object.entries(files)) writeFileSync(join(dir, file), body);
}

function scenarioIn(consumer) {
	copyFileSync(SCENARIO, join(consumer, 'real-cluster-oracle.mjs'));
	const r = run(join(consumer, 'real-cluster-oracle.mjs'), [], { ORACLE_CLUSTER_NODES: '[{"host":"127.0.0.1","port":7000}]' }, consumer);
	return { ...r, report: JSON.parse(r.stdout.trim()) };
}

describe('real-cluster oracle scenario', () => {
	it.each(['null', '[]', '[{"host":"127.0.0.1","port":0}]', '[{"port":7000}]'])(
		'refuses seed configuration %s as a failed setup, never as a pass',
		(nodes) => {
			const r = run(SCENARIO, [], { ORACLE_CLUSTER_NODES: nodes });
			expect(r.code).toBe(1);
			const report = JSON.parse(r.stdout.trim());
			expect(report).toMatchObject({
				schemaVersion: 1,
				status: 'failed',
				seed: null,
				faultMode: 'none',
				counts: { planned: 5, executed: 2, passed: 1, failed: 1, notRun: 3 }
			});
			expect(report.cases.map((c) => [c.name, c.status])).toEqual([
				['setup', 'failed'],
				['cluster-topology', 'not_run'],
				['single-fanout', 'not_run'],
				['batch-fanout', 'not_run'],
				['cleanup', 'passed']
			]);
			expect(report.provenance.scenarioSha256).toMatch(/^[0-9a-f]{64}$/);
		}
	);

	it.skipIf(!hasHooks)('records each package root inside the consumer node_modules', () => {
		const consumer = join(workDir(), 'consumer');
		for (const name of Object.keys(STUBS)) stub(join(consumer, 'node_modules'), name);
		const { code, report } = scenarioIn(consumer);
		expect(code).toBe(1);
		expect(report.cases[0]).toMatchObject({ name: 'setup', status: 'failed', error: { message: 'stub cluster' } });
		for (const name of Object.keys(STUBS)) {
			expect(report.provenance.packages[name]).toMatchObject({ version: '0.0.0-stub', root: `node_modules/${name}` });
		}
	});

	it.skipIf(!hasHooks)('fails setup when a package resolves from a directory above the consumer', () => {
		const outer = workDir();
		const consumer = join(outer, 'consumer');
		for (const name of Object.keys(STUBS)) {
			stub(name === 'ws' ? join(outer, 'node_modules') : join(consumer, 'node_modules'), name);
		}
		const { code, report } = scenarioIn(consumer);
		expect(code).toBe(1);
		expect(report.cases[0].status).toBe('failed');
		expect(report.cases[0].error.message).toContain('ws resolved outside the consumer node_modules');
		expect(report.counts.notRun).toBe(3);
	});

	it.skipIf(!hasHooks)('refuses a transitive import that would climb out of the consumer', () => {
		const outer = workDir();
		const consumer = join(outer, 'consumer');
		for (const name of Object.keys(STUBS)) stub(join(consumer, 'node_modules'), name);
		writeFileSync(join(consumer, 'node_modules', 'svelte-realtime', 'server.js'), "import 'only-above';\nexport function createMessage() {}\nexport function setBus() {}");
		const above = join(outer, 'node_modules', 'only-above');
		mkdirSync(above, { recursive: true });
		writeFileSync(join(above, 'package.json'), JSON.stringify({ name: 'only-above', version: '1.0.0', type: 'module', main: 'index.js' }));
		writeFileSync(join(above, 'index.js'), 'export {};');
		const { code, report } = scenarioIn(consumer);
		expect(code).toBe(1);
		expect(report.cases[0].status).toBe('failed');
		expect(report.cases[0].error.message).toContain('only-above resolved outside the consumer node_modules');
		expect(report.provenance.resolution).toMatchObject({ confined: true, refused: [{ specifier: 'only-above' }] });
	});

	it.skipIf(!hasHooks)('refuses a worker thread or child process a package starts, and fails the run even when it catches that', () => {
		const consumer = join(workDir(), 'consumer');
		for (const name of Object.keys(STUBS)) stub(join(consumer, 'node_modules'), name);
		writeFileSync(join(consumer, 'node_modules', 'svelte-realtime', 'server.js'), [
			"import { Worker } from 'node:worker_threads';",
			"import { ChildProcess, spawnSync } from 'node:child_process';",
			"try { new Worker('setTimeout(() => {}, 1)', { eval: true }); } catch {}",
			"try { spawnSync(process.execPath, ['-e', '0']); } catch {}",
			"try { new ChildProcess().spawn({ file: process.execPath, args: [process.execPath, '-e', '0'], stdio: 'ignore' }); } catch {}",
			'export function createMessage() {}',
			'export function setBus() {}'
		].join('\n'));
		const { code, report } = scenarioIn(consumer);
		expect(code).toBe(1);
		expect(report.provenance.resolution.blocked).toEqual(['node:worker_threads.Worker', 'node:child_process.spawnSync', 'node:child_process.ChildProcess.spawn']);
		expect(report.cases.find((c) => c.name === 'cleanup')).toMatchObject({ status: 'failed', error: { message: expect.stringContaining('worker thread or child process') } });
	});
});

describe('real-cluster oracle inputs', () => {
	it('takes a gzip tarball by digest and names it relative to the work dir', () => {
		const dir = workDir();
		const path = join(dir, 'tarballs', 'pkg-1.0.0.tgz');
		mkdirSync(join(dir, 'tarballs'));
		writeFileSync(path, gzipSync(Buffer.from('package')));
		const source = describeSource('svelte-realtime', path, dir);
		expect(source).toMatchObject({ kind: 'tarball', tarball: 'tarballs/pkg-1.0.0.tgz', path: resolve(path) });
		expect(source.sha256).toMatch(/^[0-9a-f]{64}$/);
		expect(source.integrity).toMatch(/^sha512-/);
		expect(describeSource('svelte-realtime', path, workDir()).tarball).toBe('pkg-1.0.0.tgz');
	});

	it('refuses a directory and a file that is not gzip', () => {
		const dir = workDir();
		expect(() => describeSource('svelte-realtime', dir, dir)).toThrow(expect.objectContaining({ step: 'inputs', message: expect.stringContaining('is not a file') }));
		const text = join(dir, 'notes.tgz');
		writeFileSync(text, 'not a tarball');
		expect(() => describeSource('svelte-realtime', text, dir)).toThrow(expect.objectContaining({ step: 'inputs', message: expect.stringContaining('is not a gzip tarball') }));
		expect(() => describeSource('svelte-realtime', join(dir, 'gone.tgz'), dir)).toThrow(expect.objectContaining({ step: 'inputs', message: 'svelte-realtime tarball not found: gone.tgz' }));
	});

	it('takes anything else as a registry spec, with or without the package name', () => {
		expect(describeSource('svelte-adapter-uws-extensions', 'svelte-adapter-uws-extensions@0.6.0-next.63', workDir())).toEqual({ name: 'svelte-adapter-uws-extensions', kind: 'registry', spec: '0.6.0-next.63' });
		expect(describeSource('svelte-realtime', 'next', workDir())).toEqual({ name: 'svelte-realtime', kind: 'registry', spec: 'next' });
		expect(() => describeSource('svelte-realtime', 'svelte-realtime@', workDir())).toThrow(expect.objectContaining({ step: 'inputs' }));
		expect(describeSource('svelte-realtime', '>=0.6.0-next.90', workDir()).spec).toBe('>=0.6.0-next.90');
	});

	it.each(['file:../dir', 'link:../dir', 'github:a/b', 'git+https://example.com/x.git', 'https://example.com/x.tgz', 'npm:other@1.0.0', '../no-such-dir', 'a/b'])(
		'refuses the spec %s before anything is packed or installed',
		(value) => {
			expect(() => describeSource('svelte-realtime', value, workDir())).toThrow(expect.objectContaining({ step: 'inputs', message: expect.stringContaining('is not supported') }));
		}
	);

	it.each(['--adapter', 'latest;rm', 'next&calc', 'other@1.0.0'])('refuses %s as not a version, tag or range', (value) => {
		expect(() => describeSource('svelte-realtime', value, workDir())).toThrow(expect.objectContaining({ step: 'inputs', message: expect.stringContaining('is not a version, tag or range') }));
	});
});

describe('real-cluster oracle Node floor', () => {
	it('refuses a Node without module.registerHooks at the inputs step', () => {
		expect(() => assertResolveHooks({}, 'v22.14.0')).toThrow(expect.objectContaining({ step: 'inputs', message: expect.stringContaining('Node 22.15 or later') }));
		expect(() => assertResolveHooks({ registerHooks() {} })).not.toThrow();
	});

	it.runIf(!hasHooks)('exits 2 on this Node before packing anything', () => {
		const dir = workDir();
		const r = run(ORACLE, ['--realtime', 'next', '--work-dir', dir]);
		expect(r.code).toBe(2);
		const result = JSON.parse(readFileSync(join(dir, 'result.json'), 'utf8'));
		expect(result).toMatchObject({ status: 'error', packages: {}, error: { step: 'inputs', message: expect.stringContaining('Node 22.15 or later') } });
	});
});

describe('real-cluster oracle installed tree', () => {
	const manifests = { 'svelte-realtime': { version: '1.0.0' }, 'svelte-adapter-uws': { version: '2.0.0' } };
	const manifest = (...parts) => (parts.length === 1 ? manifests[parts[0]] : null);

	it('matches a tarball input against the integrity the consumer lock records', () => {
		const sources = { 'svelte-realtime': { name: 'svelte-realtime', kind: 'tarball', tarball: 'rt.tgz', integrity: 'sha512-a' } };
		const lock = (integrity) => ({ packages: { 'node_modules/svelte-realtime': integrity ? { integrity } : {} } });
		expect(installedProblems(sources, { lock: lock('sha512-a'), manifest })).toEqual([]);
		expect(sources['svelte-realtime'].version).toBe('1.0.0');
		expect(installedProblems(sources, { lock: lock('sha512-b'), manifest })).toEqual([expect.stringContaining('but the tarball digests to sha512-a')]);
		expect(installedProblems(sources, { lock: lock(null), manifest })).toEqual([expect.stringContaining('has no integrity in the consumer lock')]);
	});

	it('records a registry input from the lock and refuses one without integrity or with another', () => {
		const sources = { 'svelte-adapter-uws': { name: 'svelte-adapter-uws', kind: 'registry', spec: '2.0.0', expectedIntegrity: 'sha512-x' } };
		const entry = { resolved: 'https://registry.npmjs.org/a.tgz', integrity: 'sha512-x' };
		expect(installedProblems(sources, { lock: { packages: { 'node_modules/svelte-adapter-uws': entry } }, manifest })).toEqual([]);
		expect(sources['svelte-adapter-uws']).toMatchObject({ resolved: entry.resolved, integrity: 'sha512-x', version: '2.0.0' });
		expect(installedProblems(sources, { lock: { packages: {} }, manifest })).toEqual([
			'svelte-adapter-uws has no registry integrity in the consumer lock',
			'svelte-adapter-uws installed with integrity null, expected sha512-x'
		]);
	});

	it('refuses a missing package and a nested copy of another named package', () => {
		const sources = {
			'svelte-realtime': { name: 'svelte-realtime', kind: 'tarball', tarball: 'rt.tgz', integrity: 'sha512-a' },
			'svelte-adapter-uws-extensions': { name: 'svelte-adapter-uws-extensions', kind: 'tarball', tarball: 'ext.tgz', integrity: 'sha512-e' }
		};
		const nested = (...parts) => (parts.join('/') === 'svelte-realtime/node_modules/svelte-adapter-uws-extensions' ? { version: '9.9.9' } : manifest(...parts));
		const lock = { packages: { 'node_modules/svelte-realtime': { integrity: 'sha512-a' } } };
		expect(installedProblems(sources, { lock, manifest: nested })).toEqual([
			'svelte-realtime carries a nested svelte-adapter-uws-extensions@9.9.9, so the named svelte-adapter-uws-extensions is not the one in use',
			'svelte-adapter-uws-extensions is not installed'
		]);
	});
});

describe('real-cluster oracle result paths', () => {
	it('names paths relative to the work dir, or by file name outside it', () => {
		const dir = workDir();
		expect(portablePath(join(dir, 'consumer', 'x.json'), dir)).toBe('consumer/x.json');
		expect(portablePath(dir, dir)).toBe('.');
		expect(portablePath(join(tmpdir(), 'elsewhere', 'a.tgz'), dir)).toBe('a.tgz');
	});

	it('removes the work dir, checkout and home directory from every string', () => {
		const result = scrubPaths(
			{ a: ['/home/me/repo/x', '/home/me/repo/work/y'], b: { c: 'see /home/me/notes' }, n: 3 },
			{ workDir: '/home/me/repo/work', repo: '/home/me/repo', home: '/home/me' }
		);
		expect(result).toEqual({ a: ['<repo>/x', '<work-dir>/y'], b: { c: 'see ~/notes' }, n: 3 });
	});

	// Windows paths are built from parts so the test reads the same on every
	// platform: B is one backslash.
	const B = String.fromCharCode(92);
	const win = (...parts) => parts.join(B);
	const windowsRoots = { workDir: win('C:', 'Users', 'me', 'repo', 'work'), repo: win('C:', 'Users', 'me', 'repo'), home: win('C:', 'Users', 'me') };

	it('lists the native, forward-slash, JSON-escaped and MSYS spellings of a Windows path', () => {
		expect(pathSpellings(win('C:', 'Users', 'me') + B)).toEqual({
			caseInsensitive: true,
			spellings: [win('C:', 'Users', 'me'), 'C:/Users/me', ['C:', 'Users', 'me'].join(B + B), '/c/Users/me']
		});
		expect(pathSpellings('/home/me/')).toEqual({ caseInsensitive: false, spellings: ['/home/me'] });
		for (const root of ['/', '', null, 'C:' + B, 'C:/', 'c:']) expect(pathSpellings(root).spellings).toEqual([]);
	});

	it('scrubs every spelling of a Windows path, in any letter case', () => {
		const text = {
			native: 'ENOENT ' + win('C:', 'Users', 'me', 'repo', 'work', 'consumer'),
			escaped: JSON.stringify({ path: win('C:', 'Users', 'me', 'repo', 'x') }),
			forward: 'at file:///C:/Users/me/repo/scripts/a.js:1:2',
			msys: 'error at /c/Users/me/notes',
			lower: win('c:', 'users', 'ME', 'repo', 'y')
		};
		expect(scrubPaths(text, windowsRoots)).toEqual({
			native: 'ENOENT <work-dir>' + B + 'consumer',
			escaped: JSON.stringify({ path: '<repo>' + B + 'x' }),
			forward: 'at file:///<repo>/scripts/a.js:1:2',
			msys: 'error at ~/notes',
			lower: '<repo>' + B + 'y'
		});
	});

	it('scrubs a UNC directory in every spelling and letter case, but never a share root', () => {
		const unc = B + B + win('srv', 'share', 'w');
		expect(pathSpellings(unc).spellings).toEqual([unc, '//srv/share/w', B + B + B + B + ['srv', 'share', 'w'].join(B + B)]);
		expect(pathSpellings(B + B + win('srv', 'share')).spellings).toEqual([]);
		const roots = { workDir: unc, repo: '/r', home: '/h' };
		expect(scrubPaths({
			native: unc + B + 'consumer',
			upper: B + B + win('SRV', 'share', 'w', 'consumer'),
			forward: '//srv/share/w/consumer',
			escaped: JSON.stringify(unc + B + 'consumer')
		}, roots)).toEqual({
			native: '<work-dir>' + B + 'consumer',
			upper: '<work-dir>' + B + 'consumer',
			forward: '<work-dir>/consumer',
			escaped: JSON.stringify('<work-dir>' + B + 'consumer')
		});
	});

	it('labels a JSON-escaped directory followed by a full stop and a quote correctly', () => {
		const text = JSON.stringify('at ' + win('C:', 'Users', 'me', 'repo') + '.');
		expect(scrubPaths(text, windowsRoots)).toBe(JSON.stringify('at <repo>.'));
	});

	it('scrubs short-name aliases in every spelling', () => {
		const short = win('C:', 'Users', 'LONGNA~1');
		const roots = { workDir: win('C:', 'w'), repo: win('C:', 'r'), home: win('C:', 'Users', 'longname'), aliases: [[short, '~']] };
		expect(scrubPaths({ a: short + B + 'AppData', b: JSON.stringify(short + B + 'x'), c: 'C:/Users/longna~1/y' }, roots)).toEqual({
			a: '~' + B + 'AppData',
			b: JSON.stringify('~' + B + 'x'),
			c: '~/y'
		});
	});

	it('derives a short name only on Windows and only when it differs', () => {
		const dir = workDir();
		const long = join(dir, 'a directory name longer than eight');
		mkdirSync(long);
		const short = shortPathName(long);
		if (process.platform !== 'win32') expect(short).toBeNull();
		else if (short !== null) {
			expect(short.toLowerCase()).not.toBe(long.toLowerCase());
			expect(existsSync(short)).toBe(true);
		}
		expect(shortPathName(dir.replace(/[\\/]+$/, '') + '"x')).toBeNull();
	});

	it('replaces whole path prefixes only and never a filesystem root', () => {
		const roots = { workDir: '/w', repo: '/app', home: '/' };
		expect(scrubPaths({
			url: 'https://example.com/app/x',
			sibling: '/apple/x and /app-old/y and /x/app/z',
			own: '/app/x, then (/app) and /app.',
			dotted: '/app.txt',
			slashes: 'a/b/c'
		}, roots)).toEqual({
			url: 'https://example.com/app/x',
			sibling: '/apple/x and /app-old/y and /x/app/z',
			own: '<repo>/x, then (<repo>) and <repo>.',
			dotted: '/app.txt',
			slashes: 'a/b/c'
		});
	});

	it('leaves digests, integrities and registry URLs untouched', () => {
		const value = { integrity: 'sha512-ab/cd+ef==', sha256: 'abc', resolved: 'https://registry.npmjs.org/app/-/app-1.0.0.tgz', note: '/app/x' };
		expect(scrubPaths(value, { workDir: '/w', repo: '/app', home: '/' })).toEqual({ ...value, note: '<repo>/x' });
		expect(scrubPaths({ integrity: 'sha512-ab/cd+ef==' }, { workDir: '/w', repo: '/r', home: '/' })).toEqual({ integrity: 'sha512-ab/cd+ef==' });
	});
});

describe('real-cluster oracle teardown', () => {
	const clean = () => ({ containers: 0, networks: 0 });

	it('reports removed when the setup teardown leaves nothing behind', async () => {
		const down = vi.fn(() => ({ code: 0 }));
		const outcome = await stopCluster({ teardown: async () => {}, down, leftovers: clean });
		expect(outcome).toEqual({ status: 'removed' });
		expect(down).not.toHaveBeenCalled();
	});

	it('records a failing teardown and falls back to compose down', async () => {
		const down = vi.fn(() => ({ code: 0 }));
		const outcome = await stopCluster({ teardown: async () => { throw new Error('down exited 1'); }, down, leftovers: clean });
		expect(outcome).toEqual({ status: 'removed', teardownError: 'down exited 1', fallback: 'compose down' });
		expect(down).toHaveBeenCalledOnce();
	});

	it('runs compose down when setup never handed over a teardown', async () => {
		const down = vi.fn(() => ({ code: 0 }));
		expect((await stopCluster({ teardown: null, down, leftovers: clean })).status).toBe('removed');
		expect(down).toHaveBeenCalledOnce();
	});

	it('reports a stack that survives the fallback as leaked, and an unanswered check as unverified', async () => {
		const left = { containers: 2, networks: 1 };
		const leaked = await stopCluster({ teardown: async () => {}, down: () => ({ code: 1, stderr: 'daemon gone' }), leftovers: () => left });
		expect(leaked).toMatchObject({ status: 'leaked', left, fallbackError: 'daemon gone' });
		const unknown = await stopCluster({ teardown: async () => {}, down: () => ({ code: 0 }), leftovers: () => null });
		expect(unknown.status).toBe('unverified');
	});

	it('turns a passed verdict into an error when the stack may still run', () => {
		const passed = { status: 'passed', services: {}, error: null };
		expect(settleTeardown(passed, { status: 'leaked', left: { containers: 1, networks: 0 } })).toBe(2);
		expect(passed).toMatchObject({ status: 'error', error: { step: 'teardown' }, services: { teardown: { status: 'leaked' } } });
		const kept = { status: 'passed', services: {}, error: null };
		expect(settleTeardown(kept, { status: 'removed' })).toBeNull();
		expect(kept.status).toBe('passed');
	});
});

describe('real-cluster oracle interrupt', () => {
	it('removes the stack, writes the result and exits 130 once', async () => {
		const result = { status: 'error', services: {}, error: null };
		const stop = vi.fn(async () => ({ status: 'removed' }));
		const lifecycle = { stop, stopping: null, interrupted: false };
		const write = vi.fn();
		const exit = vi.fn();
		await interrupt('SIGINT', { result, lifecycle, write, exit });
		await interrupt('SIGINT', { result, lifecycle, write, exit });
		expect(stop).toHaveBeenCalledOnce();
		expect(write).toHaveBeenCalledOnce();
		expect(exit).toHaveBeenCalledWith(130);
		expect(result).toMatchObject({ status: 'error', error: { step: 'interrupted', message: 'received SIGINT' }, services: { teardown: { status: 'removed' } } });
	});

	it('waits for a teardown already under way instead of starting another', async () => {
		let finish;
		const stopping = new Promise((done) => { finish = done; });
		const result = { status: 'passed', services: {}, error: null };
		const lifecycle = { stop: null, stopping, interrupted: false };
		const write = vi.fn();
		const pending = interrupt('SIGTERM', { result, lifecycle, write, exit: () => {} });
		await Promise.resolve();
		expect(write).not.toHaveBeenCalled();
		finish({ status: 'leaked', left: { containers: 1, networks: 1 } });
		await pending;
		expect(write).toHaveBeenCalledOnce();
		expect(result).toMatchObject({ status: 'error', error: { step: 'interrupted', teardown: expect.stringContaining('still present') } });
	});
});

describe('real-cluster oracle runner', () => {
	it('rejects an unknown argument with exit code 2', () => {
		const r = run(ORACLE, ['--no-such-flag']);
		expect(r.code).toBe(2);
		expect(r.stderr).toContain('unknown argument: --no-such-flag');
	});

	it('refuses a flag where a value belongs', () => {
		const r = run(ORACLE, ['--realtime', '--adapter', '0.6.0']);
		expect(r.code).toBe(2);
		expect(r.stderr).toContain('--realtime needs a value');
	});

	it.skipIf(!hasHooks).each([
		['130', 130],
		['STATUS_CONTROL_C_EXIT', -1073741510]
	])('records a child that died of an interrupt (%s) as interrupted and exits 130', (_label, windowsStatus) => {
		const dir = workDir();
		const bin = join(dir, 'bin');
		mkdirSync(bin);
		// A stand-in npm that dies the way a Ctrl-C ends it: by SIGINT on POSIX,
		// with a Ctrl-C exit status on Windows, which has no signals.
		if (process.platform === 'win32') writeFileSync(join(bin, 'npm.cmd'), `@exit /b ${windowsStatus}\r\n`);
		else {
			writeFileSync(join(bin, 'npm'), '#!/bin/sh\nkill -INT $$\n');
			chmodSync(join(bin, 'npm'), 0o755);
		}
		const tarball = join(dir, 'ext.tgz');
		writeFileSync(tarball, gzipSync(Buffer.from('package')));
		const pathKey = Object.keys(process.env).find((key) => key.toUpperCase() === 'PATH') || 'PATH';
		const r = run(ORACLE, ['--extensions', tarball, '--realtime', 'next', '--work-dir', join(dir, 'work')], { [pathKey]: bin + delimiter + process.env[pathKey] });
		expect(r.code).toBe(130);
		const result = JSON.parse(readFileSync(join(dir, 'work', 'result.json'), 'utf8'));
		expect(result).toMatchObject({ status: 'error', scenario: null, error: { step: 'interrupted', message: 'received SIGINT during install' } });
	});

	it('prints its usage with --help', () => {
		const r = run(ORACLE, ['--help']);
		expect(r.code).toBe(0);
		expect(r.stdout).toContain('--extensions <tgz|spec>');
		expect(r.stdout).toContain('ORACLE_CLUSTER_NODES');
	});

	it('writes an error result naming the step and no machine path when an input is missing', () => {
		const dir = workDir();
		const r = run(ORACLE, ['--realtime', join(dir, 'missing.tgz'), '--work-dir', dir]);
		expect(r.code).toBe(2);
		const text = readFileSync(join(dir, 'result.json'), 'utf8');
		const result = JSON.parse(text);
		expect(result).toMatchObject({ schemaVersion: 1, status: 'error', scenario: null, error: { step: 'inputs', message: 'svelte-realtime tarball not found: missing.tgz' } });
		expect(result.command).toEqual(['node', 'scripts/real-cluster-oracle.js', '--realtime', 'missing.tgz', '--work-dir', '.']);
		expect(text).not.toContain(dir.replace(/\\/g, '\\\\'));
		expect(text).not.toContain(resolve('.').replace(/\\/g, '\\\\'));
	});

	it('refuses a directory given as the extensions input before packing anything', () => {
		const dir = workDir();
		const r = run(ORACLE, ['--extensions', dir, '--realtime', 'next', '--work-dir', dir]);
		expect(r.code).toBe(2);
		const result = JSON.parse(readFileSync(join(dir, 'result.json'), 'utf8'));
		expect(result).toMatchObject({ status: 'error', error: { step: 'inputs' }, packages: {} });
		expect(result.error.message).toContain('is not a file');
	});

	it('refuses an external cluster description that is not JSON before packing anything', () => {
		const dir = workDir();
		const out = join(dir, 'nested', 'verdict.json');
		const r = run(ORACLE, ['--realtime', 'next', '--work-dir', dir, '--out', out], { ORACLE_CLUSTER_NODES: '127.0.0.1:7000' });
		expect(r.code).toBe(2);
		const result = JSON.parse(readFileSync(out, 'utf8'));
		expect(result).toMatchObject({ status: 'error', error: { step: 'inputs' }, packages: {} });
	});
});
