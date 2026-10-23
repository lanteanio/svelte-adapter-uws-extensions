import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { gzipSync } from 'node:zlib';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
	describeSource,
	interrupt,
	portablePath,
	scrubPaths,
	settleTeardown,
	stopCluster
} from '../scripts/real-cluster-oracle.js';

const SCENARIO = 'scripts/fixtures/real-cluster-oracle.mjs';
const ORACLE = 'scripts/real-cluster-oracle.js';
const dirs = [];

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

	it('records each package root inside the consumer node_modules', () => {
		const consumer = join(workDir(), 'consumer');
		for (const name of Object.keys(STUBS)) stub(join(consumer, 'node_modules'), name);
		const { code, report } = scenarioIn(consumer);
		expect(code).toBe(1);
		expect(report.cases[0]).toMatchObject({ name: 'setup', status: 'failed', error: { message: 'stub cluster' } });
		for (const name of Object.keys(STUBS)) {
			expect(report.provenance.packages[name]).toMatchObject({ version: '0.0.0-stub', root: `node_modules/${name}` });
		}
	});

	it('fails setup when a package resolves from a directory above the consumer', () => {
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
