import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

const SCENARIO = 'scripts/fixtures/real-cluster-oracle.mjs';
const ORACLE = 'scripts/real-cluster-oracle.js';
const dirs = [];

function run(script, args = [], env = {}) {
	const r = spawnSync(process.execPath, [script, ...args], {
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
		expect(r.stdout).toContain('--realtime <tgz|spec>');
		expect(r.stdout).toContain('ORACLE_CLUSTER_NODES');
	});

	it('writes an error result naming the step when the realtime tarball is missing', () => {
		const dir = workDir();
		const r = run(ORACLE, ['--realtime', join(dir, 'missing.tgz'), '--work-dir', dir]);
		expect(r.code).toBe(2);
		const result = JSON.parse(readFileSync(join(dir, 'result.json'), 'utf8'));
		expect(result).toMatchObject({ schemaVersion: 1, status: 'error', scenario: null, error: { step: 'inputs' } });
		expect(result.error.message).toContain('missing.tgz');
		expect(result.source.repository).toBe('svelte-adapter-uws-extensions');
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
