import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { parse } from 'yaml';
import { runDependencyAudit, validateDependencyAudit } from '../scripts/check-dependency-audit.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const auditText = readFileSync(join(root, 'test/fixtures/dependency-audit.json'), 'utf8');
const lockText = readFileSync(join(root, 'package-lock.json'), 'utf8');
const now = Date.parse('2026-11-02T00:00:00Z');
const input = () => ({ auditText, lockText, registryText: '"4.3.1"', auditStatus: 1, registryStatus: 0, now });
const cleanReport = {
	auditReportVersion: 2, vulnerabilities: {},
	metadata: { vulnerabilities: { info: 0, low: 0, moderate: 0, high: 0, critical: 0, total: 0 } }
};
const cleanInput = () => ({ auditText: JSON.stringify(cleanReport), auditStatus: 0 });
const directories = [];
const workRoot = join(root, 'source');

afterEach(() => {
	vi.unstubAllEnvs();
	for (const directory of directories.splice(0)) {
		expect(directory.startsWith(workRoot + sep)).toBe(true);
		rmSync(directory, { recursive: true, force: true });
	}
});

function auditMutation(change) {
	const value = JSON.parse(auditText);
	change(value);
	return { ...input(), auditText: JSON.stringify(value) };
}

function lockMutation(change) {
	const value = JSON.parse(lockText);
	change(value);
	return { ...input(), lockText: JSON.stringify(value) };
}

describe('dependency audit exception', () => {
	it('accepts a clean report after expiry without consulting exception inputs', () => {
		expect(validateDependencyAudit({
			...cleanInput(), now: Date.parse('2026-12-01T00:00:00Z'),
			lockText: 'not the pinned tree', registryText: '"4.3.2"', registryStatus: 1, registryFailure: true
		})).toEqual({ findings: 0 });
		expect(validateDependencyAudit(cleanInput())).toEqual({ findings: 0 });
	});

	it('accepts a clean report for a changed, patched dependency tree', () => {
		const changed = lockMutation(value => {
			value.packages['node_modules/deepmerge'].version = '4.3.2';
			value.packages['node_modules/deepmerge'].integrity = 'different patched artifact';
		});
		expect(validateDependencyAudit({ ...changed, ...cleanInput() })).toEqual({ findings: 0 });
	});

	it.each([
		value => { delete value.metadata; },
		value => { delete value.metadata.vulnerabilities.low; },
		value => { value.metadata.vulnerabilities.total = 1; },
		value => { value.metadata.vulnerabilities.low = 1; },
		value => { value.metadata.vulnerabilities.unknown = 0; },
		value => { value.metadata.vulnerabilities.low = '0'; },
		value => { value.metadata.vulnerabilities.high = -1; },
		value => { value.auditReportVersion = 1; },
		value => { value.vulnerabilities = []; },
		value => { value.error = { code: 'ENOTFOUND' }; }
	])('refuses a malformed or inconsistent zero-finding report', mutate => {
		const value = structuredClone(cleanReport);
		mutate(value);
		expect(() => validateDependencyAudit({ ...cleanInput(), auditText: JSON.stringify(value) })).toThrow();
	});

	it.each([
		{ auditStatus: 1 }, { auditStatus: 2 }, { auditStatus: null }, { auditFailure: true }
	])('refuses a zero-finding report with a failed or inconsistent command: %j', altered => {
		expect(() => validateDependencyAudit({ ...cleanInput(), ...altered })).toThrow();
	});

	it('accepts exactly the captured four low development findings without changing the report', () => {
		const captured = input();
		expect(validateDependencyAudit(captured)).toEqual({
			findings: 4, severity: 'low',
			advisory: 'https://github.com/advisories/GHSA-gcqx-w48m-w2h4',
			expires: '2026-11-30T00:00:00Z'
		});
		expect(captured.auditText).toBe(auditText);
	});

	it('refuses an additional low advisory even on the same dependency', () => {
		const captured = auditMutation(value => {
			value.vulnerabilities.deepmerge.via.push({
				source: 999999, name: 'deepmerge', dependency: 'deepmerge',
				url: 'https://github.com/advisories/GHSA-aaaa-bbbb-cccc',
				severity: 'low', range: '*'
			});
		});
		expect(() => validateDependencyAudit(captured)).toThrow('unreviewed advisory');
	});

	it('refuses another low finding instead of accepting a severity-wide exception', () => {
		const captured = auditMutation(value => {
			value.vulnerabilities.other = { ...value.vulnerabilities.deepmerge, name: 'other' };
			value.metadata.vulnerabilities.low++;
			value.metadata.vulnerabilities.total++;
		});
		expect(() => validateDependencyAudit(captured)).toThrow('additional findings');
	});

	it.each(['finding', 'advisory'])('refuses %s severity escalation', kind => {
		const captured = auditMutation(value => {
			if (kind === 'finding') value.vulnerabilities.deepmerge.severity = 'moderate';
			else value.vulnerabilities.deepmerge.via[0].severity = 'moderate';
		});
		expect(() => validateDependencyAudit(captured)).toThrow();
	});

	it.each(['version', 'integrity', 'dev'])('refuses a changed %s on every affected artifact', field => {
		for (const name of Object.keys(JSON.parse(auditText).vulnerabilities)) {
			const captured = lockMutation(value => {
				value.packages['node_modules/' + name][field] = field === 'dev' ? false : 'changed';
			});
			expect(() => validateDependencyAudit(captured), name).toThrow('development artifact changed');
		}
	});

	it('refuses an injected dependency, nested copy or additional production edge', () => {
		const mutations = [
			value => { value.packages['node_modules/extra'] = { version: '1.0.0', dependencies: { deepmerge: '4.3.1' }, dev: true }; },
			value => { value.packages['node_modules/extra/node_modules/deepmerge'] = { ...value.packages['node_modules/deepmerge'] }; },
			value => { value.packages[''].dependencies.deepmerge = '4.3.1'; },
			value => { value.packages['node_modules/@sveltejs/kit'].dependencies.deepmerge = '*'; }
		];
		for (const mutate of mutations) expect(() => validateDependencyAudit(lockMutation(mutate))).toThrow('dependency graph changed');
	});

	it('refuses an additional advisory path for an otherwise pinned package', () => {
		const captured = auditMutation(value => {
			value.vulnerabilities.deepmerge.nodes.push('node_modules/extra/node_modules/deepmerge');
		});
		expect(() => validateDependencyAudit(captured)).toThrow('scope or severity changed');
	});

	it('refuses missing references and cycles without trusting a transitive allowlist', () => {
		const missing = auditMutation(value => {
			value.vulnerabilities['@sveltejs/kit'].via = ['missing'];
		});
		const cycle = auditMutation(value => {
			value.vulnerabilities['@sveltejs/vite-plugin-svelte'].via = ['svelte-adapter-uws'];
		});
		expect(() => validateDependencyAudit(missing)).toThrow('missing advisory reference');
		expect(() => validateDependencyAudit(cycle)).toThrow('cyclic advisory references');
	});

	it('refuses a new acyclic relationship among known packages', () => {
		const captured = auditMutation(value => {
			value.vulnerabilities['svelte-adapter-uws'].via.push('@sveltejs/vite-plugin-svelte');
		});
		expect(() => validateDependencyAudit(captured)).toThrow('unreviewed advisory dependency');
	});

	it.each(['2026-11-30T00:00:00Z', '2026-12-01T00:00:00Z', 'invalid'])('expires at the boundary: %s', date => {
		expect(() => validateDependencyAudit({ ...input(), now: Date.parse(date) })).toThrow('expired');
	});

	it.each(['"4.3.2"', '"5.0.0"', '{}', 'null', 'not JSON', ''])('refuses a changed or unusable registry reply: %s', registryText => {
		expect(() => validateDependencyAudit({ ...input(), registryText })).toThrow();
	});

	it.each([
		{ auditStatus: 0 }, { auditStatus: 2 }, { auditStatus: null }, { auditFailure: true },
		{ registryStatus: 1 }, { registryStatus: null }, { registryFailure: true }
	])('refuses command failures or inconsistent status: %j', altered => {
		expect(() => validateDependencyAudit({ ...input(), ...altered })).toThrow();
	});

	it.each(['', 'not JSON', 'null', '[]', '{"error":{"code":"ENOTFOUND"}}', '{"auditReportVersion":2,"vulnerabilities":{}}'])('refuses malformed or CLI-error audit output: %s', altered => {
		expect(() => validateDependencyAudit({ ...input(), auditText: altered })).toThrow();
	});

	it('checks report totals and all reported severities', () => {
		for (const mutate of [
			value => { value.metadata.vulnerabilities.total = 0; },
			value => { value.metadata.vulnerabilities.high = 1; },
			value => { value.metadata.vulnerabilities.unrecognized = 1; },
			value => { value.vulnerabilities['svelte-adapter-uws'].severity = 'high'; }
		]) expect(() => validateDependencyAudit(auditMutation(mutate))).toThrow();
	});
});

describe('audit execution evidence', () => {
	function temporaryRoot() {
		mkdirSync(workRoot, { recursive: true });
		const directory = mkdtempSync(join(workRoot, 'dependency-policy-test-'));
		directories.push(directory);
		writeFileSync(join(directory, 'package-lock.json'), lockText);
		return directory;
	}

	it('retains every raw byte and bounds both registry commands', () => {
		const directory = temporaryRoot();
		const run = vi.fn()
			.mockReturnValueOnce({ status: 1, stdout: auditText, stderr: '' })
			.mockReturnValueOnce({ status: 0, stdout: '"4.3.1"\n', stderr: '' });
		expect(runDependencyAudit(directory, run, now).findings).toBe(4);
		expect(readFileSync(join(directory, 'source/dependency-audit.json'), 'utf8')).toBe(auditText);
		expect(run.mock.calls[0][0]).toEqual(['audit', '--json', '--include=dev', '--include=optional', '--include=peer', '--audit-level=low', '--registry=https://registry.npmjs.org', '--fetch-retries=0', '--fetch-timeout=15000']);
		expect(run.mock.calls[1][0]).toEqual(['view', 'deepmerge', 'version', '--json', '--registry=https://registry.npmjs.org', '--prefer-online', '--fetch-retries=0', '--fetch-timeout=10000']);
		expect(run.mock.calls.map(call => call[2])).toEqual([60000, 15000]);
	});

	it('skips registry access and lock validation for a clean audit after expiry', () => {
		const directory = temporaryRoot();
		writeFileSync(join(directory, 'package-lock.json'), 'a different, patched tree');
		const run = vi.fn().mockReturnValueOnce({ status: 0, stdout: JSON.stringify(cleanReport), stderr: '' });
		expect(runDependencyAudit(directory, run, Date.parse('2026-12-01T00:00:00Z'))).toEqual({ findings: 0 });
		expect(run).toHaveBeenCalledTimes(1);
		expect(readFileSync(join(directory, 'source/dependency-audit.json'), 'utf8')).toBe(JSON.stringify(cleanReport));
	});

	it.each([
		{ status: 1, stdout: JSON.stringify(cleanReport), stderr: '' },
		{ status: 0, stdout: auditText, stderr: '' },
		{ status: 0, stdout: JSON.stringify(cleanReport), stderr: 'network failure' },
		{ status: 0, stdout: '{"auditReportVersion":2,"vulnerabilities":{}}', stderr: '' }
	])('refuses malformed or inconsistent reports before requesting registry data', audit => {
		const directory = temporaryRoot();
		const run = vi.fn().mockReturnValueOnce(audit);
		expect(() => runDependencyAudit(directory, run, now)).toThrow();
		expect(run).toHaveBeenCalledTimes(1);
		expect(readFileSync(join(directory, 'source/dependency-audit.json'), 'utf8')).toBe(audit.stdout);
	});

	it.each(['dev', 'optional', 'peer'])('includes %s findings despite ambient omit and severity settings in the real subprocess argv', kind => {
		const directory = temporaryRoot();
		const additional = JSON.parse(auditText);
		additional.vulnerabilities.extra = {
			name: 'extra', severity: 'low', isDirect: false, via: [{
				source: 999999, name: 'extra', dependency: 'extra', severity: 'low',
				url: 'https://github.com/advisories/GHSA-aaaa-bbbb-cccc', range: '*'
			}], effects: [], nodes: ['node_modules/extra']
		};
		additional.metadata.vulnerabilities.low++;
		additional.metadata.vulnerabilities.total++;
		writeFileSync(join(directory, 'complete.json'), JSON.stringify(additional));
		writeFileSync(join(directory, 'omitted.json'), auditText);
		const npm = join(directory, 'npm-cli.js');
		writeFileSync(npm, `
import { appendFileSync, readFileSync } from 'node:fs';
const args = process.argv.slice(2);
appendFileSync(new URL('./argv.jsonl', import.meta.url), JSON.stringify(args) + '\\n');
if (args[0] === 'audit') {
	const included = args.includes('--include=' + process.env.npm_config_omit);
	process.stdout.write(readFileSync(new URL(included ? './complete.json' : './omitted.json', import.meta.url), 'utf8'));
	process.exitCode = args.includes('--audit-level=low') ? 1 : 0;
} else if (args[0] === 'view') process.stdout.write('"4.3.1"');
else throw new Error('unexpected command');
`);
		vi.stubEnv('npm_execpath', npm);
		vi.stubEnv('NODE_ENV', 'production');
		vi.stubEnv('npm_config_omit', kind);
		vi.stubEnv('npm_config_audit_level', 'critical');
		expect(() => runDependencyAudit(directory, undefined, now)).toThrow('additional findings');
		const argv = JSON.parse(readFileSync(join(directory, 'argv.jsonl'), 'utf8').split('\n')[0]);
		expect(argv).toEqual(expect.arrayContaining([
			'--include=dev', '--include=optional', '--include=peer', '--audit-level=low'
		]));
		expect(JSON.parse(readFileSync(join(directory, 'source/dependency-audit.json'), 'utf8')).vulnerabilities).toHaveProperty('extra');
	});

	it('prints a clean verdict without exception wording or a registry request', () => {
		const directory = temporaryRoot();
		const scripts = join(directory, 'scripts');
		mkdirSync(scripts);
		const script = join(scripts, 'check-dependency-audit.js');
		writeFileSync(script, readFileSync(join(root, 'scripts/check-dependency-audit.js')));
		writeFileSync(join(directory, 'clean.json'), JSON.stringify(cleanReport));
		const npm = join(directory, 'npm-cli.js');
		writeFileSync(npm, `
import { readFileSync } from 'node:fs';
if (process.argv[2] !== 'audit') throw new Error('registry must not be requested');
process.stdout.write(readFileSync(new URL('./clean.json', import.meta.url), 'utf8'));
`);
		const fixtureEnv = Object.fromEntries(Object.entries(process.env)
			.filter(([key]) => key.toLowerCase() !== 'npm_execpath'));
		fixtureEnv.npm_execpath = npm;
		const result = spawnSync(process.execPath, [script], {
			cwd: directory, env: fixtureEnv,
			encoding: 'utf8', timeout: 10000, windowsHide: true
		});
		expect(result.error).toBeUndefined();
		expect(result.status).toBe(0);
		expect(result.stderr).toBe('');
		expect(result.stdout).toBe('dependency audit: no reported vulnerabilities; full raw report: source/dependency-audit.json\n');
		expect(result.stdout).not.toMatch(/low development findings|exception|expires/);
	});

	it.each([
		{ status: 1, stdout: '{"error":{"code":"ENOTFOUND"}}', stderr: '' },
		{ status: null, stdout: 'partial output', error: new Error('spawn failed'), stderr: '' },
		{ status: 1, stdout: auditText, stderr: 'network failure' },
		{ status: 1, stdout: auditText, signal: 'SIGTERM', stderr: '' }
	])('retains failed audit output without converting it to clean evidence', audit => {
		const directory = temporaryRoot();
		const run = vi.fn().mockReturnValueOnce(audit).mockReturnValueOnce({ status: 0, stdout: '"4.3.1"', stderr: '' });
		expect(() => runDependencyAudit(directory, run, now)).toThrow();
		expect(readFileSync(join(directory, 'source/dependency-audit.json'), 'utf8')).toBe(audit.stdout);
	});

	it('retains the complete audit when the live version query fails', () => {
		const directory = temporaryRoot();
		const run = vi.fn()
			.mockReturnValueOnce({ status: 1, stdout: auditText, stderr: '' })
			.mockReturnValueOnce({ status: 1, stdout: '', stderr: 'ETIMEDOUT' });
		expect(() => runDependencyAudit(directory, run, now)).toThrow('registry check failed');
		expect(readFileSync(join(directory, 'source/dependency-audit.json'), 'utf8')).toBe(auditText);
	});

	it('keeps npm ci and retains raw audit evidence even when verification fails', () => {
		const workflow = parse(readFileSync(join(root, '.github/workflows/dependencies.yml'), 'utf8'));
		const steps = workflow.jobs.dependencies.steps;
		const install = steps.findIndex(step => step.run === 'npm ci');
		const verify = steps.findIndex(step => step.run === 'npm run verify:dependencies');
		expect(install).toBeGreaterThan(-1);
		expect(verify).toBeGreaterThan(install);
		const retain = steps.find(step => step.with?.name === 'dependency-audit');
		expect(retain).toMatchObject({
			if: 'always()', uses: 'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02',
			with: { path: 'source/dependency-audit.json', 'if-no-files-found': 'error' }
		});
		expect(workflow.permissions).toEqual({ contents: 'read' });
		const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
		expect(pkg.scripts['verify:dependencies']).toBe('npm run check:lockfile && npm ls --all && node scripts/check-dependency-audit.js');
	});
});
