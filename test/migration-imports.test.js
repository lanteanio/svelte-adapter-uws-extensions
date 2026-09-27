import { spawnSync } from 'node:child_process';
import {
	afterAll,
	beforeAll,
	describe,
	expect,
	it
} from 'vitest';
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	renameSync,
	rmSync
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const migration = readFileSync(join(root, 'MIGRATION.md'), 'utf8');
const escapedPackageName = pkg.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const importPattern = new RegExp(
	String.raw`(?:from\s+|import\s+|import\s*\(\s*|require\s*\(\s*)['"](${escapedPackageName}(?:\/[^'"]+)?)['"]`,
	'g'
);
const migrationSpecifiers = [...new Set(
	[...migration.matchAll(importPattern)].map((match) => match[1])
)].sort();

let workDir;
let consumerDir;

function run(command, args, cwd = root) {
	const result = spawnSync(command, args, {
		cwd,
		encoding: 'utf8',
		timeout: 60_000
	});
	if (result.status !== 0) {
		throw new Error(
			`${command} ${args.join(' ')} failed\n${result.stdout || ''}\n${result.stderr || ''}`
		);
	}
	return result;
}

/**
 * The npm CLI to pack with: the one running this script under npm, else the
 * one installed next to this node binary, so a run from an editor or a bare
 * `vitest` packs the same way `npm test` does.
 */
function npmCli() {
	if (process.env.npm_execpath && /npm-cli\.js$/.test(process.env.npm_execpath)) return process.env.npm_execpath;
	const bin = dirname(process.execPath);
	return [
		join(bin, 'node_modules', 'npm', 'bin', 'npm-cli.js'),
		join(bin, '..', 'lib', 'node_modules', 'npm', 'bin', 'npm-cli.js')
	].find((path) => existsSync(path)) ?? null;
}

beforeAll(() => {
	const npm = npmCli();
	if (npm === null) {
		throw new Error('migration import pack test: no npm CLI found next to ' + process.execPath + '; run it through npm test');
	}
	workDir = mkdtempSync(join(tmpdir(), 'extensions-migration-imports-'));
	const packed = run(
		process.execPath,
		[npm, 'pack', '--json', '--pack-destination', workDir]
	);
	const metadata = JSON.parse(packed.stdout);
	const tarball = join(workDir, metadata[0].filename);
	const extractDir = join(workDir, 'extract');
	consumerDir = join(workDir, 'consumer');
	mkdirSync(extractDir, { recursive: true });
	mkdirSync(join(consumerDir, 'node_modules'), { recursive: true });
	// Relative names: GNU tar reads a Windows drive letter in an absolute
	// path as a remote host, so the archive and target are addressed from workDir.
	run('tar', ['-xf', metadata[0].filename, '-C', 'extract'], workDir);
	renameSync(
		join(extractDir, 'package'),
		join(consumerDir, 'node_modules', pkg.name)
	);
});

afterAll(() => {
	if (workDir) rmSync(workDir, { recursive: true, force: true });
});

describe('migration-guide package imports', () => {
	it('imports every documented package subpath from the packed artifact', () => {
		expect(migrationSpecifiers.length).toBeGreaterThan(0);
		const probe = run(process.execPath, [
			'--input-type=module',
			'--eval',
			`for (const specifier of ${JSON.stringify(migrationSpecifiers)}) await import(specifier);`
		], consumerDir);
		expect(probe.stderr).toBe('');
	});

	it('ships the lease scripts the lock actually runs, and nothing else from redis/lock', () => {
		const probe = run(process.execPath, [
			'--input-type=module',
			'--eval',
			`import { LEASE_RENEW_SCRIPT, LEASE_RELEASE_SCRIPT } from '${pkg.name}/redis/lease-scripts';\n` +
			`import * as lock from '${pkg.name}/redis/lock';\n` +
			`if (!/pexpire/i.test(LEASE_RENEW_SCRIPT) || !/del/i.test(LEASE_RELEASE_SCRIPT)) process.exit(2);\n` +
			`if ('HEARTBEAT_SCRIPT' in lock || 'RELEASE_SCRIPT' in lock) process.exit(3);`
		], consumerDir);
		expect(probe.stderr).toBe('');
	});
});
