import { copyFileSync, existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

export function checkLockfile(directory, { npmCli = process.env.npm_execpath, run = spawnSync } = {}) {
	if (!npmCli) throw new Error('Run this check with npm run check:lockfile');
	const lockfile = readFileSync(join(directory, 'package-lock.json'));
	const scratch = mkdtempSync(join(tmpdir(), 'extensions-lockfile-'));
	try {
		for (const name of ['package.json', 'package-lock.json', '.npmrc']) {
			if (existsSync(join(directory, name))) copyFileSync(join(directory, name), join(scratch, name));
		}
		const result = run(process.execPath, [npmCli, 'install', '--package-lock-only', '--package-lock=true', '--dry-run=false', '--ignore-scripts', '--no-audit', '--no-fund'], {
			cwd: scratch, encoding: 'utf8', timeout: 120_000, windowsHide: true
		});
		if (result.error || result.status !== 0) {
			throw new Error(`Lockfile regeneration failed: ${result.error?.message || result.stderr || result.stdout || result.status}`);
		}
		if (!lockfile.equals(readFileSync(join(scratch, 'package-lock.json')))) {
			throw new Error('package-lock.json is stale; regenerate it with npm install --package-lock-only and commit the diff');
		}
	} finally {
		rmSync(scratch, { recursive: true, force: true });
	}
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	try {
		checkLockfile(root);
		console.log('check-lockfile: isolated regeneration leaves package-lock.json unchanged');
	} catch (error) {
		console.error(error.message);
		process.exitCode = 1;
	}
}
