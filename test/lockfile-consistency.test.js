import { describe, expect, it } from 'vitest';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { checkLockfile } from '../scripts/check-lockfile.js';

function fixture(run) {
	const root = mkdtempSync(join(tmpdir(), 'extensions-lock-test-'));
	const manifest = { name: 'lock-test', version: '1.0.0' };
	const lock = { ...manifest, lockfileVersion: 3, requires: true, packages: { '': manifest } };
	const encode = (value) => JSON.stringify(value, null, 2) + '\n';
	writeFileSync(join(root, 'package.json'), encode(manifest));
	writeFileSync(join(root, 'package-lock.json'), encode(lock));
	try { return run(root); } finally { rmSync(root, { recursive: true, force: true }); }
}

describe('isolated lockfile regeneration', { timeout: 30_000 }, () => {
	it('accepts a complete graph with the real npm resolver', () => fixture((root) => {
		expect(() => checkLockfile(root)).not.toThrow();
	}));
	it('rejects stale root metadata with the real npm resolver without changing the checkout', () => fixture((root) => {
		const path = join(root, 'package-lock.json');
		const lock = JSON.parse(readFileSync(path, 'utf8'));
		lock.packages[''].version = '0.9.0';
		writeFileSync(path, JSON.stringify(lock, null, 2) + '\n');
		const before = readFileSync(path);
		expect(() => checkLockfile(root)).toThrow('package-lock.json is stale');
		expect(readFileSync(path).equals(before)).toBe(true);
	}));
	it.each(['dry-run=true', 'package-lock=false'])('does not let npm settings hide a stale lock: %s', (setting) => fixture((root) => {
		writeFileSync(join(root, '.npmrc'), setting + '\n');
		const path = join(root, 'package-lock.json');
		const lock = JSON.parse(readFileSync(path, 'utf8'));
		lock.packages[''].version = '0.9.0';
		writeFileSync(path, JSON.stringify(lock, null, 2) + '\n');
		expect(() => checkLockfile(root)).toThrow('package-lock.json is stale');
	}));
	it('copies project npm settings, disables install scripts and removes scratch files', () => fixture((root) => {
		writeFileSync(join(root, '.npmrc'), 'legacy-peer-deps=true\n');
		let scratch;
		checkLockfile(root, { npmCli: 'npm-cli.js', run: (command, args, options) => {
			scratch = options.cwd;
			expect(command).toBe(process.execPath);
			expect(args).toContain('--package-lock-only');
			expect(args).toContain('--ignore-scripts');
			expect(args).toContain('--no-audit');
			expect(readFileSync(join(scratch, '.npmrc'), 'utf8')).toBe('legacy-peer-deps=true\n');
			expect(existsSync(join(scratch, 'node_modules'))).toBe(false);
			return { status: 0 };
		} });
		expect(existsSync(scratch)).toBe(false);
	}));
	it.each([
		{ status: 1, stderr: 'resolution failed' },
		{ status: null, error: new Error('timed out') }
	])('fails closed when regeneration fails and cleans scratch files: %j', (result) => fixture((root) => {
		let scratch;
		expect(() => checkLockfile(root, { npmCli: 'npm-cli.js', run: (_command, _args, options) => {
			scratch = options.cwd;
			return result;
		} })).toThrow('Lockfile regeneration failed');
		expect(existsSync(scratch)).toBe(false);
	}));
	it('requires the invoking npm executable instead of guessing a shell command', () => fixture((root) => {
		expect(() => checkLockfile(root, { npmCli: null })).toThrow('npm run check:lockfile');
	}));
});
