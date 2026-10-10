import { execFileSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

const root = fileURLToPath(new URL('../../../', import.meta.url));

it('rehearses automatic and manual 0.5 schema migration through the packed public API', () => {
	const temp = mkdtempSync(join(tmpdir(), 'extensions-migration-'));
	const links = [];
	try {
		const npm = process.env.npm_execpath || join(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
		const packed = JSON.parse(execFileSync(process.execPath, [npm, 'pack', '--json', '--ignore-scripts', '--pack-destination', temp], {
			cwd: root, encoding: 'utf8', timeout: 120_000, windowsHide: true
		}));
		const name = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')).name;
		const installed = join(temp, 'node_modules', name);
		mkdirSync(installed, { recursive: true });
		execFileSync('tar', ['-xf', packed[0].filename, '-C', `node_modules/${name}`, '--strip-components=1'], {
			cwd: temp, encoding: 'utf8', timeout: 60_000, windowsHide: true
		});
		for (const entry of readdirSync(join(root, 'node_modules'))) {
			if (entry.startsWith('.') || entry === name) continue;
			const target = join(temp, 'node_modules', entry);
			if (existsSync(target)) continue;
			symlinkSync(join(root, 'node_modules', entry), target, 'junction');
			links.push(target);
		}
		writeFileSync(join(temp, 'package.json'), '{"private":true,"type":"module"}\n');
		for (const name of ['migration-0.5.10.json', 'migration-rehearsal.mjs']) {
			copyFileSync(new URL(`../fixtures/${name}`, import.meta.url), join(temp, name));
		}
		copyFileSync(join(installed, 'MIGRATION.md'), join(temp, 'MIGRATION.md'));
		const output = execFileSync(process.execPath, ['migration-rehearsal.mjs'], {
			cwd: temp, encoding: 'utf8', timeout: 60_000, windowsHide: true,
			env: process.env
		});
		expect(output).toContain('packed migration automatic: preserved legacy rows and trace round-trip');
		expect(output).toContain('packed migration manual: preserved legacy rows and trace round-trip');
	} finally {
		for (const path of links) unlinkSync(path);
		rmSync(temp, { recursive: true, force: true });
	}
}, 180_000);
