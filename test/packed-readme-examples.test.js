import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { inventoryMarkdown } from '../scripts/check-doc-code.js';

const root = fileURLToPath(new URL('..', import.meta.url));
const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');
const manifest = JSON.parse(readFileSync(new URL('../docs/code-blocks.v1.json', import.meta.url), 'utf8'));
const packageName = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')).name;
const fences = new Map(inventoryMarkdown(readme).map(block => [`${block.fingerprint}:${block.occurrence}`, block]));
const executed = manifest.blocks.filter(block => block.classification === 'executed');

function fenceBody(block) {
	const fence = fences.get(`${block.fingerprint}:${block.occurrence}`);
	if (!fence) throw new Error(`Missing README fence ${block.fingerprint}:${block.occurrence}; run node scripts/check-doc-code.js --write`);
	return fence.content.endsWith('\n') ? fence.content : fence.content + '\n';
}

describe('packed README examples', () => {
	let temp;
	let consumer;
	const dependencyLinks = [];

	beforeAll(() => {
		temp = mkdtempSync(join(tmpdir(), 'extensions-readme-'));
		const npmCli = process.env.npm_execpath || join(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
		const packed = JSON.parse(execFileSync(process.execPath, [npmCli, 'pack', '--json', '--ignore-scripts', '--pack-destination', temp], {
			cwd: root, encoding: 'utf8', timeout: 120_000, windowsHide: true
		}));
		consumer = join(temp, 'consumer');
		const installed = join(consumer, 'node_modules', packageName);
		mkdirSync(installed, { recursive: true });
		execFileSync('tar', ['-xf', join(temp, packed[0].filename), '-C', installed, '--strip-components=1'], {
			encoding: 'utf8', timeout: 120_000, windowsHide: true
		});
		writeFileSync(join(consumer, 'package.json'), JSON.stringify({ name: 'readme-consumer', private: true, type: 'module' }));
		// Supply the lockfile-resolved dependency tree while the package itself
		// always resolves to the extracted tarball, never the source checkout.
		for (const entry of readdirSync(join(root, 'node_modules'))) {
			if (entry.startsWith('.') || entry === packageName) continue;
			const target = join(consumer, 'node_modules', entry);
			if (existsSync(target)) continue;
			symlinkSync(join(root, 'node_modules', entry), target, 'junction');
			dependencyLinks.push(target);
		}
	}, 180_000);

	afterAll(() => {
		for (const path of dependencyLinks) unlinkSync(path);
		if (temp) rmSync(temp, { recursive: true, force: true });
	});

	it('executes every packed-runtime fence using the packed public exports', () => {
		expect(executed.length).toBeGreaterThan(0);
		const failures = [];
		for (const block of executed) {
			const path = join(consumer, `readme-${block.line}.mjs`);
			const source = fenceBody(block);
			writeFileSync(path, block.language === 'ts' ? ts.transpileModule(source, {
				compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext }
			}).outputText : source);
			try {
				execFileSync(process.execPath, [path], {
					cwd: consumer, encoding: 'utf8', timeout: 15_000, windowsHide: true,
					stdio: ['ignore', 'pipe', 'pipe']
				});
			} catch (error) {
				const detail = (error.stderr || error.message || '').toString().split('\n').slice(0, 6).join('\n');
				failures.push(`README.md:${block.line} (${block.section}) failed:\n${detail}`);
			}
		}
		expect(failures, failures.join('\n\n')).toEqual([]);
	}, 180_000);
});
