import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import {
	buildManifest,
	coverageOf,
	inventoryMarkdown,
	packageImportProblems,
	memberSurfaceProblems,
	syntaxProblem,
	validateManifest
} from '../scripts/check-doc-code.js';

const read = (relative) => readFileSync(new URL('../' + relative, import.meta.url), 'utf8');
const README = read('README.md');
const MANIFEST = JSON.parse(read('docs/code-blocks.v1.json'));
const PACKAGE = JSON.parse(read('package.json'));

describe('README code-block contract', () => {
	it('classifies every real README fence and compiles every compiler channel', () => {
		expect(validateManifest(MANIFEST, { 'README.md': README })).toEqual([]);
		const coverage = coverageOf(MANIFEST);
		expect(coverage.total).toBe(inventoryMarkdown(README).length);
		expect(coverage.total).toBeGreaterThan(150);
		expect(coverage.classifications.syntax).toBeGreaterThan(50);
		expect(coverage.classifications.executed).toBeGreaterThan(0);
	}, 60000);

	it('fails closed when a fence is added, changed, or moved under another section', () => {
		const added = README + '\n## Unreviewed\n\n```js\nexport const missed = true;\n```\n';
		expect(validateManifest(MANIFEST, { 'README.md': added })
			.some((error) => error.includes('unclassified js fence'))).toBe(true);

		const fence = inventoryMarkdown(README).find(block => block.language === 'js');
		const changed = README.replace(fence.content, fence.content + '// changed example' + '\n');
		expect(validateManifest(MANIFEST, { 'README.md': changed }).length).toBeGreaterThan(0);
	}, 60000);

	it('refuses a manifest whose recorded lines drifted, without any fence changing', () => {
		// Prose changes still require current source locations.
		const shifted = README.replace('\n## ', '\n<!-- prose -->\n\n## ');
		expect(shifted).not.toBe(README);

		const errors = validateManifest(MANIFEST, { 'README.md': shifted });
		expect(errors.some((error) => error.includes('line is stale'))).toBe(true);
		expect(errors.some((error) => error.includes('node scripts/check-doc-code.js --write'))).toBe(true);
		// Content is untouched, so nothing may be reported as reclassified.
		expect(errors.some((error) => error.includes('unclassified'))).toBe(false);
	}, 60000);

	it('distinguishes standalone syntax, intentional fragments, and manual commands', () => {
		const marker = '<!-- doc-code: fragment reason="deliberate excerpt" -->';
		const source = [
			'# Samples',
			'```js',
			'export const valid = config.value;',
			'```',
			marker,
			'```js',
			'const = broken;',
			'```',
			'```bash',
			'npm test',
			'```'
		].join('\n');
		const manifest = buildManifest({ 'README.md': source });
		expect(manifest.blocks.map((block) => block.classification))
			.toEqual(['syntax', 'fragment', 'command']);
		expect(validateManifest(manifest, { 'README.md': source })).toEqual([]);

		// An unmarked parse failure fails closed instead of inventing a fragment.
		const unmarked = source.replace(marker + '\n', '');
		expect(() => buildManifest({ 'README.md': unmarked }))
			.toThrow(/deliberate excerpt needs <!-- doc-code: fragment/);
	});

	it('uses the language compiler rather than accepting malformed code as metadata', () => {
		expect(syntaxProblem({ language: 'js', content: 'export const ok = 1;' })).toBeNull();
		expect(syntaxProblem({ language: 'js', content: 'export const = 1;' })).not.toBeNull();
		expect(syntaxProblem({ language: 'ts', content: 'const value: number = 1;' })).toBeNull();
		expect(syntaxProblem({ language: 'svelte', content: '<button>{label}</button>' })).toBeNull();
		expect(syntaxProblem({ language: 'json', content: '{"ok":true}' })).toBeNull();
		expect(syntaxProblem({ language: 'yaml', content: 'ok: true\n' })).toBeNull();
		for (const block of [
			{ language: 'ts', content: 'const value: = 1;' },
			{ language: 'svelte', content: '<button>{broken</button>' },
			{ language: 'json', content: '{broken}' },
			{ language: 'yaml', content: 'key: true\nkey: false' }
		]) expect(syntaxProblem(block)).not.toBeNull();
	});

	it('rejects README imports that are not present on the packaged declaration', () => {
		expect(packageImportProblems({
			language: 'js',
			content: "import { createDistributedLock } from 'svelte-adapter-uws-extensions/redis/lock';\n"
		})).toEqual([]);
		expect(packageImportProblems({
			language: 'js',
			content: "import { withLock } from 'svelte-adapter-uws-extensions/redis/lock';\n"
		})).toContain('svelte-adapter-uws-extensions/redis/lock has no declared export withLock');
	});


	it('resolves barrel exports and native members without accepting nonexistent APIs', () => {
		const block = content => ({ language: 'js', content });
		expect(packageImportProblems(block("import { isSafeUrl } from 'svelte-adapter-uws-extensions/safe-url';"))).toEqual([]);
		const prefix = "import { PRESSURE_REASONS } from 'svelte-adapter-uws-extensions/admission';\n";
		expect(memberSurfaceProblems(block(prefix + 'PRESSURE_REASONS.filter(Boolean);'))).toEqual([]);
		expect(memberSurfaceProblems(block(prefix + 'PRESSURE_REASONS.missingMethod();'))).toHaveLength(1);
		expect(memberSurfaceProblems(block(prefix + 'PRESSURE_REASONS.then();'))).toHaveLength(1);
		expect(memberSurfaceProblems(block(prefix + 'const { missingMethod } = PRESSURE_REASONS;'))).toHaveLength(1);
	});

	it('keeps application imports contextual and rejects unexplained exceptions', () => {
		const fenced = content => '# Sample\n\n' + '```js\n' + content + '\n```\n';
		const tutorial = buildManifest({ 'README.md': fenced("import { config } from '$lib/config';\nexport const value = config.value;") });
		expect(tutorial.blocks[0].classification).toBe('tutorial');
		expect(() => buildManifest({ 'README.md': '<!-- doc-code: fragment -->\n```js\nreturn;\n```' })).toThrow();
		const badReason = '<!-- doc-code: no-run reason="skip it" -->\n```js\nexport const value = 1;\n```';
		expect(() => buildManifest({ 'README.md': badReason })).toThrow(/no-run reason/);
	});

	it('requires an explicit reviewed update before the fragment ceiling can grow', () => {
		const fragment = '<!-- doc-code: fragment reason="Handler body" -->\n```js\nreturn;\n```\n';
		const previous = buildManifest({ 'README.md': fragment });
		expect(() => buildManifest({ 'README.md': fragment + fragment }, { previousManifest: previous })).toThrow(/exceeds the pinned ceiling/);
		const grown = buildManifest({ 'README.md': fragment + fragment }, { previousManifest: previous, acceptFragmentCount: true });
		expect(grown.fragmentCeiling).toBe(2);
		grown.fragmentCeiling = 1;
		expect(validateManifest(grown, { 'README.md': fragment + fragment }).some(error => error.includes('exceeds the pinned ceiling'))).toBe(true);
	});

	it('runs in normal, docs, test, and publication gates', () => {
		expect(PACKAGE.scripts['check:docs-code']).toBe('node scripts/check-doc-code.js');
		expect(PACKAGE.scripts.check).toContain('node scripts/check-doc-code.js');
		expect(PACKAGE.scripts['verify:docs']).toContain('node scripts/check-doc-code.js');
		expect(PACKAGE.scripts.pretest).toBe('npm run check');
		expect(PACKAGE.scripts.prepublishOnly).toContain('npm run check');
	});
});
