// The vocabulary gate's own tests. Every banned term below is assembled from
// pieces, because this file is itself inside the scanned surface.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it, expect } from 'vitest';
import { scanVocabulary, scanTree, vocabularyFiles, SCAN_ROOTS, EXTRA_FILES } from '../scripts/check-vocabulary.js';

const join2 = (...parts) => parts.join('');
const families = (text) => scanVocabulary(text).hits.map((hit) => hit.family);

describe('tracker identifiers', () => {
	it('rejects task keys and review labels', () => {
		for (const id of [join2('R', 'T-1985'), join2('LO', 'AD-12'), join2('D', 'S-02'), join2('C', 'S-03'), join2('DB', 'OPS-4'), join2('AB', 'USE-7')]) {
			expect(families(`// fixes ${id} for the relay`)).toEqual(['planning']);
		}
	});

	it('leaves standard names of the same shape alone', () => {
		for (const name of ['UTF-8', 'SHA-256', 'CRC-32', 'BSD-3', 'ES-2022', 'PART-1', join2('xR', 'T-1')]) {
			expect(scanVocabulary(`// encoded as ${name}`).hits).toEqual([]);
		}
	});
});

describe('provenance narration', () => {
	it('rejects narration of where a change came from', () => {
		for (const phrase of [
			join2('the work', 'stream owns this'),
			join2('the rev', 'iewer found a leak'),
			join2('the au', 'dit flagged it'),
			join2('a rev', 'iew finding'),
			join2('the exact collision from the fin', 'ding'),
			join2('filed up', 'stream'),
			join2('the ca', 'rd asked for it'),
			join2('it was bou', 'nced back'),
			join2('unlike the last ba', 'tch'),
			join2('these rev', 'iews')
		]) {
			expect(families(`// ${phrase}`)).toContain('provenance');
		}
	});

	it('leaves ordinary process and domain words alone', () => {
		for (const line of [
			'A reviewer may accept a named exception when a lane is unrelated.',
			'Treat every finding as an error with --strict.',
			'An email bounce is retried.',
			'The batch error is reported once.'
		]) {
			expect(scanVocabulary(line).hits).toEqual([]);
		}
	});

	it('keeps the allow marker as the recorded exemption', () => {
		const line = join2('// filed up', 'stream as a dependency note') + ' vocabulary-allow: names the dependency';
		expect(scanVocabulary(line)).toEqual({ hits: [], allowed: 1 });
		expect(scanVocabulary(join2('// filed up', 'stream') + ' vocabulary-allow:').hits).toHaveLength(1);
	});
});

describe('scanned roots', () => {
	it('covers tooling, workflows and the public policy documents', () => {
		expect(SCAN_ROOTS).toEqual(expect.arrayContaining(['scripts', '.github']));
		expect(EXTRA_FILES).toEqual(expect.arrayContaining(['CONTRIBUTING.md', 'SECURITY.md']));
		const files = vocabularyFiles();
		for (const file of ['scripts/check-links.js', '.github/workflows/verify.yml', '.github/pull_request_template.md', 'CONTRIBUTING.md', 'SECURITY.md']) {
			expect(files).toContain(file);
		}
		expect(files).not.toContain('scripts/check-vocabulary.js');
	});

	it('reports a hit in every new root of a fixture tree', () => {
		const root = fileURLToPath(new URL('..', import.meta.url));
		const workRoot = join(root, '.cross-repo-gate');
		mkdirSync(workRoot, { recursive: true });
		const work = mkdtempSync(join(workRoot, 'vocabulary-'));
		const put = (relative, text) => {
			const target = join(work, relative);
			mkdirSync(dirname(target), { recursive: true });
			writeFileSync(target, text);
		};
		try {
			const id = join2('R', 'T-42');
			put('scripts/tool.js', `// ${id}\n`);
			put('.github/workflows/ci.yml', `# ${id}\n`);
			put('.github/pull_request_template.md', `${id}\n`);
			put('CONTRIBUTING.md', `# Contributing\n\n${join2('Owned by the work', 'stream.')}\n`);
			put('SECURITY.md', `# Security\n\n${id}\n`);
			put('scripts/check-vocabulary.js', `// ${id}\n`);
			const { files, hits } = scanTree(work);
			expect(files).not.toContain('scripts/check-vocabulary.js');
			expect(hits.map((hit) => `${hit.rel}:${hit.line}:${hit.family}`).sort()).toEqual([
				'.github/pull_request_template.md:1:planning',
				'.github/workflows/ci.yml:1:planning',
				'CONTRIBUTING.md:3:provenance',
				'SECURITY.md:3:planning',
				'scripts/tool.js:1:planning'
			]);
		} finally { rmSync(work, { recursive: true, force: true }); }
	});

	it('passes on the repository itself', () => {
		expect(scanTree().hits).toEqual([]);
	});
});
