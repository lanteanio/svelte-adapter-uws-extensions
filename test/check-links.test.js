// The documentation link checker's own tests.
//
// It exists because a dead anchor is invisible: GitHub renders it as an
// ordinary link and does nothing when it is clicked, so nothing short of
// clicking every link in a several-thousand-line README finds one.
//
// The slug cases below are the ones an approximation gets wrong, and getting
// them wrong in either direction makes the checker useless - a rule that
// collapses hyphen runs rejects a link that works today, and a rule that
// replaces punctuation instead of deleting it accepts one that does not.

import { readFileSync, mkdirSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { describe, it, expect } from 'vitest';
import {
	slugify,
	anchorsOf,
	linksOf,
	checkLink,
	stagedTree,
	mainTreeSnapshot,
	treeContains,
	resolvableMainCommit,
	onMainLine,
	documentationFiles,
	packageFiles,
	validateDocumentationMap,
	resolveDocumentPath
} from '../scripts/check-links.js';

describe('heading slugs follow GitHub exactly', () => {
	it('lowercases and hyphenates spaces', () => {
		expect(slugify('Clone to green')).toBe('clone-to-green');
	});

	// Punctuation is DELETED, not replaced, so the spaces either side of a
	// removed word survive as two hyphens. This is a real heading in the README
	// and a checker that normalizes the pair reports its live link as dead.
	it('leaves the double hyphen a deleted word produces', () => {
		expect(slugify('Development, Preview & Production')).toBe('development-preview--production');
	});

	it('drops code spans, emphasis and link syntax but keeps the text', () => {
		expect(slugify('The `platform` API')).toBe('the-platform-api');
		expect(slugify('**Bold** and _thin_')).toBe('bold-and-thin');
		expect(slugify('See [the guide](x.md)')).toBe('see-the-guide');
	});

	it('uses rendered ATX text for closing hashes and HTML entities', () => {
		expect(slugify('Canonical setup ##')).toBe('canonical-setup');
		expect(slugify('Fish &amp; chips')).toBe('fish--chips');
	});

	it('keeps letters outside ASCII', () => {
		expect(slugify('Grundsätzliches zur Größe')).toBe('grundsätzliches-zur-größe');
	});
});

describe('collecting the anchors a document defines', () => {
	it('numbers repeated headings in document order', () => {
		const anchors = anchorsOf('# Options\n## Options\n### Options\n');
		expect([...anchors]).toEqual(['options', 'options-1', 'options-2']);
	});

	it('ignores a comment inside a fenced code block', () => {
		const anchors = anchorsOf('# Real\n\n```bash\n# Not a heading\n```\n');
		expect([...anchors]).toEqual(['real']);
	});

	it('accepts an explicit html anchor', () => {
		expect(anchorsOf('<a id="wire-format"></a>\n').has('wire-format')).toBe(true);
		expect(anchorsOf('<a name="legacy"></a>\n').has('legacy')).toBe(true);
	});
});

describe('collecting the links a document makes', () => {
	it('reads targets with their line numbers', () => {
		expect(linksOf('intro\n\nsee [x](#here) and [y](./MIGRATION.md)\n')).toEqual([
			{ line: 3, target: '#here' },
			{ line: 3, target: './MIGRATION.md' }
		]);
	});

	it('does not read a link written as an example inside a code span', () => {
		expect(linksOf('write `[label](#anchor)` to link\n')).toEqual([]);
	});

	it('reports a link inside a table cell on its own row', () => {
		expect(linksOf('intro\n\n| a | b |\n|---|---|\n| one | two |\n| [x](#here) | three |\n'))
			.toEqual([{ line: 6, target: '#here' }]);
	});

	it('ignores a link title', () => {
		expect(linksOf('[x](#here "the title")\n')).toEqual([{ line: 1, target: '#here' }]);
	});

	it('collects rendered reference, angle-destination, and raw HTML links', () => {
		const source = [
			'[reference][route]',
			'',
			'[route]: ./NOT-HERE.md',
			'[angle](<./NOT HERE.md>)',
			'<a href="./ALSO-NOT-HERE.md">HTML link</a>'
		].join('\n');
		expect(linksOf(source)).toEqual([
			{ line: 1, target: './NOT-HERE.md' },
			{ line: 4, target: './NOT%20HERE.md' },
			{ line: 5, target: './ALSO-NOT-HERE.md' }
		]);
	});
});

describe('resolving a documentation path', () => {
	it('resolves a leading slash from the repository root', () => {
		expect(resolveDocumentPath('docs/guide.md', '/README.md')).toBe('README.md');
		expect(resolveDocumentPath('docs/guide.md', '/docs/getting-started.md')).toBe('docs/getting-started.md');
		expect(resolveDocumentPath('docs/guide.md', '/docs/')).toBe('docs/');
		expect(resolveDocumentPath('docs/guide.md', '/')).toBe('');
		expect(resolveDocumentPath('docs/guide.md', '/../x.md')).toBe('../x.md');
	});

	it('resolves every other path from the linking document', () => {
		expect(resolveDocumentPath('docs/guide.md', 'getting-started.md')).toBe('docs/getting-started.md');
		expect(resolveDocumentPath('docs/guide.md', '../README.md')).toBe('README.md');
		expect(resolveDocumentPath('README.md', './')).toBe('');
		expect(resolveDocumentPath('README.md', '../x.md')).toBe('../x.md');
	});
});

describe('resolving one link', () => {
	const docs = new Map([
		['README.md', { anchors: new Set(['cross-origin-and-native-app-usage']) }],
		['MIGRATION.md', { anchors: new Set(['upgrading']) }]
	]);

	it('accepts an anchor the document defines', () => {
		expect(checkLink('README.md', '#cross-origin-and-native-app-usage', docs)).toBe(null);
	});

	// The defect this was written for: a section renamed, the link left behind.
	it('rejects an anchor no heading produces', () => {
		expect(checkLink('README.md', '#renamed-away-heading', docs)).toContain('no heading in README.md');
	});

	it('follows an anchor into another document', () => {
		expect(checkLink('README.md', './MIGRATION.md#upgrading', docs)).toBe(null);
		expect(checkLink('README.md', './MIGRATION.md#gone', docs)).toContain('no heading in MIGRATION.md');
	});

	it('rejects a relative path that does not exist', () => {
		expect(checkLink('README.md', './NOT-HERE.md', docs)).toContain('no such file');
		expect(checkLink('README.md', './README.md', docs)).toBe(null);
	});

	it('rejects repository escape and wrong-case paths before anchor lookup', () => {
		expect(checkLink('README.md', '../svelte-adapter-uws/README.md', docs))
			.toContain('path leaves repository');
		expect(checkLink('README.md', './migration.md#definitely-missing', docs))
			.toContain('exact case');
	});

	it('rejects a relative target omitted from the package surface', () => {
		const packagedDocs = new Set(['README.md']);
		const packagedFiles = new Set(['README.md']);
		expect(checkLink('README.md', './MIGRATION.md', docs, { packagedDocs, packagedFiles }))
			.toContain('target omitted from package');
	});

	it('applies missing/package checks to rendered reference and angle destinations', () => {
		const packagedDocs = new Set(['README.md']);
		const packagedFiles = new Set(['README.md']);
		const [reference, angle] = linksOf([
			'[reference][route]',
			'[angle](<./MIGRATION.md>)',
			'',
			'[route]: ./MIGRATION.md'
		].join('\n'));
		expect(checkLink('README.md', reference.target, docs, { packagedDocs, packagedFiles }))
			.toContain('target omitted from package');
		expect(checkLink('README.md', angle.target, docs, { packagedDocs, packagedFiles }))
			.toContain('target omitted from package');
	});

	it('leaves external links alone rather than fetching them', () => {
		expect(checkLink('README.md', 'https://svti.me/smooth', docs)).toBe(null);
		expect(checkLink('README.md', 'mailto:someone@example.com', docs)).toBe(null);
		expect(checkLink('README.md', '//example.com/x', docs)).toBe(null);
	});

	it('rejects non-canonical spellings of this repository on GitHub', () => {
		const path = '/lanteanio/svelte-adapter-uws-extensions/blob/dev/README.md';
		expect(checkLink('README.md', 'http://github.com' + path, docs)).toContain('canonical https://github.com/ form');
		expect(checkLink('README.md', 'https://www.github.com' + path, docs)).toContain('canonical https://github.com/ form');
		expect(checkLink('README.md', 'https://github.com/lanteanio/svelte-adapter-uws-extensions-fork', docs)).toBe(null);
	});

	it('decodes a percent-escaped anchor before looking it up', () => {
		const encoded = new Map([['README.md', { anchors: new Set(['größe']) }]]);
		expect(checkLink('README.md', '#gr%C3%B6%C3%9Fe', encoded)).toBe(null);
	});
});

it('validates GitHub Markdown fragments against the named tree', () => {
	const base = 'https://github.com/lanteanio/svelte-adapter-uws-extensions/blob/';
	expect(checkLink('README.md', base + 'dev/README.md#not-a-real-heading', new Map())).toContain('No source heading');
	expect(checkLink('README.md', base + 'dev/README.md#what-you-get', new Map())).toBeNull();
	expect(checkLink('README.md', base + 'main/README.md#not-a-real-heading', new Map())).toContain('No source heading');
});



/**
 * A throwaway git checkout holding a copy of the checker, so the CLI runs
 * against a real index, package allowlist and directory layout.
 */
function linkFixture() {
	const root = fileURLToPath(new URL('..', import.meta.url));
	const workRoot = join(root, '.cross-repo-gate');
	mkdirSync(workRoot, { recursive: true });
	const work = mkdtempSync(join(workRoot, 'link-inventory-'));
	const put = (relative, text) => {
		const target = join(work, relative);
		mkdirSync(dirname(target), { recursive: true });
		writeFileSync(target, text);
	};
	const git = (...args) => execFileSync('git', ['-C', work, ...args], { stdio: 'pipe', windowsHide: true });
	const run = () => spawnSync(process.execPath, ['scripts/check-links.js'], { cwd: work, encoding: 'utf8', windowsHide: true });
	put('package.json', JSON.stringify({ name: 'fixture', version: '1.0.0', type: 'module', files: ['README.md', 'MIGRATION.md', 'CHANGELOG.md', 'docs'] }));
	put('README.md', '# Fixture\n[Migration](MIGRATION.md)\n[History](CHANGELOG.md)\n[Entries](docs/public-entries.md)\n[Guide](docs/guide.md)\n');
	put('MIGRATION.md', '# Migration\n[Overview](README.md)\n[History](CHANGELOG.md)\n');
	put('CHANGELOG.md', '# History\n[Overview](README.md)\n');
	put('docs/public-entries.md', '# Entries\n[Overview](../README.md)\n');
	put('docs/guide.md', '# Guide\n[Overview](../README.md)\n');
	put('docs/main-source-tree.v1.json', JSON.stringify({ commit: '0'.repeat(40), paths: [], anchors: {} }));
	put('scripts/check-links.js', readFileSync(join(root, 'scripts/check-links.js'), 'utf8'));
	return { put, git, run, cleanup: () => rmSync(work, { recursive: true, force: true }) };
}

it('runs the CLI against tracked and packaged documentation inventories', () => {
	const { put, git, run, cleanup } = linkFixture();
	try {
		put('docs/guide.md', '# Guide\n[Overview](../README.md)\n[Local](../private.md)\n');
		put('.gitignore', 'ignored.md\n');
		put('private.md', '# Local scratch\n');
		put('ignored.md', '# Ignored scratch\n');
		git('init', '--initial-branch=docs-fixture');
		git('add', '--', 'package.json', 'README.md', 'MIGRATION.md', 'CHANGELOG.md', 'docs', '.gitignore', 'scripts');
		let result = run();
		expect(result.status).toBe(1);
		expect(result.stderr).toContain('docs/guide.md:3');
		expect(result.stderr).toContain('relative target absent from the staged tree');
		put('docs/guide.md', '# Guide\n[Overview](../README.md)\n[Local](../ignored.md)\n');
		result = run();
		expect(result.status).toBe(1);
		expect(result.stderr).toContain('relative target absent from the staged tree');
		put('docs/guide.md', '# Guide\n[Overview](../README.md)\n[Folder](./)\n[Entries folder](../docs/)\n');
		result = run();
		expect(result.stderr).toBe('');
		expect(result.status).toBe(0);
		put('docs/guide.md', '# Guide\n[Overview](../README.md/)\n');
		result = run();
		expect(result.status).toBe(1);
		expect(result.stderr).toContain('relative target absent from the staged tree (the git index): README.md/');
		put('docs/guide.md', '# Guide\n[Overview](../README.md)\n');
		expect(run().status).toBe(0);
		put('package.json', JSON.stringify({ name: 'fixture', version: '1.0.0', type: 'module', files: ['README.md', 'docs'] }));
		result = run();
		expect(result.status).toBe(1);
		expect(result.stderr).toContain('target omitted from package: MIGRATION.md');
	} finally { cleanup(); }
}, 30_000);

// GitHub resolves a link that starts with a slash from the repository root,
// not from the directory of the document holding it. The probe document is
// tracked but not packaged, the only place such a link is allowed.
it('resolves a leading slash from the repository root in a nested document', () => {
	const { put, git, run, cleanup } = linkFixture();
	try {
		put('README.md', '# Fixture\n[Migration](MIGRATION.md)\n[History](CHANGELOG.md)\n[Entries](docs/public-entries.md)\n[Guide](docs/guide.md)\n[Start](docs/getting-started.md)\n');
		put('docs/getting-started.md', '# Start here\n## First steps\n[Overview](../README.md)\n');
		put('notes/guide.md', '# Notes\n');
		git('init', '--initial-branch=docs-fixture');
		git('add', '--', 'package.json', 'README.md', 'MIGRATION.md', 'CHANGELOG.md', 'docs', 'scripts', 'notes');
		put('notes/guide.md', [
			'# Notes',
			'[Overview](/README.md)',
			'[Overview heading](/README.md#fixture)',
			'[Start](/docs/getting-started.md)',
			'[Start steps](/docs/getting-started.md#first-steps)',
			'[Docs folder](/docs/)',
			'[Docs folder without slash](/docs)',
			'[Repository root](/)',
			'[Repository home heading](/#fixture)',
			'[Scripts folder](/scripts/)',
			''
		].join('\n'));
		let result = run();
		expect(result.stderr).toBe('');
		expect(result.status).toBe(0);
		for (const [target, failure] of [
			['/getting-started.md', 'no such file with exact case: getting-started.md'],
			['/docs/getting-started.md#missing-steps', 'no heading in docs/getting-started.md produces #missing-steps'],
			['/README.md#missing', 'no heading in README.md produces #missing'],
			['/#missing', 'no heading in README.md produces #missing'],
			['/docs/#first-steps', 'directory docs/ has no README.md to hold #first-steps'],
			['/missing/', 'no such file with exact case: missing/'],
			['/README.md/', 'relative target absent from the staged tree (the git index): README.md/'],
			['/../outside.md', 'path leaves repository: ../outside.md'],
			['/%2fREADME.md', 'root-relative path starts with a doubled slash']
		]) {
			put('notes/guide.md', '# Notes\n[Overview](../README.md)\n[Probe](' + target + ')\n');
			result = run();
			expect(result.status, target).toBe(1);
			expect(result.stderr, target).toContain('notes/guide.md:3');
			expect(result.stderr, target).toContain(failure);
		}
	} finally { cleanup(); }
}, 30_000);

// A packaged document is also read from node_modules and the registry page,
// where a leading slash does not name the repository root.
it('rejects a root-relative link in a packaged document', () => {
	const { put, git, run, cleanup } = linkFixture();
	try {
		git('init', '--initial-branch=docs-fixture');
		git('add', '--', 'package.json', 'README.md', 'MIGRATION.md', 'CHANGELOG.md', 'docs', 'scripts');
		put('docs/guide.md', '# Guide\n[Overview](../README.md)\n[Rooted](/README.md)\n');
		const result = run();
		expect(result.status).toBe(1);
		expect(result.stderr).toContain('docs/guide.md:3');
		expect(result.stderr).toContain('root-relative link in a packaged document (write it relative to docs/guide.md)');
	} finally { cleanup(); }
}, 30_000);

// Only Markdown is a document; scripts beneath a documentation directory are
// not parsed as Markdown, so text in them cannot pass or fail as a link.
it('reads only Markdown beneath the repository documentation directories', () => {
	const { put, git, run, cleanup } = linkFixture();
	try {
		put('bench/run.mjs', '// [not a link](./missing.md)\n');
		put('bench/README.md', '# Bench\n[Gone](./missing.md)\n');
		git('init', '--initial-branch=docs-fixture');
		git('add', '--', 'package.json', 'README.md', 'MIGRATION.md', 'CHANGELOG.md', 'docs', 'scripts', 'bench');
		const result = run();
		expect(result.status).toBe(1);
		expect(result.stderr).toContain('bench/README.md:2');
		expect(result.stderr).not.toContain('bench/run.mjs');
	} finally { cleanup(); }
}, 30_000);


it('protects package navigation, reciprocal parent links and reachable companion docs', () => {
	const source = new Map([
		['README.md', { text: '[History](CHANGELOG.md) [Migration](MIGRATION.md) [Entries](docs/public-entries.md)' }],
		['MIGRATION.md', { text: '[Overview](README.md) [History](CHANGELOG.md)' }],
		['CHANGELOG.md', { text: '[Overview](README.md)' }],
		['docs/public-entries.md', { text: '[Overview](../README.md) [Guide](guide.md)' }],
		['docs/guide.md', { text: '[Overview](../README.md)' }]
	]);
	const packaged = new Set(source.keys());
	expect(validateDocumentationMap(source, packaged)).toEqual([]);
	for (const [file, expected] of [
		['README.md', 'README.md must link to CHANGELOG.md'],
		['MIGRATION.md', 'MIGRATION.md must link to README.md'],
		['CHANGELOG.md', 'CHANGELOG.md must link to README.md'],
		['docs/guide.md', 'docs/guide.md needs a package overview or public-entry parent link']
	]) {
		const docs = new Map(source); docs.set(file, { text: '' });
		expect(validateDocumentationMap(docs, packaged)).toContain(expected);
	}
	const selfLinkedCatalog = new Map(source);
	selfLinkedCatalog.set('docs/public-entries.md', { text: '[Self](public-entries.md) [Guide](guide.md)' });
	expect(validateDocumentationMap(selfLinkedCatalog, packaged)).toContain('docs/public-entries.md needs a package overview or public-entry parent link');
	const rooted = new Map(source);
	rooted.set('docs/public-entries.md', { text: '[Overview](/README.md) [Guide](/docs/guide.md)' });
	rooted.set('docs/guide.md', { text: '[Overview](/README.md)' });
	expect(validateDocumentationMap(rooted, packaged)).toEqual([]);
	const orphan = new Map(source); orphan.set('docs/orphan.md', { text: '[Overview](../README.md)' });
	expect(validateDocumentationMap(orphan, packaged)).toContain('docs/orphan.md is unreachable from README.md');
	packaged.delete('CHANGELOG.md');
	expect(validateDocumentationMap(source, packaged)).toContain('CHANGELOG.md must be included in the package');
});
