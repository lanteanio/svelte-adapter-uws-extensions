#!/usr/bin/env node
/**
 * Guard that the published surface carries no borrowed vocabulary and no
 * pointers into internal planning documents.
 *
 * Three families are enforced:
 *
 *   attribution  Prose that credits an outside project, person, or paper as
 *                the source of a design ("inspired by", "ported from"). Credit
 *                belongs in the planning notes, never in shipped source.
 *   foreign      Names coined by another project. These are the dangerous
 *                class: they name nothing to a reader who does not already
 *                know the originating project, so they read as jargon while
 *                silently importing someone else's vocabulary. A grep for
 *                attribution phrasing cannot find them, because the borrowed
 *                word IS the name.
 *   planning     Item identifiers and planning vocabulary. Planning notes
 *                reference the code; the code never references the notes.
 *
 * Dependency-free (no eslint), modeled on the sibling check-determinism /
 * check-slugs scripts, wired into pretest.
 *
 * A line may opt out with a trailing `vocabulary-allow: <reason>` marker when a
 * match is genuinely the right word - naming a real dependency in its own
 * integration note, or a rename entry that must spell the superseded option so
 * migrating callers can find it. The reason is required, so an exemption is a
 * decision on the record rather than a silent suppression. In markdown, wrap
 * the marker in an HTML comment so it stays invisible to readers.
 *
 * Flags:
 *   --verbose  list every hit, not just the first few per pattern.
 *
 * @module scripts/check-vocabulary
 */
import { readdirSync, readFileSync, statSync, existsSync } from 'node:fs';
import { dirname, resolve, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

// Everything a reader can see: the shipped tree, the shipped docs, the suites
// and benches whose describe() strings surface in test output, and the public
// repository files (tooling scripts, workflows, issue and pull-request
// templates, contributor and security policy).
export const SCAN_ROOTS = ['src', 'test', 'bench', 'docs', 'scripts', '.github'];
export const EXTRA_FILES = ['README.md', 'MIGRATION.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'SECURITY.md'];

const SKIP_SEGMENTS = new Set([
	'node_modules', '.git', 'coverage', 'dist', 'build', '.svelte-kit',
	'source', 'audits', '.cross-repo-gate', 'dst-goldens'
]);

const SCAN_EXT = /\.(js|mjs|cjs|ts|d\.ts|md|yml|yaml)$/;

// This file necessarily spells every banned term, so it never scans itself.
const SELF = 'scripts/check-vocabulary.js';

const RULES = [
	{
		family: 'attribution',
		hint: 'credit the source in the planning notes, not in shipped code',
		patterns: [
			/\binspired by\b/i,
			/\bborrowed from\b/i,
			/\b(?:ported|lifted|cribbed|copied|adapted|taken) from\b/i,
			/\bcarbon.?cop/i,
			/\bprior art\b/i,
			/\bmodell?ed (?:on|after) [A-Z]/,
			/\bthanks to [A-Z]/,
			/\bcourtesy of\b/i,
			/\bmirrors the \w+ knob\b/i,
			/\bmatches mature \w+ libraries\b/i,
			/\bde-facto \w+ cap across\b/i
		]
	},
	{
		family: 'foreign',
		hint: 'another project coined this; pick a name that explains itself',
		patterns: [
			/\bbuggif/i,
			/"unseed"/i,
			/\bfoundationdb\b/i,
			/\btigerbeetle\b/i,
			/\bantithesis\b/i,
			/\bjepsen\b/i,
			/\bnemesis\b/i,
			/\byjs\b/i,
			/\bautomerge\b/i,
			/\bhocuspocus\b/i,
			/\bliveblocks\b/i,
			/\bpartykit\b/i,
			/\bsharedb\b/i,
			/\bcolyseus\b/i,
			/\breplicache\b/i,
			/\bsocket\.io\b/i,
			/\bbullmq\b/i,
			/\bpg-boss\b/i,
			/\bpgmq\b/i,
			/\bsidekiq\b/i,
			/\bredlock\b/i,
			/\brate-limiter-flexible\b/i,
			/\bioredis-mock\b/i,
			/\bclient_golang\b/i,
			/\bmicrometer\b/i,
			/\bgrafana\b/i
		]
	},
	{
		family: 'planning',
		hint: 'planning notes reference the code; the code never references them',
		patterns: [
			/\bcredo\b/i,
			/\bshipped-log\b/i,
			/\bthe plan doc\b/i,
			/\bDesign-[A-Z]\b/,
			/\b[A-Z]-[A-Z]{2,}-[A-Z]{2,}\b/,
			/\b(?:Phase|Tier|Batch|Slice|Keystone|Milestone) \d+\b/,
			/\bper the [A-Z]\d+[a-z]? spec\b/,
			/\bfor [A-Z]\d+[a-z]?\.$/,
			/(?:^|\s)\/(?:simplify|code-review|security-review|ultrareview)\b/,
			// Task-tracker keys and review labels. The prefixes are spelled
			// out, so standard names of the same shape (UTF-8, SHA-256,
			// CRC-32) never match.
			/\b(?:RT|ABUSE|CS|DBOPS|DC|DEL|DOC|DS|DX|INC|LOAD|OBS|PRIV|SEC|SF)-\d+\b/
		]
	},
	{
		family: 'provenance',
		hint: 'say what the code does, not where the change came from',
		patterns: [
			/\bworkstreams?\b/i,
			/\bthe (?:reviewer|review|audit) (?:found|flagged|asked|caught|noted|reported)\b/i,
			/\b(?:audit|review) findings?\b/i,
			/\bthe finding\b/i,
			/\bfiled upstream\b/i,
			/\bthe card (?:asks|asked|says|said|wants)\b/i,
			/\bbounced back\b/i,
			/\bunlike the last batch\b/i,
			/\bthese reviews\b/i
		]
	}
];

const ALLOW_RE = /vocabulary-allow:\s*\S/;

/**
 * Scan one document's text. A line carrying a `vocabulary-allow:` marker with
 * a reason is counted as allowed instead of reported.
 *
 * @param {string} text
 * @returns {{ hits: { line: number, family: string, hint: string, term: string }[], allowed: number }}
 */
export function scanVocabulary(text) {
	const hits = [];
	let allowed = 0;
	const lines = text.split(/\r?\n/);
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		for (const rule of RULES) {
			for (const re of rule.patterns) {
				const m = re.exec(line);
				if (!m) continue;
				if (ALLOW_RE.test(line)) { allowed++; continue; }
				hits.push({ line: i + 1, family: rule.family, hint: rule.hint, term: m[0].trim() });
			}
		}
	}
	return { hits, allowed };
}

/** Recursively collect scannable files under `dir` (base-relative, slash paths). */
function walk(base, dir, out) {
	if (!existsSync(dir)) return;
	for (const name of readdirSync(dir)) {
		if (SKIP_SEGMENTS.has(name)) continue;
		const abs = join(dir, name);
		if (statSync(abs).isDirectory()) walk(base, abs, out);
		else if (SCAN_EXT.test(name)) out.push(relative(base, abs).split(/[\\/]/).join('/'));
	}
}

/**
 * Every file the gate reads under `base`, sorted, excluding this script.
 *
 * @param {string} [base]
 * @returns {string[]}
 */
export function vocabularyFiles(base = root) {
	const files = [];
	for (const r of SCAN_ROOTS) walk(base, join(base, r), files);
	for (const f of EXTRA_FILES) if (existsSync(join(base, f))) files.push(f);
	return files.filter((rel) => rel !== SELF).sort();
}

/**
 * Scan every file the gate reads under `base`.
 *
 * @param {string} [base]
 */
export function scanTree(base = root) {
	const files = vocabularyFiles(base);
	const hits = [];
	let allowed = 0;
	for (const rel of files) {
		const result = scanVocabulary(readFileSync(join(base, rel), 'utf8'));
		allowed += result.allowed;
		for (const hit of result.hits) hits.push({ rel, ...hit });
	}
	return { files, hits, allowed };
}

function main() {
	const verbose = process.argv.includes('--verbose');
	const { files, hits, allowed } = scanTree(root);
	const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
	console.log(`check-vocabulary: ${pkg.name}@${pkg.version}`);
	console.log(`  ${files.length} file(s) scanned, ${RULES.reduce((n, r) => n + r.patterns.length, 0)} pattern(s), ${allowed} allow-marked line(s).`);

	if (hits.length) {
		console.error(`\ncheck-vocabulary FAILED (${hits.length} hit(s)):`);
		for (const rule of RULES) {
			const own = hits.filter((h) => h.family === rule.family);
			if (!own.length) continue;
			console.error(`  ${rule.family} - ${rule.hint}`);
			const show = verbose ? own : own.slice(0, 5);
			for (const h of show) console.error(`      ${h.rel}:${h.line}  "${h.term}"`);
			if (!verbose && own.length > 5) console.error(`      ... +${own.length - 5} more (--verbose)`);
		}
		console.error(`  Reword the line, or append \`vocabulary-allow: <reason>\` when the term is genuinely correct.`);
		process.exit(1);
	}

	console.log(`  OK - no borrowed vocabulary or planning references in the published surface.`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
