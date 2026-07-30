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
const verbose = process.argv.includes('--verbose');

// Everything a consumer can read: the shipped tree, the shipped docs, and the
// suites and benches whose describe() strings surface in test output.
const SCAN_ROOTS = ['src', 'test', 'bench'];
const EXTRA_FILES = ['README.md', 'MIGRATION.md', 'CHANGELOG.md'];

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
			/(?:^|\s)\/(?:simplify|code-review|security-review|ultrareview)\b/
		]
	}
];

const ALLOW_RE = /vocabulary-allow:\s*\S/;

/** Recursively collect scannable files under `dir` (root-relative, slash paths). */
function walk(dir, out) {
	if (!existsSync(dir)) return;
	for (const name of readdirSync(dir)) {
		if (SKIP_SEGMENTS.has(name)) continue;
		const abs = join(dir, name);
		if (statSync(abs).isDirectory()) walk(abs, out);
		else if (SCAN_EXT.test(name)) out.push(relative(root, abs).split(/[\\/]/).join('/'));
	}
}

const files = [];
for (const r of SCAN_ROOTS) walk(join(root, r), files);
for (const f of EXTRA_FILES) if (existsSync(join(root, f))) files.push(f);
files.sort();

const hits = [];
let allowed = 0;
for (const rel of files) {
	if (rel === SELF) continue;
	const lines = readFileSync(join(root, rel), 'utf8').split(/\r?\n/);
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		for (const rule of RULES) {
			for (const re of rule.patterns) {
				const m = re.exec(line);
				if (!m) continue;
				if (ALLOW_RE.test(line)) { allowed++; continue; }
				hits.push({ rel, line: i + 1, family: rule.family, hint: rule.hint, term: m[0].trim() });
			}
		}
	}
}

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
