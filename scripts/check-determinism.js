#!/usr/bin/env node
/**
 * Guard that framework source reads the clock, RNG and timers through the
 * injectable runtime module (runtime.js) rather than the raw native primitives.
 * Routing every nondeterministic primitive through one swappable module is what
 * lets a seeded harness replay behavior exactly; a raw call left anywhere is a
 * silent hole that makes a replay diverge.
 *
 * Dependency-free (no eslint), modeled on check-types.js, wired into pretest.
 *
 * Modes:
 *   - default: WARN. Prints a per-file summary of raw call sites and exits 0, so
 *     the guard can land before every call site is routed through the module.
 *   - any file under src/ must be clean: a raw call in it FAILS (exit 1). The
 *     ratchet is a subtree prefix (ENFORCE_EXCEPT carves out dev-only doubles),
 *     so a relocated or new module under src/ is enforced automatically.
 *   - `--strict`: treat every finding as an error (the end state, once the whole
 *     surface is migrated).
 *   - `--verbose`: list every finding, not just per-file counts.
 *
 * A single line may opt out with a trailing `// determinism-allow: <reason>`
 * comment (for the genuinely cosmetic init-time log timers and the like).
 *
 * Most runtime helpers are named so they cannot collide with a native
 * primitive pattern (`now`/`monotonicNow` vs `Date.now`, `randomUuid` vs
 * `randomUUID`, `randomFloat`/`randomU32` vs `Math.random`, `setTimer` /
 * `setIntervalTimer` / `clearTimer` / `clearIntervalTimer` vs the native
 * timers). The one helper whose name matches its native counterpart is
 * `randomBytes`. So a file that imports a primitive name FROM the runtime
 * module is using the seam, and the bare-name pattern for that name is
 * suppressed in that file; a file that imports the same name from
 * `node:crypto` (or calls it on `crypto.`/`globalThis`) still trips. This
 * keeps `randomBytes()` routed through the seam clean while a stray native
 * `randomBytes()` stays a violation.
 *
 * @module scripts/check-determinism
 */
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, resolve, join, relative, basename } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const strict = process.argv.includes('--strict');
const verbose = process.argv.includes('--verbose');

// The only files permitted to touch the native primitives: the runtime module
// is the single binding point. Matched by basename so the per-repo path (files/
// vs shared/) does not matter.
const ALLOW_FILES = new Set(['runtime.js']);

// Enforcement is a subtree prefix: every framework source file under src/ must
// stay clean (the runtime module in ALLOW_FILES is exempt by basename, and any
// dev-only test double in ENFORCE_EXCEPT is too). The ratchet is structural - a
// relocated or newly added module under src/ is enforced automatically, with no
// per-file list to maintain.
const ENFORCE_EXCEPT = new Set([
	// The unit-test platform double seeds ids/timestamps for fixtures; it is a
	// testing helper, never replayed by the deterministic harness, so its raw
	// reads are legitimate. The other testing doubles route through the seam.
	'src/testing/mock-platform.js'
]);

function isEnforced(rel) {
	return rel.startsWith('src/') && !ENFORCE_EXCEPT.has(rel);
}

// Path segments that are never framework runtime source.
const SKIP_SEGMENTS = new Set([
	'node_modules', 'test', 'tests', '__tests__', 'bench', 'benchmarks', 'scripts',
	'fixture', 'fixtures', '.svelte-kit', 'dist', 'build', 'coverage', 'examples',
	'example', 'docs', '.git'
]);
const SKIP_SUFFIX = ['.test.js', '.spec.js', '.config.js', '.config.mjs', '.d.ts'];

// Ordered most-specific-first so a dotted form wins over the bare form on the
// same line (we report one primitive per line).
const PATTERNS = [
	['Date.now', /\bDate\s*\.\s*now\s*\(/],
	['new Date', /\bnew\s+Date\s*\(/],
	['performance.now', /\bperformance\s*\.\s*now\s*\(/],
	['Math.random', /\bMath\s*\.\s*random\s*\(/],
	['crypto.randomUUID', /\bcrypto\s*\.\s*randomUUID\s*\(/],
	['crypto.randomBytes', /\bcrypto\s*\.\s*randomBytes\s*\(/],
	['crypto.randomInt', /\bcrypto\s*\.\s*randomInt\s*\(/],
	['getRandomValues', /\bgetRandomValues\s*\(/],
	['randomUUID', /\brandomUUID\s*\(/],
	['randomBytes', /\brandomBytes\s*\(/],
	['randomInt', /\brandomInt\s*\(/],
	['setTimeout', /\bsetTimeout\s*\(/],
	['setInterval', /\bsetInterval\s*\(/],
	['setImmediate', /\bsetImmediate\s*\(/],
	['clearTimeout', /\bclearTimeout\s*\(/],
	['clearInterval', /\bclearInterval\s*\(/],
	['queueMicrotask', /\bqueueMicrotask\s*\(/]
];

function shouldSkipPath(rel) {
	const segs = rel.split(/[\\/]/);
	if (segs.some((s) => SKIP_SEGMENTS.has(s))) return true;
	if (SKIP_SUFFIX.some((suf) => rel.endsWith(suf))) return true;
	if (ALLOW_FILES.has(basename(rel))) return true;
	return false;
}

function walk(dir, out) {
	for (const entry of readdirSync(dir)) {
		const abs = join(dir, entry);
		let st;
		try { st = statSync(abs); } catch { continue; }
		const rel = relative(root, abs);
		if (st.isDirectory()) {
			if (SKIP_SEGMENTS.has(entry)) continue;
			walk(abs, out);
		} else if ((abs.endsWith('.js') || abs.endsWith('.mjs')) && !shouldSkipPath(rel)) {
			// Normalize to forward slashes so the src/ prefix rule (and the printed
			// report) read the same on Windows and POSIX; a raw backslash path would
			// silently skip enforcement on Windows.
			out.push(rel.split('\\').join('/'));
		}
	}
}

// The bare-name patterns that a runtime-module import of the same name
// legitimately satisfies (the helper IS the seam). Qualified native forms
// (`crypto.*`, `Date.now`, `Math.random`, `performance.now`) are never
// seam-satisfiable and are intentionally absent here.
const SEAM_SUPPRESSIBLE = new Set(['randomBytes', 'randomUUID', 'randomInt', 'getRandomValues']);

// Names imported from the injectable runtime module: a leading `import { ... }`
// whose source path ends in `runtime.js` or the `shared/time.js` re-export.
// Multi-line import blocks are handled by scanning to the closing brace.
function runtimeImportedNames(text) {
	const names = new Set();
	const re = /import\s*\{([^}]*)\}\s*from\s*['"]([^'"]+)['"]/g;
	let mm;
	while ((mm = re.exec(text)) !== null) {
		const source = mm[2];
		if (!/(^|\/)runtime\.js$/.test(source) && !/(^|\/)time\.js$/.test(source)) continue;
		for (const raw of mm[1].split(',')) {
			const part = raw.trim();
			if (!part) continue;
			// Honor `as` aliases: the local name is what appears in call sites.
			const local = /\bas\b/.test(part) ? part.split(/\s+as\s+/)[1].trim() : part;
			if (local) names.add(local);
		}
	}
	return names;
}

function scanFile(rel) {
	const findings = [];
	const text = readFileSync(join(root, rel), 'utf8');
	const seamNames = runtimeImportedNames(text);
	const lines = text.split(/\r?\n/);
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		const trimmed = line.trimStart();
		if (trimmed.startsWith('//') || trimmed.startsWith('*')) continue; // comment line
		if (line.includes('determinism-allow:')) continue; // explicit opt-out
		for (const [name, re] of PATTERNS) {
			if (re.test(line)) {
				// A bare primitive whose name was imported from the runtime seam is
				// the seam helper, not the native primitive - skip it. The qualified
				// native forms are never in SEAM_SUPPRESSIBLE, so `crypto.randomBytes`
				// and the like still trip even in a seam-importing file.
				if (SEAM_SUPPRESSIBLE.has(name) && seamNames.has(name)) break;
				findings.push({ rel, line: i + 1, primitive: name, snippet: trimmed.slice(0, 80) });
				break; // one primitive per line keeps the report readable
			}
		}
	}
	return findings;
}

const files = [];
walk(root, files);
files.sort();

const all = [];
for (const rel of files) all.push(...scanFile(rel));

const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
console.log(`check-determinism: ${pkg.name}@${pkg.version}`);
console.log(`  ${files.length} framework source file(s) scanned, ${all.length} raw native-primitive call site(s) found.`);

const errors = all.filter((f) => strict || isEnforced(f.rel));
const warnings = all.filter((f) => !errors.includes(f));

// Per-file summary so pretest output stays bounded; --verbose lists each site.
const byFile = new Map();
for (const f of warnings) byFile.set(f.rel, (byFile.get(f.rel) || 0) + 1);
if (byFile.size) {
	console.log(`  warn (route these through runtime.js as their area is migrated):`);
	for (const [rel, n] of [...byFile.entries()].sort()) {
		console.log(`    ~ ${rel}: ${n}`);
		if (verbose) for (const f of warnings.filter((w) => w.rel === rel)) {
			console.log(`        ${f.line}: ${f.primitive}  ${f.snippet}`);
		}
	}
}

if (errors.length) {
	console.error(`\ncheck-determinism FAILED (${errors.length} enforced violation(s)):`);
	for (const f of errors) console.error(`  x ${f.rel}:${f.line}  ${f.primitive}  ${f.snippet}`);
	console.error(`  Route these through runtime.js, or annotate the line with "// determinism-allow: <reason>".`);
	process.exit(1);
}

console.log(`  OK - no enforced violations${strict ? '' : ' (warn mode)'}.`);
