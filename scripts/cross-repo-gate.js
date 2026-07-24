#!/usr/bin/env node
/**
 * Prove that the adapter, extensions and realtime heads pack and install as one
 * coherent set, and that their shipped type surface holds up for a real
 * consumer.
 *
 * Nothing else covers this. Each repo's own `npm run check` / `npm test` sees
 * that repo plus whatever its devDependencies pulled from the registry, so the
 * heads are never exercised together before publish. The e2e fixtures show the
 * shape of the hole: `svelte-realtime/test/fixture` installs its own package
 * with `file:../..` but takes `svelte-adapter-uws` and
 * `svelte-adapter-uws-extensions` from the registry, so a local adapter change
 * that breaks realtime stays invisible until both are published.
 *
 * What each rung actually proves, stated plainly because a rung that overstates
 * its reach is worse than no rung:
 *
 *   pack     every head packs as `npm publish` would.
 *   install  the three PACKED heads plus peers install together into one clean
 *            consumer, and the versions that land really are the local ones.
 *            npm resolves peer ranges strictly, so this is where an
 *            incompatible peer floor between the heads fails.
 *   peers    the resolved peer graph carries no problem against a head.
 *   types    a consumer importing every public subpath of the three PACKED
 *            heads typechecks under `strict`.
 *
 * Those four are the cross-head proof: they run against the tarballs. The rest
 * re-run each repo's own suite IN ITS OWN TREE, where the sibling heads come
 * from that tree's node_modules (the registry), NOT from the packed heads:
 *
 *   check / unit / realtime-e2e / extensions-integration
 *
 * They are a sanity net, not a cross-head proof, and their output says so.
 * Running those suites against the packed heads means injecting the tarballs
 * into each tree first, which rewrites trees this harness does not own; that is
 * not done here.
 *
 * Zero-config, from this repo:
 *
 *   node scripts/cross-repo-gate.js
 *
 * The sibling heads are discovered on disk; every knob below is optional.
 *
 *   --repo <path>        Add or override a repo path (repeatable). The package
 *                        name is read from its package.json, so order is free.
 *   --rungs a,b,c        Run only these rungs. Prerequisites are pulled in
 *                        automatically, so `--rungs types` also packs and
 *                        installs.
 *   --quick              Only the rungs that prove the heads work together
 *                        (pack, install, peers, types).
 *   --work-dir <path>    Where tarballs and the consumer are built.
 *                        Default: .cross-repo-gate/ under this repo.
 *   --require-clean      Fail unless every head is a clean git checkout. Use in
 *                        CI, where the gate must describe committed SHAs.
 *   --strict             Treat SKIPPED rungs as failures. Use in CI, where a
 *                        skipped rung is an untested claim.
 *   --clean              Remove the work dir before starting.
 *
 * Exit codes:
 *   0  something was proven and nothing failed (the banner says whether any
 *      rung reached the packed heads).
 *   1  a rung failed, or a rung was skipped under --strict, or nothing was
 *      proven at all because every selected rung skipped.
 *   2  the gate could not run: bad usage, a head not found, or --require-clean
 *      against a tree that is dirty or unreadable.
 *
 * A rung reports PASS only when it can name what it exercised. "Nothing went
 * wrong" is not evidence: an empty failure list from a rung that ran nothing is
 * a SKIP or a FAIL, never a pass.
 *
 * @module scripts/cross-repo-gate
 */
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const NPM = process.platform === 'win32' ? 'npm.cmd' : 'npm';

// The heads, in dependency order: each may only peer on the ones before it.
const HEADS = ['svelte-adapter-uws', 'svelte-adapter-uws-extensions', 'svelte-realtime'];

// What the consumer supplies for itself: the heads' true peers (see each head's
// peerDependencies). `ioredis` is a direct dependency of extensions and `pg` an
// optional one, so npm installs both on its own and neither belongs here.
const CONSUMER_PEERS = {
	'@sveltejs/kit': '^2.0.0',
	svelte: '^5.0.0',
	ws: '^8.0.0'
};

// Declaration packages a real consumer of these surfaces would have.
//
// `pg` ships no declarations of its own and the postgres surface's public .d.ts
// imports from it, so a strict app using that surface needs @types/pg. Without
// it the types rung reports TS7016 against pg rather than against our own
// declarations, which is not what this gate is asking about.
//
// @types/node is load-bearing for the heads themselves, not a convenience: the
// shipped declarations use node globals in exported positions (Buffer in
// extensions' redis/ntp-source, NodeJS.ProcessEnv in the adapter's testing
// surface). The tsconfig below names it explicitly, because nothing else would
// pull it in and the heads would then report an unresolved Buffer as if it were
// their own bug.
const CONSUMER_TYPES = {
	'@types/pg': '^8.0.0',
	'@types/node': '^22.0.0'
};

const TYPESCRIPT_RANGE = '^5.6.0';

// - argv ------------------------------------------------------------

function parseArgs(argv) {
	const opts = { repos: [], rungs: null, quick: false, workDir: null, requireClean: false, strict: false, clean: false };
	const value = (i, flag) => {
		if (i >= argv.length) die(`${flag} needs a value`);
		return argv[i];
	};
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (a === '--repo') opts.repos.push(resolve(value(++i, '--repo')));
		else if (a === '--rungs') opts.rungs = value(++i, '--rungs').split(',').map((s) => s.trim()).filter(Boolean);
		else if (a === '--work-dir') opts.workDir = resolve(value(++i, '--work-dir'));
		else if (a === '--quick') opts.quick = true;
		else if (a === '--require-clean') opts.requireClean = true;
		else if (a === '--strict') opts.strict = true;
		else if (a === '--clean') opts.clean = true;
		else if (a === '--help' || a === '-h') opts.help = true;
		else die(`unknown argument: ${a} (try --help)`);
	}
	if (opts.rungs && !opts.rungs.length) die('--rungs needs at least one rung name');
	return opts;
}

function die(msg) {
	console.error(`cross-repo-gate: ${msg}`);
	process.exit(2);
}

/** Print the header block without its comment markers. */
function help() {
	const src = readFileSync(fileURLToPath(import.meta.url), 'utf8');
	const header = src.slice(src.indexOf('/**') + 3, src.indexOf('*/'));
	console.log(
		header
			.split(/\r?\n/)
			.map((l) => l.replace(/^\s*\* ?/, ''))
			.filter((l) => !l.startsWith('@module'))
			.join('\n')
			.trim()
	);
}

// - process ---------------------------------------------------------

/**
 * Run an executable and capture it. Never throws on a non-zero exit: the rung
 * decides what a failure means. A spawn error is surfaced in `stderr` rather
 * than swallowed, otherwise a rung reports a bare "failed" with no cause.
 */
function run(cmd, args, cwd, { env, shell = false } = {}) {
	const r = spawnSync(cmd, shell ? args.map(quoteArg) : args, {
		cwd,
		encoding: 'utf8',
		shell,
		maxBuffer: 64 * 1024 * 1024,
		env: env ? { ...process.env, ...env } : process.env
	});
	if (r.error) {
		const missing = r.error.code === 'ENOENT';
		return { code: missing ? 127 : (r.status ?? 1), stdout: r.stdout ?? '', stderr: `${cmd}: ${r.error.message}`, missing };
	}
	return { code: r.status ?? 1, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
}

/**
 * Run npm. On Windows npm is a .cmd shim and node refuses to spawn one without
 * a shell, so npm goes through the shell there and its arguments are quoted for
 * cmd.exe.
 */
function npm(args, cwd, opts) {
	return run(NPM, args, cwd, { ...opts, shell: process.platform === 'win32' });
}

function quoteArg(arg) {
	const s = String(arg);
	return /[\s"^&|<>()]/.test(s) ? `"${s.replace(/"/g, '\\"')}"` : s;
}

/**
 * Delete a directory tree. Windows hands back a transient EPERM when anything
 * still holds a handle inside it (a virus scanner sweeping a fresh
 * node_modules, an npm that has just exited), so this retries rather than
 * dying with a stack trace on a directory that is about to be free.
 */
function removeTree(dir) {
	rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}

function tail(text, lines = 25) {
	return String(text || '').trimEnd().split(/\r?\n/).slice(-lines).join('\n');
}

function indent(text, pad) {
	return String(text || '').split(/\r?\n/).map((l) => pad + l).join('\n');
}

// - repo discovery --------------------------------------------------

function readPkg(dir) {
	const p = join(dir, 'package.json');
	if (!existsSync(p)) return null;
	try {
		return JSON.parse(readFileSync(p, 'utf8'));
	} catch {
		return null;
	}
}

/**
 * Locate the heads. Explicit --repo paths win; otherwise each name is looked up
 * as a sibling of this repo (and this repo itself counts).
 */
function discoverRepos(explicit) {
	/** @type {Record<string, { dir: string, pkg: any }>} */
	const found = {};

	for (const dir of explicit) {
		const pkg = readPkg(dir);
		if (!pkg) die(`--repo ${dir} has no readable package.json`);
		if (!HEADS.includes(pkg.name)) die(`--repo ${dir} is "${pkg.name}", not one of: ${HEADS.join(', ')}`);
		found[pkg.name] = { dir, pkg };
	}

	const parent = resolve(HERE, '..');
	for (const name of HEADS) {
		if (found[name]) continue;
		for (const candidate of [HERE, join(parent, name)]) {
			const pkg = readPkg(candidate);
			if (pkg && pkg.name === name) {
				found[name] = { dir: candidate, pkg };
				break;
			}
		}
	}

	const missing = HEADS.filter((n) => !found[n]);
	if (missing.length) {
		die(
			`could not find ${missing.join(', ')} next to ${HERE}.\n` +
				`  Expected each as a sibling directory, or pass --repo <path> for each.`
		);
	}
	return found;
}

/**
 * Describe a repo's git state so the summary says exactly what was gated. An
 * unreadable git state counts as not-clean: a gate that cannot see the tree
 * must not claim the tree is clean.
 */
function gitState(dir) {
	const sha = run('git', ['rev-parse', '--short', 'HEAD'], dir);
	if (sha.code !== 0) return { sha: 'unknown', dirty: false, unknown: true, files: 0 };
	const status = run('git', ['status', '--porcelain'], dir);
	if (status.code !== 0) return { sha: sha.stdout.trim(), dirty: false, unknown: true, files: 0 };
	const files = String(status.stdout || '').trim().split(/\r?\n/).filter(Boolean).length;
	return { sha: sha.stdout.trim(), dirty: files > 0, unknown: false, files };
}

// - rung results ----------------------------------------------------

const PASS = 'PASS';
const FAIL = 'FAIL';
const SKIP = 'SKIP';

/** @type {Array<{ name: string, status: string, detail: string, output?: string }>} */
const results = [];
let state = null;

function record(name, status, detail, output) {
	results.push({ name, status, detail, output });
	const mark = status === PASS ? 'ok  ' : status === FAIL ? 'FAIL' : 'skip';
	console.log(`  [${mark}] ${name}: ${detail}`);
	if (status === FAIL && output) console.log(indent(tail(output), '        '));
}

// - rungs -----------------------------------------------------------

/**
 * Pack each head exactly as `npm publish` would. This is what makes the rest
 * meaningful: a file missing from the `files` allowlist resolves fine in the
 * source tree and 404s for a real consumer, and only a tarball shows that.
 */
function rungPack() {
	const dest = join(state.workDir, 'tarballs');
	mkdirSync(dest, { recursive: true });

	for (const name of HEADS) {
		const { dir } = state.repos[name];
		const r = npm(['pack', '--json', '--pack-destination', dest], dir);
		if (r.code !== 0) {
			record('pack', FAIL, `npm pack failed for ${name}`, r.stderr || r.stdout);
			return false;
		}
		let filename;
		try {
			const parsed = JSON.parse(r.stdout);
			filename = Array.isArray(parsed) ? parsed[0].filename : parsed.filename;
		} catch {
			record('pack', FAIL, `could not parse npm pack output for ${name}`, r.stdout);
			return false;
		}
		const tarball = join(dest, filename);
		if (!existsSync(tarball)) {
			record('pack', FAIL, `npm pack reported ${filename} for ${name} but it is not on disk`, '');
			return false;
		}
		state.tarballs[name] = tarball;
	}

	record('pack', PASS, HEADS.map((n) => `${n}@${state.repos[n].pkg.version}`).join(', '));
	return true;
}

/**
 * Install the three tarballs into one clean consumer. npm resolves peer ranges
 * strictly, so an unsatisfiable floor between the local heads fails outright.
 */
function rungInstall() {
	const consumer = join(state.workDir, 'consumer');
	mkdirSync(consumer, { recursive: true });
	state.consumer = consumer;

	const deps = { ...CONSUMER_PEERS };
	for (const name of HEADS) deps[name] = `file:${state.tarballs[name].replace(/\\/g, '/')}`;

	writeFileSync(
		join(consumer, 'package.json'),
		JSON.stringify(
			{
				name: 'cross-repo-gate-consumer',
				private: true,
				version: '0.0.0',
				type: 'module',
				dependencies: deps,
				devDependencies: { typescript: TYPESCRIPT_RANGE, ...CONSUMER_TYPES }
			},
			null,
			2
		) + '\n'
	);

	// Start from nothing. A repeat run rewrites each tarball under the SAME
	// filename with the same version and the same `file:` spec, so npm sees no
	// spec change and may keep what it already extracted - which would quietly
	// gate the PREVIOUS heads. Dropping the lock alone does not settle that; the
	// installed tree has to go too.
	rmSync(join(consumer, 'package-lock.json'), { force: true });
	removeTree(join(consumer, 'node_modules'));

	const r = npm(['install', '--no-audit', '--no-fund', '--loglevel=error'], consumer);
	if (r.code !== 0) {
		record('install', FAIL, 'the three tarballs do not install together', r.stderr || r.stdout);
		return false;
	}

	// Prove the heads resolved to the LOCAL tarballs. A registry copy would make
	// every later rung a statement about published code instead. Nested copies
	// matter too: when a peer floor outruns a sibling, npm keeps the hoisted one
	// and buries a second copy under the dependent, which a top-level-only check
	// would never see.
	const bad = [];
	for (const name of HEADS) {
		const installed = readPkg(join(consumer, 'node_modules', name));
		if (!installed) {
			bad.push(`${name} is not installed`);
			continue;
		}
		if (installed.version !== state.repos[name].pkg.version) {
			bad.push(`${name} resolved to ${installed.version}, expected the local ${state.repos[name].pkg.version}`);
		}
		for (const other of HEADS) {
			if (other === name) continue;
			const nested = readPkg(join(consumer, 'node_modules', name, 'node_modules', other));
			if (nested) bad.push(`${name} carries a nested copy of ${other}@${nested.version}, so it is not using the local head`);
		}
	}
	if (bad.length) {
		record('install', FAIL, 'the installed tree does not match the local heads', bad.join('\n'));
		return false;
	}

	record('install', PASS, `${HEADS.length} packed heads + peers installed clean into ${relative(state.workDir, consumer) || 'consumer'}`);
	return true;
}

/**
 * Report the peer graph npm resolved. `install` already fails hard on an
 * unsatisfiable range, so this rung is here for what npm tolerates: a missing
 * or invalid peer link it downgrades to a warning.
 */
function rungPeers() {
	const r = npm(['ls', '--json', '--all'], state.consumer);
	let tree;
	try {
		tree = JSON.parse(r.stdout);
	} catch {
		record('peers', FAIL, 'could not parse npm ls output', r.stdout || r.stderr);
		return false;
	}

	// Classify by the dependency PATH, never by a substring of the message: npm
	// embeds absolute paths in problem strings and the work dir sits inside the
	// extensions checkout, so every message would contain a head's name.
	//
	// The root entry is dropped: `npm ls --json` hangs a GLOBAL aggregate of
	// every problem in the tree off the root node, so a root entry is always a
	// duplicate of a per-node one and carries no path to say whose it is.
	// Keeping it would blame third-party noise on a head and count every real
	// problem twice.
	const problems = collectProblems(tree).filter((p) => p.path.length > 0);
	const ours = problems.filter((p) => p.path.some((n) => HEADS.includes(n)));
	const other = problems.filter((p) => !p.path.some((n) => HEADS.includes(n)));

	if (ours.length) {
		const shown = ours.map((p) => `${p.path.join(' > ') || '(root)'}: ${p.problem}`);
		record('peers', FAIL, `${ours.length} peer problem(s) involving the heads`, shown.join('\n'));
		return false;
	}
	const note = other.length ? ` (${other.length} unrelated third-party peer warning(s))` : '';
	record('peers', PASS, `peer graph resolves with no problem against any head${note}`);
	return true;
}

/** Walk an `npm ls --json` tree, keeping each problem with the path it sits on. */
function collectProblems(node, path = [], out = []) {
	if (!node || typeof node !== 'object') return out;
	for (const problem of node.problems || []) out.push({ path, problem: String(problem) });
	for (const [name, child] of Object.entries(node.dependencies || {})) {
		collectProblems(child, [...path, name], out);
	}
	return out;
}

/**
 * Typecheck a consumer that imports every public subpath of the packed heads,
 * with `strict` on.
 *
 * `check-types.js` proves a types condition points at a .d.ts that exists and
 * ships. It cannot prove that .d.ts is VALID: a declaration referencing an
 * undefined type parameter satisfies it and still hands a strict consumer a
 * TS2304. Only a real tsc over the installed tarballs closes that.
 *
 * The import list is generated per head from its packed `exports` map, and a
 * head contributing no subpath fails the rung: a restructure that hid a whole
 * surface would otherwise leave it unchecked and still green.
 */
function rungTypes() {
	/** @type {Record<string, string[]>} */
	const perHead = {};
	const specifiers = [];
	for (const name of HEADS) {
		const pkg = readPkg(join(state.consumer, 'node_modules', name));
		if (!pkg) {
			record('types', FAIL, `${name} is not installed in the consumer`, '');
			return false;
		}
		perHead[name] = [];
		for (const sub of Object.keys(pkg.exports || {})) {
			if (!sub.startsWith('.')) continue; // a condition, not a subpath
			if (sub.includes('*')) continue; // a pattern has no single specifier
			perHead[name].push(sub === '.' ? name : `${name}/${sub.slice(2)}`);
		}
		specifiers.push(...perHead[name]);
	}

	const empty = HEADS.filter((n) => !perHead[n].length);
	if (empty.length) {
		record(
			'types',
			FAIL,
			`no importable subpath found for: ${empty.join(', ')}`,
			'Every head must expose at least one exports subpath, or its declarations go unchecked while this rung stays green.'
		);
		return false;
	}

	const lines = [
		'// Generated by scripts/cross-repo-gate.js. Imports every public subpath',
		'// of the packed heads so tsc checks the shipped declarations.',
		''
	];
	specifiers.forEach((spec, i) => lines.push(`import type * as m${i} from '${spec}';`));
	lines.push('');
	lines.push(`export type Loaded = [${specifiers.map((_, i) => `typeof m${i}`).join(', ')}];`);
	lines.push('');
	writeFileSync(join(state.consumer, 'consumer.ts'), lines.join('\n'));

	writeFileSync(
		join(state.consumer, 'tsconfig.json'),
		JSON.stringify(
			{
				compilerOptions: {
					strict: true,
					noEmit: true,
					target: 'es2022',
					module: 'esnext',
					moduleResolution: 'bundler',
					// Checking the shipped .d.ts files is the whole point, so lib
					// checking stays on. An error in a third-party declaration is
					// still a reason this surface does not typecheck - our own
					// .d.ts files are what pull those types in - so it fails the
					// rung too, just under a message that does not blame a head.
					skipLibCheck: false,
					// Named, not empty and not automatic: the heads use node
					// globals in exported positions, so `node` has to be present
					// or their declarations fail for a reason that is ours only in
					// appearance. Everything else stays out, so an unrelated
					// @types package in the tree cannot colour the verdict.
					types: ['node']
				},
				files: ['consumer.ts']
			},
			null,
			2
		) + '\n'
	);

	const r = npm(['exec', '--no', '--', 'tsc', '--noEmit', '--pretty', 'false'], state.consumer);
	const errors = parseTscErrors(r.stdout + '\n' + r.stderr);

	// Ours: a diagnostic inside a head's installed tree, OR any diagnostic
	// against the generated consumer. The consumer imports nothing but the
	// heads, so an unresolved specifier there IS a head's defect - that is the
	// missing-from-`files` case this gate exists to catch, and tsc reports it
	// against the importing file rather than against the package.
	const ours = errors.filter((e) => {
		const f = e.file.replace(/\\/g, '/');
		return HEADS.some((h) => f.includes(`node_modules/${h}/`)) || f.endsWith('consumer.ts');
	});
	const other = errors.filter((e) => !ours.includes(e));

	if (ours.length) {
		const shown = ours.slice(0, 20).map((e) => `${e.file}(${e.line}): ${e.code} ${e.message}`);
		if (ours.length > shown.length) shown.push(`... and ${ours.length - shown.length} more`);
		record('types', FAIL, `${ours.length} type error(s) in the shipped surface of the heads (${specifiers.length} subpaths imported)`, shown.join('\n'));
		return false;
	}
	// A non-zero tsc with nothing attributable to a head is still a failure: the
	// rung cannot claim the surface typechecks when tsc did not agree.
	if (r.code !== 0) {
		const detail = other.length
			? `tsc reported ${other.length} error(s), none against a head`
			: 'tsc exited non-zero with no parseable diagnostic';
		record('types', FAIL, detail, other.length ? other.map((e) => `${e.file}(${e.line}): ${e.code} ${e.message}`).join('\n') : tail(r.stdout + '\n' + r.stderr));
		return false;
	}
	const breakdown = HEADS.map((n) => `${n} ${perHead[n].length}`).join(', ');
	record('types', PASS, `${specifiers.length} public subpaths typecheck strict from the packed tarballs (${breakdown})`);
	return true;
}

/** tsc --pretty false emits `path(line,col): error TSxxxx: message`. */
function parseTscErrors(text) {
	const out = [];
	for (const line of String(text || '').split(/\r?\n/)) {
		const m = /^(.+?)\((\d+),(\d+)\):\s+error\s+(TS\d+):\s+(.*)$/.exec(line.trim());
		if (m) out.push({ file: m[1], line: Number(m[2]), col: Number(m[3]), code: m[4], message: m[5] });
	}
	return out;
}

/**
 * Run one npm script in each head's OWN tree. The siblings there come from that
 * tree's node_modules, i.e. the registry, so this rung says nothing about the
 * packed heads and its output says so. A rung that ran nowhere is a SKIP: an
 * empty failure list is not a pass.
 */
function rungPerRepo(name, script, what) {
	const ran = [];
	const failures = [];
	for (const head of HEADS) {
		const { dir, pkg } = state.repos[head];
		if (!pkg.scripts || !pkg.scripts[script]) continue;
		ran.push(head);
		const r = npm(['run', script, '--silent'], dir);
		if (r.code !== 0) {
			failures.push(`${head}: npm run ${script} exited ${r.code}\n${indent(tail(r.stdout + '\n' + r.stderr, 15), '    ')}`);
		}
	}
	if (!ran.length) {
		record(name, SKIP, `no head declares an npm "${script}" script`);
		return null;
	}
	if (failures.length) {
		record(name, FAIL, `${failures.length} of ${ran.length} head(s) failed`, failures.join('\n\n'));
		return false;
	}
	const missing = HEADS.filter((h) => !ran.includes(h));
	const note = missing.length ? `; no "${script}" script in ${missing.join(', ')}` : '';
	record(name, PASS, `${what} in ${ran.length}/${HEADS.length} heads (${ran.join(', ')}), each against its own installed deps${note}`);
	return true;
}

/**
 * Realtime's browser e2e, in realtime's own tree. Its fixture pins the sibling
 * heads from the registry, so this does NOT exercise the packed heads. No
 * browser is a SKIP: a missing browser is not evidence of a working head.
 */
function rungRealtimeE2E() {
	const { dir, pkg } = state.repos['svelte-realtime'];
	if (!pkg.scripts?.['test:e2e']) {
		record('realtime-e2e', SKIP, 'svelte-realtime declares no test:e2e script');
		return null;
	}
	if (npm(['exec', '--no', '--', 'playwright', '--version'], dir).code !== 0) {
		record('realtime-e2e', SKIP, 'playwright is unavailable in svelte-realtime');
		return null;
	}
	const r = npm(['run', 'test:e2e', '--silent'], dir);
	if (r.code !== 0) {
		record('realtime-e2e', FAIL, 'realtime e2e failed in its own tree', r.stdout + '\n' + r.stderr);
		return false;
	}
	record('realtime-e2e', PASS, 'realtime e2e green in its own tree (its fixture resolves the sibling heads from the registry, not from the packed heads)');
	return true;
}

/**
 * Extensions integration, in the extensions tree. Each suite's globalSetup
 * brings its own backend up with docker compose on a dedicated host port
 * (test/integration/global-setup-*.js), so the only precondition is docker.
 * Probing a fixed port guesses wrong in both directions: a false negative skips
 * a suite that would have run, and a false positive names a backend the suite
 * never talks to.
 *
 * The probe is `docker info`, which reaches the daemon. `docker compose
 * version` answers from the CLI plugin alone, so an installed-but-stopped
 * engine would pass the precondition and then fail every suite on connect,
 * turning "docker is not running" into a head's failure.
 *
 * No docker is a SKIP, never a pass, and every suite runs or none does - a
 * partial run reported as PASS would read as "the backends were exercised".
 */
function rungExtensionsIntegration() {
	const { dir, pkg } = state.repos['svelte-adapter-uws-extensions'];
	const scripts = ['test:integration:redis', 'test:integration:postgres'].filter((s) => pkg.scripts?.[s]);

	if (!scripts.length) {
		record('extensions-integration', SKIP, 'svelte-adapter-uws-extensions declares no integration scripts');
		return null;
	}
	if (run('docker', ['info'], state.workDir).code !== 0) {
		record('extensions-integration', SKIP, 'the docker daemon is unreachable and the integration suites bring their own backends up with it');
		return null;
	}

	const failures = [];
	for (const script of scripts) {
		const r = npm(['run', script, '--silent'], dir);
		if (r.code !== 0) failures.push(`${script} exited ${r.code}\n${indent(tail(r.stdout + '\n' + r.stderr, 15), '    ')}`);
	}
	if (failures.length) {
		record('extensions-integration', FAIL, `${failures.length} of ${scripts.length} integration suite(s) failed`, failures.join('\n\n'));
		return false;
	}
	record('extensions-integration', PASS, `${scripts.length} integration suite(s) green in the extensions tree, against its own installed deps (${scripts.join(', ')})`);
	return true;
}

// - main ------------------------------------------------------------

const RUNGS = [
	{ name: 'pack', quick: true, needs: [], run: rungPack },
	{ name: 'install', quick: true, needs: ['pack'], run: rungInstall },
	{ name: 'peers', quick: true, needs: ['install'], run: rungPeers },
	{ name: 'types', quick: true, needs: ['install'], run: rungTypes },
	{ name: 'check', quick: false, needs: [], run: () => rungPerRepo('check', 'check', 'own check passes') },
	{ name: 'unit', quick: false, needs: [], run: () => rungPerRepo('unit', 'test', 'own unit suite passes') },
	{ name: 'realtime-e2e', quick: false, needs: [], run: rungRealtimeE2E },
	{ name: 'extensions-integration', quick: false, needs: [], run: rungExtensionsIntegration }
];

/** Expand a selection to include what it depends on, keeping rung order. */
function withPrerequisites(names) {
	const byName = new Map(RUNGS.map((r) => [r.name, r]));
	const wanted = new Set();
	const add = (n) => {
		if (wanted.has(n)) return;
		wanted.add(n);
		for (const need of byName.get(n).needs) add(need);
	};
	for (const n of names) add(n);
	return RUNGS.filter((r) => wanted.has(r.name));
}

function main() {
	const opts = parseArgs(process.argv.slice(2));
	if (opts.help) {
		help();
		process.exit(0);
	}

	const workDir = opts.workDir || join(HERE, '.cross-repo-gate');
	if (opts.clean) removeTree(workDir);
	mkdirSync(workDir, { recursive: true });

	const repos = discoverRepos(opts.repos);
	state = { repos, workDir, tarballs: {}, consumer: null };

	console.log('cross-repo gate: adapter + extensions + realtime, packed and installed as one set\n');
	const unclean = [];
	for (const name of HEADS) {
		const g = gitState(repos[name].dir);
		if (g.unknown) unclean.push(`${name} (git state unreadable)`);
		else if (g.dirty) unclean.push(`${name} (${g.files} uncommitted file(s))`);
		const suffix = g.unknown ? ' git state unreadable' : g.dirty ? ` +${g.files} uncommitted` : '';
		console.log(`  ${name.padEnd(30)} ${String(repos[name].pkg.version).padEnd(16)} ${g.sha}${suffix}`);
	}
	console.log(`  work dir: ${workDir}\n`);

	if (unclean.length && opts.requireClean) {
		console.error(`cross-repo-gate: --require-clean, but: ${unclean.join(', ')}`);
		process.exit(2);
	}
	if (unclean.length) console.log(`  note: gating WORKING TREES, not committed SHAs: ${unclean.join(', ')}\n`);

	let selected = RUNGS;
	if (opts.rungs) {
		const known = new Set(RUNGS.map((r) => r.name));
		for (const n of opts.rungs) if (!known.has(n)) die(`unknown rung: ${n} (known: ${[...known].join(', ')})`);
		selected = withPrerequisites(opts.rungs);
	}
	if (opts.quick) selected = selected.filter((r) => r.quick);
	if (!selected.length) {
		die(
			'that selection runs no rungs' +
				(opts.quick ? ` (--quick keeps only: ${RUNGS.filter((r) => r.quick).map((r) => r.name).join(', ')})` : '') +
				'.\n  Refusing to report a pass for a gate that ran nothing.'
		);
	}

	console.log('rungs:');
	const done = new Set();
	for (const rung of selected) {
		const unmet = rung.needs.filter((n) => !done.has(n));
		if (unmet.length) {
			record(rung.name, SKIP, `prerequisite rung did not pass: ${unmet.join(', ')}`);
			continue;
		}
		if (rung.run() === true) done.add(rung.name);
	}

	const failed = results.filter((r) => r.status === FAIL);
	const skipped = results.filter((r) => r.status === SKIP);
	const passed = results.filter((r) => r.status === PASS);

	console.log('\nsummary');
	console.log(`  passed:  ${passed.length ? passed.map((r) => r.name).join(', ') : 'none'}`);
	if (skipped.length) console.log(`  SKIPPED: ${skipped.map((r) => `${r.name} (${r.detail})`).join('; ')}`);
	if (failed.length) console.log(`  FAILED:  ${failed.map((r) => r.name).join(', ')}`);

	if (failed.length) {
		console.log('\ncross-repo gate: FAILED');
		process.exit(1);
	}
	if (skipped.length && opts.strict) {
		console.log('\ncross-repo gate: INCOMPLETE (--strict: a skipped rung is an untested claim)');
		process.exit(1);
	}
	if (!passed.length) {
		console.log('\ncross-repo gate: INCOMPLETE (nothing was proven)');
		process.exit(1);
	}

	// The banner is the line CI and people actually read, so it must not imply a
	// cross-head proof that never ran: without a tarball rung, all that passed
	// is each tree checking itself.
	const crossHead = RUNGS.filter((r) => r.quick).map((r) => r.name);
	const proved = passed.some((p) => crossHead.includes(p.name));
	const qualifier = proved ? '' : ' (own-tree rungs only; no rung exercised the packed heads)';

	if (skipped.length) {
		console.log(`\ncross-repo gate: passed what it ran, INCOMPLETE (see SKIPPED above)${qualifier}`);
		process.exit(0);
	}
	console.log(`\ncross-repo gate: PASSED${qualifier}`);
	process.exit(0);
}

main();
