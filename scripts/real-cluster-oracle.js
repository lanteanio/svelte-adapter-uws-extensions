#!/usr/bin/env node
/**
 * Run the real-cluster delivery oracle against packed or published packages
 * and write a machine-readable verdict.
 *
 * What it does, in order:
 *
 *   1. packs this repository as `npm publish` would, unless --extensions names
 *      a tarball or a published version;
 *   2. installs extensions, the adapter and realtime into a throwaway consumer,
 *      together with the adapter's pinned native uWebSockets.js;
 *   3. brings up the integration Redis Cluster (three masters, three replicas,
 *      test/integration/docker-compose.cluster.yml) unless an external cluster
 *      is named;
 *   4. runs scripts/fixtures/real-cluster-oracle.mjs from the consumer: two
 *      native uWebSockets.js servers, the realtime message hook and two
 *      extensions pub/sub buses relaying through the cluster to local and
 *      remote WebSockets, one pair without and one pair with the batch
 *      capability;
 *   5. tears the stack down, checks that no container is left, and writes the
 *      result JSON.
 *
 * The scenario has five cases: setup, cluster-topology, single-fanout (six
 * ordered publishes), batch-fanout (one six-member publishBatched plus one
 * ordered barrier) and cleanup. A pass means each of the four sockets received
 * exactly thirteen values, in order, with no duplicate.
 *
 * Zero-config, from this repo (Docker required):
 *
 *   node scripts/real-cluster-oracle.js
 *
 * Every knob is optional:
 *
 *   --extensions <tgz|spec> svelte-adapter-uws-extensions to install: a tarball
 *                          path or a published version. Default: pack this
 *                          checkout.
 *   --realtime <tgz|spec>  svelte-realtime to install. Default: next.
 *   --adapter <tgz|spec>   svelte-adapter-uws to install. Default: the exact
 *                          version this checkout installed, verified against
 *                          the integrity in package-lock.json.
 *   --work-dir <path>      Where tarballs, the consumer and the result go.
 *                          Default: .real-cluster-oracle/ under this repo.
 *   --out <file>           Result JSON path. Default: <work-dir>/result.json.
 *
 * Environment:
 *
 *   ORACLE_CLUSTER_NODES   JSON array of {host, port} seeds of an EXTERNAL
 *                          cluster. When set, no Docker stack is started.
 *   ORACLE_CLUSTER_NAT_MAP JSON natMap for that cluster, when its announced
 *                          addresses need translating.
 *   ORACLE_REDIS_USERNAME, ORACLE_REDIS_PASSWORD
 *                          Credentials for that cluster; never written out.
 *   INTEGRATION_COMPOSE_PROJECT, INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE,
 *   INTEGRATION_REDIS_CLUSTER_SUBNET
 *                          Isolation for the Docker stack. Defaults:
 *                          svelte-adapter-uws-extensions-oracle, 57200 and
 *                          10.249.2, apart from the integration lanes.
 *
 * Exit codes:
 *
 *   0  every case passed and the stack is gone.
 *   1  the scenario ran and a case failed or did not run.
 *   2  the oracle could not run or could not remove its stack: bad usage, an
 *      input, pack, install, cluster or teardown failure. The result file
 *      says which step failed.
 *   130 interrupted; the stack is removed and the result file written first.
 *
 * @module scripts/real-cluster-oracle
 */
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { closeSync, copyFileSync, existsSync, mkdirSync, openSync, readFileSync, readSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const NPM = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const SCENARIO = join(HERE, 'scripts', 'fixtures', 'real-cluster-oracle.mjs');
const CLUSTER_SETUP = join(HERE, 'test', 'integration', 'global-setup-cluster.js');
const CLUSTER_COMPOSE = join(HERE, 'test', 'integration', 'docker-compose.cluster.yml');
const SCENARIO_TIMEOUT_MS = 120000;

// The consumer supplies the peers of the three packages.
const CONSUMER_PEERS = { '@sveltejs/kit': '^2.0.0', svelte: '^5.0.0', ws: '^8.0.0' };

const ISOLATION_DEFAULTS = {
	INTEGRATION_COMPOSE_PROJECT: 'svelte-adapter-uws-extensions-oracle',
	INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE: '57200',
	INTEGRATION_REDIS_CLUSTER_SUBNET: '10.249.2'
};

export class StepError extends Error {
	constructor(step, message, output) {
		super(message);
		this.step = step;
		this.output = output;
	}
}

// - argv ------------------------------------------------------------

function parseArgs(argv) {
	const opts = { extensions: null, realtime: null, adapter: null, workDir: null, out: null, help: false };
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];
		const value = () => {
			if (i + 1 >= argv.length) usage(`${arg} needs a value`);
			return argv[++i];
		};
		if (arg === '--extensions') opts.extensions = value();
		else if (arg === '--realtime') opts.realtime = value();
		else if (arg === '--adapter') opts.adapter = value();
		else if (arg === '--work-dir') opts.workDir = resolve(value());
		else if (arg === '--out') opts.out = resolve(value());
		else if (arg === '--help' || arg === '-h') opts.help = true;
		else usage(`unknown argument: ${arg} (try --help)`);
	}
	return opts;
}

function usage(message) {
	console.error(`real-cluster-oracle: ${message}`);
	process.exit(2);
}

function help() {
	const src = readFileSync(fileURLToPath(import.meta.url), 'utf8');
	const header = src.slice(src.indexOf('/**') + 3, src.indexOf('*/'));
	console.log(header.split(/\r?\n/).map((l) => l.replace(/^\s*\* ?/, '')).filter((l) => !l.startsWith('@module')).join('\n').trim());
}

// - process ---------------------------------------------------------

function quoteArg(arg) {
	const s = String(arg);
	return /[\s"^&|<>()]/.test(s) ? `"${s.replace(/"/g, '\\"')}"` : s;
}

function run(cmd, args, cwd, { env, shell = false, timeout } = {}) {
	// Through a shell the command goes as one quoted line.
	const r = spawnSync(shell ? [cmd, ...args].map(quoteArg).join(' ') : cmd, shell ? [] : args, {
		cwd,
		encoding: 'utf8',
		shell,
		timeout,
		maxBuffer: 64 * 1024 * 1024,
		env: env ? { ...process.env, ...env } : process.env
	});
	if (r.error) return { code: r.status ?? 1, stdout: r.stdout ?? '', stderr: `${cmd}: ${r.error.message}\n${r.stderr ?? ''}`, timedOut: r.error.code === 'ETIMEDOUT' };
	return { code: r.status ?? 1, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
}

// npm is a .cmd shim on Windows, which node only spawns through a shell.
function npm(args, cwd) {
	return run(NPM, args, cwd, { shell: process.platform === 'win32' });
}

function tail(text, lines = 40) {
	return String(text || '').trimEnd().split(/\r?\n/).slice(-lines).join('\n');
}

function readJson(path) {
	try {
		return JSON.parse(readFileSync(path, 'utf8'));
	} catch {
		return null;
	}
}

function digest(path) {
	const bytes = readFileSync(path);
	return {
		sha256: createHash('sha256').update(bytes).digest('hex'),
		integrity: 'sha512-' + createHash('sha512').update(bytes).digest('base64')
	};
}

function gitState() {
	const sha = run('git', ['rev-parse', 'HEAD'], HERE);
	const status = run('git', ['status', '--porcelain'], HERE);
	if (sha.code !== 0 || status.code !== 0) return { commit: null, dirty: null };
	return { commit: sha.stdout.trim(), dirty: status.stdout.trim().length > 0 };
}

// - result paths ----------------------------------------------------

/**
 * Name a path the way the result file does: relative to the work dir when it
 * lies inside it, otherwise by its file name alone.
 */
export function portablePath(path, workDir) {
	const inside = relative(workDir, path);
	if (inside && !inside.startsWith('..') && !isAbsolute(inside)) return inside.split(sep).join('/');
	if (!inside) return '.';
	return basename(path);
}

/**
 * Replace the machine's directories in every string of the result: the work
 * dir, this checkout and the home directory. The file then names no user path,
 * also inside captured npm or Docker output.
 */
export function scrubPaths(value, { workDir, repo = HERE, home = homedir() }) {
	const roots = [[workDir, '<work-dir>'], [repo, '<repo>'], [home, '~']]
		.filter(([path]) => path)
		.flatMap(([path, label]) => [[path, label], [path.split(sep).join('/'), label]])
		.sort((a, b) => b[0].length - a[0].length);
	const flags = process.platform === 'win32' ? 'gi' : 'g';
	const patterns = roots.map(([path, label]) => [new RegExp(path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), flags), label]);
	const walk = (item) => {
		if (typeof item === 'string') return patterns.reduce((text, [pattern, label]) => text.replace(pattern, label), item);
		if (Array.isArray(item)) return item.map(walk);
		if (item && typeof item === 'object') return Object.fromEntries(Object.entries(item).map(([key, entry]) => [key, walk(entry)]));
		return item;
	};
	return walk(value);
}

// - inputs ----------------------------------------------------------

function isGzip(path) {
	const fd = openSync(path, 'r');
	try {
		const head = Buffer.alloc(2);
		return readSync(fd, head, 0, 2, 0) === 2 && head[0] === 0x1f && head[1] === 0x8b;
	} finally {
		closeSync(fd);
	}
}

/**
 * Describe one package input. A value naming an existing path, or ending in
 * .tgz / .tar.gz, must be a gzip tarball and is installed by file: spec and
 * identified by its digest. Anything else is an npm version, tag or range,
 * identified by the registry integrity the consumer lock records.
 */
export function describeSource(name, value, workDir) {
	const pathLike = /\.(tgz|tar\.gz)$/i.test(value) || existsSync(value);
	if (pathLike) {
		const path = resolve(value);
		const label = portablePath(path, workDir);
		let stat;
		try {
			stat = statSync(path);
		} catch {
			throw new StepError('inputs', `${name} tarball not found: ${label}`);
		}
		if (!stat.isFile()) throw new StepError('inputs', `${name} input ${label} is not a file; pass a packed .tgz or a version`);
		if (!isGzip(path)) throw new StepError('inputs', `${name} input ${label} is not a gzip tarball; pass a packed .tgz or a version`);
		return { name, kind: 'tarball', path, tarball: label, spec: `file:${path.replace(/\\/g, '/')}`, ...digest(path) };
	}
	const spec = value.startsWith(`${name}@`) ? value.slice(name.length + 1) : value;
	if (!spec || /[\s]/.test(spec)) throw new StepError('inputs', `${name} spec ${JSON.stringify(value)} is not a version, tag or range`);
	return { name, kind: 'registry', spec };
}

function defaultAdapter() {
	const installed = readJson(join(HERE, 'node_modules', 'svelte-adapter-uws', 'package.json'));
	const locked = readJson(join(HERE, 'package-lock.json'))?.packages?.['node_modules/svelte-adapter-uws'];
	if (!installed || !locked) throw new StepError('inputs', 'svelte-adapter-uws is not installed in this checkout; run npm ci or pass --adapter');
	if (installed.version !== locked.version) throw new StepError('inputs', `installed svelte-adapter-uws ${installed.version} differs from the locked ${locked.version}; run npm ci or pass --adapter`);
	return { name: 'svelte-adapter-uws', kind: 'registry', spec: locked.version, expectedIntegrity: locked.integrity };
}

// - steps -----------------------------------------------------------

function packThis(tarballs) {
	const r = npm(['pack', '--json', '--pack-destination', tarballs], HERE);
	if (r.code !== 0) throw new StepError('pack', 'npm pack failed for this repository', r.stderr || r.stdout);
	let filename;
	try {
		const parsed = JSON.parse(r.stdout);
		filename = (Array.isArray(parsed) ? parsed[0] : parsed).filename;
	} catch {
		throw new StepError('pack', 'could not parse npm pack output', r.stdout);
	}
	const path = join(tarballs, filename);
	if (!existsSync(path)) throw new StepError('pack', `npm pack reported ${filename} but it is not on disk`);
	return path;
}

function installConsumer(consumer, sources) {
	mkdirSync(consumer, { recursive: true });
	const deps = { ...CONSUMER_PEERS };
	for (const source of Object.values(sources)) deps[source.name] = source.spec;
	writeFileSync(join(consumer, 'package.json'), JSON.stringify({ name: 'real-cluster-oracle-consumer', private: true, version: '0.0.0', type: 'module', dependencies: deps }, null, 2) + '\n');
	// A rerun rewrites tarballs under the same names, so npm could keep what it
	// extracted last time: start from an empty tree.
	rmSync(join(consumer, 'package-lock.json'), { force: true });
	rmSync(join(consumer, 'node_modules'), { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
	let r = npm(['install', '--no-audit', '--no-fund', '--loglevel=error'], consumer);
	if (r.code !== 0) throw new StepError('install', 'the consumer did not install', r.stderr || r.stdout);

	// The native binding is optional for applications and mandatory here. Take
	// the exact pin of the adapter that actually landed.
	const landedAdapter = readJson(join(consumer, 'node_modules', 'svelte-adapter-uws', 'package.json'));
	const native = landedAdapter?.optionalDependencies?.['uWebSockets.js'] || landedAdapter?.dependencies?.['uWebSockets.js'];
	if (!readJson(join(consumer, 'node_modules', 'uWebSockets.js', 'package.json'))) {
		if (typeof native !== 'string') throw new StepError('install', 'the adapter declares no uWebSockets.js pin');
		r = npm(['install', '--no-audit', '--no-fund', '--loglevel=error', `uWebSockets.js@${native}`], consumer);
		if (r.code !== 0 || !readJson(join(consumer, 'node_modules', 'uWebSockets.js', 'package.json'))) {
			throw new StepError('install', 'the native uWebSockets.js binding did not install', r.stderr || r.stdout);
		}
	}

	const lock = readJson(join(consumer, 'package-lock.json'));
	const names = Object.keys(sources);
	const problems = [];
	for (const name of names) {
		const source = sources[name];
		const pkg = readJson(join(consumer, 'node_modules', name, 'package.json'));
		if (!pkg) {
			problems.push(`${name} is not installed`);
			continue;
		}
		source.version = pkg.version;
		const entry = lock?.packages?.[`node_modules/${name}`];
		if (source.kind === 'registry') {
			source.resolved = entry?.resolved || null;
			source.integrity = entry?.integrity || null;
			if (!source.integrity) problems.push(`${name} has no registry integrity in the consumer lock`);
			if (source.expectedIntegrity && source.integrity !== source.expectedIntegrity) {
				problems.push(`${name} installed with integrity ${source.integrity}, expected ${source.expectedIntegrity}`);
			}
		} else if (entry?.integrity && entry.integrity !== source.integrity) {
			problems.push(`${name} installed with integrity ${entry.integrity}, but the tarball digests to ${source.integrity}`);
		}
		for (const other of names) {
			if (other === name) continue;
			const nested = readJson(join(consumer, 'node_modules', name, 'node_modules', other, 'package.json'));
			if (nested) problems.push(`${name} carries a nested ${other}@${nested.version}, so the named ${other} is not the one in use`);
		}
	}
	if (problems.length) throw new StepError('install', 'the installed tree does not match the named packages', problems.join('\n'));
	const nativePkg = readJson(join(consumer, 'node_modules', 'uWebSockets.js', 'package.json'));
	return { native: { spec: native || null, version: nativePkg?.version || null } };
}

// - cluster ---------------------------------------------------------

// The variables test/integration/global-setup-cluster.js hands to Compose, so
// a fallback `down` interpolates the same ports and subnet.
function composeEnv() {
	const base = Number(process.env.INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE);
	const env = { INTEGRATION_REDIS_CLUSTER_SUBNET: process.env.INTEGRATION_REDIS_CLUSTER_SUBNET, INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE: String(base) };
	for (let node = 1; node < 6; node++) env[`INTEGRATION_REDIS_CLUSTER_NODE_${node}_PORT`] = String(base + node);
	return env;
}

function composeDown(project) {
	return run('docker', ['compose', '-p', project, '-f', CLUSTER_COMPOSE, 'down', '-v'], HERE, { env: composeEnv() });
}

// Containers and networks still carrying the project label, or null when
// Docker cannot be asked.
function composeLeftovers(project) {
	const label = `label=com.docker.compose.project=${project}`;
	const containers = run('docker', ['ps', '-a', '-q', '--filter', label], HERE);
	const networks = run('docker', ['network', 'ls', '-q', '--filter', label], HERE);
	if (containers.code !== 0 || networks.code !== 0) return null;
	const ids = (text) => text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
	return { containers: ids(containers.stdout).length, networks: ids(networks.stdout).length };
}

/**
 * Remove the stack and say whether it is really gone. `teardown` is the
 * setup's own teardown (absent while setup is still running), `down` the
 * fallback, `leftovers` the check. Only `removed` means nothing is left.
 */
export async function stopCluster({ teardown, down, leftovers }) {
	const outcome = { status: 'removed' };
	if (teardown) {
		try {
			await teardown();
		} catch (error) {
			outcome.teardownError = String(error?.message || error);
		}
	}
	let left = leftovers();
	if (!teardown || outcome.teardownError || !left || left.containers || left.networks) {
		const r = down();
		if (r.code !== 0) outcome.fallbackError = tail(r.stderr || r.stdout, 10);
		else outcome.fallback = 'compose down';
		left = leftovers();
	}
	if (!left) outcome.status = 'unverified';
	else if (left.containers || left.networks) {
		outcome.status = 'leaked';
		outcome.left = left;
	}
	return outcome;
}

/**
 * Fold the teardown outcome into the result. A stack that may still be running
 * turns any verdict into an error: a pass must not hide a leaked stack.
 * Returns the exit code to use instead, or null when the verdict stands.
 */
export function settleTeardown(result, outcome) {
	result.services.teardown = outcome;
	if (outcome.status === 'removed') return null;
	result.status = 'error';
	const message = outcome.status === 'leaked'
		? `the Compose stack is still present after teardown (${outcome.left.containers} container(s), ${outcome.left.networks} network(s))`
		: 'Docker could not confirm the Compose stack was removed';
	if (!result.error) result.error = { step: 'teardown', message };
	else result.error.teardown = message;
	return 2;
}

// Start the integration cluster with the lane's own global setup, so the
// topology is the one the cluster tests use. `lifecycle.stop` is armed with a
// plain `compose down` before the setup is awaited, so an interrupt or a
// failure in the middle of it still removes the stack.
async function startCluster(services, lifecycle) {
	for (const [key, value] of Object.entries(ISOLATION_DEFAULTS)) {
		if (!process.env[key]) process.env[key] = value;
	}
	services.mode = 'docker-compose';
	services.composeFile = 'test/integration/docker-compose.cluster.yml';
	services.project = process.env.INTEGRATION_COMPOSE_PROJECT;
	services.hostPortBase = Number(process.env.INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE);
	services.subnet = process.env.INTEGRATION_REDIS_CLUSTER_SUBNET;
	const down = () => composeDown(services.project);
	const leftovers = () => composeLeftovers(services.project);
	lifecycle.stop = () => stopCluster({ teardown: null, down, leftovers });

	const log = console.log;
	// The setup reports progress on stdout; keep stdout for this script's own
	// summary.
	const quiet = () => { console.log = (...args) => console.error(...args); };
	quiet();
	try {
		const { default: setup } = await import(pathToFileURL(CLUSTER_SETUP).href);
		const teardown = await setup();
		lifecycle.stop = async () => {
			quiet();
			try {
				return await stopCluster({ teardown, down, leftovers });
			} finally {
				console.log = log;
			}
		};
		services.nodes = process.env.INTEGRATION_REDIS_CLUSTER_NODES.split(',').map((address) => {
			const [host, port] = address.split(':');
			return { host, port: Number(port) };
		});
		services.natMap = JSON.parse(process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP);
	} catch (error) {
		throw new StepError('cluster', `the Redis Cluster did not come up: ${error.message}`);
	} finally {
		console.log = log;
	}
}

function externalCluster(services) {
	let nodes, natMap;
	try {
		nodes = JSON.parse(process.env.ORACLE_CLUSTER_NODES);
		natMap = process.env.ORACLE_CLUSTER_NAT_MAP ? JSON.parse(process.env.ORACLE_CLUSTER_NAT_MAP) : null;
	} catch (error) {
		throw new StepError('inputs', `ORACLE_CLUSTER_NODES / ORACLE_CLUSTER_NAT_MAP is not valid JSON: ${error.message}`);
	}
	Object.assign(services, { mode: 'external', nodes, natMap });
}

function runScenario(consumer, services, logPath) {
	const entry = join(consumer, 'real-cluster-oracle.mjs');
	copyFileSync(SCENARIO, entry);
	const env = { ORACLE_CLUSTER_NODES: JSON.stringify(services.nodes) };
	if (services.natMap) env.ORACLE_CLUSTER_NAT_MAP = JSON.stringify(services.natMap);
	const r = run(process.execPath, [entry], consumer, { env, timeout: SCENARIO_TIMEOUT_MS });
	writeFileSync(logPath, r.stderr);
	const line = r.stdout.trim().split(/\r?\n/).pop();
	let report = null;
	try {
		report = JSON.parse(line);
	} catch { /* handled below */ }
	if (!report || report.schemaVersion !== 1 || !Array.isArray(report.cases)) {
		throw new StepError('scenario', r.timedOut ? 'the scenario process was killed at its deadline' : 'the scenario printed no report', tail(r.stdout + '\n' + r.stderr));
	}
	return report;
}

// - interrupt -------------------------------------------------------

/**
 * Handle SIGINT/SIGTERM: remove whatever stack is up, record the interrupt in
 * the result, write it, and exit. Repeated signals while this runs are
 * ignored, so a second Ctrl-C cannot cut the teardown short.
 */
export async function interrupt(signal, { result, lifecycle, write, exit }) {
	if (lifecycle.interrupted) return;
	lifecycle.interrupted = true;
	result.status = 'error';
	result.error = { step: 'interrupted', message: `received ${signal}` };
	// A teardown already under way is awaited, not repeated or cut short.
	const stop = lifecycle.stop;
	lifecycle.stop = null;
	const pending = lifecycle.stopping || (stop ? stop() : null);
	if (pending) settleTeardown(result, await pending);
	write();
	exit(130);
}

// - main ------------------------------------------------------------

async function main() {
	const opts = parseArgs(process.argv.slice(2));
	if (opts.help) {
		help();
		return 0;
	}
	const workDir = opts.workDir || join(HERE, '.real-cluster-oracle');
	const out = opts.out || join(workDir, 'result.json');
	const result = {
		schemaVersion: 1,
		oracle: 'real-cluster-fanout',
		status: 'error',
		startedAt: new Date().toISOString(),
		command: ['node', 'scripts/real-cluster-oracle.js', ...process.argv.slice(2).map((arg) => (isAbsolute(arg) ? portablePath(arg, workDir) : arg))],
		source: { repository: 'svelte-adapter-uws-extensions', ...gitState() },
		packages: {},
		services: {},
		scenario: null,
		error: null
	};
	const write = () => {
		result.finishedAt = new Date().toISOString();
		mkdirSync(dirname(out), { recursive: true });
		// Seeds and the natMap name the service; credentials never reach the file.
		writeFileSync(out, JSON.stringify(scrubPaths(result, { workDir }), null, 2) + '\n');
	};
	const lifecycle = { stop: null, stopping: null, interrupted: false };
	const onSignal = (signal) => { interrupt(signal, { result, lifecycle, write, exit: (code) => process.exit(code) }); };
	process.on('SIGINT', onSignal);
	process.on('SIGTERM', onSignal);

	let exitCode = 2;
	try {
		mkdirSync(workDir, { recursive: true });
		const tarballs = join(workDir, 'tarballs');
		rmSync(tarballs, { recursive: true, force: true });
		mkdirSync(tarballs, { recursive: true });

		const realtime = describeSource('svelte-realtime', opts.realtime || 'next', workDir);
		const adapter = opts.adapter ? describeSource('svelte-adapter-uws', opts.adapter, workDir) : defaultAdapter();
		let extensions = opts.extensions ? describeSource('svelte-adapter-uws-extensions', opts.extensions, workDir) : null;
		if (process.env.ORACLE_CLUSTER_NODES) externalCluster(result.services);

		if (!extensions) {
			console.log('real-cluster-oracle: packing this repository');
			extensions = describeSource('svelte-adapter-uws-extensions', packThis(tarballs), workDir);
		}
		const sources = { 'svelte-adapter-uws': adapter, 'svelte-adapter-uws-extensions': extensions, 'svelte-realtime': realtime };
		console.log('real-cluster-oracle: installing the consumer');
		const consumer = join(workDir, 'consumer');
		const { native } = installConsumer(consumer, sources);
		for (const source of Object.values(sources)) {
			const { name, path, spec, expectedIntegrity, ...rest } = source;
			result.packages[name] = source.kind === 'registry' ? { ...rest, spec } : rest;
		}
		result.packages['uWebSockets.js'] = native;

		if (result.services.mode !== 'external') {
			console.log('real-cluster-oracle: starting the Redis Cluster');
			await startCluster(result.services, lifecycle);
		}
		if (lifecycle.interrupted) return 130;
		console.log('real-cluster-oracle: running the scenario');
		const report = runScenario(consumer, result.services, join(workDir, 'scenario-stderr.log'));
		result.scenario = report;
		result.status = report.status;
		exitCode = report.status === 'passed' ? 0 : 1;
	} catch (error) {
		if (lifecycle.interrupted) return 130;
		result.error = { step: error.step || 'oracle', message: error.message, ...(error.output ? { output: tail(error.output) } : {}) };
	}
	if (lifecycle.interrupted) return 130;
	if (lifecycle.stop) {
		const stop = lifecycle.stop;
		lifecycle.stop = null;
		console.log('real-cluster-oracle: tearing the Redis Cluster down');
		lifecycle.stopping = stop();
		const outcome = await lifecycle.stopping;
		if (lifecycle.interrupted) return 130;
		exitCode = settleTeardown(result, outcome) ?? exitCode;
	}
	process.off('SIGINT', onSignal);
	process.off('SIGTERM', onSignal);
	write();

	const counts = result.scenario?.counts;
	console.log(`real-cluster-oracle: ${result.status}${counts ? ` (${counts.passed}/${counts.planned} cases passed, ${counts.failed} failed, ${counts.notRun} not run)` : ''}`);
	for (const c of result.scenario?.cases || []) if (c.error) console.log(`  ${c.name}: ${c.error.message.split(/\r?\n/)[0]}`);
	if (result.error) console.log(`  ${result.error.step}: ${result.error.message}${result.error.output ? '\n' + result.error.output : ''}`);
	console.log(`real-cluster-oracle: result written to ${out}`);
	return exitCode;
}

const invoked = process.argv[1] ? resolve(process.argv[1]) : '';
const self = fileURLToPath(import.meta.url);
if (process.platform === 'win32' ? invoked.toLowerCase() === self.toLowerCase() : invoked === self) {
	const code = await main();
	// An interrupt exits on its own once the stack is gone and the result is
	// written; never race it here.
	if (code !== 130) process.exitCode = code;
}
