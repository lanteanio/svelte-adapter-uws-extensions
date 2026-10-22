#!/usr/bin/env node
/**
 * Run the real-cluster delivery oracle against packed packages and write a
 * machine-readable verdict.
 *
 * What it does, in order:
 *
 *   1. packs this repository as `npm publish` would;
 *   2. installs that tarball, the adapter and realtime into a throwaway
 *      consumer, together with the adapter's pinned native uWebSockets.js;
 *   3. brings up the integration Redis Cluster (three masters, three replicas,
 *      test/integration/docker-compose.cluster.yml) unless an external cluster
 *      is named;
 *   4. runs scripts/fixtures/real-cluster-oracle.mjs from the consumer: two
 *      native uWebSockets.js servers, the realtime message hook and two
 *      extensions pub/sub buses relaying through the cluster to a local and a
 *      remote WebSocket;
 *   5. tears the stack down and writes the result JSON.
 *
 * The scenario has five cases: setup, cluster-topology, single-fanout (six
 * ordered publishes), batch-fanout (one six-member publishBatched plus one
 * ordered barrier) and cleanup. A pass means each of the two sockets received
 * exactly thirteen values, in order, with no duplicate.
 *
 * Zero-config, from this repo (Docker required):
 *
 *   node scripts/real-cluster-oracle.js
 *
 * Every knob is optional:
 *
 *   --realtime <tgz|spec>  svelte-realtime to install: a tarball path or an npm
 *                          spec. Default: svelte-realtime@next.
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
 *   0  every case passed.
 *   1  the scenario ran and a case failed or did not run.
 *   2  the oracle could not run: bad usage, a pack, install or cluster
 *      failure. The result file says which step failed.
 *
 * @module scripts/real-cluster-oracle
 */
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
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

class StepError extends Error {
	constructor(step, message, output) {
		super(message);
		this.step = step;
		this.output = output;
	}
}

// - argv ------------------------------------------------------------

function parseArgs(argv) {
	const opts = { realtime: null, adapter: null, workDir: null, out: null, help: false };
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];
		const value = () => {
			if (i + 1 >= argv.length) usage(`${arg} needs a value`);
			return argv[++i];
		};
		if (arg === '--realtime') opts.realtime = value();
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

function looksLikeTarball(value) {
	return /\.(tgz|tar\.gz)$/i.test(value) || existsSync(value);
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

// Each package is either a local tarball (installed by file: spec and
// identified by its digest) or an npm spec (identified by the registry
// integrity the consumer lock records).
function describeSource(name, value) {
	if (looksLikeTarball(value)) {
		const path = resolve(value);
		if (!existsSync(path)) throw new StepError('inputs', `${name} tarball not found: ${path}`);
		return { name, kind: 'tarball', tarball: path, spec: `file:${path.replace(/\\/g, '/')}`, ...digest(path) };
	}
	return { name, kind: 'registry', spec: value.startsWith(`${name}@`) ? value.slice(name.length + 1) : value };
}

function defaultAdapter() {
	const installed = readJson(join(HERE, 'node_modules', 'svelte-adapter-uws', 'package.json'));
	const locked = readJson(join(HERE, 'package-lock.json'))?.packages?.['node_modules/svelte-adapter-uws'];
	if (!installed || !locked) throw new StepError('inputs', 'svelte-adapter-uws is not installed in this checkout; run npm ci or pass --adapter');
	if (installed.version !== locked.version) throw new StepError('inputs', `installed svelte-adapter-uws ${installed.version} differs from the locked ${locked.version}; run npm ci or pass --adapter`);
	return { name: 'svelte-adapter-uws', kind: 'registry', spec: locked.version, expectedIntegrity: locked.integrity };
}

function installConsumer(consumer, sources) {
	mkdirSync(consumer, { recursive: true });
	const adapterManifest = readJson(join(HERE, 'node_modules', 'svelte-adapter-uws', 'package.json'));
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
	const landedAdapter = readJson(join(consumer, 'node_modules', 'svelte-adapter-uws', 'package.json')) || adapterManifest;
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

// Start the integration cluster with the lane's own global setup, so the
// topology is the one the cluster tests use. Returns its teardown.
async function startCluster(services) {
	for (const [key, value] of Object.entries(ISOLATION_DEFAULTS)) {
		if (!process.env[key]) process.env[key] = value;
	}
	services.mode = 'docker-compose';
	services.composeFile = 'test/integration/docker-compose.cluster.yml';
	services.project = process.env.INTEGRATION_COMPOSE_PROJECT;
	services.hostPortBase = Number(process.env.INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE);
	services.subnet = process.env.INTEGRATION_REDIS_CLUSTER_SUBNET;
	const down = () => run('docker', ['compose', '-p', services.project, '-f', CLUSTER_COMPOSE, 'down', '-v'], HERE);
	const log = console.log;
	// The setup reports progress on stdout; keep stdout for this script's own
	// summary.
	console.log = (...args) => console.error(...args);
	try {
		const { default: setup } = await import(pathToFileURL(CLUSTER_SETUP).href);
		const teardown = await setup();
		services.nodes = process.env.INTEGRATION_REDIS_CLUSTER_NODES.split(',').map((address) => {
			const [host, port] = address.split(':');
			return { host, port: Number(port) };
		});
		services.natMap = JSON.parse(process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP);
		return async () => {
			console.log = (...args) => console.error(...args);
			try {
				await teardown();
			} catch {
				down();
			} finally {
				console.log = log;
			}
		};
	} catch (error) {
		down();
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
	return { exitCode: r.code, report };
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
		command: ['node', 'scripts/real-cluster-oracle.js', ...process.argv.slice(2)],
		source: { repository: 'svelte-adapter-uws-extensions', ...gitState() },
		packages: {},
		services: {},
		scenario: null,
		error: null
	};
	let teardown = null;
	let exitCode = 2;
	const stopOnSignal = () => {
		if (teardown) {
			const pending = teardown;
			teardown = null;
			pending().finally(() => process.exit(130));
		} else process.exit(130);
	};
	process.once('SIGINT', stopOnSignal);
	process.once('SIGTERM', stopOnSignal);

	try {
		mkdirSync(workDir, { recursive: true });
		const tarballs = join(workDir, 'tarballs');
		rmSync(tarballs, { recursive: true, force: true });
		mkdirSync(tarballs, { recursive: true });

		const realtime = describeSource('svelte-realtime', opts.realtime || 'next');
		const adapter = opts.adapter ? describeSource('svelte-adapter-uws', opts.adapter) : defaultAdapter();
		if (process.env.ORACLE_CLUSTER_NODES) externalCluster(result.services);

		console.log('real-cluster-oracle: packing this repository');
		const extensions = describeSource('svelte-adapter-uws-extensions', packThis(tarballs));
		const sources = { 'svelte-adapter-uws': adapter, 'svelte-adapter-uws-extensions': extensions, 'svelte-realtime': realtime };
		console.log('real-cluster-oracle: installing the consumer');
		const consumer = join(workDir, 'consumer');
		const { native } = installConsumer(consumer, sources);
		for (const source of Object.values(sources)) {
			const { name, expectedIntegrity, ...rest } = source;
			result.packages[name] = rest;
		}
		result.packages['uWebSockets.js'] = native;

		if (result.services.mode !== 'external') {
			console.log('real-cluster-oracle: starting the Redis Cluster');
			teardown = await startCluster(result.services);
		}
		console.log('real-cluster-oracle: running the scenario');
		const { report } = runScenario(consumer, result.services, join(workDir, 'scenario-stderr.log'));
		result.scenario = report;
		result.status = report.status;
		exitCode = report.status === 'passed' ? 0 : 1;
	} catch (error) {
		result.error = { step: error.step || 'oracle', message: error.message, ...(error.output ? { output: tail(error.output) } : {}) };
	} finally {
		if (teardown) {
			const pending = teardown;
			teardown = null;
			console.log('real-cluster-oracle: tearing the Redis Cluster down');
			await pending();
		}
	}
	// Seeds and the natMap name the service; credentials never reach the file.
	result.finishedAt = new Date().toISOString();
	mkdirSync(dirname(out), { recursive: true });
	writeFileSync(out, JSON.stringify(result, null, 2) + '\n');

	const counts = result.scenario?.counts;
	console.log(`real-cluster-oracle: ${result.status}${counts ? ` (${counts.passed}/${counts.planned} cases passed, ${counts.failed} failed, ${counts.notRun} not run)` : ''}`);
	for (const c of result.scenario?.cases || []) if (c.error) console.log(`  ${c.name}: ${c.error.message.split(/\r?\n/)[0]}`);
	if (result.error) console.log(`  ${result.error.step}: ${result.error.message}${result.error.output ? '\n' + result.error.output : ''}`);
	console.log(`real-cluster-oracle: result written to ${out}`);
	return exitCode;
}

process.exitCode = await main();
