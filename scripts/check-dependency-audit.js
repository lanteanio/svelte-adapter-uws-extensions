#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ADVISORY = 'https://github.com/advisories/GHSA-gcqx-w48m-w2h4';
const EXPIRES = '2026-11-30T00:00:00Z';
const LOCK_DIGEST = '1ce41c7e56d7877f3a7b3ebf770eaaae4bd9c2f56296b88484a9bd9136d86879';

// GHSA-gcqx-w48m-w2h4 (deepmerge prototype poisoning) reaches this tree only
// through build tooling: Kit's Svelte plugin merges local configuration during
// config setup, before any module compiles, and never merges request data.
// Any other tree, advisory or a date past EXPIRES fails; update these pins
// only after rechecking that.
const PINS = {
	'deepmerge': ['4.3.1', 'sha512-3sUqbMEc77XqpdNO7FRyRog+eW3ph+GYCbj+rK+uYyRMuwsVy0rMiVtPn+QJlKFvWP/1PYpapqYn0Me2knFn+A=='],
	'@sveltejs/vite-plugin-svelte': ['7.0.0', 'sha512-ILXmxC7HAsnkK2eslgPetrqqW1BKSL7LktsFgqzNj83MaivMGZzluWq32m25j2mDOjmSKX7GGWahePhuEs7P/g=='],
	'@sveltejs/kit': ['3.0.1', 'sha512-8JMCCq7ZUJELi3hbhb0+c9Rs8qnbHW90vhOfM+t1FSF4fhIpG9/FOsH4IYgcITMhbdkSrrn+Kvd9G+q8A+UO6Q=='],
	'svelte-adapter-uws': ['0.6.0-next.97', 'sha512-c0piWbMNxfNa12uZGHXvPwjaWq1lP2MUPjdi/xijCWjvpuGGoRZguBTiOrCv/SoGcuuHiBQ1CC/oeLRGciOCjw==']
};
const VIA = {
	'deepmerge': [],
	'@sveltejs/vite-plugin-svelte': ['deepmerge'],
	'@sveltejs/kit': ['@sveltejs/vite-plugin-svelte'],
	'svelte-adapter-uws': ['@sveltejs/kit']
};
const DIRECT = new Set(['@sveltejs/kit', 'svelte-adapter-uws']);
const EFFECTS = {
	'deepmerge': ['@sveltejs/vite-plugin-svelte'],
	'@sveltejs/vite-plugin-svelte': ['@sveltejs/kit'],
	'@sveltejs/kit': ['svelte-adapter-uws'],
	'svelte-adapter-uws': []
};
const names = Object.keys(PINS);

function refuse(message) {
	throw new Error('dependency audit: ' + message);
}

function record(value) {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function sameStrings(actual, expected) {
	return Array.isArray(actual) && actual.every(value => typeof value === 'string') &&
		JSON.stringify(actual.slice().sort()) === JSON.stringify(expected.slice().sort());
}

function parseJson(text, label) {
	if (typeof text !== 'string' || text.trim() === '') refuse(label + ' is empty');
	try { return JSON.parse(text); }
	catch { refuse(label + ' is not valid JSON'); }
}

function auditReport(input) {
	if (!record(input)) refuse('missing verification input');
	if (input.auditFailure || ![0, 1].includes(input.auditStatus)) refuse('npm audit failed');
	const audit = parseJson(input.auditText, 'audit report');
	if (!record(audit) || Object.hasOwn(audit, 'error') || audit.auditReportVersion !== 2 || !record(audit.vulnerabilities)) {
		refuse('npm returned a malformed report or a command error');
	}
	const counts = audit.metadata?.vulnerabilities;
	const levels = ['info', 'low', 'moderate', 'high', 'critical'];
	if (!record(counts) || !sameStrings(Object.keys(counts), [...levels, 'total']) ||
		Object.values(counts).some(count => !Number.isSafeInteger(count) || count < 0)) refuse('malformed advisory totals');
	const actual = Object.fromEntries(levels.map(level => [level, 0]));
	for (const finding of Object.values(audit.vulnerabilities)) {
		if (!record(finding) || !Object.hasOwn(actual, finding.severity)) refuse('malformed finding severity');
		actual[finding.severity]++;
	}
	if (levels.some(level => counts[level] !== actual[level]) ||
		counts.total !== Object.keys(audit.vulnerabilities).length) refuse('inconsistent advisory totals');
	if (input.auditStatus !== (counts.total === 0 ? 0 : 1)) refuse('npm audit returned an inconsistent findings status');
	return audit;
}

/**
 * Validate captured command output without changing the report.
 * The clock, command status and registry reply are inputs so tests can drive
 * the same decision without running npm or touching the network.
 *
 * @param {{ auditText: string, lockText?: string, registryText?: string,
 *   auditStatus: number | null, registryStatus?: number | null,
 *   auditFailure?: boolean, registryFailure?: boolean, now?: number }} input
 */
export function validateDependencyAudit(input) {
	const audit = auditReport(input);
	if (Object.keys(audit.vulnerabilities).length === 0) return { findings: 0 };
	if (!Number.isFinite(input.now) || input.now >= Date.parse(EXPIRES)) refuse('the development exception has expired');
	if (input.registryFailure || input.registryStatus !== 0) refuse('the live registry check failed');
	if (parseJson(input.registryText, 'registry response') !== '4.3.1') {
		refuse('the registry release changed; review the available deepmerge release');
	}
	const lock = parseJson(input.lockText, 'package lock');
	if (!record(lock) || lock.lockfileVersion !== 3 || !record(lock.packages)) refuse('package lock has an unsupported shape');
	for (const name of names) {
		const node = lock.packages['node_modules/' + name];
		const [version, integrity] = PINS[name];
		if (!record(node) || node.version !== version || node.integrity !== integrity || node.dev !== true) {
			refuse('reviewed development artifact changed: ' + name);
		}
	}
	const digest = createHash('sha256').update(JSON.stringify(lock)).digest('hex');
	if (digest !== LOCK_DIGEST) refuse('the reviewed dependency graph changed');

	const findings = audit.vulnerabilities;
	if (!sameStrings(Object.keys(findings), names)) refuse('the report has missing or additional findings');
	const visiting = new Set(), visited = new Set();
	function visit(name) {
		if (!Object.hasOwn(findings, name)) refuse('missing advisory reference: ' + name);
		if (visiting.has(name)) refuse('cyclic advisory references');
		if (visited.has(name)) return;
		const node = findings[name];
		if (!record(node) || !Array.isArray(node.via)) refuse('malformed advisory references: ' + name);
		visiting.add(name);
		for (const reference of node.via) if (typeof reference === 'string') visit(reference);
		visiting.delete(name);
		visited.add(name);
	}
	for (const name of names) visit(name);
	for (const name of names) {
		const node = findings[name];
		if (node.name !== name || node.severity !== 'low' || node.isDirect !== DIRECT.has(name) ||
			!sameStrings(node.nodes, ['node_modules/' + name])) refuse('finding scope or severity changed: ' + name);
		if (name === 'deepmerge') {
			const advisory = node.via[0];
			if (node.via.length !== 1 || !record(advisory) || advisory.url !== ADVISORY ||
				advisory.source !== 1243505 || advisory.name !== name || advisory.dependency !== name ||
				advisory.severity !== 'low' || advisory.range !== '>=4.2.0 <=4.3.1') refuse('unreviewed advisory');
		} else if (!sameStrings(node.via, VIA[name])) refuse('unreviewed advisory dependency: ' + name);
		if (!sameStrings(node.effects, EFFECTS[name])) refuse('advisory effects changed: ' + name);
	}
	return { findings: names.length, severity: 'low', advisory: ADVISORY, expires: EXPIRES };
}

function npmCommand(args, root, timeout) {
	const configured = process.env.npm_execpath;
	const npm = configured?.endsWith('npm-cli.js') && existsSync(configured)
		? configured : [
			resolve(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js'),
			resolve(dirname(process.execPath), '../lib/node_modules/npm/bin/npm-cli.js')
		].find(path => existsSync(path));
	if (!npm) refuse('cannot locate npm; run npm run verify:dependencies');
	return spawnSync(process.execPath, [npm, ...args], {
		cwd: root, encoding: 'utf8', timeout, windowsHide: true, maxBuffer: 8 * 1024 * 1024
	});
}

function failed(command) {
	return Boolean(command.error || command.signal || command.stderr?.trim());
}

export function runDependencyAudit(root, run = npmCommand, now = Date.now()) {
	const report = resolve(root, 'source/dependency-audit.json');
	mkdirSync(dirname(report), { recursive: true });
	writeFileSync(report, '');
	const audit = run(['audit', '--json', '--include=dev', '--include=optional', '--include=peer',
		'--audit-level=low', '--registry=https://registry.npmjs.org',
		'--fetch-retries=0', '--fetch-timeout=15000'], root, 60000);
	// Retain the complete stdout before judging exit status or parsing it.
	writeFileSync(report, audit.stdout ?? '');
	const captured = { auditText: audit.stdout ?? '', auditStatus: audit.status, auditFailure: failed(audit) };
	if (Object.keys(auditReport(captured).vulnerabilities).length === 0) return { findings: 0 };
	const registry = run(['view', 'deepmerge', 'version', '--json',
		'--registry=https://registry.npmjs.org', '--prefer-online',
		'--fetch-retries=0', '--fetch-timeout=10000'], root, 15000);
	const verdict = validateDependencyAudit({
		...captured, lockText: readFileSync(resolve(root, 'package-lock.json'), 'utf8'),
		registryText: registry.stdout ?? '', registryStatus: registry.status,
		registryFailure: failed(registry), now
	});
	return verdict;
}

function main() {
	const verdict = runDependencyAudit(resolve(dirname(fileURLToPath(import.meta.url)), '..'));
	if (verdict.findings === 0) {
		console.log('dependency audit: no reported vulnerabilities; full raw report: source/dependency-audit.json');
		return;
	}
	console.log('dependency audit: ' + verdict.findings + ' low development findings from ' + verdict.advisory);
	console.log('dependency audit: reviewed exception expires ' + verdict.expires + '; full raw report: source/dependency-audit.json');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	try { main(); }
	catch (error) {
		console.error(error.message);
		console.error('dependency audit: verification failed; raw output retained at source/dependency-audit.json');
		process.exitCode = 1;
	}
}
