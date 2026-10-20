import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { parse } from 'acorn';

// Invalid caller inputs are not operational failures. Keep this exception
// list explicit so a new runtime rejection cannot silently inherit it.
const invalidInputs = {
	'postgres/_client-lifecycle.js': [],
	'postgres/_tasks-worker-pool.js': [
		'postgres tasks: worker.path for "', 'postgres tasks: worker.pool.size for "',
		'postgres tasks: worker.pool.idleTimeout for "',
		// An idle thread has no job to reject; this value only retires it.
		'postgres tasks: idle worker retired'
	],
	'shared/redis-subscriber.js': [],
	'shared/forget-store.js': [
		'forget-store: tenant id must be null (untenanted scope) or a string - got ',
		'forget-store: tenant id must be a non-empty string of [a-zA-Z0-9_.:-], at most ',
		'createForgetStore: pass a map or array of stores',
		'createForgetStore: options.redis must be an ioredis instance (or a client wrapping one on .redis)'
	],
	'redis/cursor.js': [
		'redis cursor: select() must return JSON-serializable data',
		'Topic must not contain quotes, backslashes, or control characters',
		'cursor.attach: '
	],
	'redis/pubsub.js': [
		'pubsub bus: onDegraded must be a function', 'pubsub bus: onRecovered must be a function',
		'pubsub bus: degradationPolicy must be a createDegradationPolicy() result',
		'pubsub bus: systemChannel must be a string, null, or false'
	],
	'redis/sharded-pubsub.js': [
		'sharded bus: channelPrefix must be a string', 'sharded bus: shardKey must be a function',
		'sharded bus: subscribersAggregator must expose subscribersOf(topic)',
		'sharded bus: followBatch requires an array of topics'
	],
	'redis/registry.js': [
		'registry: options object is required', 'registry: identify must be a function',
		'registry: attributes must be a function returning a flat object of string|number|boolean',
		'registry: sessionIdentify must be a function returning a string sessionId or null',
		'registry: ttl must be a positive number (seconds)', 'registry: heartbeat must be a positive number (ms)',
		'registry: requestTimeoutMs must be a positive number (ms)',
		'registry.sendCoalesced: message must be an object', 'registry.sendCoalesced: message.key must be a non-empty string',
		'registry.send: topic must be a non-empty string', 'registry.send: event must be a non-empty string',
		'registry.sendTo: criteria must be a non-empty object', 'registry.sendTo: topic must be a non-empty string',
		'registry.sendTo: event must be a non-empty string', 'registry.sendTo: requires `attributes` option on createConnectionRegistry',
		'registry.sendTo: criteria must include at least one attribute key'
	]
};
const invalidDiagnostics = new Set([
	'REGISTRY_REQUEST_TARGET', 'REGISTRY_REQUEST_EVENT', 'REGISTRY_SESSION_TARGET',
	'REGISTRY_SESSION_EVENT', 'REGISTRY_SEND_COALESCED_TARGET', 'REGISTRY_SEND_TARGET'
]);
function walk(node, visit) {
	if (!node || typeof node !== 'object') return;
	if (Array.isArray(node)) { for (const child of node) walk(child, visit); return; }
	if (typeof node.type === 'string') visit(node);
	for (const child of Object.values(node)) if (child && typeof child === 'object') walk(child, visit);
}
function head(node) {
	if (node?.type === 'Literal') return node.value;
	if (node?.type === 'TemplateLiteral') return node.quasis[0].value.cooked;
	if (node?.type === 'BinaryExpression' && node.operator === '+') return head(node.left);
	return undefined;
}
function uncoded(source, path) {
	const failures = [];
	walk(parse(source, { ecmaVersion: 'latest', sourceType: 'module', locations: true }), node => {
		if (!['NewExpression', 'CallExpression'].includes(node.type) || !['Error', 'RangeError', 'TypeError'].includes(node.callee?.name)) return;
		const message = node.arguments[0];
		if (invalidInputs[path]?.includes(head(message))) return;
		if (path === 'redis/registry.js' && message?.callee?.name === 'extensionsError'
			&& invalidDiagnostics.has(message.arguments[0]?.property?.name)) return;
		failures.push({ line: node.loc.start.line, expression: source.slice(node.start, node.end) });
	});
	return failures;
}

describe('operational error coverage', () => {
	it.each(Object.keys(invalidInputs))('%s does not construct uncoded runtime errors', path => {
		const source = readFileSync(new URL('../../src/' + path, import.meta.url), 'utf8');
		expect(uncoded(source, path)).toEqual([]);
	});
	it('rejects new uncoded throws, returned failures and rejected promises', () => {
		for (const source of ["throw new Error('backend unavailable')", "function fail() { return new RangeError('capacity'); }", "Promise.reject(Error('closed'))"]) {
			expect(uncoded(source, 'redis/registry.js')).toHaveLength(1);
		}
	});
	it('documents every stable code introduced by the covered runtime paths', () => {
		const reference = readFileSync(new URL('../../docs/public-error-codes.md', import.meta.url), 'utf8');
		const codes = new Set(['PG_CAPACITY_EXCEEDED']);
		for (const path of Object.keys(invalidInputs)) {
			const source = readFileSync(new URL('../../src/' + path, import.meta.url), 'utf8');
			walk(parse(source, { ecmaVersion: 'latest', sourceType: 'module' }), node => {
				if (node.type === 'CallExpression' && node.callee.name === 'operationError') {
					expect(node.arguments[0].type).toBe('Literal');
					codes.add(node.arguments[0].value);
				}
			});
		}
		for (const code of codes) expect(reference, code).toContain('`' + code + '`');
	});
	it('documents each code in exactly one reference row', () => {
		const reference = readFileSync(new URL('../../docs/public-error-codes.md', import.meta.url), 'utf8');
		const rows = reference.split('\n').map(line => /^\| `([A-Z0-9_]+)` \|/.exec(line)?.[1]).filter(Boolean);
		expect(rows.length).toBeGreaterThan(0);
		expect(rows.filter((code, index) => rows.indexOf(code) !== index)).toEqual([]);
		expect(rows).toEqual(expect.arrayContaining(['PG_CAPACITY_EXCEEDED', 'PG_NOTIFY_SESSION_UNSTABLE']));
	});
});
