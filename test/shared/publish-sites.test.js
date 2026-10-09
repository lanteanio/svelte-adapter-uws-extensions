// Every platform publish in src declares its options. A multi-worker adapter
// refuses a publish that would draw its per-worker topic counter, and a call
// without options is exactly that; the module that made it then either throws
// on every frame or, where it swallows the error (a flush timer, a batch
// fallback), drops every frame silently. The behaviour of each module on such
// a runtime is pinned by test/shared/cluster-sequence-authority.test.js; this
// scan is what catches a NEW publish site before it reaches one.
//
// What it checks, on the parsed source:
// - `.publish(` passes four arguments and `.publishWire(` five. The Redis
//   client's `publish(channel, message)` takes two and is recognised by its
//   receiver (REDIS_RECEIVERS); any other short call fails.
// - A `.publishBatched(` / `.batch(` entry written in place - an array
//   literal, a `.map()` callback's object, or an object pushed or assigned
//   into the array the call is given - carries an `options` key.
// - Options it can read declare a seq: an object literal (or either branch of
//   a conditional, or a `const` of the same file holding one, frozen or not)
//   names `seq` with a value other than `true`, `null` or `undefined`, or
//   spreads another object, and is never `undefined` or `null` itself.
//   Options it cannot read - a parameter, an import, a helper's result, a
//   property of another object, a `let`, a `??` / `||` default, a name the
//   file declares twice - are the caller's or another module's choice and
//   pass.
// src/testing (mocks that record whatever they are given) and src/sim.js (a
// single-worker simulator driving its instances the way an application would)
// are left out.

import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import * as acorn from 'acorn';

const srcDir = fileURLToPath(new URL('../../src', import.meta.url));
const EXCLUDED = [path.join(srcDir, 'testing'), path.join(srcDir, 'sim.js')];
const MIN_ARGS = { publish: 4, publishWire: 5 };
const BATCH_METHODS = new Set(['publishBatched', 'batch']);
const REDIS_RECEIVERS = new Set(['redis', 'client.redis', 'pipe', 't']);

function sourceFiles(dir) {
	const out = [];
	for (const name of readdirSync(dir)) {
		const full = path.join(dir, name);
		if (EXCLUDED.some((x) => full === x || full.startsWith(x + path.sep))) continue;
		if (statSync(full).isDirectory()) out.push(...sourceFiles(full));
		else if (name.endsWith('.js')) out.push(full);
	}
	return out;
}

function walk(node, visit) {
	if (!node || typeof node.type !== 'string') return;
	visit(node);
	for (const key in node) {
		const value = node[key];
		if (Array.isArray(value)) for (const child of value) walk(child, visit);
		else if (value && typeof value.type === 'string') walk(value, visit);
	}
}

/** `name` for `x.name(...)` / `x?.name(...)`, else null. */
function calledMethod(node) {
	const callee = node.callee;
	if (callee.type !== 'MemberExpression' || callee.computed || callee.property.type !== 'Identifier') return null;
	return callee.property.name;
}

/** Object literals an expression can evaluate to: itself, either branch of a conditional. */
function objectLiterals(node) {
	if (!node) return [];
	if (node.type === 'ObjectExpression') return [node];
	if (node.type === 'ConditionalExpression') return [...objectLiterals(node.consequent), ...objectLiterals(node.alternate)];
	return [];
}

/** A batch entry literal must name `options`; a spread may carry it, so it passes. */
function entryDeclaresOptions(obj) {
	return obj.properties.some((p) => p.type === 'SpreadElement'
		|| (p.type === 'Property' && !p.computed && ((p.key.type === 'Identifier' && p.key.name === 'options') || p.key.value === 'options')));
}

/** The value of an entry literal's `options` key, or null. */
function entryOptions(obj) {
	const p = obj.properties.find((q) => q.type === 'Property' && !q.computed
		&& ((q.key.type === 'Identifier' && q.key.name === 'options') || q.key.value === 'options'));
	return p ? p.value : null;
}

/** Every `const NAME = init` in the file; a name declared twice maps to null. */
function localConstants(ast) {
	const out = new Map();
	walk(ast, (node) => {
		if (node.type !== 'VariableDeclaration' || node.kind !== 'const') return;
		for (const d of node.declarations) {
			if (d.id.type !== 'Identifier' || !d.init) continue;
			out.set(d.id.name, out.has(d.id.name) ? null : d.init);
		}
	});
	return out;
}

/** Whether options the scan can read declare a seq (see the header). */
function declaresSeq(node, constants, depth = 0) {
	if (!node || depth > 6) return true;
	if ((node.type === 'Identifier' && node.name === 'undefined') || (node.type === 'Literal' && node.value === null)) return false;
	if (node.type === 'ObjectExpression') {
		if (node.properties.some((p) => p.type === 'SpreadElement')) return true;
		const seq = node.properties.find((p) => p.type === 'Property' && !p.computed
			&& ((p.key.type === 'Identifier' && p.key.name === 'seq') || p.key.value === 'seq'));
		if (!seq) return false;
		// The counter (`true`) and the spellings the peer floor reads as the
		// counter (`null`, `undefined`) are what a multi-worker adapter refuses.
		const v = seq.value;
		return !((v.type === 'Literal' && (v.value === true || v.value === null)) || (v.type === 'Identifier' && v.name === 'undefined'));
	}
	if (node.type === 'ConditionalExpression') {
		return declaresSeq(node.consequent, constants, depth + 1) && declaresSeq(node.alternate, constants, depth + 1);
	}
	if (node.type === 'CallExpression' && node.callee.type === 'MemberExpression' && !node.callee.computed
		&& node.callee.object.type === 'Identifier' && node.callee.object.name === 'Object' && node.callee.property.name === 'freeze') {
		return declaresSeq(node.arguments[0], constants, depth + 1);
	}
	if (node.type === 'Identifier' && constants.get(node.name)) return declaresSeq(constants.get(node.name), constants, depth + 1);
	return true;
}

function failuresIn(source, label) {
	const ast = acorn.parse(source, { ecmaVersion: 'latest', sourceType: 'module', allowHashBang: true, locations: true });
	const failures = [];
	const at = (node, what) => failures.push(`${label}:${node.loc.start.line} ${what}: ${source.slice(node.start, node.end).replace(/\s+/g, ' ').slice(0, 110)}`);
	const constants = localConstants(ast);
	const batchArrays = new Set();
	const checkEntries = (node) => {
		for (const obj of objectLiterals(node)) {
			if (!entryDeclaresOptions(obj)) at(obj, 'batch entry without options');
			else if (!declaresSeq(entryOptions(obj), constants)) at(obj, 'batch entry options without a seq');
		}
	};

	walk(ast, (node) => {
		if (node.type !== 'CallExpression') return;
		const method = calledMethod(node);
		if (method === null) return;
		if (method in MIN_ARGS) {
			const receiver = source.slice(node.callee.object.start, node.callee.object.end);
			if (method === 'publish' && node.arguments.length === 2 && REDIS_RECEIVERS.has(receiver)) return;
			if (node.arguments.length < MIN_ARGS[method] || node.arguments.some((a) => a.type === 'SpreadElement')) at(node, `${method} without options`);
			else if (!declaresSeq(node.arguments[MIN_ARGS[method] - 1], constants)) at(node, `${method} options without a seq`);
			return;
		}
		if (!BATCH_METHODS.has(method)) return;
		const arg = node.arguments[0];
		if (!arg) return;
		if (arg.type === 'ArrayExpression') {
			for (const el of arg.elements) checkEntries(el);
		} else if (arg.type === 'Identifier') {
			batchArrays.add(arg.name);
		} else if (arg.type === 'CallExpression' && calledMethod(arg) === 'map') {
			const fn = arg.arguments[0];
			if (fn && (fn.type === 'ArrowFunctionExpression' || fn.type === 'FunctionExpression')) {
				if (fn.body.type !== 'BlockStatement') checkEntries(fn.body);
				else walk(fn.body, (n) => { if (n.type === 'ReturnStatement') checkEntries(n.argument); });
			}
		}
	});

	// Entries built up in an array the file hands to a batch call.
	walk(ast, (node) => {
		if (node.type === 'CallExpression' && calledMethod(node) === 'push'
			&& node.callee.object.type === 'Identifier' && batchArrays.has(node.callee.object.name)) {
			for (const arg of node.arguments) checkEntries(arg);
		} else if (node.type === 'AssignmentExpression' && node.left.type === 'MemberExpression' && node.left.computed
			&& node.left.object.type === 'Identifier' && batchArrays.has(node.left.object.name)) {
			checkEntries(node.right);
		}
	});
	return failures;
}

describe('platform publishes in src declare their options', () => {
	it('every publish and every batch entry written in place passes options', () => {
		const failures = [];
		let files = 0;
		for (const file of sourceFiles(srcDir)) {
			files++;
			failures.push(...failuresIn(readFileSync(file, 'utf8'), path.relative(srcDir, file)));
		}
		expect(files).toBeGreaterThan(90);
		expect(failures).toEqual([]);
	});

	it('the scan itself can fail', () => {
		const bad = (code) => failuresIn(code, 'synthetic').length;
		// Arity, including a two-argument platform publish and an optional call.
		expect(bad("platform.publish(topic, 'e', { a: 1, b: [2, 3] });")).toBe(1);
		expect(bad("platform.publish(topic, 'reset');")).toBe(1);
		expect(bad("platform?.publish(topic, 'e', data);")).toBe(1);
		expect(bad('p.publishWire(t, e, d, codec);')).toBe(1);
		expect(bad('platform.publish(...args);')).toBe(1);
		expect(bad("platform.publish(topic, 'e', f(x, y), OPTIONS);")).toBe(0);
		expect(bad('redis.publish(channel, msg).catch(() => {});')).toBe(0);
		// Commas, parentheses and backticks inside literals are not arguments.
		expect(bad("platform.publish('a,b', `c,${d(`e,f`)}`, /,\\(/g);")).toBe(1);
		expect(bad("platform.publish('a,b', `c,${d(`e,f`)}`, /,\\(/g, x / y);")).toBe(0);
		// Batch entries written in place.
		expect(bad("platform.publishBatched([{ topic: t, event: 'e', data: 1 }]);")).toBe(1);
		expect(bad("platform.publishBatched([{ topic: t, event: 'e', data: 1, options: O }]);")).toBe(0);
		expect(bad("platform.publishBatched(list.map((m) => ({ topic: m.t, event: 'e', data: m })));")).toBe(1);
		expect(bad("platform.batch(list.map((m) => { return { topic: m.t, event: 'e', data: m, options: O }; }));")).toBe(0);
		expect(bad("const ms = []; ms.push({ topic: t, event: 'e', data: 1 }); platform.publishBatched(ms);")).toBe(1);
		expect(bad("const ms = []; ms[0] = c ? { topic: t, event: 'e', options: O } : { topic: t, event: 'e' }; platform.batch(ms);")).toBe(1);
		expect(bad("const ms = []; ms.push({ ...m, options }); platform.publishBatched(ms);")).toBe(0);
		// Options that do not declare a seq.
		expect(bad("platform.publish(t, 'e', d, { relay: false });")).toBe(1);
		expect(bad("platform.publishWire(t, 'e', d, codec, { compress: false });")).toBe(1);
		expect(bad("platform.publish(t, 'e', d, ok ? { seq } : undefined);")).toBe(1);
		expect(bad("platform.publish(t, 'e', d, null);")).toBe(1);
		expect(bad("const O = Object.freeze({ relay: false }); platform.publish(t, 'e', d, O);")).toBe(1);
		expect(bad("const O = Object.freeze({ relay: false, seq: false }); platform.publish(t, 'e', d, O);")).toBe(0);
		expect(bad("const o = c ? { ...x, seq: false } : DEFAULTS; platform.publish(t, 'e', d, o);")).toBe(0);
		expect(bad("platform.publishBatched([{ topic: t, event: 'e', options: { compress: false } }]);")).toBe(1);
		expect(bad("function f(options) { platform.publish(t, 'e', d, options); }")).toBe(0);
		expect(bad("platform.publish(t, 'e', d, receivedOptions(s));")).toBe(0);
		// A seq that names the counter.
		expect(bad("platform.publish(t, 'e', d, { seq: true });")).toBe(1);
		expect(bad("platform.publish(t, 'e', d, { seq: null });")).toBe(1);
		expect(bad("platform.publish(t, 'e', d, { seq: undefined, relay: false });")).toBe(1);
		expect(bad("platform.publish(t, 'e', d, { seq: n, relay: false });")).toBe(0);
		expect(bad("platform.publish(t, 'e', d, { seq });")).toBe(0);
	});
});
