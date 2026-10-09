#!/usr/bin/env node
/**
 * Every console-printed FAILURE under `src` is either indexed or deliberately
 * not, and this gate is where that decision is recorded.
 *
 * docs/errors.md is generated from the error registry, so it can only describe
 * lines that go THROUGH the registry. A `console.warn('[extensions] x broke')`
 * added anywhere would print an operator-facing failure no search of the
 * reference resolves, while the reference generator, which walks only the
 * entries that exist, stays green.
 *
 * So this gate starts from the CALL SITES. Every console failure must be one of:
 *
 *   - printed through `extensionsConsoleLine(EXTENSIONS_ERROR_IDS.X, ...)`, where
 *     X resolves to a real console-emitting registry entry whose severity
 *     agrees with the console method - which puts the line in docs/errors.md
 *     and makes the emitted text the registry's own prefix;
 *   - printed through a diagnostic formatter, which enters the structured
 *     diagnostic pipeline and is indexed by its event name; or
 *   - named in UNINDEXED below, with a reason why an operator who saw the line
 *     needs no reference entry.
 *
 * Anything else fails.
 *
 * THE KEY IS THE WHOLE STATIC SKELETON of the printed text - every literal
 * part it will always contain, with `{}` where a value is interpolated, and
 * nothing dropped - so a line that MOVES keeps its decision and a line that is
 * REWORDED loses it and comes back for a fresh one. The key collapses
 * whitespace runs, so two lines differing only there would share one key; the
 * collision check below refuses that. A stale entry, matching nothing, fails
 * too. A template's RAW text is read, so a `\n` written in a template keys as
 * the two literal characters while the same line built from string literals
 * carries a real newline.
 *
 * SCOPE is EVERY module under `src`, minus a named list of exclusions with a
 * reason each (EXCLUDED below), so a new file is covered the day it is added.
 * `console.log`/`info`/`debug` are not failures.
 *
 * WHAT IT CANNOT SEE. It reads one module at a time and follows a console
 * printer as far as a local binding: a declaration, an object-pattern
 * destructuring, a parameter or destructuring default, a later assignment,
 * `.bind(console)`, and a binding of the bare `console` identifier. A binding of
 * a QUALIFIED console (`const c = globalThis.console`), a printer stored on an
 * object property, one passed in as an argument with no default, one imported
 * from another module, and a line whose text is built at runtime all evade it.
 * Alias resolution is by NAME and blind to scope, so a false positive is
 * possible where a module binds `log` to console in one function only. A
 * method chosen at runtime (`console[method](line)`) is not read either; the
 * one such call in the tree is the diagnostic pipeline's own printer in
 * src/shared/diagnostic.js, which writes records already built from a registry
 * entry or a formatter. Every other failure line uses a shape this gate reads,
 * and `process.stderr.write` is refused outright.
 */
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'acorn';
import { EXTENSIONS_ERROR_IDS, EXTENSIONS_ERROR_REGISTRY } from '../src/error-registry.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const sourceRoot = join(root, 'src');

/**
 * Modules under `src` this gate does NOT read, and why. Everything else is
 * scanned, so a file that belongs here has to be put here deliberately.
 */
export const EXCLUDED = new Map(Object.entries({}));

/** Formatters whose output is a structured diagnostic event, indexed by event name. */
const PIPELINE_FORMATTERS = new Set(['formatDiagnostic', 'assertionDiagnostic']);

/** Console methods that print a failure. */
const FAILURE_METHODS = new Set(['warn', 'error']);

const ENTRY_BY_ID = new Map(EXTENSIONS_ERROR_REGISTRY.map((entry) => [entry.id, entry]));

/**
 * Console failures that stay out of the reference, and why. The key is
 * `<path under src>::<static skeleton of the printed text>`.
 *
 * The bar for an entry here: an operator who reads this line needs nothing
 * the reference could add - because the line is a developer-only debug aid,
 * because it merely narrates a decision whose real failure is indexed
 * elsewhere, or because the line already carries its own complete guidance
 * and a stable route.
 */
export const UNINDEXED = new Map(Object.entries({
	// Configuration advisories printed once at construction. Each names the
	// field, what the default did with it, and the option that changes that.
	"redis/presence/state.js::[extensions] redis presence: key field '{}' is private or transport metadata, so the default projection drops it and each connection gets its own presence entry. A presence key is broadcast as a roster property: use a non-secret id, or pass an explicit select() to opt in.": 'names the field, the consequence for the roster, and the explicit select that resolves it; the adapter records its presence twin the same way',
	'shared/sensitive.js::[extensions] [{}] userData key "{}" looks sensitive; use the select option to strip it before broadcasting See: https://svti.me/userdata-sensitive': 'names the key, the exposure and the remedy, with a short link; the adapter records its userData twin the same way',

	// Development-only advisory, once per reserved identity name, for a server
	// update() whose field was refused; it names the field and the option that
	// admits it, and production never prints it.
	"redis/presence/field-policy.js::[extensions] redis presence.update(): field '{}' is reserved and was dropped. Reserved names are the dedup key field, id, userId, role, roles, userRole, __-prefixed, constructor/prototype and credential-shaped names, so a client cannot overwrite the identity its peers see. If this call is server-owned and intentional, pass clientUpdateFields: [{}, ...] to replace the guard with an explicit allowlist; clients can then write every listed name too, so a server-owned value is better kept under an unreserved name of its own.": 'a development-only hint that names the refused field and the clientUpdateFields entry that admits it; the adapter prints its twin the same way',

	// Wiring advisory for the developer who connected the hook.
	'redis/cursor/diagnostics.js::[redis/cursor] hooks.message called with unexpected shape ({}). Expected a parsed object {type:"cursor", topic, data} or {type:"cursor-snapshot", topic}. If you wired this from `createMessage({ onUnhandled })` and got raw bytes, switch to `createMessage({ onJsonMessage(ws, msg, platform) { ... } })` which forwards the parsed JSON envelope. This warning fires once per process. See: https://svti.me/cursor-hooks-message': 'names the shape received, the shape expected, the hook that forwards the parsed envelope, and a short link; printed once per process'
}));

/** @param {string} dir */
function jsFiles(dir) {
	return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
		const path = join(dir, entry.name);
		return entry.isDirectory() ? jsFiles(path) : entry.name.endsWith('.js') ? [path] : [];
	});
}

/** Path under `src`, in the one spelling both maps are keyed by. */
function relPath(path) {
	return relative(sourceRoot, path).split(String.fromCharCode(92)).join('/');
}

function scannedFiles() {
	return jsFiles(sourceRoot).filter((path) => !EXCLUDED.has(relPath(path)));
}

/** Exclusions that no longer name a file, so the list cannot rot into fiction. */
export function staleExclusions() {
	return [...EXCLUDED.keys()].filter((rel) => {
		try { return !statSync(join(sourceRoot, rel)).isFile(); } catch { return true; }
	});
}

/**
 * The static skeleton of a printed argument: every literal character it will
 * always contain, with `{}` where a value is interpolated. This is the text an
 * operator would recognise, minus the values that differ per occurrence.
 *
 * @param {any} node
 * @returns {string}
 */
export function staticSkeleton(node) {
	if (node == null) return '{}';
	if (node.type === 'Literal') return typeof node.value === 'string' ? node.value : '{}';
	if (node.type === 'TemplateLiteral') {
		let out = '';
		for (let i = 0; i < node.quasis.length; i++) {
			out += node.quasis[i].value.raw;
			if (i < node.expressions.length) out += '{}';
		}
		return out;
	}
	if (node.type === 'BinaryExpression' && node.operator === '+') {
		return staticSkeleton(node.left) + staticSkeleton(node.right);
	}
	return '{}';
}

/**
 * The allowlist key carries the WHOLE skeleton, collapsed to one line.
 *
 * A truncated key would let a line whose tail is reworded - the remediation an
 * entry's reason describes, say - keep a decision made about different text.
 * The whole skeleton makes "a reworded line comes back for a fresh decision"
 * hold exactly, and two different lines can never share a key.
 *
 * @param {string} skeleton
 */
export function skeletonKey(skeleton) {
	return skeleton.replace(/\s+/g, ' ').trim();
}

function walk(node, visit) {
	if (!node || typeof node !== 'object') return;
	visit(node);
	for (const value of Object.values(node)) {
		if (Array.isArray(value)) for (const child of value) walk(child, visit);
		else if (value && typeof value === 'object' && typeof value.type === 'string') walk(value, visit);
	}
}

/** A console failure method, under any spelling this gate follows. */
const SOLE_CONSOLE = new Set(['console']);

function consoleMethod(node, consoleObjectNames = SOLE_CONSOLE) {
	if (node?.type !== 'MemberExpression') return null;
	const object = node.object;
	// `console.error`, and `globalThis.console.error` / `globalThis['console'].error`.
	const isConsole = (object?.type === 'Identifier' && consoleObjectNames.has(object.name)) ||
		(object?.type === 'MemberExpression' &&
			(object.property?.name === 'console' ||
				(object.computed && object.property?.type === 'Literal' && object.property.value === 'console')));
	if (!isConsole) return null;
	const method = node.computed
		? (node.property?.type === 'Literal' ? node.property.value : null)
		: node.property?.name;
	return typeof method === 'string' && FAILURE_METHODS.has(method) ? method : null;
}

/** The id an `extensionsConsoleLine(...)` call names, or null when it is not that call. */
function consoleLineId(node) {
	if (node?.type !== 'CallExpression' || node.callee?.type !== 'Identifier' ||
		node.callee.name !== 'extensionsConsoleLine') return null;
	const first = node.arguments[0];
	if (first?.type === 'MemberExpression' && first.object?.name === 'EXTENSIONS_ERROR_IDS' && !first.computed) {
		return { key: first.property?.name, id: EXTENSIONS_ERROR_IDS[first.property?.name] };
	}
	if (first?.type === 'Literal' && typeof first.value === 'string') return { key: first.value, id: first.value };
	return { key: null, id: null };
}

/**
 * Classify one module's console failures.
 *
 * Pure with respect to the supplied source, so a test drives it with a
 * synthetic module rather than by editing the runtime.
 *
 * @param {string} source module text
 * @param {string} rel path under src, used for the allowlist key
 * @param {Map<string, string>} [allowlist]
 * @returns {{ indexed: number, pipeline: number, used: string[], failures: string[] }}
 */
export function auditConsoleFailures(source, rel, allowlist = UNINDEXED) {
	const ast = parse(source, { ecmaVersion: 'latest', sourceType: 'module', locations: true });
	const failures = [];
	const used = [];
	/** key -> the skeleton that produced it, so a whitespace collision is caught. */
	const keyed = new Map();
	let indexed = 0;
	let pipeline = 0;

	// Printers that ARE a console failure method under another name. A module
	// that takes `log = console.error` as an injectable default, so a test can
	// capture its output, prints through a call this walk would otherwise not
	// recognise. The bindings are
	// collected first, then their calls are classified exactly like a direct
	// one, so the indirection buys testability and not an exemption.
	/** @type {Map<string, string>} name -> method */
	const aliases = new Map();
	/** `console.error`, and `console.error.bind(console)` - the same printer. */
	const boundConsoleMethod = (node, objects) => consoleMethod(node, objects) ??
		(node?.type === 'CallExpression' && node.callee?.type === 'MemberExpression' &&
			node.callee.property?.name === 'bind' ? consoleMethod(node.callee.object, objects) : null);
	// Bindings of the console OBJECT, so `const c = console; c.error(...)` is
	// the same call under another name.
	const consoleObjects = new Set(['console']);
	walk(ast, (node) => {
		if (node.type === 'VariableDeclarator' && node.id?.type === 'Identifier' &&
			(node.init?.name === 'console' || consoleObjects.has(node.init?.name))) {
			consoleObjects.add(node.id.name);
		}
	});
	walk(ast, (node) => {
		if (node.type === 'VariableDeclarator' && node.id?.type === 'Identifier') {
			const method = boundConsoleMethod(node.init, consoleObjects);
			if (method) aliases.set(node.id.name, method);
		}
		// `const { error } = console` / `const { error: fail } = console`
		if (node.type === 'VariableDeclarator' && node.id?.type === 'ObjectPattern' &&
			consoleObjects.has(node.init?.name)) {
			for (const prop of node.id.properties) {
				const from = prop.key?.name;
				const to = prop.value?.type === 'Identifier' ? prop.value.name : null;
				if (to !== null && FAILURE_METHODS.has(from)) aliases.set(to, from);
			}
		}
		// A default: `{ log = console.error } = options`, or a parameter default.
		if (node.type === 'AssignmentPattern' && node.left?.type === 'Identifier') {
			const method = boundConsoleMethod(node.right, consoleObjects);
			if (method) aliases.set(node.left.name, method);
		}
		// A later assignment to an already-declared binding.
		if (node.type === 'AssignmentExpression' && node.left?.type === 'Identifier') {
			const method = boundConsoleMethod(node.right, consoleObjects);
			if (method) aliases.set(node.left.name, method);
		}
	});

	walk(ast, (node) => {
		if (node.type !== 'CallExpression') return;
		// Writing the failure straight to the stream skips every classification
		// above, so it is refused outright rather than classified.
		if (node.callee?.type === 'MemberExpression' && node.callee.property?.name === 'write' &&
			node.callee.object?.type === 'MemberExpression' && node.callee.object.property?.name === 'stderr') {
			failures.push(`src/${rel}:${node.loc.start.line}: process.stderr.write bypasses the console index, which ` +
				'reads console calls only.\n' +
				'    If it reports a failure, print it through the error registry with console.error instead. If it is a\n' +
				'    raw dump or a forwarded child stream, record that decision next to this rule: the gate keeps no\n' +
				'    allowlist for stderr writes.');
			return;
		}
		const method = consoleMethod(node.callee, consoleObjects) ?? aliases.get(node.callee?.name);
		if (!method) return;
		const printer = node.callee?.type === 'Identifier' ? node.callee.name + '() (console.' + method + ' alias)' : 'console.' + method;
		const first = node.arguments[0];

		const named = consoleLineId(first);
		if (named !== null) {
			// The call SHAPE is not the contract; the id is. A dangling id makes
			// extensionsConsoleLine throw at the moment the failure it was meant to
			// report happens - inside a catch block, where the throw replaces the
			// recovery that catch was performing.
			const entry = named.id === null ? undefined : ENTRY_BY_ID.get(named.id);
			if (entry === undefined) {
				failures.push(`src/${rel}:${node.loc.start.line}: ${printer} prints through extensionsConsoleLine with ` +
					`${named.key === null ? 'a computed id' : 'EXTENSIONS_ERROR_IDS.' + named.key}, which is not a registry entry.\n` +
					'    extensionsConsoleLine throws on an unknown id, so this would replace the failure with a TypeError.');
				return;
			}
			if (entry.emission !== 'console') {
				failures.push(`src/${rel}:${node.loc.start.line}: ${printer} prints ${entry.id}, whose emission is ` +
					`"${entry.emission}" rather than "console"; extensionsConsoleLine refuses it at runtime.`);
				return;
			}
			// A warn-level line indexed as an error (or the reverse) misfiles
			// wherever severity routes, and the reference then describes the
			// failure at an urgency the log does not carry.
			const expected = method === 'warn' ? 'warn' : 'error';
			if (entry.severity !== expected && !(method === 'error' && entry.severity === 'fatal')) {
				failures.push(`src/${rel}:${node.loc.start.line}: ${printer} prints ${entry.id}, whose registry severity is ` +
					`"${entry.severity}"; a console.${method} line is severity "${expected}".`);
				return;
			}
			// The PRINTER's file must be listed. That is all this can check: an
			// entry's prose routinely describes behaviour decided in another
			// module, and no gate can tell which files a sentence depends on.
			// What it does buy is that an entry cannot silently acquire a call
			// site nobody checked its prose against. The reference generator
			// validates none of this: it short-circuits as soon as any file
			// mentions the id.
			const declared = (entry.sources || []).map((source) => source.replace(/^src\//, ''));
			if (!declared.includes(rel)) {
				failures.push(`src/${rel}:${node.loc.start.line}: ${printer} prints ${entry.id}, whose sources do not ` +
					`name this file (declared: ${declared.map((d) => 'src/' + d).join(', ') || 'none'}).\n` +
					"    Add it, and re-read the entry's cause/consequence/recovery/next-action against THIS call site -\n" +
					'    an entry written for one site is routinely false at another.');
				return;
			}
			indexed++;
			return;
		}
		if (first?.type === 'CallExpression' && first.callee?.type === 'Identifier' &&
			PIPELINE_FORMATTERS.has(first.callee.name)) { pipeline++; return; }

		const skeleton = staticSkeleton(first);
		// A line with no invariant words of its own cannot be searched for and
		// cannot be keyed: its text is whatever the values happened to be.
		if (!/[a-z]{3}/i.test(skeleton.replace(/^(?:\s*\[[^\]]*\])+/, '').replace(/\{\}/g, ' '))) {
			failures.push(
				`src/${rel}:${node.loc.start.line}: ${printer} prints a line with no invariant text of its own ` +
				`(skeleton ${JSON.stringify(skeleton)}), so there is nothing for an operator to search for.\n` +
				'    Give the line a fixed subject, then index it or record it in UNINDEXED in\n' +
				'    scripts/check-console-index.js.'
			);
			return;
		}
		const key = rel + '::' + skeletonKey(skeleton);
		// Whitespace normalisation can map two different lines onto one key, and
		// a recorded decision would then cover the one nobody judged.
		const seen = keyed.get(key);
		if (seen !== undefined && seen !== skeleton) {
			failures.push(`src/${rel}:${node.loc.start.line}: this line and an earlier one in the same file ` +
				`produce the same key ${JSON.stringify(key)}, so one recorded decision would cover both.\n` +
				'    They differ only in whitespace; give one of them wording that differs in words.');
			return;
		}
		keyed.set(key, skeleton);
		if (allowlist.has(key)) { used.push(key); return; }
		failures.push(
			`src/${rel}:${node.loc.start.line}: ${printer} prints a failure that the error reference cannot resolve.\n` +
			`    skeleton: ${JSON.stringify(skeleton)}\n` +
			'    Index it: add an entry to EXTENSIONS_ERROR_REGISTRY with emission "console" and print it through\n' +
			'    extensionsConsoleLine(EXTENSIONS_ERROR_IDS.<ID>, detail) - the emitted text then IS the indexed prefix.\n' +
			'    Or, if an operator who read it needs nothing the reference could add, record that decision in\n' +
			`    UNINDEXED in scripts/check-console-index.js under the key ${JSON.stringify(key)}.`
		);
	});
	return { indexed, pipeline, used, failures };
}

/**
 * Audit every scanned surface.
 * @param {Map<string, string>} [allowlist]
 */
export function auditRuntimeConsoleFailures(allowlist = UNINDEXED) {
	const failures = [];
	const used = new Set();
	let indexed = 0;
	let pipeline = 0;
	let modules = 0;
	for (const path of scannedFiles()) {
		modules++;
		const rel = relPath(path);
		const result = auditConsoleFailures(readFileSync(path, 'utf8'), rel, allowlist);
		indexed += result.indexed;
		pipeline += result.pipeline;
		for (const key of result.used) used.add(key);
		failures.push(...result.failures);
	}
	for (const rel of staleExclusions()) {
		failures.push(`stale EXCLUDED entry in scripts/check-console-index.js: ${JSON.stringify(rel)} is not a file under src.
` +
			'    An exclusion that names nothing is a decision about a module that no longer exists; delete it.');
	}
	for (const key of allowlist.keys()) {
		if (!used.has(key)) {
			failures.push(
				`stale UNINDEXED entry in scripts/check-console-index.js: ${JSON.stringify(key)} matches no call site.\n` +
				'    The line was removed or reworded. A reworded failure needs a fresh decision, not an inherited one:\n' +
				'    re-key the entry to the new text, or delete it if the line is gone.'
			);
		}
	}
	return { indexed, pipeline, unindexed: used.size, modules, failures };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
	const { indexed, pipeline, unindexed, modules, failures } = auditRuntimeConsoleFailures();
	console.log(`check-console-index: ${pkg.name}@${pkg.version}`);
	console.log(`  ${modules} module(s) scanned under src (${EXCLUDED.size} excluded by name): ${indexed} indexed through the registry, ${pipeline} through the diagnostic pipeline, ${unindexed} deliberately unindexed.`);
	if (failures.length > 0) {
		console.error(`\n  ${failures.length} console failure(s) unaccounted for:\n`);
		for (const failure of failures) console.error('  ' + failure + '\n');
		process.exit(1);
	}
	console.log('  OK - every console failure on a scanned surface is indexed or accounted for.');
}
