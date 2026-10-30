#!/usr/bin/env node
/**
 * Guard that every identifier a src module reads resolves to something: a
 * local or module-scope binding, an import, or a global the runtime provides
 * (the ES library plus Node's globals). A name that resolves to nothing throws
 * a ReferenceError only when its line runs, and inside a try/catch that error
 * is swallowed, so a dropped import can leave a call that silently does
 * nothing while every parse and syntax gate stays green.
 *
 * Name resolution is TypeScript's binder and checker over every .js file
 * under src with checkJs on, keeping only the "cannot find name" diagnostics; every other
 * type complaint is ignored. Two adjustments keep the answer about runtime
 * names only:
 *   - names inside comments (JSDoc type expressions) are not runtime
 *     references and are dropped;
 *   - `@ts-ignore`, `@ts-expect-error` and `@ts-nocheck` directives are blanked
 *     out before checking, so a directive placed for an unrelated type
 *     complaint cannot also hide an undeclared name on the next line.
 *
 * TypeScript and acorn are existing devDependencies; nothing is emitted. Run
 * via `npm run check`; wired into `pretest` so the unit run enforces it.
 *
 * `--root <dir>` checks another package directory; the tests point it at a
 * fixture so the gate can be shown to fail.
 *
 * @module scripts/check-undeclared
 */
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as acorn from 'acorn';
import ts from 'typescript';

const rootArg = process.argv.indexOf('--root');
const root = rootArg !== -1 && process.argv[rootArg + 1]
	? resolve(process.argv[rootArg + 1])
	: resolve(dirname(fileURLToPath(import.meta.url)), '..');
const srcDir = join(root, 'src');

// 2304 "Cannot find name", 2552 "... Did you mean", the 258x/259x family
// TypeScript uses for well-known names whose declarations are not loaded, and
// 18004, an object shorthand property (`{ now }`) with no binding behind it.
const UNRESOLVED = new Set([2304, 2552, 2580, 2581, 2582, 2583, 2584, 2591, 2592, 2593, 18004]);

/** @param {string} dir @returns {string[]} */
function walk(dir) {
	const out = [];
	for (const entry of readdirSync(dir, { withFileTypes: true })) {
		const full = join(dir, entry.name);
		if (entry.isDirectory()) out.push(...walk(full));
		else if (entry.name.endsWith('.js')) out.push(full);
	}
	return out;
}

const DIRECTIVE = /@ts-(?:ignore|expect-error|nocheck)\b/g;
/** @param {string} text */
const blankDirectives = (text) => text.replace(DIRECTIVE, (m) => ' '.repeat(m.length));

const files = walk(srcDir).sort();
const fileSet = new Set(files.map((f) => ts.sys.resolvePath(f)));

const options = {
	allowJs: true,
	checkJs: true,
	noEmit: true,
	skipLibCheck: true,
	target: ts.ScriptTarget.ES2022,
	module: ts.ModuleKind.NodeNext,
	moduleResolution: ts.ModuleResolutionKind.NodeNext,
	lib: ['lib.es2023.d.ts'],
	types: ['node'],
	typeRoots: [join(resolve(dirname(fileURLToPath(import.meta.url)), '..'), 'node_modules', '@types')]
};

const host = ts.createCompilerHost(options);
const readFile = host.readFile.bind(host);
host.readFile = (name) => {
	const text = readFile(name);
	return text !== undefined && fileSet.has(ts.sys.resolvePath(name)) ? blankDirectives(text) : text;
};

const program = ts.createProgram({ rootNames: files, options, host });

/** @param {string} text @returns {Array<[number, number]>} */
function commentRanges(text) {
	/** @type {Array<[number, number]>} */
	const ranges = [];
	try {
		acorn.parse(text, {
			ecmaVersion: 'latest',
			sourceType: 'module',
			allowHashBang: true,
			onComment: (_block, _text, start, end) => ranges.push([start, end])
		});
	} catch {
		// check-syntax owns parse failures; without comment ranges every
		// diagnostic in the file is kept, which can only over-report.
	}
	return ranges;
}

const errors = [];
for (const file of files) {
	const source = program.getSourceFile(file);
	if (!source) continue;
	const diagnostics = program.getSemanticDiagnostics(source).filter((d) => UNRESOLVED.has(d.code));
	if (diagnostics.length === 0) continue;
	const comments = commentRanges(source.text);
	for (const d of diagnostics) {
		const at = d.start ?? 0;
		if (comments.some(([start, end]) => at >= start && at < end)) continue;
		const { line, character } = source.getLineAndCharacterOfPosition(at);
		const name = source.text.slice(at, at + (d.length ?? 0));
		errors.push(`${relative(root, file).replace(/\\/g, '/')}:${line + 1}:${character + 1}: '${name}' is neither declared, imported, nor a global`);
	}
}

if (errors.length > 0) {
	for (const e of errors) console.error(`check-undeclared: ${e}`);
	console.error(`check-undeclared: ${errors.length} undeclared reference(s) in ${files.length} src files`);
	process.exit(1);
}
console.log(`check-undeclared: ${files.length} src files, 0 undeclared references`);
