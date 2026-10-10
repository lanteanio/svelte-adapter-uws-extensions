import { tokenizer } from 'acorn';
import { TextDecoder } from 'node:util';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

export function globToRegExp(glob) {
	let out = '';
	for (let index = 0; index < glob.length; index++) {
		const char = glob[index];
		if (char === '*') {
			if (glob[index + 1] === '*') {
				out += '.*';
				index++;
			} else {
				out += '[^/]*';
			}
		} else if (char === '?') {
			out += '[^/]';
		} else if (char === '{') {
			const close = glob.indexOf('}', index);
			if (close === -1) {
				out += '\\{';
			} else {
				const parts = glob.slice(index + 1, close).split(',');
				out += '(?:' + parts.map((part) => part.replace(/[.+^${}()|[\]\\]/g, '\\$&')).join('|') + ')';
				index = close;
			}
		} else if ('.+^$()|[]\\'.includes(char)) {
			out += '\\' + char;
		} else {
			out += char;
		}
	}
	const anchored = glob.includes('/') ? '^' + out + '$' : '^(?:.*/)?' + out + '$';
	return new RegExp(anchored);
}

export function parseEditorConfig(source) {
	const sections = [];
	let current = null;
	for (const rawLine of source.split(/\r?\n/)) {
		const line = rawLine.trim();
		if (line === '' || line.startsWith('#') || line.startsWith(';')) continue;
		if (line.startsWith('[') && line.endsWith(']')) {
			current = { glob: line.slice(1, -1), pattern: globToRegExp(line.slice(1, -1)), properties: {} };
			sections.push(current);
			continue;
		}
		const split = line.indexOf('=');
		if (split === -1) continue;
		const key = line.slice(0, split).trim().toLowerCase();
		const value = line.slice(split + 1).trim();
		if (current) current.properties[key] = value;
	}
	return sections;
}

export function propertiesFor(sections, file) {
	const effective = {};
	for (const section of sections) {
		if (section.pattern.test(file)) Object.assign(effective, section.properties);
	}
	return effective;
}

export function indentRunIsValid(run, rest, style) {
	if (run === '') return true;
	if (style === 'tab') {
		if (run.startsWith('\t')) return true;
		return /^ +$/.test(run) && rest.startsWith('*');
	}
	if (style === 'space') return /^ +$/.test(run);
	return true;
}

function trackedFiles() {
	return execFileSync('git', ['ls-files', '--cached', '--others', '--exclude-standard', '-z'], { cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 })
		.split('\0')
		.filter(Boolean);
}

export function indexEol() {
	const map = new Map();
	const output = execFileSync('git', ['ls-files', '--eol'], { cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
	for (const line of output.split('\n')) {
		if (!line.trim()) continue;
		const match = /^i\/(\S+)\s+w\/(\S+)\s+attr\/([^\t]*?)\s*\t(.*)$/.exec(line);
		if (match) map.set(match[4], match[1]);
	}
	return map;
}

export function checkText(file, source, properties, recorded) {
	const offenses = [];
	if (properties.charset === 'utf-8' && source.charCodeAt(0) === 0xfeff) offenses.push(`${file}: UTF-8 byte-order mark`);
	if (properties.end_of_line === 'lf' && recorded && recorded !== 'none' && recorded !== 'lf') {
		offenses.push(`${file}: committed end-of-line is ${recorded}, declared lf`);
	}
	if (properties.insert_final_newline === 'true' && source !== '' && !source.endsWith('\n')) {
		offenses.push(`${file}: no final newline`);
	}

	const literalLines = new Set();
	if (/\.(?:js|mjs|cjs)$/.test(file)) {
		for (const token of tokenizer(source, { ecmaVersion: 'latest', sourceType: 'module', locations: true })) {
			if ((token.type.label === 'template' || token.type.label === 'string') && token.loc.end.line > token.loc.start.line) {
				for (let line = token.loc.start.line; line <= token.loc.end.line; line++) literalLines.add(line);
			}
		}
	}
	const lines = source.split('\n').map((line) => line.replace(/\r$/, ''));
	if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();

	for (const [index, line] of lines.entries()) {
		if (literalLines.has(index + 1)) continue;
		const where = `${file}:${index + 1}`;
		if (properties.trim_trailing_whitespace === 'true' && /[ \t]$/.test(line)) {
			offenses.push(`${where}: trailing whitespace`);
		}
		if (properties.indent_style) {
			const run = /^[ \t]*/.exec(line)[0];
			if (properties.indent_style === 'space' && /^\d+$/.test(properties.indent_size || '') && run.length % Number(properties.indent_size) !== 0) offenses.push(`${where}: indentation is not a multiple of ${properties.indent_size}`);
			if (!indentRunIsValid(run, line.slice(run.length), properties.indent_style)) {
				offenses.push(`${where}: indent starts with a ${run[0] === '\t' ? 'tab' : 'space'}, declared ${properties.indent_style}`);
			}
		}
	}
	return offenses;
}

export function findOffenses() {
	const sections = parseEditorConfig(readFileSync(resolve(root, '.editorconfig'), 'utf8'));
	const eol = indexEol();
	const offenses = [];
	const files = trackedFiles();

	for (const file of files) {
		const properties = propertiesFor(sections, file);
		const recorded = eol.get(file);
		if (recorded === '-text') continue;
		const absolute = resolve(root, file);
		let source;
		try {
			const bytes = readFileSync(absolute);
			source = properties.charset === 'utf-8' ? new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes) : bytes.toString('utf8');
		} catch (error) {
			if (error.code !== 'ENOENT') offenses.push(`${file}: invalid UTF-8 or unreadable file`);
			continue;
		}
		offenses.push(...checkText(file, source, properties, recorded));
	}
	return { offenses, count: files.length };
}

function main() {
	const { offenses, count } = findOffenses();
	if (count < 100) throw new Error(`only ${count} tracked file(s) were read; the file list is not trustworthy`);
	if (offenses.length > 0) {
		const shown = offenses.slice(0, 40);
		throw new Error(
			`${offenses.length} formatting offense(s) against .editorconfig:\n- ` + shown.join('\n- ') +
			(offenses.length > shown.length ? `\n- ... and ${offenses.length - shown.length} more` : '')
		);
	}
	console.log(`check-formatting: ${count} tracked files conform to .editorconfig`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	try {
		main();
	} catch (error) {
		console.error('check-formatting failed:\n' + error.message);
		process.exitCode = 1;
	}
}
