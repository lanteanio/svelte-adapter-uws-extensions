import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import MarkdownIt from 'markdown-it';
import { slugify, anchorsOf } from './check-links.js';
import { installedCompatibility } from './check-compatibility.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const markdown = new MarkdownIt({ html: true });
const keys = ['identity', 'first-success', 'compatibility', 'tutorial', 'how-to', 'reference', 'explanation', 'operations'];
const types = ['Identity', 'First success', 'Compatibility', 'Tutorial', 'How-to', 'Reference', 'Explanation', 'Operations'];
const compatibilitySource = 'svelte-adapter-uws/docs/compatibility.v1.csv';
export function replaceBlock(source, name, content) {
	const start = `<!-- ${name}:start -->`, end = `<!-- ${name}:end -->`;
	if (source.split(start).length !== 2 || source.split(end).length !== 2 || !source.split(/\r?\n/).includes(start) || !source.split(/\r?\n/).includes(end) || source.indexOf(end) < source.indexOf(start)) throw new Error('Expected one ordered ' + name + ' block');
	return source.slice(0, source.indexOf(start) + start.length) + '\n' + content + '\n' + source.slice(source.indexOf(end));
}
export function headingsOf(source) {
	const tokens = markdown.parse(source, {}), headings = [], seen = new Map(), used = new Set();
	for (let i = 0; i < tokens.length; i++) {
		if (tokens[i].type !== 'heading_open') continue;
		const text = tokens[i + 1].content, base = slugify(text);
		let count = seen.get(base) || 0, anchor = base + (count ? '-' + count : '');
		while (used.has(anchor)) anchor = base + '-' + (++count);
		seen.set(base, count + 1);
		used.add(anchor);
		if (tokens[i].tag === 'h2') headings.push({ text, anchor });
	}
	return headings;
}
function category(title) {
	if (['What you get', 'Version compatibility', 'Installation', 'Authorize every extension call'].includes(title)) return 'Start here';
	if (/client$/.test(title)) return 'Clients';
	if (/metrics|Sensitive|assertions|Capacity/.test(title)) return 'Observability and capacity';
	if (/Failure|breaker|admission|Admission|erasure|shutdown|tenant|Testing|Related|License|URL safety/.test(title)) return 'Operations and support';
	return 'Extensions';
}
export function renderTopics(readme) {
	const groups = new Map();
	for (const heading of headingsOf(readme)) {
		if (heading.text === 'Table of contents') continue;
		const group = category(heading.text);
		if (!groups.has(group)) groups.set(group, []);
		groups.get(group).push(`- [${heading.text}](#${heading.anchor})`);
	}
	return [...groups].map(([name, items]) => `### ${name}\n\n${items.join('\n')}`).join('\n\n');
}
export function renderPaths(manifest) {
	return '| Need | Type | Route | Owner |\n| --- | --- | --- | --- |\n' + manifest.paths.map((path) => `| ${path.need} | ${path.type} | [${path.label}](${path.destination}) | ${path.owner} |`).join('\n');
}
export function validateManifest(manifest, pkg) {
	const errors = [];
	// Version 2 explicitly permits package-owned docs routes alongside README and site routes.
	if (manifest?.schemaVersion !== 2 || manifest.owner !== pkg.name) errors.push('Invalid documentation manifest owner/version');
	if (!manifest || Object.keys(manifest).sort().join(',') !== 'compatibilitySource,entryBudgetLines,owner,paths,readmeMaxLines,schemaVersion') errors.push('Unexpected documentation manifest fields');
	if (manifest?.compatibilitySource !== compatibilitySource) errors.push('Invalid compatibility source');
	if (!Number.isInteger(manifest?.entryBudgetLines) || manifest.entryBudgetLines < 1 || manifest.entryBudgetLines > 140) errors.push('Invalid entry budget');
	if (!Number.isInteger(manifest?.readmeMaxLines) || manifest.readmeMaxLines < 1) errors.push('Invalid README size ratchet');
	if (!Array.isArray(manifest?.paths) || manifest.paths.length !== keys.length) return [...errors, 'Expected eight ordered reader paths'];
	const destinations = new Set();
	for (let i = 0; i < keys.length; i++) {
		const path = manifest.paths[i];
		if (path?.key !== keys[i]) errors.push('Reader paths must retain declared order');
		if (path?.type !== types[i]) errors.push('Reader path types must match their declared purpose');
		if (!path || Object.keys(path).sort().join(',') !== 'destination,key,label,need,owner,type') { errors.push('Unexpected reader path fields'); continue; }
		if (Object.values(path).some((v) => typeof v !== 'string' || !v.trim() || v !== v.trim() || /[\u0000-\u001f\u007f`|<>]/.test(v))) errors.push('Reader path values must be printable lines');
		if (destinations.has(path.destination)) errors.push('Duplicate reader destination');
		destinations.add(path.destination);
		if (path.destination.startsWith('#')) {
			if (path.owner !== 'README.md') errors.push('README anchor owner mismatch');
		} else if (path.destination.startsWith('./docs/')) {
			if (path.destination.slice(2) !== path.owner || path.destination.includes('..', 2)) errors.push('Document owner mismatch');
		} else {
			try {
				const url = new URL(path.destination);
				if (url.protocol !== 'https:' || url.hostname !== 'svelte-realtime.dev' || url.username || url.password || url.search || url.hash || path.owner !== url.hostname) errors.push('Expected a clean owned HTTPS route');
			} catch { errors.push('Invalid documentation route'); }
		}
	}
	return errors;
}
export function governedNativeRefs(rows) {
	return [...new Set(rows.map(row => /(?:#|tags\/)(v\d+\.\d+\.\d+)/.exec(row.uwebsockets)?.[1]).filter(Boolean))];
}
export function validatePackageScripts(pkg) {
	const errors = [];
	const command = 'node scripts/check-documentation-contract.js';
	if (pkg.scripts?.['check:documentation'] !== command) errors.push('scripts.check:documentation must execute the documentation contract directly');
	for (const name of ['check', 'verify:docs']) {
		const commands = (pkg.scripts?.[name] || '').split('&&').map(value => value.trim());
		if (commands.filter(value => value === 'npm run check:documentation').length !== 1 || commands.some(value => value.includes(command))) errors.push('scripts.' + name + ' must execute check:documentation exactly once');
	}
	return errors;
}
export function validateReadme(readme, manifest, nativeRefs = []) {
	readme = readme.replaceAll('\r\n', '\n');
	const errors = [];
	try {
		if (replaceBlock(readme, 'documentation-paths', renderPaths(manifest)) !== readme) errors.push('Stale reader paths');
		if (replaceBlock(readme, 'topic-index', renderTopics(readme)) !== readme) errors.push('Stale topic index');
	} catch (error) { errors.push(error.message); }
	const end = readme.indexOf('<!-- documentation-paths:end -->');
	if (end < 0 || readme.slice(0, end).split('\n').length > manifest.entryBudgetLines) errors.push('Reader paths exceed entry budget');
	if (readme.split('\n').length > manifest.readmeMaxLines) errors.push('README exceeds readmeMaxLines; move content into docs before adding more');
	if (end > readme.indexOf('\n## What you get')) errors.push('Reader paths must precede the feature overview');
	let ungenerated = readme;
	try {
		for (const name of ['table', 'install', 'floor']) ungenerated = replaceBlock(ungenerated, 'compatibility-' + name, '');
		const patterns = [
			/\buWebSockets\.js\/archive\/refs\/tags\/v\d+\.\d+\.\d+/g,
			/\bv\d+\.\d+\.\d+\b/g,
			...nativeRefs.map(ref => {
				const [major, minor] = ref.replace(/^v/, '').split('.');
				return new RegExp('\\b' + major + '\\.' + minor + '(?:\\.\\d+)?\\b', 'g');
			})
		];
		if (patterns.some(pattern => pattern.test(ungenerated))) errors.push('Native compatibility facts must stay in generated compatibility blocks');
	} catch (error) { errors.push(error.message); }
	const anchors = anchorsOf(readme);
	for (const path of manifest.paths) if (path.destination.startsWith('#') && !anchors.has(path.destination.slice(1))) errors.push('Missing reader path anchor: ' + path.destination);
	return errors;
}
export function renderEntries(pkg, catalog, readme) {
	const paths = Object.keys(pkg.exports).sort();
	if (catalog.schemaVersion !== 1 || !Array.isArray(catalog.entries)) throw new Error('Invalid public entry catalog');
	if (catalog.entries.length !== paths.length || new Set(catalog.entries.map((entry) => entry.path)).size !== paths.length || catalog.entries.some((entry) => !paths.includes(entry.path))) throw new Error('Public entry metadata must match every package export exactly');
	const headings = new Map(headingsOf(readme).map((h) => [h.text, h.anchor]));
	const rows = [...catalog.entries].sort((a, b) => a.path.localeCompare(b.path)).map((entry) => {
		if (Object.keys(entry).sort().join(',') !== 'backend,goal,heading,path,prerequisites,status' || Object.values(entry).some((value) => typeof value !== 'string' || !value.trim() || /[\r\n|<>]/.test(value))) throw new Error('Invalid public entry metadata');
		if (!headings.has(entry.heading)) throw new Error('Missing public entry heading: ' + entry.heading);
		const target = pkg.exports[entry.path];
		const declaration = typeof target === 'object' ? target.types : null;
		if (typeof declaration !== 'string') throw new Error('Entry missing declaration: ' + entry.path);
		return `| [${pkg.name}/${entry.path.slice(2)}](../${declaration.replace(/^\.\//, '')}) | ${entry.goal} | ${entry.backend} | ${entry.prerequisites} | ${entry.status} | [${entry.heading}](../README.md#${headings.get(entry.heading)}) |`;
	});
	return '# Public entry points\n\n[Package overview](../README.md) | [Installation](../README.md#installation) | [Release history](../CHANGELOG.md)\n\nChoose an import below, satisfy its prerequisites, then follow its reference.\nAll entries follow the package version; testing utilities are for development.\n\n| Import | Goal | Backend | Prerequisites | Status | Reference |\n| --- | --- | --- | --- | --- | --- |\n' + rows.join('\n') + '\n';
}
function main() {
	const read = (path) => readFileSync(resolve(root, path), 'utf8');
	const pkg = JSON.parse(read('package.json')), manifest = JSON.parse(read('docs/documentation.v2.json')), catalog = JSON.parse(read('docs/public-entries.v1.json'));
	const errors = [...validateManifest(manifest, pkg), ...validatePackageScripts(pkg)];
	const nativeRefs = governedNativeRefs(installedCompatibility().rows);
	if (errors.length) throw new Error(errors.join('\n'));
	let readme = read('README.md').replaceAll('\r\n', '\n');
	const rendered = replaceBlock(replaceBlock(readme, 'documentation-paths', renderPaths(manifest)), 'topic-index', renderTopics(readme));
	const entries = renderEntries(pkg, catalog, rendered);
	if (process.argv.includes('--write')) {
		writeFileSync(resolve(root, 'README.md'), rendered);
		writeFileSync(resolve(root, 'docs/public-entries.md'), entries);
		readme = rendered;
	} else if (read('docs/public-entries.md') !== entries) errors.push('Stale public entry index');
	errors.push(...validateReadme(readme, manifest, nativeRefs));
	if (errors.length) throw new Error(errors.join('\n'));
	console.log(`check-documentation-contract: ${manifest.paths.length} reader paths, ${catalog.entries.length} public entries and every topic indexed; README ${readme.split('\n').length}/${manifest.readmeMaxLines} lines`);
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
