import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseAllDocuments } from 'yaml';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const start = '<!-- verification-inventory:start -->';
const end = '<!-- verification-inventory:end -->';

export function expandLanes(name, scripts, seen = new Set()) {
	if (!(name in scripts)) throw new Error(`Unknown npm script: ${name}`);
	if (seen.has(name)) throw new Error(`Cyclic npm script: ${name}`);
	const children = [...scripts[name].matchAll(/npm run ([a-zA-Z0-9:-]+)|npm (test)\b/g)].map((match) => match[1] || match[2]);
	const chain = new Set(seen).add(name);
	return [name, ...children.flatMap((child) => expandLanes(child, scripts, chain))];
}

function workflows(source) {
	return parseAllDocuments(source).map(document => {
		if (document.errors.length) throw new Error('Invalid workflow YAML: ' + document.errors[0].message);
		return document.toJS();
	});
}

export function workflowLanes(source) {
	const lanes = [];
	function visit(value) {
		if (!value || typeof value !== 'object') return;
		if (typeof value.run === 'string') {
			const command = value.run;
			if (/\bnpm\s+(?:run|test)\b/.test(command)) {
				if (command.includes('\n') || command.includes('$' + '{{')) throw new Error('Workflow npm lanes must use literal single-line commands');
				for (const script of command.matchAll(/npm run ([a-zA-Z0-9:-]+)|npm (test)\b/g)) lanes.push(script[1] || script[2]);
			}
		}
		for (const child of Object.values(value)) visit(child);
	}
	for (const workflow of workflows(source)) visit(workflow);
	return lanes;
}

export function inventory(scripts, workflow) {
	const hostedRoots = workflowLanes(workflow);
	if (hostedRoots.length === 0) throw new Error('No hosted npm lane found');
	const hosted = new Set(hostedRoots.flatMap((name) => expandLanes(name, scripts)));
	const jobs = workflows(workflow).flatMap(document => Object.keys(document.jobs || {}));
	if (jobs.length === 0) throw new Error('No hosted jobs found');
	const names = Object.keys(scripts).filter((name) => name.startsWith('verify:') || ['smoke', 'gate:cross-repo', 'check:format', 'check:contributor-map'].includes(name)).sort();
	const rows = names.map((name) => {
		const expanded = [...new Set(expandLanes(name, scripts).slice(1))];
		return `| \`npm run ${name}\` | ${expanded.length ? expanded.map((part) => '\`' + part + '\`').join(', ') : '\`' + scripts[name].replaceAll('|', '\\|') + '\`'} | ${hosted.has(name) ? 'Hosted' : 'Local'} |`;
	});
	return `Hosted jobs: ${jobs.map((name) => '\`' + name + '\`').join(', ')}.\n\n| Command | Script expansion | Coverage |\n|---|---|---|\n${rows.join('\n')}\n`;
}

export function checkMap(source, scripts, workflow) {
	const first = source.indexOf(start), last = source.indexOf(end);
	if (first < 0 || last <= first) throw new Error('Missing verification inventory markers');
	for (const match of source.matchAll(/npm run ([a-zA-Z0-9:-]+)/g)) {
		if (!(match[1] in scripts)) throw new Error(`Documented npm script does not exist: ${match[1]}`);
	}
	const expected = source.slice(0, first + start.length) + '\n' + inventory(scripts, workflow) + source.slice(last);
	return { expected, matches: expected === source };
}

function main() {
	const file = resolve(root, 'CONTRIBUTING.md');
	const source = readFileSync(file, 'utf8');
	const { scripts } = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
	const workflow = readdirSync(resolve(root, '.github/workflows')).filter((name) => /\.ya?ml$/.test(name)).sort().map((name) => readFileSync(resolve(root, '.github/workflows', name), 'utf8')).join('\n---\n');
	const result = checkMap(source, scripts, workflow);
	if (process.argv.includes('--write')) writeFileSync(file, result.expected);
	else if (!result.matches) throw new Error('CONTRIBUTING.md inventory is stale; run node scripts/check-contributor-map.js --write');
	console.log('check-contributor-map: package scripts and hosted lanes agree');
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
