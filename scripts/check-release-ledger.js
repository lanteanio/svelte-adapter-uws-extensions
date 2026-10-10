import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { valid } from 'semver';
import { channelForVersion, eventTime, validateCandidateLifecycles, validateRoutingEvents } from './release-policy.js';

const PACKAGE = 'svelte-adapter-uws-extensions';
export const CANDIDATE_MARKER = '*Not published to npm; development candidate.*';
export const SUPERSEDED_MARKER = '*Not published to npm; superseded before release.*';
const HEADERS = {
	'Candidate event log': ['Candidate', 'Event', 'Event UTC', 'Git head', 'Planned tag', 'Target channel', 'Evidence'],
	'Published releases': ['Package', 'Version', 'Target channel', 'Git head', 'Git tag', 'npm integrity', 'npm shasum', 'Published UTC', 'Notes'],
	'Routing, corrections and rollback events': ['Event ID', 'Event UTC', 'Event', 'Corrected event', 'Corrected UTC', 'Package', 'Channel', 'From version', 'To version', 'Corrects event', 'Evidence', 'Notes']
};

export function manifestTable(manifest, heading) {
	const lines = manifest.replaceAll('\r\n', '\n').split('\n');
	const start = lines.indexOf('## ' + heading);
	if (start < 0 || lines.lastIndexOf('## ' + heading) !== start) throw new Error('missing or duplicate manifest section: ' + heading);
	const end = lines.findIndex((line, index) => index > start && line.startsWith('## '));
	const table = lines.slice(start + 1, end < 0 ? lines.length : end).filter(line => line.startsWith('|'));
	const cells = line => line.split('|').slice(1, -1).map(cell => cell.trim());
	const headers = HEADERS[heading];
	if (table.length < 2 || JSON.stringify(cells(table[0])) !== JSON.stringify(headers) || cells(table[1]).length !== headers.length || !cells(table[1]).every(cell => /^:?-{3,}:?$/.test(cell))) throw new Error('invalid manifest table headers: ' + heading);
	return table.slice(2).map(line => {
		const values = cells(line);
		if (!line.endsWith('|') || values.length !== headers.length) throw new Error('invalid manifest row width: ' + heading);
		return Object.fromEntries(headers.map((header, index) => [header, values[index]]));
	});
}

export function changelogHeadings(changelog) {
	const lines = changelog.replaceAll('\r\n', '\n').split('\n');
	const headings = new Map();
	let unreleased = 0;
	for (let index = 0; index < lines.length; index++) {
		const line = lines[index];
		if (!line.startsWith('## [')) continue;
		if (line === '## [Unreleased]') { unreleased++; continue; }
		const match = /^## \[([^\]]+)\] - (\d{4}-\d{2}-\d{2})$/.exec(line);
		if (!match || valid(match[1]) !== match[1] || match[1].includes('+') || !Number.isFinite(Date.parse(match[2])) || new Date(match[2]).toISOString().slice(0, 10) !== match[2]) throw new Error('invalid release heading: ' + line);
		if (headings.has(match[1])) throw new Error('duplicate release heading: ' + match[1]);
		let body = index + 1;
		while (lines[body]?.trim() === '') body++;
		const marker = [CANDIDATE_MARKER, SUPERSEDED_MARKER].includes(lines[body]?.trim()) ? lines[body].trim() : null;
		headings.set(match[1], { marker });
	}
	if (unreleased !== 1) throw new Error('CHANGELOG must have exactly one Unreleased heading');
	return headings;
}

export function validateReleaseLedger(manifest, changelog, baseline) {
	const releases = manifestTable(manifest, 'Published releases');
	const headings = changelogHeadings(changelog);
	if (baseline.schemaVersion !== 1 || baseline.package !== PACKAGE || !Array.isArray(baseline.releases)) throw new Error('invalid historical identity baseline');
	const historical = new Map(baseline.releases.map(row => [row.Version, row]));
	if (historical.size !== baseline.releases.length) throw new Error('duplicate historical baseline identity');
	const versions = new Set();
	for (const row of releases) {
		const version = row.Version;
		if (row.Package !== PACKAGE || valid(version) !== version || version.includes('+')) throw new Error('invalid published package/version: ' + version);
		if (versions.has(version)) throw new Error('duplicate published identity: ' + version);
		versions.add(version);
		if (row['Target channel'] !== channelForVersion(version)) throw new Error('invalid target channel: ' + version);
		if (!/^[0-9a-f]{40}$/.test(row['Git head']) || !/^[0-9a-f]{40}$/.test(row['npm shasum'])) throw new Error('invalid source or artifact digest: ' + version);
		const integrity = row['npm integrity'];
		if (!/^sha512-[A-Za-z0-9+/]{86}==$/.test(integrity) || Buffer.from(integrity.slice(7), 'base64').toString('base64') !== integrity.slice(7)) throw new Error('invalid npm integrity: ' + version);
		eventTime(row['Published UTC'], 'published ' + version);
		if (!row.Notes) throw new Error('missing identity evidence notes: ' + version);
		if (row['Git tag'] === 'legacy-none') {
			const original = historical.get(version);
			if (!original || HEADERS['Published releases'].some(field => original[field] !== row[field])) throw new Error('historical identity changed or fabricated: ' + version);
		} else if (row['Git tag'] !== PACKAGE + '@' + version || historical.has(version)) throw new Error('invalid immutable tag: ' + version);
		if (!headings.has(version)) throw new Error('published version has no changelog heading: ' + version);
		if (headings.get(version).marker) throw new Error('published version is marked unpublished: ' + version);
	}
	for (const version of historical.keys()) if (!versions.has(version)) throw new Error('historical published identity removed: ' + version);
	for (const [version, heading] of headings) if (!versions.has(version) && !heading.marker) throw new Error('unrecorded release heading needs an explicit unpublished marker: ' + version);
	const events = manifestTable(manifest, 'Candidate event log');
	const lifecycle = validateCandidateLifecycles(events, releases);
	validateRoutingEvents(manifestTable(manifest, 'Routing, corrections and rollback events'), releases, lifecycle);
	return { releases: releases.length, headings: headings.size, unpublished: headings.size - releases.length };
}

function main() {
	const read = path => readFileSync(new URL('../' + path, import.meta.url), 'utf8');
	const result = validateReleaseLedger(read('docs/release-manifest.md'), read('CHANGELOG.md'), JSON.parse(read('docs/release-identities.v1.json')));
	console.log(`check-release-ledger: ${result.releases} immutable registry identities, ${result.headings} individual headings, ${result.unpublished} explicitly unpublished`);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	try { main(); } catch (error) { console.error('check-release-ledger: ' + error.message); process.exitCode = 1; }
}
