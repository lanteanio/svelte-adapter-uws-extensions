import { describe, it, expect } from 'vitest';
import { validateCandidateLifecycles } from '../scripts/release-policy.js';
import { readFileSync } from 'node:fs';
import { validateReleaseLedger, CANDIDATE_MARKER } from '../scripts/check-release-ledger.js';
const read = path => readFileSync(new URL('../' + path, import.meta.url), 'utf8');
const manifest = read('docs/release-manifest.md');
const changelog = read('CHANGELOG.md');
const baseline = JSON.parse(read('docs/release-identities.v1.json'));
const validate = (m = manifest, c = changelog, b = baseline) => validateReleaseLedger(m, c, b);
const historical = baseline.releases[0];
const identityLine = manifest.split('\n').find(line => line.startsWith('| ' + historical.Package + ' | ' + historical.Version + ' |'));

describe('offline release ledger', () => {
	it('reconciles every historical identity without assuming the working package version is unpublished', () => {
		const result = validate();
		expect(result.releases).toBeGreaterThanOrEqual(99);
		expect(result.headings).toBe(result.releases + result.unpublished);
	});
	it('rejects deletion of a published identity even if its changelog still exists', () => {
		expect(() => validate(manifest.replace(identityLine + '\n', ''))).toThrow('historical published identity removed');
	});
	it('rejects source, artifact and publication-time rewriting', () => {
		for (const field of ['Git head', 'npm shasum', 'npm integrity', 'Published UTC']) {
			const replacement = field === 'Published UTC' ? '2020-01-01T00:00:00.000Z' : field === 'npm integrity' ? 'sha512-' + Buffer.alloc(64).toString('base64') : 'a'.repeat(40);
			expect(() => validate(manifest.replace(identityLine, identityLine.replace(historical[field], replacement)))).toThrow('historical identity changed');
		}
	});
	it('rejects fabricated legacy entries', () => {
		const invented = identityLine.replace('| ' + historical.Version + ' |', '| 8.8.8 |');
		expect(() => validate(manifest.replace(identityLine, identityLine + '\n' + invented))).toThrow('historical identity changed or fabricated');
	});
	it('rejects duplicate published rows', () => {
		expect(() => validate(manifest.replace(identityLine, identityLine + '\n' + identityLine))).toThrow('duplicate published identity');
	});
	it('rejects missing and duplicate individual release headings', () => {
		const heading = changelog.split('\n').find(line => line.startsWith('## [' + historical.Version + ']'));
		expect(() => validate(manifest, changelog.replace(heading, '### Historical entry'))).toThrow('published version has no changelog heading');
		expect(() => validate(manifest, changelog + '\n' + heading + '\n')).toThrow('duplicate release heading');
	});
	it('rejects historical archive coverage instead of a distinct heading', () => {
		expect(() => validate(manifest, changelog.replace('## [0.1.9] -', '## [0.1.9] and earlier -'))).toThrow('invalid release heading');
	});
	it('requires unpublished headings to state their status explicitly', () => {
		const unrecorded = changelog + '\n## [8.8.8] - 2026-10-10\n\nCandidate changes.\n';
		expect(() => validate(manifest, unrecorded)).toThrow('explicit unpublished marker');
		expect(() => validate(manifest, unrecorded.replace('Candidate changes.', CANDIDATE_MARKER))).not.toThrow();
	});
	it('rejects published headings marked as unpublished', () => {
		const heading = changelog.split('\n').find(line => line.startsWith('## [' + historical.Version + ']'));
		expect(() => validate(manifest, changelog.replace(heading, heading + '\n\n' + CANDIDATE_MARKER))).toThrow('published version is marked unpublished');
	});
	it('rejects malformed integrity, timestamps and table structure', () => {
		expect(() => validate(manifest.replace(historical['npm integrity'], 'sha512-YQ=='))).toThrow('invalid npm integrity');
		expect(() => validate(manifest.replace(historical['Published UTC'], '2026-02-30T00:00:00.000Z'))).toThrow('invalid published');
		expect(() => validate(manifest.replace('| npm shasum |', '| shasum |'))).toThrow('table headers');
		expect(() => validate(manifest.replace(identityLine, identityLine.replace(' | Historical npm', ' | extra | Historical npm')))).toThrow('row width');
	});
	it('rejects build metadata in unpublished headings and candidate identities', () => {
		const candidate = 'svelte-adapter-uws-extensions@8.8.8-next.1+build';
		const event = { Candidate: candidate, Event: 'proposed', 'Event UTC': '2026-10-10T00:00:00.000Z', 'Git head': 'a'.repeat(40), 'Planned tag': candidate, 'Target channel': 'next', Evidence: 'review' };
		expect(() => validateCandidateLifecycles([event], [])).toThrow('invalid candidate identity');
		expect(() => validate(manifest, changelog + '\n## [8.8.8-next.1+build] - 2026-10-10\n\n' + CANDIDATE_MARKER + '\n')).toThrow('invalid release heading');
	});
	it('requires one Unreleased heading', () => {
		expect(() => validate(manifest, changelog.replace('## [Unreleased]', ''))).toThrow('exactly one Unreleased');
	});
});
