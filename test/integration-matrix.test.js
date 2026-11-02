import { readdirSync, readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';
import { expandLanes, workflowLanes } from '../scripts/check-contributor-map.js';

const read = (path) => readFileSync(new URL('../' + path, import.meta.url), 'utf8');
const { scripts, engines } = JSON.parse(read('package.json'));

// Every backend tier is a vitest.config.integration.<tier>.js file; the
// directory listing, not a list kept beside it, is what the umbrella and the
// hosted workflow must cover.
const tiers = readdirSync(new URL('..', import.meta.url))
	.map((name) => /^vitest\.config\.integration\.([a-z-]+)\.js$/.exec(name)?.[1])
	.filter(Boolean)
	.sort();
const tierScripts = tiers.map((tier) => 'test:integration:' + tier);

function covered(roots) {
	return new Set(roots.flatMap((name) => expandLanes(name, scripts)));
}

describe('integration tier coverage', () => {
	it('finds every backend tier', () => {
		expect(tiers).toEqual(['cluster', 'cluster-mirror', 'ntp', 'pgbouncer', 'postgres', 'redis', 'valkey', 'valkey-cluster', 'valkey-cluster-mirror']);
		for (const tier of tiers) {
			expect(scripts['test:integration:' + tier]).toBe('vitest run --config vitest.config.integration.' + tier + '.js');
		}
	});

	it('runs every tier from the umbrella, the full lane and the release lane', () => {
		for (const lane of ['test:integration', 'verify:full', 'verify:release']) {
			const reached = covered([lane]);
			expect(tierScripts.filter((name) => !reached.has(name)), lane + ' misses tiers').toEqual([]);
		}
	});

	it('gives every tier a hosted job on the Node floor and on a newer line', () => {
		const source = read('.github/workflows/integration.yml');
		const reached = covered(workflowLanes(source));
		expect(tierScripts.filter((name) => !reached.has(name)), 'hosted workflow misses tiers').toEqual([]);
		const job = parse(source).jobs.backend;
		expect(job.strategy['fail-fast']).toBe(false);
		const nodes = job.strategy.matrix.node.map(Number);
		expect(nodes[0]).toBe(Number(/^>=(\d+)\./.exec(engines.node)[1]));
		expect(Math.max(...nodes)).toBeGreaterThan(nodes[0]);
		// Each matrix tier selects exactly one lane step, and each lane step
		// belongs to a matrix tier, so no tier can sit in the matrix running
		// nothing and no step can be unreachable.
		const lanes = job.steps.filter((step) => /^npm run /.test(step.run ?? ''));
		for (const tier of job.strategy.matrix.tier) {
			expect(lanes.filter((step) => step.if === "matrix.tier == '" + tier + "'"), tier).toHaveLength(1);
		}
		const selectors = job.strategy.matrix.tier.map((tier) => "matrix.tier == '" + tier + "'");
		for (const step of lanes) expect(selectors, step.run).toContain(step.if);
	});
});

describe('backend images', () => {
	const digest = /@sha256:[0-9a-f]{64}$/;

	it('pins every Compose service image by digest', () => {
		const files = readdirSync(new URL('integration/', import.meta.url)).filter((name) => /^docker-compose.*\.yml$/.test(name));
		expect(files.length).toBeGreaterThanOrEqual(5);
		for (const file of files) {
			const services = parse(read('test/integration/' + file)).services;
			for (const [name, service] of Object.entries(services)) {
				// An override variable may stand in front of the default; the
				// default itself must still be pinned.
				const image = /^\$\{[A-Z_]+:-(.+)\}$/.exec(service.image)?.[1] ?? service.image;
				expect(image, file + ' ' + name).toMatch(digest);
			}
		}
	});

	it('pins the smoke Redis image to the integration Redis image', () => {
		const redis = parse(read('test/integration/docker-compose.yml')).services.redis.image;
		expect(read('scripts/smoke.js')).toContain("'" + redis + "'");
		expect(parse(read('test/integration/docker-compose.cluster.yml')).services['redis-node-0'].image).toBe(redis);
	});
});
