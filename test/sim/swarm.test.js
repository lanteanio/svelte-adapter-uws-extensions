import { describe, it, expect } from 'vitest';
import { runRedisSimSwarm, runPgSimSwarm } from '../../src/sim.js';

// faultMode uses only SURVIVABLE relay faults (reorder / duplicate / jitter, no
// loss): quiescent cross-instance convergence holds under these, so any
// violation a faulted run reports is a real bug. drop / corrupt legitimately
// diverge (a lost relay frame shorts an instance) and are covered by the
// detection tests in redis.test.js / pg.test.js, not by the swarm pass/fail.
const SURVIVABLE = { reorder: 0.6, duplicate: 0.3, maxJitterMs: 30 };

describe('runRedisSimSwarm', () => {
	it('runs a clean swarm and reports all passed', async () => {
		const { summary, runs } = await runRedisSimSwarm({ count: 4, startSeed: 1, base: { instances: 3 } });
		expect(summary.total).toBe(4);
		expect(summary.passed).toBe(4);
		expect(summary.failed).toBe(0);
		expect(summary.firstFailingSeed).toBeNull();
		expect(summary.ok).toBe(true);
		expect(runs.map((r) => r.seed)).toEqual(['1', '2', '3', '4']);
		for (const r of runs) expect(r.fingerprint).toMatch(/^[0-9a-f]{8}$/);
	});

	it('accepts an explicit seed list and stringifies numeric seeds', async () => {
		const { summary, runs } = await runRedisSimSwarm({ seeds: ['a', 7], base: { instances: 2 } });
		expect(summary.total).toBe(2);
		expect(runs.map((r) => r.seed)).toEqual(['a', '7']);
	});

	it('is deterministic: two swarms produce identical runs and summary', async () => {
		const a = await runRedisSimSwarm({ count: 3, startSeed: 50, base: { instances: 2 } });
		const b = await runRedisSimSwarm({ count: 3, startSeed: 50, base: { instances: 2 } });
		expect(b.runs).toEqual(a.runs);
		expect(b.summary).toEqual(a.summary);
	});

	it('faultMode:on faults the relay yet convergence holds; fingerprints change vs clean', async () => {
		const on = await runRedisSimSwarm({ count: 4, startSeed: 1, base: { instances: 3 }, faultMode: 'on', faultProfile: SURVIVABLE });
		const off = await runRedisSimSwarm({ count: 4, startSeed: 1, base: { instances: 3 } });
		expect(on.runs.every((r) => r.faulted)).toBe(true);
		expect(on.summary.faulted).toBe(4);
		// Survivable faults change the cross-instance delivery order, so the
		// structural fingerprint differs (proving the faults were applied), but no
		// message is lost, so convergence still holds.
		expect(on.runs.some((r, i) => r.fingerprint !== off.runs[i].fingerprint)).toBe(true);
		expect(on.summary.ok).toBe(true);
	});

	it('faultMode:random faults a reproducible, non-trivial subset', async () => {
		const a = await runRedisSimSwarm({ count: 10, startSeed: 1, base: { instances: 2 }, faultMode: 'random', faultProfile: SURVIVABLE, faultProbability: 0.5 });
		const b = await runRedisSimSwarm({ count: 10, startSeed: 1, base: { instances: 2 }, faultMode: 'random', faultProfile: SURVIVABLE, faultProbability: 0.5 });
		expect(a.summary.faulted).toBeGreaterThan(0);
		expect(a.summary.faulted).toBeLessThan(10);
		expect(b.runs.map((r) => r.faulted)).toEqual(a.runs.map((r) => r.faulted));
		expect(a.summary.ok).toBe(true);
	});

	it('re-checks determinism at checkRatio 1 and all reproduce', async () => {
		const { summary, runs } = await runRedisSimSwarm({ count: 4, startSeed: 1, base: { instances: 2 }, checkRatio: 1 });
		expect(summary.determinismChecks).toBe(4);
		expect(summary.determinismFailures).toBe(0);
		expect(summary.determinismFailingSeeds).toEqual([]);
		expect(runs.every((r) => r.reproduced === true)).toBe(true);
		expect(summary.ok).toBe(true);
	});
});

describe('runPgSimSwarm', () => {
	it('runs a clean pg swarm and reports all passed', async () => {
		const { summary, runs } = await runPgSimSwarm({ count: 3, startSeed: 1, base: { instances: 2 } });
		expect(summary.total).toBe(3);
		expect(summary.passed).toBe(3);
		expect(summary.ok).toBe(true);
		for (const r of runs) expect(r.fingerprint).toMatch(/^[0-9a-f]{8}$/);
	});

	it('faultMode:on holds convergence under a survivable NOTIFY relay', async () => {
		const { summary } = await runPgSimSwarm({ count: 3, startSeed: 1, base: { instances: 3 }, faultMode: 'on', faultProfile: SURVIVABLE });
		expect(summary.faulted).toBe(3);
		expect(summary.ok).toBe(true);
	});
});
