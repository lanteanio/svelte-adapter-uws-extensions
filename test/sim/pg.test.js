import { describe, it, expect } from 'vitest';
import { runPgSim, replayPgSim } from '../../src/sim.js';

const ticksOn = (result, instance, topic = 'room', event = 'tick') =>
	result.clusterFrames[instance].clients.flat().filter((f) => f && f.event === event && f.topic === topic);

describe('runPgSim - cross-instance LISTEN/NOTIFY convergence', () => {
	it("fans a pg_notify out to subscribed clients on every instance ('all' mode)", async () => {
		const r = await runPgSim({ instances: 3, clients: 2, topics: ['room'], seed: 'pg-conv' });
		expect(r.invariantViolations).toEqual([]);
		// 3 notifies, each instance has 2 clients => 6 tick frames per instance.
		for (let i = 0; i < 3; i++) expect(ticksOn(r, i).length).toBe(6);
	});

	it('reproduces a clean run and a run under NOTIFY drop + reorder faults', async () => {
		const clean = await runPgSim({ instances: 3, seed: 'pg-clean' });
		expect((await replayPgSim(clean)).reproduced).toBe(true);
		const faulted = await runPgSim({ instances: 4, seed: 'pg-faults', relayFaults: { drop: 0.3, reorder: 0.6, duplicate: 0.2, maxJitterMs: 30 } });
		expect((await replayPgSim(faulted)).reproduced).toBe(true);
	});

	it('drops only faulted NOTIFY frames; a seed sweep stays reproducible', async () => {
		for (const seed of ['p0', 'p1', 'p2', 'p3']) {
			const r = await runPgSim({ instances: 3, seed, relayFaults: { drop: 0.25, reorder: 0.5, maxJitterMs: 25 } });
			expect((await replayPgSim(r)).reproduced).toBe(true);
		}
	});
});

describe('runPgSim - replay (cross-instance resume over the shared durable ring)', () => {
	async function replayScenario(api) {
		for (let n = 0; n < 5; n++) await api.instance(0).replay.publish(api.instance(0).platform, 'room', 'tick', { n });
		await api.advance();
		const epoch = await api.instance(1).replay.currentEpoch('room');
		const late = api.instance(1).connect();
		await api.advance();
		late.subscribe('room');
		await api.advance();
		late.send({ type: 'resume', sessionId: 's1', lastSeenSeqs: { room: 0 }, lastSeenEpochs: { room: epoch } });
		await api.advance();
	}

	it('gap-fills a resume on instance B from rows instance A persisted, with no gaps', async () => {
		const r = await runPgSim({ instances: 2, clients: 0, topics: ['room'], plugins: ['replay'], seed: 'pg-replay-conv', scenario: replayScenario });
		const frames = r.clusterFrames[1].clients[0];
		const seqs = frames.filter((f) => f && f.topic === '__replay:room' && f.event === 'msg').map((f) => f.data.seq);
		expect(seqs).toEqual([1, 2, 3, 4, 5]);
		expect(frames.some((f) => f && f.topic === '__replay:room' && f.event === 'end')).toBe(true);
	});

	it('reproduces the pg replay run bit-for-bit', async () => {
		const original = await runPgSim({ instances: 2, clients: 0, topics: ['room'], plugins: ['replay'], seed: 'pg-replay-repro', scenario: replayScenario });
		expect((await replayPgSim(original)).reproduced).toBe(true);
	});
});

describe('runPgSim - advisory-lock leader election (cross-instance contention)', () => {
	// N instances each take a dedicated connection off the ONE shared mock-pg and
	// contend for the same advisory lock; exactly one wins, deterministically.
	async function leaderScenario(api, winners) {
		const conns = [];
		for (let i = 0; i < api.instances; i++) { const c = api.client.createClient(); await c.connect(); conns.push(c); }
		const results = await Promise.all(conns.map((c) => c.query('SELECT pg_try_advisory_lock($1) AS acquired', [42])));
		winners.count = results.filter((res) => res.rows[0].acquired === true).length;
		winners.holder = api.client._getAdvisoryLocks().get(42);
		await api.advance();
		for (const c of conns) await c.end();
	}

	it('elects exactly one leader across the cohort', async () => {
		const winners = {};
		await runPgSim({ instances: 5, clients: 0, seed: 'leader-1', scenario: (api) => leaderScenario(api, winners) });
		expect(winners.count).toBe(1);
		expect(typeof winners.holder).toBe('number');
	});

	it('elects the same leader bit-for-bit across two runs of a seed', async () => {
		const a = {}, b = {};
		await runPgSim({ instances: 5, clients: 0, seed: 'leader-2', scenario: (api) => leaderScenario(api, a) });
		await runPgSim({ instances: 5, clients: 0, seed: 'leader-2', scenario: (api) => leaderScenario(api, b) });
		expect(b.holder).toBe(a.holder);
		expect(b.count).toBe(1);
	});
});
