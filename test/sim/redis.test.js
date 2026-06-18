import { describe, it, expect } from 'vitest';
import { runRedisSim, replayRedisSim } from '../../src/sim.js';

// Decoded data frames a client received for a topic+event.
const ticksOn = (result, instance, topic = 'room', event = 'tick') =>
	result.clusterFrames[instance].clients.flat().filter((f) => f && f.event === event && f.topic === topic);

describe('runRedisSim - cross-instance pub/sub convergence', () => {
	it('relays a publish on one instance to subscribed clients on every other instance', async () => {
		const r = await runRedisSim({ instances: 3, clients: 2, topics: ['room'], seed: 'conv-1' });
		expect(r.invariantViolations).toEqual([]);
		// instance 0 published 3 ticks; each instance has 2 clients => 6 tick frames each.
		for (let i = 0; i < 3; i++) expect(ticksOn(r, i).length).toBe(6);
		expect(r.metrics.instances).toBe(3);
	});

	it('suppresses the publishing instance from re-delivering its own relayed frame (echo suppression)', async () => {
		// Each client on instance 0 must see each tick EXACTLY once (the local fan-out),
		// not twice (local + its own bus subscriber echoing the relay back).
		const r = await runRedisSim({ instances: 2, clients: 1, topics: ['room'], seed: 'echo-1' });
		const inst0 = r.clusterFrames[0].clients[0].filter((f) => f && f.event === 'tick');
		expect(inst0.length).toBe(3);
		expect(inst0.map((f) => f.data.n)).toEqual([0, 1, 2]);
	});
});

describe('runRedisSim - determinism self-gate', () => {
	it('reproduces a clean run', async () => {
		const original = await runRedisSim({ instances: 3, seed: 'replay-clean' });
		expect((await replayRedisSim(original)).reproduced).toBe(true);
	});

	it('reproduces a run under relay drop + reorder + duplicate faults', async () => {
		const original = await runRedisSim({
			instances: 4, seed: 'replay-faults',
			relayFaults: { drop: 0.3, reorder: 0.6, duplicate: 0.2, maxJitterMs: 30 }
		});
		expect((await replayRedisSim(original)).reproduced).toBe(true);
	});

	it('diverges across seeds under relay faults while each seed still reproduces', async () => {
		const a = await runRedisSim({ instances: 3, seed: 'div-a', relayFaults: { drop: 0.5, reorder: 0.7, maxJitterMs: 40 } });
		const b = await runRedisSim({ instances: 3, seed: 'div-b', relayFaults: { drop: 0.5, reorder: 0.7, maxJitterMs: 40 } });
		expect((await replayRedisSim(a)).reproduced).toBe(true);
		expect(JSON.stringify(a.clusterFrames)).not.toBe(JSON.stringify(b.clusterFrames));
	});
});

describe('runRedisSim - replay (cross-instance resume over the shared ring)', () => {
	// A late client on a DIFFERENT instance resumes from the shared replay ring that
	// another instance populated, gap-filling the contiguous tail.
	async function replayScenario(api) {
		for (let n = 0; n < 5; n++) api.instance(0).replay.publish(api.instance(0).platform, 'room', 'tick', { n });
		await api.advance();
		const epoch = await api.instance(1).replay.currentEpoch('room');
		const late = api.instance(1).connect();
		await api.advance();
		late.subscribe('room');
		await api.advance();
		late.send({ type: 'resume', sessionId: 's1', lastSeenSeqs: { room: 0 }, lastSeenEpochs: { room: epoch } });
		await api.advance();
	}

	it('gap-fills a resume on instance B from the ring instance A wrote, with no gaps', async () => {
		const r = await runRedisSim({ instances: 2, clients: 0, topics: ['room'], plugins: ['replay'], seed: 'replay-conv', scenario: replayScenario });
		const frames = r.clusterFrames[1].clients[0];
		const seqs = frames.filter((f) => f && f.topic === '__replay:room' && f.event === 'msg').map((f) => f.data.seq);
		expect(seqs).toEqual([1, 2, 3, 4, 5]);
		expect(frames.some((f) => f && f.topic === '__replay:room' && f.event === 'end')).toBe(true);
	});

	it('reproduces the replay run bit-for-bit', async () => {
		const original = await runRedisSim({ instances: 2, clients: 0, topics: ['room'], plugins: ['replay'], seed: 'replay-repro', scenario: replayScenario });
		expect((await replayRedisSim(original)).reproduced).toBe(true);
	});
});

describe('runRedisSim - presence (cross-instance roster convergence)', () => {
	const presenceConfig = {
		instances: 2, clients: 0, topics: ['room'], plugins: ['presence'],
		pluginOptions: { presence: { key: 'id', select: (ud) => ({ id: ud.id, name: ud.name }), heartbeat: 60000, ttl: 180 } },
		handler: { upgrade: ({ headers }) => ({ id: headers['x-id'], name: headers['x-name'] }) }
	};

	it('a join on instance A appears in the roster read on instance B', async () => {
		const r = await runRedisSim({
			...presenceConfig, seed: 'presence-conv',
			scenario: async (api) => {
				api.instance(0).connect({ headers: { 'x-id': 'alice', 'x-name': 'Alice' } });
				await api.advance();
				api.instance(0).clients()[0].subscribe('room');
				await api.advance();
				const roster = await api.instance(1).presence.list('room');
				expect(roster.map((u) => u.id)).toEqual(['alice']);
				api.instance(1).connect({ headers: { 'x-id': 'bob', 'x-name': 'Bob' } });
				await api.advance();
				api.instance(1).clients()[0].subscribe('room');
				await api.advance();
				const both = await api.instance(0).presence.list('room');
				expect(both.map((u) => u.id).sort()).toEqual(['alice', 'bob']);
			}
		});
		expect(r.invariantViolations).toEqual([]);
	});

	it('reproduces a presence run bit-for-bit', async () => {
		const scenario = async (api) => {
			for (const [i, id] of [[0, 'alice'], [1, 'bob'], [0, 'carol']]) {
				const c = api.instance(i).connect({ headers: { 'x-id': id, 'x-name': id } });
				await api.advance();
				c.subscribe('room');
				await api.advance();
			}
		};
		const original = await runRedisSim({ ...presenceConfig, seed: 'presence-repro', scenario });
		expect((await replayRedisSim(original)).reproduced).toBe(true);
	});
});

describe('runRedisSim - convergence under faults', () => {
	it('drops only the faulted relay frames; the local instance is unaffected', async () => {
		const r = await runRedisSim({ instances: 2, clients: 1, topics: ['room'], seed: 'drop-local', relayFaults: { drop: 1 } });
		// drop:1 => no cross-instance delivery at all, but instance 0's local clients
		// still see every tick (the relay fault is on the bus, not the local fan-out).
		expect(ticksOn(r, 0).length).toBe(3);
		expect(ticksOn(r, 1).length).toBe(0);
		expect((await replayRedisSim(r)).reproduced).toBe(true);
	});

	it('reproduces a run under relay corrupt faults (byte-flipped frames drop mode-invariantly)', async () => {
		const r = await runRedisSim({ instances: 3, seed: 'corrupt-1', relayFaults: { corrupt: 0.6 } });
		// A corrupt relay frame fails to decode on the receiving instance, so its
		// per-topic delivered-seq run can legitimately trail the others - a real
		// cross-instance divergence the convergence check reports. The per-instance
		// bookkeeping invariants must still be clean.
		const bookkeeping = r.invariantViolations.filter((v) => v.category !== 'cluster.state-divergence');
		expect(bookkeeping).toEqual([]);
		expect((await replayRedisSim(r)).reproduced).toBe(true);
	});

	it('a seed sweep under faults is internally reproducible', async () => {
		for (const seed of ['s0', 's1', 's2', 's3']) {
			const r = await runRedisSim({ instances: 3, seed, relayFaults: { drop: 0.2, reorder: 0.5, maxJitterMs: 25 } });
			expect((await replayRedisSim(r)).reproduced).toBe(true);
		}
	});
});

describe('runRedisSim - cross-instance convergence invariant', () => {
	it('a clean multi-instance run converges (no divergence reported)', async () => {
		const r = await runRedisSim({ instances: 4, clients: 2, topics: ['room'], seed: 'converge-clean' });
		// Every instance subscribed to the same topic received the same delivered seq
		// run from the shared relay, so the convergence check is silent.
		expect(r.invariantViolations).toEqual([]);
		// And a clean run is its own reproducer.
		expect((await replayRedisSim(r)).reproduced).toBe(true);
	});

	it('detects and reproduces a real divergence when a partial drop shorts one instance', async () => {
		const r = await runRedisSim({ instances: 4, clients: 1, topics: ['room'], seed: 'cv0', relayFaults: { drop: 0.4, maxJitterMs: 20 } });
		const divs = r.invariantViolations.filter((v) => v.category === 'cluster.state-divergence');
		// The drop shorts exactly one instance's delivered run for the shared topic.
		expect(divs).toHaveLength(1);
		expect(divs[0].context.topics).toEqual(['room']);
		expect(divs[0].context.instances).toEqual([1]);
		expect(divs[0].context.expectedHash).not.toBe(divs[0].context.divergentHash);
		// The whole run - including the recorded divergence - reproduces bit-for-bit,
		// so the self-gate covers the new violation automatically.
		const replay = await replayRedisSim(r);
		expect(replay.reproduced).toBe(true);
		expect(replay.invariantViolations).toEqual(r.invariantViolations);
	});

	it('a clean replay run never reports a replay seq-regression', async () => {
		// The resume scenario gap-fills instance B from the ring instance A wrote; the
		// delivered replay seqs stay at or below the shared ring head on every instance.
		async function replayScenario(api) {
			for (let n = 0; n < 5; n++) api.instance(0).replay.publish(api.instance(0).platform, 'room', 'tick', { n });
			await api.advance();
			const epoch = await api.instance(1).replay.currentEpoch('room');
			const late = api.instance(1).connect();
			await api.advance();
			late.subscribe('room');
			await api.advance();
			late.send({ type: 'resume', sessionId: 's1', lastSeenSeqs: { room: 0 }, lastSeenEpochs: { room: epoch } });
			await api.advance();
		}
		const r = await runRedisSim({ instances: 2, clients: 0, topics: ['room'], plugins: ['replay'], seed: 'seq-clean', scenario: replayScenario });
		expect(r.invariantViolations.filter((v) => v.category === 'redis.replay.seq-regression')).toEqual([]);
		expect((await replayRedisSim(r)).reproduced).toBe(true);
	});
});
