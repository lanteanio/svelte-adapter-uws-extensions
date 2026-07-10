// Steady-state hypothesis layer for the store-backed DST runners: unit
// fire/not-fire per predicate over synthetic trajectories (the mirror module
// carries its own tests, same as invariants.js), then integration through the
// REAL runRedisSim / runPgSim - a clean run stays [], a fault run suppresses
// the guarded hypotheses without firing, a planted leftover-refed-work run
// fires steady.no-quiescence, and a folded run still reproduces bit-for-bit
// under its seed (the determinism self-gate).
import { describe, it, expect } from 'vitest';
import {
	faultClasses,
	checkTimeMonotonic,
	checkQuiescence,
	checkDeliveryMonotonic,
	checkStarvation,
	runSteadyState
} from '../../src/shared/steadystate.js';
import { runRedisSim, replayRedisSim, runPgSim, replayPgSim } from '../../src/sim.js';
import { setIntervalTimer } from '../../src/shared/runtime.js';

/** Build a DeliveredClient from [topic, seq?] frame pairs. */
function client(id, frames) {
	return {
		id,
		raw: frames.map(([topic]) => ({ routingTopic: topic })),
		decoded: frames.map(([, seq]) => (seq === undefined ? { event: 'ctl' } : { seq }))
	};
}

describe('steadystate - checkTimeMonotonic', () => {
	it('does not fire on a non-decreasing clock (equal samples are fine)', () => {
		expect(checkTimeMonotonic([0, 0, 5, 5, 9])).toBeNull();
	});
	it('fires on a backward clock step', () => {
		const v = checkTimeMonotonic([0, 5, 3]);
		expect(v).toEqual({ category: 'steady.time-nonmonotonic', context: { at: 2, from: 5, to: 3 } });
	});
	it('is a no-op on a non-array', () => {
		expect(checkTimeMonotonic(undefined)).toBeNull();
	});
});

describe('steadystate - checkQuiescence', () => {
	it('does not fire when the run drained with zero pending', () => {
		expect(checkQuiescence({ drained: true, pending: 0 })).toBeNull();
	});
	it('fires when refed work is left pending', () => {
		const v = checkQuiescence({ drained: true, pending: 2 });
		expect(v).toEqual({ category: 'steady.no-quiescence', context: { drained: true, pending: 2 } });
	});
	it('does NOT fire when a run settled exactly at the step budget (drained false, zero pending)', () => {
		expect(checkQuiescence({ drained: false, pending: 0 })).toBeNull();
	});
});

describe('steadystate - checkDeliveryMonotonic', () => {
	it('does not fire on strictly-increasing per-topic seqs', () => {
		expect(checkDeliveryMonotonic([client('0:1', [['room', 1], ['room', 2], ['other', 1]])], {})).toBeNull();
	});
	it('fires when a per-topic seq regresses', () => {
		const v = checkDeliveryMonotonic([client('0:1', [['room', 2], ['room', 1]])], {});
		expect(v).toEqual({
			category: 'steady.delivery-nonmonotonic',
			context: { client: '0:1', topic: 'room', seq: 1, prev: 2 }
		});
	});
	it('fires on a repeated seq (a duplicate is not strictly increasing)', () => {
		expect(checkDeliveryMonotonic([client('0:1', [['room', 1], ['room', 1]])], {})).not.toBeNull();
	});
	it('is guarded off under a reorder fault', () => {
		expect(checkDeliveryMonotonic([client('0:1', [['room', 2], ['room', 1]])], { reorder: true })).toBeNull();
	});
	it('is guarded off when a topic had more than one originating instance', () => {
		expect(checkDeliveryMonotonic([client('0:1', [['room', 2], ['room', 1]])], { multiOriginator: true })).toBeNull();
	});
	it('ignores control frames (no routingTopic) and undecodable bodies (no numeric seq)', () => {
		const c = {
			id: '0:1',
			raw: [{ routingTopic: null }, { routingTopic: 'room' }, { routingTopic: 'room' }],
			decoded: [{ seq: 99 }, { note: 'no seq' }, { seq: 1 }]
		};
		expect(checkDeliveryMonotonic([c], {})).toBeNull();
	});
});

describe('steadystate - checkStarvation', () => {
	it('does not fire when every publish-time subscriber received the topic', () => {
		const v = checkStarvation(
			[client('0:1', [['room', 1]])],
			[{ topic: 'room', subscribers: ['0:1'] }],
			{}
		);
		expect(v).toBeNull();
	});
	it('fires when a publish-time subscriber never received the topic', () => {
		const v = checkStarvation(
			[client('0:1', [['other', 1]])],
			[{ topic: 'room', subscribers: ['0:1'] }],
			{}
		);
		expect(v).toEqual({ category: 'steady.starvation', context: { client: '0:1', topic: 'room' } });
	});
	it('is guarded off under a drop fault', () => {
		expect(checkStarvation([], [{ topic: 'room', subscribers: ['0:1'] }], { drop: true })).toBeNull();
	});
	it('uses the PUBLISH-TIME subscriber set (a non-subscriber that got nothing is not starved)', () => {
		const v = checkStarvation(
			[client('0:1', [['room', 1]]), client('1:1', [])],
			[{ topic: 'room', subscribers: ['0:1'] }],
			{}
		);
		expect(v).toBeNull();
	});
	it('is a no-op with no publishes', () => {
		expect(checkStarvation([client('0:1', [])], [], {})).toBeNull();
	});
});

describe('steadystate - faultClasses', () => {
	it('OR-combines the wire faults with the relay faults', () => {
		expect(faultClasses({ drop: 0.1 }, { reorder: 0.2 })).toEqual({
			drop: true, duplicate: false, corrupt: false, reorder: true
		});
	});
	it('is all-false for a clean run', () => {
		expect(faultClasses(undefined, undefined)).toEqual({
			drop: false, duplicate: false, corrupt: false, reorder: false
		});
	});
});

describe('steadystate - runSteadyState (fold shape)', () => {
	it('returns [] for a clean trajectory', () => {
		expect(runSteadyState({
			clockSamples: [0, 1, 2],
			drained: true,
			pending: 0,
			terminal: { topicCounts: { room: 2 } },
			publishLog: [{ topic: 'room', subscribers: ['0:1'] }],
			clients: [client('0:1', [['room', 1]])],
			faults: faultClasses()
		})).toEqual([]);
	});
	it('collects multiple violations in the shared {category, context} shape', () => {
		const out = runSteadyState({
			clockSamples: [5, 3],
			drained: true,
			pending: 1,
			terminal: { topicCounts: { ghost: 0 } },
			publishLog: [],
			clients: [],
			faults: faultClasses()
		});
		expect(out.map((v) => v.category).sort()).toEqual([
			'steady.no-quiescence', 'steady.time-nonmonotonic', 'topic.zero-subscribers'
		]);
		for (const v of out) {
			expect(typeof v.category).toBe('string');
			expect(v).toHaveProperty('context');
		}
	});
});

describe('steadystate - integration through runRedisSim / runPgSim', () => {
	it('a default fault-free redis run has zero violations', async () => {
		const r = await runRedisSim({ seed: 'steady-redis-clean' });
		expect(r.invariantViolations).toEqual([]);
	});

	it('a default fault-free pg run has zero violations', async () => {
		const r = await runPgSim({ seed: 'steady-pg-clean' });
		expect(r.invariantViolations).toEqual([]);
	});

	it('a reorder-faulted relay run suppresses delivery-monotonic and stays clean', async () => {
		const r = await runRedisSim({ seed: 'steady-reorder', relayFaults: { reorder: 0.5 } });
		expect(r.invariantViolations.filter((v) => v.category.startsWith('steady.'))).toEqual([]);
	});

	it('a drop-faulted relay run suppresses starvation and stays clean', async () => {
		const r = await runRedisSim({ seed: 'steady-drop', relayFaults: { drop: 0.5 } });
		expect(r.invariantViolations.filter((v) => v.category.startsWith('steady.'))).toEqual([]);
	});

	it('leftover refed work fires steady.no-quiescence (redis)', async () => {
		// An interval armed through the store seam never drains, so the final
		// drive exhausts the (small) step budget with refed work pending.
		const r = await runRedisSim({
			seed: 'steady-noq', steps: 50,
			scenario: async (api) => {
				const c = api.instance(0).connect();
				await api.advance();
				c.subscribe('room');
				await api.advance();
				setIntervalTimer(() => {}, 1000);
			}
		});
		expect(r.invariantViolations.some((v) => v.category === 'steady.no-quiescence')).toBe(true);
	});

	it('publish-time subscriber sets flow into the fold (redis publishLog path)', async () => {
		// A publish through api.instance(i).publish must record its eligible
		// receivers; a healthy run then shows them all served (no starvation).
		const r = await runRedisSim({
			seed: 'steady-publog',
			scenario: async (api, opts) => {
				for (let i = 0; i < opts.instances; i++) api.instance(i).connect();
				await api.advance();
				for (let i = 0; i < opts.instances; i++) {
					for (const c of api.instance(i).clients()) c.subscribe('room');
				}
				await api.advance();
				api.instance(0).publish('room', 'tick', { n: 1 });
				api.instance(0).publishBatched([
					{ topic: 'room', event: 'tick', data: { n: 2 } },
					{ topic: 'room', event: 'tick', data: { n: 3 } }
				]);
				await api.advance();
			}
		});
		expect(r.invariantViolations).toEqual([]);
		expect(r.metrics.framesDelivered).toBeGreaterThan(0);
	});

	it('a folded redis run still reproduces bit-for-bit (determinism self-gate)', async () => {
		const original = await runRedisSim({ seed: 'steady-redis-repro', relayFaults: { drop: 0.3 } });
		const rerun = await replayRedisSim(original);
		expect(rerun.reproduced).toBe(true);
	});

	it('a folded pg run still reproduces bit-for-bit (determinism self-gate)', async () => {
		const original = await runPgSim({ seed: 'steady-pg-repro', relayFaults: { drop: 0.3 } });
		const rerun = await replayPgSim(original);
		expect(rerun.reproduced).toBe(true);
	});
});
