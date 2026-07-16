/**
 * Simulator-grade behavior of the in-memory Redis double.
 *
 * Covers the determinism seam (TIME + clock-controlled expiry all follow the
 * injectable runtime clock), the cluster multi-slot no-op hazard, eval
 * atomicity, seed-reproducible stream ids, and a Lua parity guard that asserts
 * the mirrored-JS evaluators are deterministic for fixed inputs - and, when a
 * real Redis is reachable through the integration harness, that the double and
 * real redis.eval agree on identical inputs.
 */
import { describe, it, expect, afterEach } from 'vitest';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { CONSUME_SCRIPT } from '../../src/redis/token-bucket-script.js';

// A fixed virtual epoch; round so seconds * 1000 reconstructs it exactly.
const FIXED_MS = 1_700_000_000_000;

// Pin the runtime clock (and optionally the RNG) so the double's server clock,
// TIME command, TTLs and stream ids are all reproducible. The double reads the
// EXACT wall-clock seam (`wallEpoch`), so that is what we override; `now` is
// pinned too so any helper that reads the cached clock stays consistent. Forced
// so the swap goes through even if NODE_ENV happens to be production in CI.
function pinClock(ms, u32) {
	const env = { clock: { wallEpoch: () => ms, now: () => ms } };
	if (typeof u32 === 'number') env.rng = { u32: () => u32 };
	setRuntimeEnv(env, { force: true });
}

afterEach(() => resetRuntimeEnv());

describe('mock-redis TIME command follows the virtual clock', () => {
	it('returns [seconds, micros] sourced from the seam clock', async () => {
		pinClock(FIXED_MS, 0);
		const client = mockRedisClient();
		const [sec, usec] = await client.redis.time();
		expect(sec).toBe(String(Math.floor(FIXED_MS / 1000)));
		// Whole-second epoch -> 0 ms-of-second; with the RNG pinned to 0 there is
		// no sub-millisecond jitter either.
		expect(usec).toBe('0');
	});

	it('advances exactly with the virtual clock, not wall time', async () => {
		pinClock(FIXED_MS, 0);
		const client = mockRedisClient();
		const a = await client.redis.time();
		// Step the virtual clock forward 3.5 seconds.
		pinClock(FIXED_MS + 3500, 0);
		const b = await client.redis.time();
		const aMs = Number(a[0]) * 1000 + Math.floor(Number(a[1]) / 1000);
		const bMs = Number(b[0]) * 1000 + Math.floor(Number(b[1]) / 1000);
		expect(bMs - aMs).toBe(3500);
	});

	it('fills sub-millisecond micros from the seeded RNG (reproducible jitter)', async () => {
		// A virtual ms cannot express sub-ms micros, so the double fills them from
		// the RNG seam. A fixed seed yields a fixed, in-range micros value.
		pinClock(FIXED_MS + 12, 654_321);
		const client = mockRedisClient();
		const [, usec] = await client.redis.time();
		// micros = (ms % 1000) * 1000 + (u32 % 1000) = 12000 + 321.
		expect(usec).toBe('12321');
		// The recombination the real Lua does discards the sub-ms jitter, so the
		// derived millisecond timestamp still equals the virtual clock.
		const [sec] = await client.redis.time();
		const derivedMs = Number(sec) * 1000 + Math.floor(Number(usec) / 1000);
		expect(derivedMs).toBe(FIXED_MS + 12);
	});
});

describe('mock-redis clock-controlled expiry through the seam', () => {
	it('HPEXPIRE / HTTL countdown tracks the virtual clock', async () => {
		pinClock(FIXED_MS);
		const client = mockRedisClient();
		const r = client.redis;
		await r.hset('h', 'f', 'v');
		// 5s field TTL.
		expect(await r.hpexpire('h', 5000, 'FIELDS', 1, 'f')).toEqual([1]);
		expect(await r.httl('h', 'FIELDS', 1, 'f')).toEqual([5]);

		// Advance the virtual clock 2s; remaining TTL drops accordingly.
		pinClock(FIXED_MS + 2000);
		expect(await r.httl('h', 'FIELDS', 1, 'f')).toEqual([3]);
	});

	it('a TTL\'d field is pruned once the virtual clock passes its expiry', async () => {
		pinClock(FIXED_MS);
		const client = mockRedisClient();
		const r = client.redis;
		await r.hset('h', 'f', 'v');
		await r.hpexpire('h', 1000, 'FIELDS', 1, 'f');
		expect(await r.hget('h', 'f')).toBe('v');

		// Step past expiry: the field is gone on the next read.
		pinClock(FIXED_MS + 1500);
		expect(await r.hget('h', 'f')).toBeNull();
		expect(await r.hexists('h', 'f')).toBe(0);
	});

	it('the field survives right up to (but not past) its expiry instant', async () => {
		pinClock(FIXED_MS);
		const client = mockRedisClient();
		const r = client.redis;
		await r.hset('h', 'f', 'v');
		await r.hpexpire('h', 1000, 'FIELDS', 1, 'f');

		// Exactly at expiry the prune (expireAt <= now) drops it.
		pinClock(FIXED_MS + 1000);
		expect(await r.hget('h', 'f')).toBeNull();
	});
});

describe('mock-redis string-key TTL (SET EX/PX/EXAT/PXAT) follows the seam', () => {
	it('a PX key survives right up to its deadline, then is pruned one ms past it', async () => {
		pinClock(FIXED_MS);
		const r = mockRedisClient().redis;
		expect(await r.set('k', 'v', 'PX', 1000)).toBe('OK');
		expect(await r.get('k')).toBe('v');

		// Real Redis expires a string key on `now > when`, so AT the exact deadline
		// the key is still alive (unlike the mock's hash-field prune, which drops at
		// `now >= expireAt`). This `>` semantics is what makes the mock agree with
		// real Redis at the forget-tombstone boundary.
		pinClock(FIXED_MS + 1000);
		expect(await r.get('k')).toBe('v');
		expect(await r.exists('k')).toBe(1);

		// One ms past the deadline the key is gone on the next read.
		pinClock(FIXED_MS + 1001);
		expect(await r.get('k')).toBeNull();
		expect(await r.exists('k')).toBe(0);
	});

	it('EX seconds and PXAT/EXAT absolute deadlines expire on the seam clock', async () => {
		pinClock(FIXED_MS);
		const r = mockRedisClient().redis;
		await r.set('ex', 'v', 'EX', 5);              // 5s relative
		await r.set('pxat', 'v', 'PXAT', FIXED_MS + 2000); // absolute ms deadline
		await r.set('exat', 'v', 'EXAT', Math.floor(FIXED_MS / 1000) + 3); // absolute s deadline

		pinClock(FIXED_MS + 2001);
		expect(await r.get('pxat')).toBeNull();       // 2s deadline passed
		expect(await r.get('exat')).toBe('v');        // 3s deadline not yet
		expect(await r.get('ex')).toBe('v');          // 5s deadline not yet

		pinClock(FIXED_MS + 5001);
		expect(await r.get('exat')).toBeNull();
		expect(await r.get('ex')).toBeNull();
	});

	it('a plain SET clears a prior TTL; KEEPTTL preserves it', async () => {
		pinClock(FIXED_MS);
		const r = mockRedisClient().redis;

		// Plain overwrite drops the TTL: the key is then persistent.
		await r.set('clear', 'v1', 'PX', 1000);
		await r.set('clear', 'v2');
		pinClock(FIXED_MS + 10_000);
		expect(await r.get('clear')).toBe('v2');

		// KEEPTTL keeps the original deadline: the key still expires.
		pinClock(FIXED_MS);
		await r.set('keep', 'v1', 'PX', 1000);
		await r.set('keep', 'v2', 'KEEPTTL');
		pinClock(FIXED_MS + 1001);
		expect(await r.get('keep')).toBeNull();
	});

	it('NX gates against post-expiry state: an expired key is re-settable with NX', async () => {
		pinClock(FIXED_MS);
		const r = mockRedisClient().redis;
		expect(await r.set('lease', 'a', 'NX', 'PX', 1000)).toBe('OK');
		// While alive, NX is refused.
		expect(await r.set('lease', 'b', 'NX', 'PX', 1000)).toBeNull();

		// Once the lease PX-expires, NX succeeds again (the prune runs first).
		pinClock(FIXED_MS + 1001);
		expect(await r.set('lease', 'c', 'NX', 'PX', 1000)).toBe('OK');
		expect(await r.get('lease')).toBe('c');
	});
});

describe('mock-redis stream ids are seed-reproducible', () => {
	it('XADD * derives its id from the virtual clock', async () => {
		pinClock(FIXED_MS);
		const client = mockRedisClient();
		const id1 = await client.redis.xadd('s', '*', 'k', 'v');
		expect(id1).toBe(`${FIXED_MS}-0`);
		// Same virtual ms -> sequence disambiguation, deterministic.
		const id2 = await client.redis.xadd('s', '*', 'k', 'v');
		expect(id2).toBe(`${FIXED_MS}-1`);
		// Advance the clock -> fresh ms, seq resets.
		pinClock(FIXED_MS + 5);
		const id3 = await client.redis.xadd('s', '*', 'k', 'v');
		expect(id3).toBe(`${FIXED_MS + 5}-0`);
	});

	it('two independent runs under the same seed produce identical ids', async () => {
		async function run() {
			pinClock(FIXED_MS, 42);
			const client = mockRedisClient();
			const ids = [];
			ids.push(await client.redis.xadd('s', '*', 'k', 'v'));
			ids.push(await client.redis.xadd('s', '*', 'k', 'v'));
			return ids;
		}
		expect(await run()).toEqual(await run());
	});
});

describe('mock-redis cluster multi-slot no-op hazard', () => {
	it('models a multi-master topology with stable slot->node mapping', () => {
		const cluster = mockRedisClient('', { cluster: true, nodeCount: 3 });
		expect(cluster._cluster).toBe(true);
		// Same hash tag -> same slot -> same node, always.
		expect(cluster._slotOf('{room}a')).toBe(cluster._slotOf('{room}b'));
		expect(cluster._nodeOf('{room}a')).toBe(cluster._nodeOf('{room}b'));
	});

	// Resolve a key pair that the topology splits across two nodes, and a pair
	// that co-locates, so the assertions do not depend on a hand-picked key
	// whose slot math could drift.
	function pickKeyPairs(client) {
		const sameSlotA = '{tag}a';
		const sameSlotB = '{tag}b';
		const baseNode = client._nodeOf(sameSlotA);
		let crossKey = null;
		for (let i = 0; i < 1000; i++) {
			const k = 'k:' + i;
			if (client._nodeOf(k) !== baseNode) { crossKey = k; break; }
		}
		return { sameSlotA, sameSlotB, onNodeKey: '{tag}c', crossKey };
	}

	it('a same-slot batch runs every command', async () => {
		const client = mockRedisClient('', { cluster: true });
		const { sameSlotA, sameSlotB } = pickKeyPairs(client);
		const p = client.redis.pipeline();
		p.set(sameSlotA, '1');
		p.set(sameSlotB, '2');
		const res = await p.exec();
		expect(res.every(([err]) => err === null)).toBe(true);
		expect(await client.redis.get(sameSlotA)).toBe('1');
		expect(await client.redis.get(sameSlotB)).toBe('2');
	});

	it('a cross-node batch silently no-ops the off-node commands', async () => {
		const client = mockRedisClient('', { cluster: true });
		const { sameSlotA, crossKey } = pickKeyPairs(client);
		expect(crossKey, 'topology should split some key onto another node').toBeTruthy();

		const p = client.redis.pipeline();
		p.set(sameSlotA, 'lands');   // first keyed command sets the target node
		p.set(crossKey, 'dropped');  // belongs to another node -> no-op + MOVED
		const res = await p.exec();

		// On-node command applied; off-node command returned a MOVED placeholder
		// error (never a thrown rejection) and never touched the store.
		expect(res[0][0]).toBeNull();
		expect(res[0][1]).toBe('OK');
		expect(res[1][0]).toBeInstanceOf(Error);
		expect(res[1][0].message).toMatch(/^MOVED /);
		expect(await client.redis.get(sameSlotA)).toBe('lands');
		expect(await client.redis.get(crossKey)).toBeNull();
	});

	it('MULTI carries the same hazard as a pipeline', async () => {
		const client = mockRedisClient('', { cluster: true });
		const { sameSlotA, crossKey } = pickKeyPairs(client);
		const tx = client.redis.multi();
		tx.set(sameSlotA, 'a');
		tx.set(crossKey, 'b');
		const res = await tx.exec();
		expect(res[1][0]).toBeInstanceOf(Error);
		expect(await client.redis.get(crossKey)).toBeNull();
	});

	it('standalone mode (default) runs every command regardless of slot', async () => {
		const client = mockRedisClient();
		expect(client._cluster).toBe(false);
		const p = client.redis.pipeline();
		p.set('aaaa', '1');
		p.set('zzzz', '2'); // different slot, but no cluster -> runs anyway
		const res = await p.exec();
		expect(res.every(([err]) => err === null)).toBe(true);
		expect(await client.redis.get('aaaa')).toBe('1');
		expect(await client.redis.get('zzzz')).toBe('2');
	});
});

describe('mock-redis eval atomicity', () => {
	it('an eval applies as one indivisible unit with no foreign interleave', async () => {
		pinClock(FIXED_MS, 0);
		const client = mockRedisClient();
		const r = client.redis;

		// Race a CONSUME eval against a concurrent direct hset on the SAME bucket
		// hash. Because the evaluator is synchronous, the eval reads and writes
		// all three fields before the awaited hset's microtask gets to run, so the
		// eval can never observe or be corrupted by a half-applied interleave.
		const evalPromise = r.eval(CONSUME_SCRIPT, 1, 'rl:atomic', 5, 1000, 1, 0);
		const hsetPromise = r.hset('rl:atomic', 'points', '999');

		const [evalRes] = await Promise.all([evalPromise, hsetPromise]);
		// The eval consumed one point from a fresh bucket of 5 -> 4 remaining.
		expect(evalRes[0]).toBe(1);
		expect(evalRes[1]).toBe(4);
	});

	it('repeated evals under a fixed clock are bit-for-bit deterministic', async () => {
		async function consumeOnce() {
			pinClock(FIXED_MS, 0);
			const client = mockRedisClient();
			return client.redis.eval(CONSUME_SCRIPT, 1, 'rl:det', 5, 1000, 2, 0);
		}
		const a = await consumeOnce();
		const b = await consumeOnce();
		expect(a).toEqual(b);
		// resetMs equals the interval exactly because the clock is pinned.
		expect(a[2]).toBe(1000);
	});
});

// Lua parity guard (unit half): the mirrored-JS evaluators must be deterministic
// for fixed inputs. The double-vs-real-Redis equality check lives in the
// integration suite (test/integration/redis/mock-redis-parity.test.js), where a
// real Redis is available - so the anti-drift oracle actually runs, never skips.
describe('mock-redis Lua parity guard', () => {
	it('CONSUME mirrored-JS is deterministic for fixed inputs', async () => {
		pinClock(FIXED_MS, 0);
		const client = mockRedisClient();
		// Drain a 3-point bucket: allow, allow, allow, then deny.
		const seq = [];
		for (let i = 0; i < 4; i++) {
			seq.push(await client.redis.eval(CONSUME_SCRIPT, 1, 'rl:p', 3, 1000, 1, 0));
		}
		expect(seq.map((r) => r[0])).toEqual([1, 1, 1, 0]);
		expect(seq.map((r) => r[1])).toEqual([2, 1, 0, 0]);
	});
});
