// evalCached: the cached-script eval helper (shared/eval-cached.js) that routes
// hot Lua through EVALSHA (via ioredis defineCommand) instead of shipping the
// full body each call. Works on any instance - single Redis, Cluster, or the
// test double - however the client was created. The caching/dispatch logic is
// tested against a fake instance; end-to-end parity (evalCached === eval) is
// tested through the Redis double, which every converted call site exercises.

import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { evalCached, evalCachedName } from '../../src/shared/eval-cached.js';
import { CONSUME_SCRIPT } from '../../src/redis/token-bucket-script.js';

/** A fake ioredis instance that records defineCommand + command invocations. */
function fakeRedis() {
	const defined = [];
	const calls = [];
	const inst = {
		defineCommand(name, opts) {
			defined.push({ name, lua: opts.lua });
			// ioredis attaches the command as an instance method.
			inst[name] = (...args) => {
				calls.push({ name, args });
				return Promise.resolve(['ok', name]);
			};
		}
	};
	return { inst, defined, calls };
}

describe('evalCached (cached-script eval helper)', () => {
	it('registers each unique script once per instance and reuses the command', async () => {
		const { inst, defined } = fakeRedis();
		await evalCached(inst, 'SCRIPT_A', 1, 'k');
		await evalCached(inst, 'SCRIPT_A', 1, 'k2');
		await evalCached(inst, 'SCRIPT_B', 1, 'k');
		expect(defined).toHaveLength(2); // A defined once, B once
		expect(defined.map((d) => d.lua)).toEqual(['SCRIPT_A', 'SCRIPT_B']);
	});

	it('invokes the defined command with numKeys first, then keys and args', async () => {
		const { inst, calls } = fakeRedis();
		await evalCached(inst, 'SCRIPT', 2, 'k1', 'k2', 'arg1');
		expect(calls).toHaveLength(1);
		expect(calls[0].args).toEqual([2, 'k1', 'k2', 'arg1']);
	});

	it('keeps per-instance registries isolated', async () => {
		const a = fakeRedis();
		const b = fakeRedis();
		await evalCached(a.inst, 'S', 0);
		await evalCached(b.inst, 'S', 0);
		expect(a.defined).toHaveLength(1); // each instance registers its own copy
		expect(b.defined).toHaveLength(1);
	});

	it('returns the command reply unchanged', async () => {
		const { inst } = fakeRedis();
		expect(await evalCached(inst, 'S', 0)).toEqual(['ok', 'ecmd0']);
	});

	it('works through the Redis double and matches eval for the token-bucket script', async () => {
		const client = mockRedisClient('test:');
		// The mock's defineCommand shim routes the generated command back through
		// eval()'s content dispatch, so evalCached behaves exactly like eval here.
		const viaEval = await client.redis.eval(CONSUME_SCRIPT, 1, client.key('a'), 5, 60000, 1, 0);
		const viaCached = await evalCached(client.redis, CONSUME_SCRIPT, 1, client.key('b'), 5, 60000, 1, 0);
		expect(viaEval[0]).toBe(1);
		expect(viaCached[0]).toBe(1);
		expect(viaCached[1]).toBe(viaEval[1]); // same remaining for a fresh bucket
	});
});

describe('evalCachedName (the pipeline form)', () => {
	it('returns a stable name per script, sharing the evalCached registry', async () => {
		const { inst, defined } = fakeRedis();
		const name = evalCachedName(inst, 'SCRIPT_A');
		expect(evalCachedName(inst, 'SCRIPT_A')).toBe(name);
		await evalCached(inst, 'SCRIPT_A', 1, 'k'); // same script: no second define
		expect(defined).toHaveLength(1);
		expect(evalCachedName(inst, 'SCRIPT_B')).not.toBe(name);
	});

	it('a pipelined cached command runs the script through the Redis double', async () => {
		const client = mockRedisClient('test:');
		const name = evalCachedName(client.redis, CONSUME_SCRIPT);
		const pipe = client.redis.pipeline();
		pipe.set(client.key('marker'), '1');
		pipe[name](1, client.key('bucket'), 5, 60000, 1, 0);
		const results = await pipe.exec();
		expect(results).toHaveLength(2);
		expect(results[1][0]).toBe(null); // no error
		expect(results[1][1][0]).toBe(1); // allowed - the script actually ran
	});

	it('a pipelined cached command slots by its KEY on a cluster, not by numKeys', async () => {
		const client = mockRedisClient('test:', { cluster: true });
		const name = evalCachedName(client.redis, CONSUME_SCRIPT);
		// One key, one slot: the hset and the script call target the SAME key, so
		// the whole batch is same-slot and nothing may be no-op'd with a MOVED.
		// Before the eval-shape registration the generic key fallback read numKeys
		// (the literal 1) as the key and mis-slotted the command.
		const key = client.key('{tag}bucket');
		const pipe = client.redis.pipeline();
		pipe.hset(key, 'f', 'v');
		pipe[name](1, key, 5, 60000, 1, 0);
		const results = await pipe.exec();
		expect(results[0][0]).toBe(null);
		expect(results[1][0]).toBe(null); // same slot: never a phantom MOVED
		expect(results[1][1][0]).toBe(1);
	});
});
