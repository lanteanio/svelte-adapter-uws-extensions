import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createTopicBroadcast } from '../../src/redis/topic-broadcast.js';

/** Flush pending microtasks + immediates so sync mock-redis pub/sub + async handlers settle. */
const flush = () => new Promise((r) => setImmediate(r));

/** A handler that serves a fixed set of per-subscriber outcomes for one instance. */
function serve(outcomes) {
	return async () => outcomes;
}

describe('redis topic-broadcast coordinator', () => {
	let client;
	/** @type {any[]} */
	let coordinators;

	beforeEach(() => {
		client = mockRedisClient('app:');
		coordinators = [];
	});

	afterEach(async () => {
		for (const c of coordinators) await c.destroy();
	});

	function make(opts) {
		const c = createTopicBroadcast(client, opts);
		coordinators.push(c);
		return c;
	}

	it('returns only the local serve when no other instance is live', async () => {
		const origin = make();
		origin.onRequest(serve([{ ok: true, reply: 'A' }]));
		await flush();
		const out = await origin.broadcast('room', 'ping', { n: 1 });
		expect(out).toEqual([{ ok: true, reply: 'A' }]);
	});

	it('aggregates local + remote replies across two instances', async () => {
		const origin = make();
		const remote = make();
		origin.onRequest(serve([{ ok: true, reply: 'origin' }]));
		remote.onRequest(serve([{ ok: true, reply: 'remote-1' }, { ok: true, reply: 'remote-2' }]));
		await flush();

		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 5000 });
		const replies = out.filter((o) => o.ok).map((o) => o.reply).sort();
		expect(replies).toEqual(['origin', 'remote-1', 'remote-2']);
		expect(out.length).toBe(3);
	});

	it('still reaches a live instance that is absent from the presence set (no cold-start drop)', async () => {
		const origin = make();
		const remote = make();
		origin.onRequest(serve([{ ok: true, reply: 'o' }]));
		remote.onRequest(serve([{ ok: true, reply: 'r-late' }]));
		await flush();
		// Simulate the remote's presence write not having landed yet: it is
		// subscribed to the channel but absent from the presence set. The origin
		// must still PUBLISH (presence gates only when-to-stop, never whether-to-ask)
		// so the remote receives the broadcast and its reply is collected.
		const presenceKey = client.key('{topic-broadcast}:instances');
		await client.redis.zrem(presenceKey, remote.instanceId);

		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 5000 });
		const replies = out.filter((o) => o.ok).map((o) => o.reply).sort();
		expect(replies).toEqual(['o', 'r-late']); // remote NOT dropped despite absent presence
	});

	it('early-completes once every live instance answers (does not wait out the timeout)', async () => {
		const origin = make();
		const remote = make();
		origin.onRequest(serve([{ ok: true, reply: 'o' }]));
		remote.onRequest(serve([{ ok: true, reply: 'r' }]));
		await flush();

		// A 60s budget: if the collector waited the whole timeout this test would
		// hang far past vitest's default. Resolving fast proves presence-driven
		// early-completion.
		const start = Date.now();
		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 60000 });
		expect(Date.now() - start).toBeLessThan(2000);
		expect(out.length).toBe(2);
	});

	it('a remote instance with no subscribers contributes an empty set, never blocks', async () => {
		const origin = make();
		const remote = make();
		origin.onRequest(serve([{ ok: true, reply: 'o' }]));
		remote.onRequest(serve([])); // no local subscribers on the remote
		await flush();

		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 60000 });
		expect(out).toEqual([{ ok: true, reply: 'o' }]);
	});

	it('partial-success: a throwing remote handler contributes nothing but the broadcast still completes', async () => {
		const origin = make();
		const remote = make();
		origin.onRequest(serve([{ ok: true, reply: 'o' }]));
		remote.onRequest(async () => { throw new Error('serve boom'); });
		await flush();

		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 60000 });
		expect(out).toEqual([{ ok: true, reply: 'o' }]);
	});

	it('carries per-subscriber error outcomes through unchanged', async () => {
		const origin = make();
		const remote = make();
		origin.onRequest(serve([{ ok: false, error: 'timeout' }]));
		remote.onRequest(serve([{ ok: true, reply: 'ok' }]));
		await flush();

		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 60000 });
		expect(out).toContainEqual({ ok: false, error: 'timeout' });
		expect(out).toContainEqual({ ok: true, reply: 'ok' });
	});

	it('falls back to the timeout ceiling when a recorded instance never answers', async () => {
		const origin = make({ requestTimeoutMs: 5000 });
		origin.onRequest(serve([{ ok: true, reply: 'o' }]));
		await flush();

		// Plant a phantom instance in the presence set that has no live subscriber.
		const presenceKey = client.key('{topic-broadcast}:instances');
		await client.redis.zadd(presenceKey, Date.now(), 'phantom-instance');

		const start = Date.now();
		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 40 });
		const elapsed = Date.now() - start;
		expect(out).toEqual([{ ok: true, reply: 'o' }]); // only the local serve
		expect(elapsed).toBeGreaterThanOrEqual(30); // waited out the (short) ceiling
	});

	it('destroy removes this instance from the presence set', async () => {
		const origin = make();
		const other = make();
		origin.onRequest(serve([]));
		other.onRequest(serve([]));
		await flush();

		const presenceKey = client.key('{topic-broadcast}:instances');
		const before = await client.redis.zrange(presenceKey, 0, -1);
		expect(before).toContain(other.instanceId);

		await other.destroy();
		coordinators = coordinators.filter((c) => c !== other);
		const after = await client.redis.zrange(presenceKey, 0, -1);
		expect(after).not.toContain(other.instanceId);
	});

	it('a stale presence stamp is evicted on the next heartbeat refresh', async () => {
		const origin = make({ presenceTtlMs: 20 });
		origin.onRequest(serve([]));
		await flush();

		const presenceKey = client.key('{topic-broadcast}:instances');
		// Plant a stale member far outside the TTL window.
		await client.redis.zadd(presenceKey, Date.now() - 10000, 'stale-instance');
		// A broadcast reads only within-window members, so the stale one is not
		// awaited; and the next refresh evicts it from the set entirely.
		const out = await origin.broadcast('room', 'ping', null, { timeoutMs: 40 });
		expect(out).toEqual([]);
	});

	it('exposes a stable instanceId per coordinator', () => {
		const a = make();
		const b = make();
		expect(typeof a.instanceId).toBe('string');
		expect(a.instanceId).not.toBe(b.instanceId);
	});
});
