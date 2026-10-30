import { beforeEach, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { sessionIdentityContract } from '../helpers/session-identity-contract.js';
import { createDistributedSession } from '../../src/redis/session.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
let client;
beforeEach(() => { client = mockRedisClient('session-identity:'); });
sessionIdentityContract(() => client);

const coverageKey = () => client.key('sess:identity:v1:legacy-active');

// Count, and optionally delay or fail, the SETs that renew the coverage key.
function watchRenewals({ delayMs = 0, fail = false } = {}) {
	const set = client.redis.set;
	const state = { renewals: 0 };
	client.redis.set = async function (key, ...args) {
		if (key !== coverageKey()) return set.call(this, key, ...args);
		state.renewals++;
		if (delayMs) await new Promise((resolve) => setTimeout(resolve, delayMs));
		if (fail) throw new Error('READONLY');
		return set.call(this, key, ...args);
	};
	state.restore = () => { client.redis.set = set; };
	return state;
}

it('times the coverage estimate from before the renewal was sent', async () => {
	const legacy = createDistributedSession(client, { ttlMs: 400 });
	// The renewal takes 200 ms to reach the server.
	const watch = watchRenewals({ delayMs: 200 });
	try {
		await legacy.set('a', { v: 1 });
		expect(watch.renewals).toBe(1);
		// Counted from the send, the 1.5 * ttlMs the server granted 200 ms
		// later leaves under 1.25 * ttlMs by the time the reply arrives, so
		// the next write waits for a renewal. Counted from the reply it would
		// not.
		await legacy.set('b', { v: 1 });
		expect(watch.renewals).toBe(2);
	} finally {
		watch.restore();
	}
});

it('renews the coverage in the background before a write has to wait for it', async () => {
	const legacy = createDistributedSession(client, { ttlMs: 400 });
	const watch = watchRenewals();
	try {
		await legacy.set('a', { v: 1 });
		// Between 1.25 and 1.375 * ttlMs of coverage is left.
		await new Promise((resolve) => setTimeout(resolve, 75));
		await legacy.set('b', { v: 1 });
		expect(watch.renewals).toBe(2);
	} finally {
		watch.restore();
	}
});

it('reports one failed renewal to the breaker once and retries nothing while idle', async () => {
	const breaker = createCircuitBreaker({ failureThreshold: 2, resetTimeout: 30_000 });
	const legacy = createDistributedSession(client, { breaker, ttlMs: 400 });
	const watch = watchRenewals({ fail: true });
	try {
		await expect(legacy.set('k', { v: 1 })).rejects.toThrow('READONLY');
		await new Promise((resolve) => setTimeout(resolve, 700));
	} finally {
		watch.restore();
	}
	expect(watch.renewals).toBe(1);
	expect(breaker.state).not.toBe('broken');
	expect(await client.redis.get(client.key('sess:k'))).toBeNull();
});
