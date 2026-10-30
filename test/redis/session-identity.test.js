import { beforeEach, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { sessionIdentityContract } from '../helpers/session-identity-contract.js';
import { createDistributedSession } from '../../src/redis/session.js';
let client;
beforeEach(() => { client = mockRedisClient('session-identity:'); });
sessionIdentityContract(() => client);

it('times the coverage estimate from before the renewal was sent', async () => {
	const ttlMs = 400;
	const legacy = createDistributedSession(client, { ttlMs });
	const evaluate = client.redis.eval;
	let renewals = 0;
	// The renewal takes 200 ms to reach the server.
	client.redis.eval = async function (...args) {
		renewals++;
		await new Promise((resolve) => setTimeout(resolve, 200));
		return evaluate.apply(this, args);
	};
	try {
		await legacy.set('a', { v: 1 });
		expect(renewals).toBe(1);
		// Counted from the send, the 1.5 * ttlMs the server granted 200 ms
		// later leaves under 1.25 * ttlMs by the time the reply arrives, so
		// the next write waits for a renewal. Counted from the reply it would
		// not.
		await legacy.set('b', { v: 1 });
		expect(renewals).toBe(2);
	} finally {
		client.redis.eval = evaluate;
	}
});

it('renews the coverage in the background before a write has to wait for it', async () => {
	const ttlMs = 400;
	const legacy = createDistributedSession(client, { ttlMs });
	const evaluate = client.redis.eval;
	let renewals = 0;
	client.redis.eval = function (...args) { renewals++; return evaluate.apply(this, args); };
	try {
		await legacy.set('a', { v: 1 });
		// Between 1.25 and 1.375 * ttlMs of coverage is left.
		await new Promise((resolve) => setTimeout(resolve, 75));
		await legacy.set('b', { v: 1 });
		expect(renewals).toBe(2);
	} finally {
		client.redis.eval = evaluate;
	}
});
