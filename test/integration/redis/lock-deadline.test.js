import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { waitRedisMs } from '../helpers/backend-clock.js';
import { createDistributedLock, LockLostError } from '../../../src/redis/lock.js';
let client, observer;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-lock-deadline:' }); observer = createBackendClient({ keyPrefix: 'inttest-lock-deadline:' }); });
beforeEach(async () => { await resetBackendKeys(client); });
afterAll(async () => { await Promise.all([client.quit(), observer.quit()]); });

it('rejects partitioned work after Redis expires its lease and admits a successor', async () => {
	const send = client.redis.sendCommand; let partitioned = false, signal;
	client.redis.sendCommand = function(command, ...args) {
		if (partitioned && /^(eval|evalsha)$/i.test(command.name)) return Promise.reject(new Error('isolated command partition'));
		return send.call(this, command, ...args);
	};
	const lock = createDistributedLock(client, { defaultTtlMs: 600, heartbeatMs: 80 });
	try {
		await expect(lock.withLock('partition', async current => {
			signal = current; partitioned = true;
			await waitRedisMs(observer, 1000);
			expect(await observer.redis.set(observer.key('lock:partition'), 'successor', 'NX', 'PX', 5000)).toBe('OK');
			return 'done';
		})).rejects.toBeInstanceOf(LockLostError);
		expect(signal.aborted).toBe(true);
		expect(await observer.redis.get(observer.key('lock:partition'))).toBe('successor');
	} finally { client.redis.sendCommand = send; }
});

it('rejects a stale acquisition reply without running work or deleting a successor', async () => {
	const set = client.redis.set; let ran = false;
	client.redis.set = async function(...args) {
		const result = await set.apply(this, args); await waitRedisMs(observer, 800);
		expect(await observer.redis.set(observer.key('lock:stale'), 'successor', 'NX', 'PX', 5000)).toBe('OK');
		return result;
	};
	try {
		const lock = createDistributedLock(client, { defaultTtlMs: 400 });
		await expect(lock.withLock('stale', () => { ran = true; })).rejects.toBeInstanceOf(LockLostError);
		expect(ran).toBe(false); expect(await observer.redis.get(observer.key('lock:stale'))).toBe('successor');
	} finally { client.redis.set = set; }
});
