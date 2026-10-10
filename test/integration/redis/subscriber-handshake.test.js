import { it, expect } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { guardSubscriber } from '../../../src/shared/redis-subscriber.js';

it.each([false, true])('orders handshake unsubscribe after offline subscribe (all=%s)', async (all) => {
	const admin = createBackendClient();
	await admin.redis.ping();
	const source = typeof admin.redis.nodes === 'function' ? admin.redis.nodes('master')[0] : admin.redis;
	const subscriber = guardSubscriber(source.duplicate({ lazyConnect: true, enableOfflineQueue: true, enableReadyCheck: true }), 'test');
	const topic = 'inttest-handshake:' + (all ? 'all' : 'single');
	let undo;
	let duringHandshake = false;
	subscriber.once('connect', () => {
		duringHandshake = subscriber.status === 'connect';
		undo = all ? subscriber.unsubscribe() : subscriber.unsubscribe(topic);
	});
	try {
		await subscriber.subscribe(topic);
		await undo;
		expect(duringHandshake).toBe(true);
		expect(Number((await source.pubsub('NUMSUB', topic))[1])).toBe(0);
	} finally {
		subscriber.disconnect();
		await admin.quit();
	}
});

it('rejects held calls and callbacks during a fatal AUTH failure', async () => {
	const admin = createBackendClient();
	await admin.redis.ping();
	const source = typeof admin.redis.nodes === 'function' ? admin.redis.nodes('master')[0] : admin.redis;
	const subscriber = guardSubscriber(source.duplicate({ lazyConnect: true, enableOfflineQueue: true, enableReadyCheck: true, username: 'missing-handshake-test-user', password: 'wrong', retryStrategy: () => 10 }), 'test');
	let count = 0;
	let callbackError;
	const attempt = new Promise((resolve) => subscriber.once('connect', () => {
		subscriber.subscribe('inttest-handshake:auth', (error) => { count++; callbackError = error; }).then(resolve, resolve);
	}));
	try {
		void subscriber.connect().catch(() => {});
		expect(await attempt).toBeInstanceOf(Error);
		expect(callbackError).toBeInstanceOf(Error);
		expect(count).toBe(1);
	} finally {
		subscriber.disconnect();
		await admin.quit();
	}
});
