import { afterEach, describe, expect, it, vi } from 'vitest';
import { createSmoothCluster } from '../../src/redis/smooth-cluster.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';

const cleanup = [];
afterEach(() => {
	for (const item of cleanup.splice(0).reverse()) item.destroy();
	vi.restoreAllMocks();
	vi.useRealTimers();
});
async function setup(cluster = false, guarded = true) {
	vi.useFakeTimers();
	const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 10 });
	cleanup.push(breaker);
	breaker.failure(new Error('offline'));
	await vi.advanceTimersByTimeAsync(10);
	const client = mockRedisClient('smooth-breaker:');
	if (cluster) {
		client.redis.nodes = () => [client.redis];
		client.redis.cluster = vi.fn(async (command) => command === 'SLOTS' ? [[0, 16383, ['localhost', 1, 'owner']]] : 'owner');
	}
	const coordinator = createSmoothCluster(client, { breaker: guarded ? breaker : undefined, onError() {} });
	cleanup.push(coordinator);
	vi.spyOn(breaker, 'success');
	vi.spyOn(breaker, 'failure');
	return { breaker, client, coordinator };
}
const drain = () => vi.advanceTimersByTimeAsync(0);
function deferred() {
	let resolve;
	let reject;
	const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
	return { promise, resolve, reject };
}
function delaySubscribe(client) {
	const pending = deferred();
	const duplicate = client.duplicate.bind(client);
	vi.spyOn(client, 'duplicate').mockImplementation((...args) => {
		const sub = duplicate(...args);
		sub.subscribe = vi.fn(() => pending.promise);
		return sub;
	});
	return pending;
}
function relay(coordinator, payload = []) {
	coordinator.relayCommand('__smooth:room', 'member', coordinator.instanceId, payload);
}

describe('smooth circuit-breaker settlement', () => {
	for (const fails of [false, true]) {
		it(`reports the actual publish verdict while healthy with failure=${fails}`, async () => {
			const { breaker, client, coordinator } = await setup();
			breaker.reset();
			if (fails) vi.spyOn(client.redis, 'publish').mockRejectedValueOnce(new Error('offline'));
			relay(coordinator);
			await drain();
			expect(breaker.state).toBe(fails ? 'broken' : 'healthy');
			expect(breaker.success).toHaveBeenCalledTimes(fails ? 0 : 1);
			expect(breaker.failure).toHaveBeenCalledTimes(fails ? 1 : 0);
		});
	}
	it('does not report a backend verdict for a healthy relay cancelled before dispatch', async () => {
		const { breaker, client, coordinator } = await setup();
		breaker.reset();
		const pending = delaySubscribe(client);
		const publish = vi.spyOn(client.redis, 'publish');
		coordinator.onMessage({});
		relay(coordinator);
		coordinator.destroy();
		pending.resolve(1);
		await drain();
		expect(publish).not.toHaveBeenCalled();
		expect(breaker.success).not.toHaveBeenCalled();
		expect(breaker.failure).not.toHaveBeenCalled();
	});
	it('cancels a deferred unguarded relay when destroyed', async () => {
		const { client, coordinator } = await setup(false, false);
		const pending = delaySubscribe(client);
		const publish = vi.spyOn(client.redis, 'publish');
		coordinator.onMessage({});
		relay(coordinator);
		coordinator.destroy();
		pending.resolve(1);
		await drain();
		expect(publish).not.toHaveBeenCalled();
	});
	it('keeps an unguarded relay behind its required subscription retry', async () => {
		const { client, coordinator } = await setup(false, false);
		const duplicate = client.duplicate.bind(client);
		const duplicates = vi.spyOn(client, 'duplicate').mockImplementation((...args) => {
			const sub = duplicate(...args);
			sub.subscribe = async () => { throw new Error('refused'); };
			return sub;
		});
		vi.spyOn(console, 'error').mockImplementation(() => {});
		const publish = vi.spyOn(client.redis, 'publish');
		coordinator.onMessage({});
		await drain();
		relay(coordinator);
		await drain();
		expect(duplicates).toHaveBeenCalledTimes(2);
		expect(publish).not.toHaveBeenCalled();
	});
	for (const cluster of [false, true]) {
		for (const subscribed of [false, true]) {
			it(`closes a publish probe with cluster=${cluster} subscriber=${subscribed}`, async () => {
				const { breaker, coordinator } = await setup(cluster);
				if (subscribed) coordinator.onMessage({});
				relay(coordinator);
				await drain();
				expect(breaker.state).toBe('healthy');
				expect(breaker.success).toHaveBeenCalledOnce();
				expect(breaker.failure).not.toHaveBeenCalled();
			});
		}
		it(`reopens a failed publish probe with cluster=${cluster}`, async () => {
			const { breaker, client, coordinator } = await setup(cluster);
			vi.spyOn(client.redis, 'publish').mockRejectedValueOnce(new Error('offline'));
			relay(coordinator);
			await drain();
			expect(breaker.state).toBe('broken');
			expect(breaker.failure).toHaveBeenCalledOnce();
			expect(breaker.success).not.toHaveBeenCalled();
		});
	}
	for (const method of ['readSnapshot', 'writeSnapshot']) {
		it(`closes a successful ${method} probe`, async () => {
			const { breaker, coordinator } = await setup();
			await coordinator[method]('__smooth:room', [{ state: 1 }]);
			expect(breaker.state).toBe('healthy');
			expect(breaker.success).toHaveBeenCalledOnce();
			expect(breaker.failure).not.toHaveBeenCalled();
		});
		it(`reopens a failed ${method} probe`, async () => {
			const { breaker, client, coordinator } = await setup();
			vi.spyOn(client.redis, method === 'readSnapshot' ? 'get' : 'set').mockRejectedValueOnce(new Error('offline'));
			await coordinator[method]('__smooth:room', []);
			expect(breaker.state).toBe('broken');
			expect(breaker.failure).toHaveBeenCalledOnce();
			expect(breaker.success).not.toHaveBeenCalled();
		});
	}
	it('treats invalid stored JSON as a successful backend read', async () => {
		const { breaker, client, coordinator } = await setup();
		await client.redis.set(client.key('smooth:snap:__smooth:room'), '{');
		expect(await coordinator.readSnapshot('__smooth:room')).toBeNull();
		expect(breaker.state).toBe('healthy');
		expect(breaker.success).toHaveBeenCalledOnce();
		expect(breaker.failure).not.toHaveBeenCalled();
	});
	for (const snapshot of [false, true]) {
		it(`preserves the probe after an encoding error with snapshot=${snapshot}`, async () => {
			const { breaker, coordinator } = await setup();
			const circular = [];
			circular.push(circular);
			if (snapshot) await coordinator.writeSnapshot('__smooth:room', circular);
			else expect(() => relay(coordinator, circular)).toThrow(TypeError);
			expect(breaker.state).toBe('probing');
			expect(breaker.failure).not.toHaveBeenCalled();
			expect(breaker.success).not.toHaveBeenCalled();
			expect(() => breaker.guard()).not.toThrow();
		});
	}
	it('reports subscribe failure once and never publishes without its reply channel', async () => {
		const { breaker, client, coordinator } = await setup();
		const pending = delaySubscribe(client);
		const publish = vi.spyOn(client.redis, 'publish');
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		coordinator.onMessage({});
		relay(coordinator);
		pending.reject(new Error('subscribe refused'));
		await drain();
		expect(publish).not.toHaveBeenCalled();
		expect(breaker.state).toBe('broken');
		expect(breaker.failure).toHaveBeenCalledOnce();
	});
	it('returns a waiting subscription probe immediately on destroy', async () => {
		const { breaker, client, coordinator } = await setup();
		const pending = delaySubscribe(client);
		const publish = vi.spyOn(client.redis, 'publish');
		coordinator.onMessage({});
		relay(coordinator);
		coordinator.destroy();
		expect(breaker.state).toBe('probing');
		expect(() => breaker.guard()).not.toThrow();
		pending.resolve(1);
		await drain();
		expect(publish).not.toHaveBeenCalled();
		expect(breaker.success).not.toHaveBeenCalled();
		expect(breaker.failure).not.toHaveBeenCalled();
	});
	for (const recovers of [false, true]) {
		it(`retries a subscription that failed before the relay call with recovery=${recovers}`, async () => {
			const { breaker, client, coordinator } = await setup();
			const duplicate = client.duplicate.bind(client);
			let attempts = 0;
			const duplicates = vi.spyOn(client, 'duplicate').mockImplementation((...args) => {
				const sub = duplicate(...args);
				if (attempts++ === 0 || !recovers) sub.subscribe = async () => { throw new Error('refused'); };
				return sub;
			});
			vi.spyOn(console, 'error').mockImplementation(() => {});
			const publish = vi.spyOn(client.redis, 'publish');
			coordinator.onMessage({});
			await drain();
			relay(coordinator);
			await drain();
			expect(duplicates).toHaveBeenCalledTimes(2);
			expect(publish).toHaveBeenCalledTimes(recovers ? 1 : 0);
			expect(breaker.state).toBe(recovers ? 'healthy' : 'broken');
			expect(breaker.success).toHaveBeenCalledTimes(recovers ? 1 : 0);
			expect(breaker.failure).toHaveBeenCalledTimes(recovers ? 0 : 1);
		});
	}
	it('returns a waiting owner-resolution probe immediately on destroy', async () => {
		const { breaker, client, coordinator } = await setup(true);
		const pending = deferred();
		client.redis.cluster.mockImplementation((command) => command === 'SLOTS' ? pending.promise : Promise.resolve('owner'));
		const publish = vi.spyOn(client.redis, 'publish');
		relay(coordinator);
		coordinator.destroy();
		expect(() => breaker.guard()).not.toThrow();
		pending.resolve([[0, 16383, ['localhost', 1, 'owner']]]);
		await drain();
		expect(publish).not.toHaveBeenCalled();
		expect(breaker.success).not.toHaveBeenCalled();
		expect(breaker.failure).not.toHaveBeenCalled();
	});
	it('records the actual verdict of a publish already dispatched before destroy', async () => {
		const { breaker, client, coordinator } = await setup();
		const pending = deferred();
		vi.spyOn(client.redis, 'publish').mockReturnValueOnce(pending.promise);
		relay(coordinator);
		coordinator.destroy();
		expect(() => breaker.guard()).toThrow();
		pending.resolve(1);
		await drain();
		expect(breaker.state).toBe('healthy');
		expect(breaker.success).toHaveBeenCalledOnce();
	});
	it('reports a synchronous publish exception exactly once', async () => {
		const { breaker, client, coordinator } = await setup();
		vi.spyOn(client.redis, 'publish').mockImplementationOnce(() => { throw new Error('closed'); });
		expect(() => relay(coordinator)).not.toThrow();
		await drain();
		expect(breaker.state).toBe('broken');
		expect(breaker.failure).toHaveBeenCalledOnce();
	});
	it('resolves a new pinned owner after a failed cluster publish', async () => {
		const { breaker, client, coordinator } = await setup(true);
		const first = { cluster: async () => 'first', publish: vi.fn(async () => { throw new Error('owner gone'); }) };
		const second = { cluster: async () => 'second', publish: vi.fn(async () => 1) };
		let owner = first;
		client.redis.nodes = () => [owner];
		client.redis.cluster.mockImplementation(async () => [[0, 16383, ['localhost', 1, await owner.cluster()]]]);
		relay(coordinator);
		await drain();
		expect(breaker.state).toBe('broken');
		owner = second;
		await vi.advanceTimersByTimeAsync(10);
		relay(coordinator);
		await drain();
		expect(first.publish).toHaveBeenCalledOnce();
		expect(second.publish).toHaveBeenCalledOnce();
		expect(breaker.state).toBe('healthy');
	});
	for (const snapshot of [false, true]) {
		it(`returns a probe if encoding destroys the coordinator with snapshot=${snapshot}`, async () => {
			const { breaker, client, coordinator } = await setup();
			const publish = vi.spyOn(client.redis, 'publish');
			const write = vi.spyOn(client.redis, 'set');
			const payload = { toJSON() { coordinator.destroy(); return []; } };
			if (snapshot) await coordinator.writeSnapshot('__smooth:room', payload);
			else relay(coordinator, payload);
			expect(publish).not.toHaveBeenCalled();
			expect(write).not.toHaveBeenCalled();
			expect(() => breaker.guard()).not.toThrow();
		});
	}
});
