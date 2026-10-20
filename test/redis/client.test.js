import { describe, it, expect, vi } from 'vitest';

// We cannot import createRedisClient directly because it imports ioredis at module level.
// Instead, test the factory logic by mocking ioredis.

vi.mock('ioredis', () => {
	const MockRedis = vi.fn(function () {
		this.duplicate = vi.fn(() => new MockRedis());
		this.quit = vi.fn(() => Promise.resolve());
		this.disconnect = vi.fn();
		this.on = vi.fn(() => this);
	});
	return { default: MockRedis };
});

const { createRedisClient } = await import('../../redis/index.js');

describe('createRedisClient', () => {
	it('returns a client with the expected API', () => {
		const client = createRedisClient();
		expect(client.redis).toBeDefined();
		expect(typeof client.key).toBe('function');
		expect(typeof client.duplicate).toBe('function');
		expect(typeof client.quit).toBe('function');
		expect(client.keyPrefix).toBe('');
	});

	it('uses the given keyPrefix', () => {
		const client = createRedisClient({ keyPrefix: 'myapp:' });
		expect(client.keyPrefix).toBe('myapp:');
		expect(client.key('foo')).toBe('myapp:foo');
		expect(client.key('bar:baz')).toBe('myapp:bar:baz');
	});

	it('key() with empty prefix returns key as-is', () => {
		const client = createRedisClient();
		expect(client.key('test')).toBe('test');
	});

	it('duplicate() creates a new connection', () => {
		const client = createRedisClient();
		const dup = client.duplicate();
		expect(dup).toBeDefined();
		expect(client.redis.duplicate).toHaveBeenCalled();
	});

	it('quit() calls quit on all connections', async () => {
		const client = createRedisClient();
		client.duplicate();
		await client.quit();
		expect(client.redis.quit).toHaveBeenCalled();
	});

	it('quit() is idempotent', async () => {
		const client = createRedisClient();
		await client.quit();
		await client.quit();
		// Should only call quit once on the redis instance
		expect(client.redis.quit).toHaveBeenCalledTimes(1);
	});

	it('quit() removes its sveltekit:shutdown listener', async () => {
		const before = process.listenerCount('sveltekit:shutdown');
		const clients = Array.from({ length: 3 }, () => createRedisClient());
		expect(process.listenerCount('sveltekit:shutdown')).toBe(before + 3);
		await Promise.all(clients.map((client) => client.quit()));
		expect(process.listenerCount('sveltekit:shutdown')).toBe(before);
	});

	it('sveltekit:shutdown still quits a client that was not released', async () => {
		const client = createRedisClient();
		process.emit('sveltekit:shutdown');
		await Promise.resolve();
		expect(client.redis.quit).toHaveBeenCalledTimes(1);
		await client.quit();
		expect(client.redis.quit).toHaveBeenCalledTimes(1);
	});
});
