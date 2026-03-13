import { describe, it, expect, vi } from 'vitest';

// We cannot import createRedisClient directly because it imports ioredis at module level.
// Instead, test the factory logic by mocking ioredis.

vi.mock('ioredis', () => {
	const MockRedis = vi.fn(function () {
		this.duplicate = vi.fn(() => new MockRedis());
		this.quit = vi.fn(() => Promise.resolve());
		this.disconnect = vi.fn();
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
});
