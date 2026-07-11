import { describe, it, expect, vi } from 'vitest';

// We cannot import createRedisClient directly because it imports ioredis at module level.
// Instead, test the factory logic by mocking ioredis.

vi.mock('ioredis', () => {
	const MockRedis = vi.fn(function () {
		this.listeners = {};
		this.duplicate = vi.fn(() => new MockRedis());
		this.quit = vi.fn(() => Promise.resolve());
		this.disconnect = vi.fn();
		this.on = vi.fn((event, cb) => {
			(this.listeners[event] ||= []).push(cb);
			return this;
		});
		this._emit = (event) => { for (const cb of this.listeners[event] || []) cb(); };
	});
	return { default: MockRedis };
});

const { createRedisClient } = await import('../../src/redis/index.js');

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

	it('a transient close does not untrack a duplicate - it still quits at shutdown', async () => {
		const client = createRedisClient();
		const dup = client.duplicate();
		// ioredis emits 'close' on every transient disconnect (followed by
		// 'reconnecting'); the duplicate is still alive and must stay owned.
		dup._emit('close');
		await client.quit();
		expect(dup.quit).toHaveBeenCalled();
	});

	it('a terminal end untracks the duplicate - shutdown does not quit it again', async () => {
		const client = createRedisClient();
		const dup = client.duplicate();
		dup._emit('end');
		await client.quit();
		expect(dup.quit).not.toHaveBeenCalled();
	});
});
