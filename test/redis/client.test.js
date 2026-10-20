import { describe, it, expect, vi } from 'vitest';

// We cannot import createRedisClient directly because it imports ioredis at module level.
// Instead, test the factory logic by mocking ioredis.

vi.mock('ioredis', () => {
	const MockRedis = vi.fn(function () {
		this.listeners = {};
		this.status = 'ready';
		this.duplicate = vi.fn(() => new MockRedis());
		this.quit = vi.fn(() => Promise.resolve());
		this.disconnect = vi.fn();
		this.on = vi.fn((event, cb) => {
			(this.listeners[event] ||= []).push(cb);
			return this;
		});
		this.removeListener = (event, cb) => { this.listeners[event] = (this.listeners[event] || []).filter(listener => listener !== cb && listener.listener !== cb); };
		this.once = (event, cb) => {
			const listener = () => { this.removeListener(event, listener); cb(); };
			listener.listener = cb;
			return this.on(event, listener);
		};
		this._emit = (event) => { for (const cb of [...(this.listeners[event] || [])]) cb(); };
	});
	return { default: MockRedis };
});

const { createRedisClient } = await import('../../src/redis/index.js');
const { EXTENSIONS_ERROR_IDS } = await import('../../src/error-registry.js');
const { MAX_REDIS_DUPLICATES_PER_CLIENT } = await import('../../src/shared/caps.js');
const { registeredRecord } = await import('../helpers/diagnostic-line.js');

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

	it('refuses new duplicates both during and after terminal shutdown', async () => {
		const client = createRedisClient({ autoShutdown: false });
		let finish;
		client.redis.quit.mockImplementation(() => new Promise(resolve => { finish = resolve; }));
		const quitting = client.quit();
		expect(() => client.duplicate()).toThrow(/shut down/);
		finish();
		await quitting;
		expect(() => client.duplicate()).toThrow(/shut down/);
		expect(client.redis.duplicate).not.toHaveBeenCalled();
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

describe('createRedisClient connection errors', () => {
	it('reports an error on the main connection through the registry, with the URL redacted', async () => {
		const { captureRegistered } = await import('../helpers/diagnostic-line.js');
		const client = createRedisClient({ autoShutdown: false });
		const onError = client.redis.listeners.error?.[0];
		expect(typeof onError).toBe('function');
		const sink = captureRegistered();
		try {
			onError(Object.assign(new Error('connect failed for redis://app:hunter2@cache.internal:6379'), { code: 'ECONNREFUSED' }));
		} finally {
			sink.stop();
		}
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_CLIENT]);
		expect(JSON.stringify(sink.records)).not.toContain('hunter2');
	});
});

describe('createRedisClient shutdown listener', () => {
	it('quit() removes its sveltekit:shutdown listener', async () => {
		const before = process.listenerCount('sveltekit:shutdown');
		const clients = Array.from({ length: 3 }, () => createRedisClient());
		expect(process.listenerCount('sveltekit:shutdown')).toBe(before + 3);
		await Promise.all(clients.map((client) => client.quit()));
		expect(process.listenerCount('sveltekit:shutdown')).toBe(before);
	});
});

describe('createRedisClient duplicate bound', () => {
	it('reports once when the duplicate count reaches the bound', () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			const client = createRedisClient({ autoShutdown: false });
			for (let i = 0; i < MAX_REDIS_DUPLICATES_PER_CLIENT + 5; i++) client.duplicate();
			expect(warn).toHaveBeenCalledOnce();
			const record = registeredRecord(warn.mock.calls[0][0], EXTENSIONS_ERROR_IDS.REDIS_DUPLICATES);
			expect(record.attributes).toEqual({ duplicates: MAX_REDIS_DUPLICATES_PER_CLIENT });
		} finally {
			warn.mockRestore();
		}
	});
});
