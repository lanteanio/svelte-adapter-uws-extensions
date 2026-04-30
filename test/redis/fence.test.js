import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createRedisFence } from '../../redis/fence.js';

describe('redis fence', () => {
	let client;
	let fence;

	beforeEach(() => {
		client = mockRedisClient('test:');
		fence = createRedisFence(client);
	});

	describe('createRedisFence', () => {
		it('returns a provider with the expected API', () => {
			expect(typeof fence.acquire).toBe('function');
			expect(typeof fence.heartbeat).toBe('function');
			expect(typeof fence.release).toBe('function');
		});

		it('throws on non-string keyPrefix', () => {
			expect(() => createRedisFence(client, { keyPrefix: 42 })).toThrow('keyPrefix');
		});

		it('accepts a custom keyPrefix', async () => {
			const f = createRedisFence(client, { keyPrefix: 'lock:' });
			await f.acquire('task-1', 'fence-a', 30);
			expect([...client._store.keys()]).toContain('test:lock:task-1');
		});
	});

	describe('acquire', () => {
		it('writes the fence value under the key', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			expect(client._store.get('test:fence:task-1')).toBe('fence-a');
		});

		it('overwrites a prior fence value', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			await fence.acquire('task-1', 'fence-b', 30);
			expect(client._store.get('test:fence:task-1')).toBe('fence-b');
		});

		it('throws on empty taskId', async () => {
			await expect(fence.acquire('', 'fence-a', 30)).rejects.toThrow('taskId');
		});

		it('throws on empty fence', async () => {
			await expect(fence.acquire('task-1', '', 30)).rejects.toThrow('fence');
		});

		it('throws on non-positive ttl', async () => {
			await expect(fence.acquire('task-1', 'fence-a', 0)).rejects.toThrow('ttl');
			await expect(fence.acquire('task-1', 'fence-a', -1)).rejects.toThrow('ttl');
			await expect(fence.acquire('task-1', 'fence-a', 1.5)).rejects.toThrow('ttl');
		});
	});

	describe('heartbeat', () => {
		it('returns true while the fence value still matches', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			expect(await fence.heartbeat('task-1', 'fence-a', 30)).toBe(true);
		});

		it('returns false when the fence value has changed (force-takeover detection)', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			// Simulate another worker overwriting the lock
			client._store.set('test:fence:task-1', 'fence-b');
			expect(await fence.heartbeat('task-1', 'fence-a', 30)).toBe(false);
		});

		it('returns false when the key is absent (TTL expired or never acquired)', async () => {
			expect(await fence.heartbeat('never-acquired', 'fence-a', 30)).toBe(false);
		});
	});

	describe('release', () => {
		it('deletes the key when the fence value matches', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			await fence.release('task-1', 'fence-a');
			expect(client._store.has('test:fence:task-1')).toBe(false);
		});

		it('does not delete when the fence value differs', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			client._store.set('test:fence:task-1', 'fence-b');
			await fence.release('task-1', 'fence-a');
			expect(client._store.get('test:fence:task-1')).toBe('fence-b');
		});

		it('is a no-op on an already-released key', async () => {
			await fence.release('never-acquired', 'fence-a');
		});
	});
});
