import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createTaskRunner } from '../../postgres/tasks.js';
import { createRedisFence } from '../../redis/fence.js';

describe('postgres tasks (Redis fence provider)', () => {
	let pg;
	let redis;
	let fence;
	let runner;

	beforeEach(() => {
		pg = mockPgClient();
		redis = mockRedisClient('test:');
		fence = createRedisFence(redis);
		runner = createTaskRunner(pg, {
			fence,
			recoveryInterval: 0,
			cleanupInterval: 0
		});
	});

	afterEach(() => {
		runner.destroy();
	});

	describe('option validation', () => {
		it('throws when fence is missing required methods', () => {
			expect(() => createTaskRunner(pg, {
				fence: {},
				recoveryInterval: 0,
				cleanupInterval: 0
			})).toThrow('fence must implement');

			expect(() => createTaskRunner(pg, {
				fence: { acquire: () => {}, heartbeat: () => {} },
				recoveryInterval: 0,
				cleanupInterval: 0
			})).toThrow('fence must implement');
		});

		it('accepts a valid fence provider', () => {
			const r = createTaskRunner(pg, {
				fence,
				recoveryInterval: 0,
				cleanupInterval: 0
			});
			r.destroy();
		});
	});

	describe('acquire / release lifecycle', () => {
		it('writes the fence to Redis on insert and deletes it on commit', async () => {
			runner.register('echo', async ({ fence: f }) => f);

			const result = await runner.run('echo', { input: null });

			// The fence should have been written and then released (deleted)
			const keys = [...redis._store.keys()].filter((k) => k.includes('fence:'));
			expect(keys).toHaveLength(0);
			expect(typeof result).toBe('string');
		});

		it('releases the fence on terminal failure', async () => {
			runner.register('boom', async () => {
				throw new Error('handler exploded');
			});

			await expect(runner.run('boom', { input: null })).rejects.toThrow('handler exploded');

			const keys = [...redis._store.keys()].filter((k) => k.includes('fence:'));
			expect(keys).toHaveLength(0);
		});
	});

	describe('heartbeat with Redis check', () => {
		it('aborts the handler when the Redis fence is force-deleted mid-run', async () => {
			const r = createTaskRunner(pg, {
				fence,
				fenceTtl: 5,
				heartbeatInterval: 50,
				recoveryInterval: 0,
				cleanupInterval: 0
			});

			let observedAbort = false;
			r.register('long', async ({ signal }) => {
				return await new Promise((resolve, reject) => {
					const timer = setTimeout(() => resolve('completed'), 5000);
					signal.addEventListener('abort', () => {
						clearTimeout(timer);
						observedAbort = true;
						reject(new Error('aborted'));
					}, { once: true });
				});
			});

			const promise = r.run('long', { input: null });

			// Wait for the row to be inserted and Redis fence written
			await new Promise((res) => setTimeout(res, 100));
			const fenceKeys = [...redis._store.keys()].filter((k) => k.includes('fence:'));
			expect(fenceKeys).toHaveLength(1);

			// Force-takeover: remove the Redis fence to simulate another
			// instance forcibly releasing the lock.
			redis._store.delete(fenceKeys[0]);

			await expect(promise).rejects.toThrow('aborted');
			expect(observedAbort).toBe(true);

			r.destroy();
		}, 5000);

		it('aborts when the Redis fence value is overwritten by another owner', async () => {
			const r = createTaskRunner(pg, {
				fence,
				fenceTtl: 5,
				heartbeatInterval: 50,
				recoveryInterval: 0,
				cleanupInterval: 0
			});

			r.register('long', async ({ signal }) => {
				return await new Promise((resolve, reject) => {
					const timer = setTimeout(() => resolve('completed'), 5000);
					signal.addEventListener('abort', () => {
						clearTimeout(timer);
						reject(new Error('aborted'));
					}, { once: true });
				});
			});

			const promise = r.run('long', { input: null });

			await new Promise((res) => setTimeout(res, 100));
			const fenceKeys = [...redis._store.keys()].filter((k) => k.includes('fence:'));
			redis._store.set(fenceKeys[0], 'someone-else');

			await expect(promise).rejects.toThrow('aborted');

			r.destroy();
		}, 5000);
	});
});
