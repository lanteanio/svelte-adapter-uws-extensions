import { describe, expect, it, vi } from 'vitest';
import { createDistributedSession } from '../../src/redis/session.js';

export function sessionTokenContract(getClient) {
	describe('reserved session token namespace', () => {
		for (const keyPrefix of [undefined, 'custom:', '']) {
			for (const victimFirst of [false, true]) {
				it(`preserves erasure with prefix ${String(keyPrefix)}, victimFirst=${victimFirst}`, async () => {
					const client = getClient();
					const sessions = createDistributedSession(client, { keyPrefix, forgetUserId: data => data.userId });
					const createVictim = () => sessions.set('valid-token', { userId: 'victim', private: 'profile' });
					if (victimFirst) await createVictim();
					for (const operation of ['get', 'set', 'touch', 'delete']) {
						await expect(sessions[operation]('byuser:victim', { userId: 'attacker' })).rejects.toThrow(/reserved prefix/);
					}
					if (!victimFirst) await createVictim();
					expect(await sessions.get('valid-token')).toEqual({ userId: 'victim', private: 'profile' });
					expect(await sessions.purgeUser(null, 'victim')).toBe(1);
					expect(await sessions.get('valid-token')).toBeNull();
				});
			}
		}
		it.each(['reject', 'anonymous'])('validates lifecycle cookies before Redis under %s policy', async onLoadError => {
			const client = getClient();
			const failure = vi.fn();
			const sessions = createDistributedSession(client, {
				identify: () => 'byuser:victim', onLoadError, breaker: { failure, success: vi.fn() }
			});
			const get = vi.spyOn(client.redis, 'get');
			try {
				expect(await sessions.withHooks().upgrade({})).toEqual(onLoadError === 'reject' ? false : {});
				expect(get).not.toHaveBeenCalled();
				expect(failure).not.toHaveBeenCalled();
			} finally { get.mockRestore(); }
		});
		it('rejects legacy indexed collisions before deleting any session or index', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, { forgetUserId: data => data.userId });
			await sessions.set('first-token', { userId: 'first' });
			await sessions.set('second-token', { userId: 'second' });
			await client.redis.hset(client.key('sess:byuser:first'), 'byuser:second', '1');
			await expect(sessions.purgeUser(null, 'first')).rejects.toThrow(/reserved prefix/);
			expect(await sessions.get('first-token')).toEqual({ userId: 'first' });
			expect(await sessions.purgeUser(null, 'second')).toBe(1);
			expect(await sessions.get('second-token')).toBeNull();
		});
		it('allows ordinary tokens containing the reserved text later or in another case', async () => {
			const sessions = createDistributedSession(getClient());
			for (const token of ['prefix:byuser:token', 'Byuser:token']) {
				await sessions.set(token, { ok: true });
				expect(await sessions.get(token)).toEqual({ ok: true });
				expect(await sessions.delete(token)).toBe(true);
			}
		});
	});
}
