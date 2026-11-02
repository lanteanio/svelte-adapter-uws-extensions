import { describe, expect, it } from 'vitest';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { mockRedisClient } from '../helpers/mock-redis.js';

describe('composed erasure failure isolation', () => {
	it.each(['synchronous', 'asynchronous'])('collects every result after a %s store failure', async mode => {
		const calls = [], cascade = { crdt: ['notes'] };
		const failure = Object.assign(new Error('store unavailable'), { removed: 3 });
		const first = { total: 2, purgeUser(...args) { calls.push(['first', ...args]); return this.total; } };
		const middle = { purgeUser(...args) { calls.push(['middle', ...args]); if (mode === 'synchronous') throw failure; return Promise.reject(failure); } };
		const last = { total: 4, purgeUser(...args) { calls.push(['last', ...args]); return Promise.resolve(this.total); } };
		const error = await createForgetStore({ first, middle, last }).purgeUser('team', 'member', cascade).catch(error => error);
		expect(error).toMatchObject({ code: 'ERASURE_INCOMPLETE', partialCounts: { first: 2, middle: 3, last: 4 }, failures: [failure] });
		expect(error.message).toContain('1 of 3 store(s) failed');
		expect(calls).toEqual(['first', 'middle', 'last'].map(name => [name, 'team', 'member', cascade]));
	});

	it('continues both cluster erasure legs after a synchronous component failure', async () => {
		const redis = mockRedisClient('').redis;
		const owner = '__live-room-owner:@t/team/room', presence = '__live-presence:@t/team/room';
		await redis.hset(owner, 'o', 'member', 'j:member', '1', 'n:member', '1', 'j:other', '2', 'n:other', '1');
		await redis.hset(presence, 'c:member', '1', 'd:member', '{"name":"Member"}', 'c:other', '1', 'd:other', '{"name":"Other"}');
		const failure = new Error('component unavailable');
		Object.defineProperty(failure, 'removed', { get() { throw new Error('count unavailable'); } });
		const error = await createForgetStore({ broken: { purgeUser() { throw failure; } }, final: { purgeUser() { return 2; } } }, { redis })
			.purgeUser('team', 'member').catch(error => error);
		expect(error).toMatchObject({ code: 'ERASURE_INCOMPLETE', failures: [failure], partialCounts: { final: 2, roomOwners: 1, presenceRoster: 2 },
			ownerSuccessions: [{ topic: '@t/team/room', owner: 'other', reason: 'succeeded' }], presenceTopics: ['@t/team/room'] });
		expect(await redis.hget(owner, 'o')).toBe('other');
		expect(await redis.hmget(owner, 'j:member', 'n:member')).toEqual([null, null]);
		expect(await redis.hmget(presence, 'c:member', 'd:member')).toEqual([null, null]);
		expect(await redis.hget(presence, 'c:other')).toBe('1');
	});

	it('keeps the original failure and successful counts when its optional count throws', async () => {
		const failure = new Error('store unavailable');
		Object.defineProperty(failure, 'removed', { get() { throw new Error('count unavailable'); } });
		const error = await createForgetStore({ first: { purgeUser() { return 2; } }, broken: { purgeUser() { throw failure; } }, last: { purgeUser() { return 4; } } })
			.purgeUser(null, 'member').catch(error => error);
		expect(error).toMatchObject({ code: 'ERASURE_INCOMPLETE', failures: [failure], partialCounts: { first: 2, last: 4 } });
		expect(error.failures[0]).toBe(failure);
	});
});
