import { describe, it, expect } from 'vitest';
import { createDistributedSession } from '../../src/redis/session.js';
import { createForgetStore } from '../../src/shared/forget-store.js';

export function sessionErasureContract(getClient) {
	describe('recoverable session erasure', () => {
		const options = { forgetUserId: (data) => data?.userId, refreshOnGet: false };
		for (const lifecycle of [false, true]) {
			it(`recovers an unindexed ${lifecycle ? 'lifecycle' : 'raw'} record after MULTI failure`, async () => {
				const client = getClient();
				const sessions = createDistributedSession(client, options);
				const multi = client.redis.multi;
				client.redis.multi = () => ({ hset() {}, pexpire() {}, hpexpire() {}, async exec() { throw new Error('unavailable'); } });
				try {
					const write = lifecycle ? sessions.create({ userId: 'target' }) : sessions.set('target-token', { userId: 'target' });
					await expect(write).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
				} finally { client.redis.multi = multi; }
				await sessions.set('other-token', { userId: 'other' });
				// A new instance proves recovery does not depend on process memory.
				const recovered = createDistributedSession(client, options);
				expect(await recovered.purgeUser(null, 'target')).toBe(1);
				expect(await recovered.purgeUser(null, 'target')).toBe(0);
				expect(await recovered.get('other-token')).toEqual({ userId: 'other' });
			});
		}

		it('touches lifecycle records with extractors that require nested data', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, { forgetUserId: (data) => data.user.id });
			const token = await sessions.create({ user: { id: 'target' } });
			expect(await sessions.touch(token)).toBe(true);
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
		});

		it('recovers string records without reading or deleting registry routing hashes', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			await client.redis.hset(client.key('sess:routing-token'), 'instanceId', 'worker-a', 'ts', '123');
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
			expect(await client.redis.hgetall(client.key('sess:routing-token'))).toEqual({ instanceId: 'worker-a', ts: '123' });
		});

		it('erases every matching token across multiple slots after losing the index', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			for (let i = 0; i < 32; i++) await sessions.set('token-' + i, { userId: 'target' });
			await client.redis.unlink(client.key('sess:byuser:target'));
			expect(await sessions.purgeUser(null, 'target')).toBe(32);
			for (let i = 0; i < 32; i++) expect(await sessions.get('token-' + i)).toBeNull();
		});

		it('rejects command errors inside an otherwise resolved MULTI', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			const multi = client.redis.multi;
			client.redis.multi = () => ({ hset() {}, pexpire() {}, hpexpire() {}, async exec() { return [[new Error('denied'), null], [null, 0]]; } });
			try { await expect(sessions.set('tok', { userId: 'target' })).rejects.toMatchObject({ erasureIndexIncomplete: true }); }
			finally { client.redis.multi = multi; }
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
		});

		it('finds records whose reverse index expired independently', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			await client.redis.unlink(client.key('sess:byuser:target'));
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
			expect(await sessions.get('tok')).toBeNull();
		});

		it('retains a replacement belonging to another user despite stale index membership', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			await sessions.set('tok', { userId: 'other' });
			expect(await sessions.purgeUser(null, 'target')).toBe(0);
			expect(await sessions.get('tok')).toEqual({ userId: 'other' });
		});

		it('preserves a replacement written between inspection and deletion', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			const get = client.redis.get;
			let replaced = false;
			client.redis.get = async function (key) {
				const raw = await get.call(this, key);
				if (key === client.key('sess:tok') && !replaced) {
					replaced = true;
					await sessions.set('tok', { userId: 'other' });
				}
				return raw;
			};
			try { expect(await sessions.purgeUser(null, 'target')).toBe(0); }
			finally { client.redis.get = get; }
			expect(replaced).toBe(true);
			expect(await sessions.get('tok')).toEqual({ userId: 'other' });
		});

		it('rejects unreadable primary data instead of claiming complete erasure', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await client.redis.set(client.key('sess:tok'), 'broken-json');
			await expect(sessions.purgeUser(null, 'target')).rejects.toMatchObject({ erasureIndexIncomplete: true });
			expect(await client.redis.get(client.key('sess:tok'))).toBe('broken-json');
		});

		it('surfaces extractor failures and recovers when extraction is repaired', async () => {
			const client = getClient();
			const broken = createDistributedSession(client, { forgetUserId() { throw new Error('invalid data'); } });
			await expect(broken.set('tok', { userId: 'target' })).rejects.toMatchObject({ erasureIndexIncomplete: true });
			await expect(broken.purgeUser(null, 'target')).rejects.toMatchObject({ erasureIndexIncomplete: true });
			expect(await createDistributedSession(client, options).purgeUser(null, 'target')).toBe(1);
		});

		it('escapes literal prefix glob characters during recovery', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, { ...options, keyPrefix: 'sess:[one]*?:' });
			await sessions.set('tok', { userId: 'target' });
			await client.redis.set(client.key('sess:oZZx:outside'), JSON.stringify({ userId: 'target' }));
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
			expect(await client.redis.get(client.key('sess:oZZx:outside'))).not.toBeNull();
		});

		it('rejects ambiguous ownership without deleting either candidate', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			const data = { userId: 'target', d: { userId: 'other' }, c: 1 };
			await sessions.set('tok', data);
			await expect(sessions.purgeUser(null, 'target')).rejects.toMatchObject({ erasureIndexIncomplete: true });
			expect(await sessions.get('tok')).toEqual(data);
		});

		it('reports incomplete erasure to the composer and permits retry', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			const hkeys = client.redis.hkeys;
			client.redis.hkeys = async () => { throw new Error('unavailable'); };
			try { await expect(createForgetStore({ sessions }).purgeUser(null, 'target')).rejects.toThrow(/incomplete/); }
			finally { client.redis.hkeys = hkeys; }
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
		});

		for (const operation of ['get', 'touch']) {
			it(`rejects ${operation} when sliding index maintenance fails`, async () => {
				const client = getClient();
				const sessions = createDistributedSession(client, { ...options, refreshOnGet: true });
				await sessions.set('tok', { userId: 'target' });
				const multi = client.redis.multi;
				client.redis.multi = () => { throw new Error('unavailable'); };
				try { await expect(sessions[operation]('tok')).rejects.toMatchObject({ erasureIndexIncomplete: true }); }
				finally { client.redis.multi = multi; }
				expect(await sessions.purgeUser(null, 'target')).toBe(1);
			});
		}
	});
}
