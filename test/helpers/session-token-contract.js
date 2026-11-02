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
			await expect(sessions.purgeUser(null, 'first')).rejects.toMatchObject({
				code: 'ERASURE_INDEX_INCOMPLETE', erasureIndexIncomplete: true, removed: 0,
				cause: expect.objectContaining({ message: expect.stringContaining('reserved prefix') })
			});
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

	describe('session token length', () => {
		const boundaryTokens = [
			['ASCII', 'a'.repeat(4096)],
			['BMP', String.fromCodePoint(0xe9).repeat(4096)],
			['supplementary', String.fromCodePoint(0x1f642).repeat(2048)]
		];
		const tooLongTokens = boundaryTokens.map(([name, token]) => [name, token + 'a']);
		const modes = [
			['plain', {}],
			['user-indexed', { forgetUserId: data => data.userId }],
			['qualified', { forgetIdentity: data => ({ tenantId: null, userId: data.userId }) }]
		];

		function observedClient(client) {
			const commands = [];
			const key = vi.fn(part => client.key(part));
			const redis = new Proxy(client.redis, {
				get(target, property) {
					const value = Reflect.get(target, property, target);
					if (typeof value !== 'function') return value;
					return (...args) => {
						commands.push(property);
						return value.apply(target, args);
					};
				}
			});
			return { client: { ...client, key, redis }, commands, key };
		}

		for (const [mode, options] of modes) {
			it(`rejects oversized raw tokens before any Redis or index work in ${mode} mode`, async () => {
				const watched = observedClient(getClient());
				const extractor = vi.fn(data => options.forgetIdentity
					? options.forgetIdentity(data) : data.userId);
				const configured = options.forgetIdentity ? { forgetIdentity: extractor }
					: options.forgetUserId ? { forgetUserId: extractor } : {};
				const sessions = createDistributedSession(watched.client, configured);
				const serialize = vi.fn(() => ({ userId: 'u' }));
				watched.commands.length = 0;
				watched.key.mockClear();
				for (const [, token] of tooLongTokens) {
					expect(token.length).toBe(4097);
					for (const operation of ['get', 'set', 'touch', 'delete']) {
						await expect(sessions[operation](token, { toJSON: serialize }))
							.rejects.toThrow('at most 4096 UTF-16 code units');
					}
				}
				expect(watched.commands).toEqual([]);
				expect(watched.key).not.toHaveBeenCalled();
				expect(extractor).not.toHaveBeenCalled();
				expect(serialize).not.toHaveBeenCalled();
			});

			it.each(boundaryTokens)(`accepts a 4096-code-unit %s token in ${mode} mode`, async (name, token) => {
				const sessions = createDistributedSession(getClient(), options);
				expect(token.length).toBe(4096);
				if (name !== 'ASCII') expect(Buffer.byteLength(token)).toBeGreaterThan(4096);
				expect(await sessions.get(token)).toBeNull();
				expect(await sessions.touch(token)).toBe(false);
				expect(await sessions.delete(token)).toBe(false);
				await sessions.set(token, { userId: 'u', value: 1 });
				expect(await sessions.get(token)).toEqual({ userId: 'u', value: 1 });
				expect(await sessions.touch(token)).toBe(true);
				expect(await sessions.delete(token)).toBe(true);
				expect(await sessions.get(token)).toBeNull();
			});
		}

		it.each(['reject', 'anonymous'])('keeps oversized lifecycle identities anonymous under %s policy', async onLoadError => {
			const watched = observedClient(getClient());
			const sessions = createDistributedSession(watched.client, { identify: ctx => ctx.token, onLoadError });
			const hooks = sessions.withHooks();
			watched.commands.length = 0;
			watched.key.mockClear();
			for (const [, token] of tooLongTokens) {
				const userData = await hooks.upgrade({ token });
				expect(userData).toEqual({});
				const ws = { getUserData: () => userData };
				expect(sessions.of(ws)).toBeNull();
				await hooks.close(ws, {});
			}
			expect(watched.commands).toEqual([]);
			expect(watched.key).not.toHaveBeenCalled();
		});

		it.each(boundaryTokens)('loads and persists a boundary %s lifecycle token', async (name, token) => {
			const client = getClient();
			const sessions = createDistributedSession(client, { identify: ctx => ctx.token, refreshOnGet: false });
			await client.redis.set(client.key('sess:' + token), JSON.stringify({ d: { value: 1 }, c: 1 }));
			const hooks = sessions.withHooks();
			const userData = await hooks.upgrade({ token });
			const ws = { getUserData: () => userData };
			expect(sessions.of(ws)).toEqual({ value: 1 });
			sessions.of(ws).value = 2;
			await hooks.close(ws, {});
			const reloaded = await hooks.upgrade({ token });
			expect(sessions.of({ getUserData: () => reloaded })).toEqual({ value: 2 });
		});

		it('refuses oversized indexed tokens before a legacy purge accesses any record', async () => {
			const client = getClient();
			const watched = observedClient(client);
			const sessions = createDistributedSession(watched.client, { forgetUserId: data => data.userId });
			await sessions.set('kept', { userId: 'u' });
			const oversized = 'a'.repeat(4097);
			await client.redis.hset(client.key('sess:byuser:u'), oversized, '1');
			watched.commands.length = 0;
			watched.key.mockClear();
			await expect(sessions.purgeUser(null, 'u')).rejects.toMatchObject({
				message: 'session: erasure index incomplete',
				code: 'ERASURE_INDEX_INCOMPLETE', erasureIndexIncomplete: true, removed: 0,
				cause: expect.objectContaining({ message: 'session: token must contain at most 4096 UTF-16 code units' })
			});
			expect(watched.commands).toEqual(['hkeys']);
			expect(watched.key.mock.calls.every(([part]) => !part.includes(oversized))).toBe(true);
			expect(await sessions.get('kept')).toEqual({ userId: 'u' });
			expect(await client.redis.hexists(client.key('sess:byuser:u'), oversized)).toBe(1);
		});

		it('purges an indexed token at the length boundary', async () => {
			const sessions = createDistributedSession(getClient(), { forgetUserId: data => data.userId });
			const token = 'a'.repeat(4096);
			await sessions.set(token, { userId: 'u' });
			expect(await sessions.purgeUser(null, 'u')).toBe(1);
			expect(await sessions.get(token)).toBeNull();
		});

		it('refuses an oversized qualified index field before constructing a record key', async () => {
			const client = getClient();
			const watched = observedClient(client);
			const sessions = createDistributedSession(watched.client, { forgetIdentity: data => data.owner });
			await sessions.set('kept-qualified', { owner: { tenantId: null, userId: 'u' } });
			const oversized = 'a'.repeat(4097);
			const identity = Buffer.from(JSON.stringify([null, 'u'])).toString('base64url');
			await client.redis.hset(client.key('sess:identity:v1:byuser:' + identity), oversized, '1');
			watched.key.mockClear();
			await expect(sessions.purgeUser(null, 'u')).rejects.toMatchObject({
				message: 'session: erasure index incomplete',
				code: 'ERASURE_INDEX_INCOMPLETE', erasureIndexIncomplete: true, removed: 0,
				cause: expect.objectContaining({ message: 'session: token must contain at most 4096 UTF-16 code units' })
			});
			expect(watched.key.mock.calls.every(([part]) => !part.includes(oversized))).toBe(true);
			expect(await client.redis.hexists(client.key('sess:identity:v1:byuser:' + identity), oversized)).toBe(1);
			expect(Number(await client.redis.hget(client.key('sess:identity:v1:byuser:' + identity), 'identity:erasure'))).toBeGreaterThan(0);
			expect(await client.redis.get(client.key('sess:kept-qualified'))).not.toBeNull();
		});

		it.each([false, true])('reports completed removals when a qualified residue check rejects an oversized legacy-index field, own record=%s', async ownRecord => {
			const client = getClient();
			const watched = observedClient(client);
			const sessions = createDistributedSession(watched.client, { forgetIdentity: data => data.owner });
			if (ownRecord) await sessions.set('own-record', { owner: { tenantId: null, userId: 'u' } });
			const oversized = 'a'.repeat(4097);
			await client.redis.hset(client.key('sess:byuser:u'), oversized, '1');
			await client.redis.set(client.key('sess:identity:v1:legacy'), JSON.stringify({ since: 1, state: 'clean' }));
			watched.key.mockClear();
			await expect(sessions.purgeUser(null, 'u')).rejects.toMatchObject({
				message: 'session: erasure index incomplete',
				code: 'ERASURE_INDEX_INCOMPLETE', erasureIndexIncomplete: true, removed: ownRecord ? 1 : 0,
				cause: expect.objectContaining({ message: 'session: token must contain at most 4096 UTF-16 code units' })
			});
			expect(watched.key.mock.calls.every(([part]) => !part.includes(oversized))).toBe(true);
			expect(await client.redis.hexists(client.key('sess:byuser:u'), oversized)).toBe(1);
			expect(await client.redis.get(client.key('sess:own-record'))).toBeNull();
			const identity = Buffer.from(JSON.stringify([null, 'u'])).toString('base64url');
			expect(Number(await client.redis.hget(client.key('sess:identity:v1:byuser:' + identity), 'identity:erasure'))).toBeGreaterThan(0);
		});
	});
}
