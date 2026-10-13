import { describe, it, expect, vi } from 'vitest';
import { createDistributedSession } from '../../src/redis/session.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { scanKeys } from '../../src/shared/redis-scan.js';

// Replace MULTI with one whose EXEC rejects outright ('reject') or resolves
// with a refused first command ('row'). Returns the restore function.
function failingMulti(redis, mode = 'reject') {
	const original = redis.multi;
	redis.multi = () => {
		const tx = {
			hset: () => tx,
			hpexpire: () => tx,
			pexpire: () => tx,
			async exec() {
				if (mode === 'reject') throw new Error('unavailable');
				return [[new Error('NOPERM denied'), null], [null, [1]], [null, 0]];
			}
		};
		return tx;
	};
	return () => { redis.multi = original; };
}

// Record every key passed to a direct PEXPIRE, the command that slides a
// session record's TTL.
function recordExpiries(redis) {
	const original = redis.pexpire;
	const keys = [];
	redis.pexpire = function (key, ...rest) {
		keys.push(key);
		return original.call(this, key, ...rest);
	};
	return { keys, restore: () => { redis.pexpire = original; } };
}

// Run `action` once, right after the first pipeline completes.
function afterFirstPipeline(redis, action) {
	const original = redis.pipeline;
	let fired = false;
	redis.pipeline = function (...args) {
		const pipe = original.apply(this, args);
		return new Proxy(pipe, {
			get(target, prop) {
				const value = Reflect.get(target, prop);
				if (typeof value !== 'function') return value;
				if (prop !== 'exec' || fired) return value.bind(target);
				return async (...execArgs) => {
					const result = await value.apply(target, execArgs);
					fired = true;
					await action();
					return result;
				};
			}
		});
	};
	return () => { redis.pipeline = original; };
}

// A view of `client` that logs the name of every command it sends, including
// commands queued in pipelines and transactions.
function recordCommands(client) {
	const seen = [];
	const local = new Set(['on', 'once', 'off', 'removeListener', 'defineCommand', 'duplicate', 'quit', 'disconnect', 'connect', 'nodes']);
	const wrapBatch = (batch) => {
		const proxy = new Proxy(batch, {
			get(target, method) {
				const value = Reflect.get(target, method);
				if (typeof value !== 'function') return value;
				if (method === 'exec') return value.bind(target);
				return (...args) => {
					seen.push(String(method).toLowerCase());
					const result = value.apply(target, args);
					return result === target ? proxy : result;
				};
			}
		});
		return proxy;
	};
	const redis = new Proxy(client.redis, {
		get(target, name) {
			const value = Reflect.get(target, name, target);
			if (typeof value !== 'function' || typeof name !== 'string') return value;
			if (name === 'pipeline' || name === 'multi') return (...args) => wrapBatch(value.apply(target, args));
			if (local.has(name) || name.startsWith('_')) return value.bind(target);
			return (...args) => {
				seen.push(name.toLowerCase());
				return value.apply(target, args);
			};
		}
	});
	return { client: { ...client, redis }, seen };
}

export function sessionErasureContract(getClient) {
	describe('session erasure index', () => {
		const options = { forgetUserId: (data) => data?.userId, refreshOnGet: false };
		const indexOf = (client, userId) => client.redis.hkeys(client.key('sess:byuser:' + userId));

		for (const mode of ['reject', 'row']) {
			for (const lifecycle of [false, true]) {
				it(`stores no ${lifecycle ? 'lifecycle' : 'raw'} record when the index write fails (${mode})`, async () => {
					const client = getClient();
					const sessions = createDistributedSession(client, options);
					const restore = failingMulti(client.redis, mode);
					try {
						const write = lifecycle ? sessions.create({ userId: 'target' }) : sessions.set('target-token', { userId: 'target' });
						await expect(write).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE', erasureIndexIncomplete: true });
					} finally { restore(); }
					expect(await scanKeys(client.redis, client.key('sess:*'))).toEqual([]);
					expect(await sessions.purgeUser(null, 'target')).toBe(0);
				});
			}
		}

		it('rejects an extractor failure before the record exists, without a breaker failure', async () => {
			const client = getClient();
			const breaker = { failure: vi.fn(), success: vi.fn() };
			const broken = createDistributedSession(client, { breaker, forgetUserId() { throw new Error('invalid data'); } });
			await expect(broken.set('tok', { userId: 'target' })).rejects.toMatchObject({ erasureIndexIncomplete: true });
			const numeric = createDistributedSession(client, { breaker, forgetUserId: () => 42 });
			await expect(numeric.set('tok', { userId: 'target' })).rejects.toMatchObject({ erasureIndexIncomplete: true });
			expect(await client.redis.get(client.key('sess:tok'))).toBeNull();
			expect(breaker.failure).not.toHaveBeenCalled();
		});

		it('never runs the extractor on the lifecycle wrapper', async () => {
			const client = getClient();
			const forgetUserId = vi.fn((data) => String(data.userId));
			const sessions = createDistributedSession(client, { forgetUserId, identify: (ctx) => ctx.token });
			const token = await sessions.create({ userId: 42 });
			const other = await sessions.create({ userId: 7 });
			expect(await sessions.touch(token)).toBe(true);
			expect(await sessions.withHooks().upgrade({ token })).not.toBe(false);
			expect(forgetUserId.mock.calls.every(([data]) => Object.keys(data).join() === 'userId')).toBe(true);
			expect(await indexOf(client, 'undefined')).toEqual([]);
			expect(await sessions.purgeUser(null, '42')).toBe(1);
			expect(await client.redis.get(client.key('sess:' + token))).toBeNull();
			expect(await client.redis.get(client.key('sess:' + other))).not.toBeNull();
		});

		it("never reads or is blocked by other users' records", async () => {
			const client = getClient();
			const forgetUserId = (data) => String(data.userId);
			const sessions = createDistributedSession(client, { forgetUserId, refreshOnGet: false });
			await sessions.set('c-token', { userId: 'C' });
			const ambiguous = { userId: 'B', d: { userId: 'X' }, c: 1 };
			await sessions.set('b-ambiguous', ambiguous);
			const lifecycle = await sessions.create({ userId: 'B' });
			await client.redis.set(client.key('sess:b-broken'), 'broken-json', 'PX', 60_000);
			await client.redis.set(client.key('sess:b-empty'), '', 'PX', 60_000);
			expect(await sessions.purgeUser(null, 'C')).toBe(1);
			expect(await sessions.get('c-token')).toBeNull();
			expect(await sessions.get('b-ambiguous')).toEqual(ambiguous);
			expect(await client.redis.get(client.key('sess:b-broken'))).toBe('broken-json');
			expect(await sessions.purgeUser(null, 'B')).toBe(2);
			expect(await client.redis.get(client.key('sess:' + lifecycle))).toBeNull();
			expect(await sessions.purgeUser(null, 'X')).toBe(0);
		});

		it("reads only the purged user's records, with no keyspace scan", async () => {
			const base = getClient();
			const writer = createDistributedSession(base, options);
			const purge = async (userId) => {
				for (let i = 0; i < 3; i++) await writer.set(`${userId}-${i}`, { userId });
				const { client, seen } = recordCommands(base);
				expect(await createDistributedSession(client, options).purgeUser(null, userId)).toBe(3);
				return seen.filter((name) => name !== 'info').sort();
			};
			for (let i = 0; i < 5; i++) await writer.set('other-' + i, { userId: 'other-' + i });
			const small = await purge('first');
			for (let i = 5; i < 80; i++) await writer.set('other-' + i, { userId: 'other-' + i });
			const large = await purge('second');
			expect(small).not.toContain('scan');
			expect(small.filter((name) => name === 'get')).toHaveLength(3);
			expect(large).toEqual(small);
			for (let i = 0; i < 80; i++) expect(await writer.get('other-' + i)).toEqual({ userId: 'other-' + i });
		});

		it('erases every indexed token across slots and pipeline chunks', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			for (let i = 0; i < 300; i++) await sessions.set('token-' + i, { userId: 'target' });
			await sessions.set('kept', { userId: 'other' });
			expect(await sessions.purgeUser(null, 'target')).toBe(300);
			for (let i = 0; i < 300; i++) expect(await client.redis.get(client.key('sess:token-' + i))).toBeNull();
			expect(await client.redis.exists(client.key('sess:byuser:target'))).toBe(0);
			expect(await sessions.get('kept')).toEqual({ userId: 'other' });
		});

		it('touches lifecycle records with extractors that require nested data', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, { forgetUserId: (data) => data.user.id });
			const token = await sessions.create({ user: { id: 'target' } });
			expect(await sessions.touch(token)).toBe(true);
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
		});

		it('attributes raw data with extra keys by the raw value and an exact { d, c } shape by d', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('raw', { userId: 'target', d: { userId: 'other' }, c: 1 });
			await sessions.set('shaped', { d: { userId: 'nested' }, c: 5 });
			expect(await sessions.touch('raw')).toBe(true);
			expect(await sessions.touch('shaped')).toBe(true);
			expect(await indexOf(client, 'other')).toEqual([]);
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
			expect(await sessions.purgeUser(null, 'nested')).toBe(1);
		});

		it('leaves registry routing hashes under the shared prefix alone', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			await client.redis.hset(client.key('sess:routing-token'), 'instanceId', 'worker-a', 'ts', '123');
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
			expect(await client.redis.hgetall(client.key('sess:routing-token'))).toEqual({ instanceId: 'worker-a', ts: '123' });
		});

		it('keeps a replacement belonging to another user and drops the stale field', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			await sessions.set('tok', { userId: 'other' });
			expect(await sessions.purgeUser(null, 'target')).toBe(0);
			expect(await sessions.get('tok')).toEqual({ userId: 'other' });
			expect(await indexOf(client, 'target')).toEqual([]);
			expect(await sessions.purgeUser(null, 'other')).toBe(1);
		});

		it('keeps a session replaced for no user', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			await sessions.set('tok', { anonymous: true });
			expect(await sessions.purgeUser(null, 'target')).toBe(0);
			expect(await sessions.get('tok')).toEqual({ anonymous: true });
			expect(await indexOf(client, 'target')).toEqual([]);
		});

		it('preserves a replacement written between inspection and deletion', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('tok', { userId: 'target' });
			let replaced = false;
			const restore = afterFirstPipeline(client.redis, async () => {
				replaced = true;
				await sessions.set('tok', { userId: 'other' });
			});
			try { expect(await sessions.purgeUser(null, 'target')).toBe(0); }
			finally { restore(); }
			expect(replaced).toBe(true);
			expect(await sessions.get('tok')).toEqual({ userId: 'other' });
		});

		it("fails closed on an unreadable record in the user's index and erases the rest", async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, options);
			await sessions.set('good', { userId: 'target' });
			await sessions.set('bad', { userId: 'target' });
			await client.redis.set(client.key('sess:bad'), 'broken-json', 'PX', 60_000);
			await expect(sessions.purgeUser(null, 'target')).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
			expect(await sessions.get('good')).toBeNull();
			expect(await client.redis.get(client.key('sess:bad'))).toBe('broken-json');
			expect(await indexOf(client, 'target')).toEqual(['bad']);
			await client.redis.unlink(client.key('sess:bad'));
			expect(await sessions.purgeUser(null, 'target')).toBe(0);
			expect(await indexOf(client, 'target')).toEqual([]);
		});

		it('fails closed when the extractor rejects an indexed record and recovers once repaired', async () => {
			const client = getClient();
			await createDistributedSession(client, options).set('tok', { userId: 'target' });
			const broken = createDistributedSession(client, { forgetUserId() { throw new Error('invalid data'); } });
			await expect(broken.purgeUser(null, 'target')).rejects.toMatchObject({ erasureIndexIncomplete: true });
			expect(await client.redis.get(client.key('sess:tok'))).not.toBeNull();
			expect(await createDistributedSession(client, options).purgeUser(null, 'target')).toBe(1);
		});

		it('supports glob characters in the key prefix', async () => {
			const client = getClient();
			const sessions = createDistributedSession(client, { ...options, keyPrefix: 'sess:[one]*?:' });
			await sessions.set('tok', { userId: 'target' });
			await client.redis.set(client.key('sess:oZZx:outside'), JSON.stringify({ userId: 'target' }));
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
			expect(await client.redis.get(client.key('sess:oZZx:outside'))).not.toBeNull();
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

		for (const mode of ['reject', 'row']) {
			it(`touch rejects without sliding when the index refresh fails (${mode})`, async () => {
				const client = getClient();
				const sessions = createDistributedSession(client, options);
				await sessions.set('tok', { userId: 'target' });
				const expiries = recordExpiries(client.redis);
				const restore = failingMulti(client.redis, mode);
				try { await expect(sessions.touch('tok')).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' }); }
				finally { restore(); expiries.restore(); }
				expect(expiries.keys).not.toContain(client.key('sess:tok'));
				expect(await sessions.purgeUser(null, 'target')).toBe(1);
			});

			it(`get and lifecycle loads return the session without sliding when the index refresh fails (${mode})`, async () => {
				const client = getClient();
				const sessions = createDistributedSession(client, { ...options, refreshOnGet: true, identify: (ctx) => ctx.token });
				await sessions.set('tok', { userId: 'target' });
				const token = await sessions.create({ userId: 'target' });
				const expiries = recordExpiries(client.redis);
				const restore = failingMulti(client.redis, mode);
				let loaded;
				try {
					expect(await sessions.get('tok')).toEqual({ userId: 'target' });
					loaded = await sessions.withHooks().upgrade({ token });
				} finally { restore(); expiries.restore(); }
				expect(loaded).not.toBe(false);
				expect(sessions.of({ getUserData: () => loaded })).toEqual({ userId: 'target' });
				expect(expiries.keys).not.toContain(client.key('sess:tok'));
				expect(expiries.keys).not.toContain(client.key('sess:' + token));
				expect(await sessions.purgeUser(null, 'target')).toBe(2);
			});
		}

		for (const refresh of ['touch', 'get', 'load']) {
			it(`indexes a session stored without an index field on its next ${refresh}`, async () => {
				const client = getClient();
				const sessions = createDistributedSession(client, { ...options, refreshOnGet: true, identify: (ctx) => ctx.token });
				const record = refresh === 'load' ? { d: { userId: 'target' }, c: 1_700_000_000_000 } : { userId: 'target' };
				await client.redis.set(client.key('sess:legacy'), JSON.stringify(record), 'PX', 60_000);
				expect(await sessions.purgeUser(null, 'target')).toBe(0);
				if (refresh === 'touch') expect(await sessions.touch('legacy')).toBe(true);
				else if (refresh === 'get') expect(await sessions.get('legacy')).toEqual(record);
				else expect(await sessions.withHooks().upgrade({ token: 'legacy' })).not.toBe(false);
				expect(await indexOf(client, 'target')).toEqual(['legacy']);
				expect(await sessions.purgeUser(null, 'target')).toBe(1);
				expect(await client.redis.get(client.key('sess:legacy'))).toBeNull();
			});
		}

		it('counts one breaker failure per failed Redis operation and none for data errors', async () => {
			const client = getClient();
			const breaker = { failure: vi.fn(), success: vi.fn() };
			const sessions = createDistributedSession(client, { ...options, refreshOnGet: true, breaker });
			await sessions.set('tok', { userId: 'target' });
			for (const operation of ['set', 'touch', 'get']) {
				breaker.failure.mockClear();
				const restore = failingMulti(client.redis);
				try { await sessions[operation]('tok', { userId: 'target' }).catch(() => {}); }
				finally { restore(); }
				expect(breaker.failure, operation).toHaveBeenCalledTimes(1);
			}
			breaker.failure.mockClear();
			await sessions.set('bad', { userId: 'target' });
			await client.redis.set(client.key('sess:bad'), 'broken-json', 'PX', 60_000);
			await expect(sessions.purgeUser(null, 'target')).rejects.toMatchObject({ erasureIndexIncomplete: true });
			expect(breaker.failure).not.toHaveBeenCalled();
		});
	});
}
