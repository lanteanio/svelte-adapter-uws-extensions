import { describe, expect, it, vi } from 'vitest';
import { createIdempotencyStore } from '../../src/redis/idempotency.js';
import { evalCachedName } from '../../src/shared/eval-cached.js';
import { ACQUIRE_SCRIPT, COMMIT_SCRIPT, OWNERSHIP_SCRIPT, ERASE_SCRIPT } from '../../src/redis/_idempotency-ownership.js';

const alice = { tenant: 'tenant-a', user: 'alice' };
const settled = promise => promise.then(value => ({ value }), error => ({ error }));

export function pauseIdempotencyCommand(redis, script, when = () => true) {
	const name = evalCachedName(redis, script);
	const original = redis[name].bind(redis);
	let signal, release, used = false;
	const entered = new Promise(resolve => { signal = resolve; });
	const gate = new Promise(resolve => { release = resolve; });
	const spy = vi.spyOn(redis, name).mockImplementation(async (...args) => {
		if (!used && when(args)) { used = true; signal(); await gate; }
		return original(...args);
	});
	return { entered, release, restore: () => spy.mockRestore() };
}

export function idempotencyErasureContract(getClient) {
	describe('idempotency owner erasure', () => {
		it('erases pending work and fences its later commit', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const slot = await store.acquire('work', 60, alice);
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
			await expect(slot.commit({ private: 'result' })).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
			expect(await client.redis.get(client.key('idem:work'))).toBeNull();
		});
		it('keeps a committed result indexed when abort follows commit', async () => {
			const store = createIdempotencyStore(getClient());
			const slot = await store.acquire('work', 60, alice);
			await slot.commit({ private: 'result' });
			await slot.abort();
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
		});
		it('does not erase another owner that replaced the same primary key', async () => {
			const store = createIdempotencyStore(getClient());
			const old = await store.acquire('work', 60, alice);
			await store.purge('work');
			const next = await store.acquire('work', 60, { tenant: 'tenant-a', user: 'bob' });
			await next.commit('successor');
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(0);
			await expect(old.commit('stale')).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
			await old.abort();
			expect((await store.acquire('work')).result).toBe('successor');
			expect(await store.purgeUser('tenant-a', 'bob')).toBe(1);
		});
		it('separates equal user ids in different tenants', async () => {
			const store = createIdempotencyStore(getClient());
			await (await store.acquire('a', 60, alice)).commit('a');
			await (await store.acquire('b', 60, { ...alice, tenant: 'tenant-b' })).commit('b');
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
			expect((await store.acquire('b')).result).toBe('b');
		});
		for (const [name, script, when, removed] of [
			['primary acquire', ACQUIRE_SCRIPT, () => true, 0],
			['registration arm', OWNERSHIP_SCRIPT, args => args[2] === 'arm', 1]
		]) {
			it(`revokes a delayed ${name} before exposing an acquired handle`, async () => {
				const client = getClient(), store = createIdempotencyStore(client);
				const pause = pauseIdempotencyCommand(client.redis, script, when);
				const acquiring = settled(store.acquire('work', 60, alice));
				try {
					await pause.entered;
					expect(await store.purgeUser('tenant-a', 'alice')).toBe(removed);
				} finally { pause.release(); pause.restore(); }
				expect((await acquiring).error).toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
				expect(await client.redis.get(client.key('idem:work'))).toBeNull();
			});
		}
		it('fences a commit command delayed until after completed erasure', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const slot = await store.acquire('work', 60, alice);
			const pause = pauseIdempotencyCommand(client.redis, COMMIT_SCRIPT);
			const committing = settled(slot.commit({ private: 'result' }));
			try {
				await pause.entered;
				expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
				expect(await client.redis.get(client.key('idem:work'))).toBeNull();
			} finally { pause.release(); pause.restore(); }
			expect((await committing).error).toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
			expect(await client.redis.get(client.key('idem:work'))).toBeNull();
		});
		it('fails a registration before creating an unindexed primary', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const command = evalCachedName(client.redis, OWNERSHIP_SCRIPT);
			const spy = vi.spyOn(client.redis, command).mockRejectedValueOnce(new Error('index unavailable'));
			try { await expect(store.acquire('work', 60, alice)).rejects.toThrow('index unavailable'); }
			finally { spy.mockRestore(); }
			expect(await client.redis.get(client.key('idem:work'))).toBeNull();
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(0);
		});
		for (const [op, afterWrite] of [['register', true], ['arm', false], ['arm', true]]) {
			it(`keeps failed ${op} acknowledgement erasable, afterWrite=${afterWrite}`, async () => {
				const client = getClient(), store = createIdempotencyStore(client);
				const command = evalCachedName(client.redis, OWNERSHIP_SCRIPT);
				const original = client.redis[command].bind(client.redis);
				const spy = vi.spyOn(client.redis, command).mockImplementation(async (...args) => {
					if (args[2] !== op) return original(...args);
					if (afterWrite) await original(...args);
					throw new Error('index acknowledgement lost');
				});
				try { await expect(store.acquire('work', 60, alice)).rejects.toThrow('index acknowledgement lost'); }
				finally { spy.mockRestore(); }
				expect(await store.purgeUser('tenant-a', 'alice')).toBe(op === 'arm' ? 1 : 0);
				expect(await client.redis.get(client.key('idem:work'))).toBeNull();
				expect(await client.redis.hkeys(client.key('idem:byuser:v2\0tenant-a\0alice'))).toEqual([]);
			});
		}
		it('rejects incomplete index removal and permits a later retry', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const slot = await store.acquire('work', 60, alice);
			const command = evalCachedName(client.redis, OWNERSHIP_SCRIPT);
			const original = client.redis[command].bind(client.redis);
			const spy = vi.spyOn(client.redis, command).mockImplementation(async (...args) => {
				if (args[2] === 'drop') throw new Error('index removal unavailable');
				return original(...args);
			});
			try { await expect(store.purgeUser('tenant-a', 'alice')).rejects.toThrow('index removal unavailable'); }
			finally { spy.mockRestore(); }
			await expect(slot.commit('late')).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(0);
			expect(await client.redis.hkeys(client.key('idem:byuser:v2\0tenant-a\0alice'))).toEqual([]);
		});
		for (const failureStage of ['erase', 'drop']) {
			it(`bounds parallel erasure and waits for admitted owners after ${failureStage} failure`, async () => {
				const client = getClient(), store = createIdempotencyStore(client);
				const keys = Array.from({ length: 12 }, (_, i) => `bounded-${i}`);
				for (const key of keys) await (await store.acquire(key, 60, alice)).commit(key);
				const redis = client.redis, eraseName = evalCachedName(redis, ERASE_SCRIPT), ownershipName = evalCachedName(redis, OWNERSHIP_SCRIPT);
				const erase = redis[eraseName].bind(redis), ownership = redis[ownershipName].bind(redis);
				const gates = [], active = new Set(), acknowledged = new Set(), dropped = new Set();
				const failure = new Error(`bounded ${failureStage} failure`);
				let maximum = 0, failedOwner, finished = false, outcome;
				const eraseSpy = vi.spyOn(redis, eraseName).mockImplementation(async (...args) => {
					const owner = args[2];
					active.add(owner); maximum = Math.max(maximum, active.size);
					try {
						await new Promise((resolve, reject) => { gates.push({ owner, key: args[1], resolve, reject }); });
						const count = await erase(...args);
						acknowledged.add(owner);
						return count;
					} catch (error) { active.delete(owner); throw error; }
				});
				const ownershipSpy = vi.spyOn(redis, ownershipName).mockImplementation(async (...args) => {
					if (args[2] !== 'drop') return ownership(...args);
					const owner = args[3];
					try {
						expect(acknowledged.has(owner)).toBe(true);
						if (failureStage === 'drop' && owner === failedOwner) throw failure;
						const result = await ownership(...args);
						dropped.add(owner);
						return result;
					} finally { active.delete(owner); }
				});
				const purging = settled(store.purgeUser('tenant-a', 'alice')).then(result => { finished = true; return result; });
				try {
					await vi.waitFor(() => expect(gates).toHaveLength(8), { timeout: 5000 });
					failedOwner = gates[0].owner;
					if (failureStage === 'erase') gates[0].reject(failure); else gates[0].resolve();
					await vi.waitFor(() => expect(active.size).toBe(7), { timeout: 5000 });
					expect(finished).toBe(false);
					expect(gates).toHaveLength(8);
					for (const gate of gates.slice(1)) gate.resolve();
					outcome = await purging;
				} finally {
					eraseSpy.mockRestore(); ownershipSpy.mockRestore();
					for (const gate of gates) gate.resolve();
					await purging;
				}
				expect(outcome.error).toBe(failure);
				expect(gates).toHaveLength(8);
				expect(maximum).toBe(8);
				expect(active.size).toBe(0);
				expect(dropped.size).toBe(7);
				const index = client.key('idem:byuser:v2\0tenant-a\0alice');
				const remaining = (await redis.hkeys(index)).filter(key => !key.startsWith('@'));
				expect(remaining).toHaveLength(5);
				expect(remaining).toContain(failedOwner);
				const successorKey = keys.find(key => client.key('idem:' + key) === gates[0].key);
				await store.purge(successorKey);
				await (await store.acquire(successorKey, 60, { tenant: 'tenant-a', user: 'bob' })).commit('successor');
				expect(await store.purgeUser('tenant-a', 'alice')).toBe(4);
				expect(await redis.exists(index)).toBe(0);
				expect((await store.acquire(successorKey)).result).toBe('successor');
				expect(await store.purgeUser('tenant-a', 'bob')).toBe(1);
			});
		}
		it('removes metadata left after a partially failed owner-record drop', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const slot = await store.acquire('work', 60, alice);
			const index = client.key('idem:byuser:v2\0tenant-a\0alice');
			const command = evalCachedName(client.redis, OWNERSHIP_SCRIPT);
			const original = client.redis[command].bind(client.redis);
			const spy = vi.spyOn(client.redis, command).mockImplementation(async (...args) => {
				if (args[2] !== 'drop') return original(...args);
				// Simulate Lua HDEL succeeding before its following HSET fails.
				await client.redis.hdel(index, args[3]);
				throw new Error('count write unavailable');
			});
			try { await expect(store.purgeUser('tenant-a', 'alice')).rejects.toThrow('count write unavailable'); }
			finally { spy.mockRestore(); }
			expect(await client.redis.hkeys(index)).toHaveLength(2);
			await (await store.acquire('work', 60, { tenant: 'tenant-a', user: 'bob' })).commit('successor');
			await expect(slot.commit('late')).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(0);
			expect(await client.redis.exists(index)).toBe(0);
			expect((await store.acquire('work')).result).toBe('successor');
			expect(await store.purgeUser('tenant-a', 'bob')).toBe(1);
		});
		it('fails closed on missing ownership metadata before losing an existing claim', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			await store.acquire('work', 60, alice);
			const index = client.key('idem:byuser:v2\0tenant-a\0alice');
			const expires = await client.redis.hget(index, '@expires');
			await client.redis.hdel(index, '@expires');
			await expect(store.acquire('other', 60, alice)).rejects.toThrow('invalid idempotency ownership metadata');
			await expect(store.purgeUser('tenant-a', 'alice')).rejects.toThrow('invalid idempotency ownership metadata');
			expect(await client.redis.get(client.key('idem:work'))).toMatch(/^__idem_pending__:/);
			expect(await client.redis.get(client.key('idem:other'))).toBeNull();
			await client.redis.hset(index, '@expires', expires);
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
		});
		it('rejects a corrupt maximum registration count before incrementing it', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const index = client.key('idem:byuser:v2\0tenant-a\0alice');
			await client.redis.hset(index, '@unarmed', String(Number.MAX_SAFE_INTEGER), '@expires', '0');
			await expect(store.acquire('work', 60, alice)).rejects.toThrow('invalid idempotency ownership count');
			expect(await client.redis.hget(index, '@unarmed')).toBe(String(Number.MAX_SAFE_INTEGER));
			expect(await client.redis.get(client.key('idem:work'))).toBeNull();
		});
		it('retains a failed deletion for a later erasure retry', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const slot = await store.acquire('work', 60, alice);
			const command = evalCachedName(client.redis, ERASE_SCRIPT);
			const spy = vi.spyOn(client.redis, command).mockRejectedValueOnce(new Error('primary unavailable'));
			try { await expect(store.purgeUser('tenant-a', 'alice')).rejects.toThrow('primary unavailable'); }
			finally { spy.mockRestore(); }
			await slot.commit({ private: 'result' });
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
			expect(await client.redis.get(client.key('idem:work'))).toBeNull();
		});
		for (const afterWrite of [false, true]) {
			it(`propagates commit failure and keeps erasure possible, afterWrite=${afterWrite}`, async () => {
				const client = getClient(), store = createIdempotencyStore(client);
				const slot = await store.acquire('work', 60, alice);
				const command = evalCachedName(client.redis, COMMIT_SCRIPT);
				const original = client.redis[command].bind(client.redis);
				const spy = vi.spyOn(client.redis, command).mockImplementationOnce(async (...args) => {
					if (afterWrite) await original(...args);
					throw new Error('commit unavailable');
				});
				try { await expect(slot.commit({ private: 'result' })).rejects.toThrow('commit unavailable'); }
				finally { spy.mockRestore(); }
				expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
				expect(await client.redis.get(client.key('idem:work'))).toBeNull();
			});
		}
		it('reads and erases indexed legacy JSON without deleting a v2 successor', async () => {
			const client = getClient(), store = createIdempotencyStore(client);
			const legacyIndex = client.key('idem:byuser:tenant-a\0alice');
			await client.redis.set(client.key('idem:legacy'), JSON.stringify({ legacy: true }), 'EX', 60);
			await client.redis.hset(legacyIndex, client.key('idem:legacy'), '1');
			expect((await store.acquire('legacy', 60, alice)).result).toEqual({ legacy: true });
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
			await (await store.acquire('legacy', 60, { tenant: 'tenant-a', user: 'bob' })).commit('successor');
			await client.redis.hset(legacyIndex, client.key('idem:legacy'), '1');
			expect(await store.purgeUser('tenant-a', 'alice')).toBe(0);
			expect((await store.acquire('legacy')).result).toBe('successor');
			expect(await store.purgeUser('tenant-a', 'bob')).toBe(1);
		});
	});
}
