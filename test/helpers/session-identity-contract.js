import { describe, it, expect } from 'vitest';
import { createDistributedSession } from '../../src/redis/session.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { scanKeys } from '../../src/shared/redis-scan.js';

const owner = (tenantId, userId) => ({ owner: { tenantId, userId } });
const options = { forgetIdentity: (data) => data?.owner, refreshOnGet: false, ttlMs: 30_000 };

// Run `action` once, right after the `n`th MULTI created from now on has
// executed. Transactions the action itself creates are not counted.
function afterNthMulti(redis, n, action) {
	const original = redis.multi;
	let created = 0;
	let fired = false;
	redis.multi = function (...args) {
		const tx = original.apply(this, args);
		if (fired || ++created !== n) return tx;
		const proxy = new Proxy(tx, {
			get(target, prop) {
				const value = Reflect.get(target, prop);
				if (typeof value !== 'function') return value;
				if (prop !== 'exec') {
					return (...callArgs) => {
						const result = value.apply(target, callArgs);
						return result === target ? proxy : result;
					};
				}
				return async (...execArgs) => {
					const result = await value.apply(target, execArgs);
					fired = true;
					await action();
					return result;
				};
			}
		});
		return proxy;
	};
	return () => { redis.multi = original; };
}

// Run `action` once, just before the first HDEL is sent.
function beforeFirstHdel(redis, action) {
	const original = redis.hdel;
	let fired = false;
	redis.hdel = async function (...args) {
		if (!fired) {
			fired = true;
			await action();
		}
		return original.apply(this, args);
	};
	return () => { redis.hdel = original; };
}

// Every PEXPIRE key, the command that slides a session record.
function recordExpiries(redis) {
	const original = redis.pexpire;
	const keys = [];
	redis.pexpire = function (key, ...rest) {
		keys.push(key);
		return original.call(this, key, ...rest);
	};
	return { keys, restore: () => { redis.pexpire = original; } };
}

/**
 * Tenant-qualified session erasure, run against the in-memory mock and a real
 * server. `getClient` returns a client whose keyspace is empty per test.
 * @param {() => any} getClient
 */
export function sessionIdentityContract(getClient) {
	const sessions = (extra = {}) => createDistributedSession(getClient(), { ...options, ...extra });
	const record = (token) => getClient().redis.get(getClient().key('sess:' + token));

	describe('session tenant-qualified erasure', () => {
		it('refuses forgetUserId together with forgetIdentity and a non-function extractor', () => {
			expect(() => sessions({ forgetUserId: () => 'u' })).toThrow('mutually exclusive');
			expect(() => createDistributedSession(getClient(), { forgetIdentity: 'owner' })).toThrow('forgetIdentity must be a function');
		});

		it('erases one identity and keeps equal user ids in other tenants and the null tenant', async () => {
			const s = sessions();
			await s.set('a', owner('ta', '42'));
			await s.set('b', owner('tb', '42'));
			await s.set('n', owner(null, '42'));
			await s.set('o', owner('ta', '43'));
			expect(await s.purgeUser('ta', '42')).toBe(1);
			expect(await s.get('a')).toBeNull();
			for (const token of ['b', 'n', 'o']) expect(await s.get(token), token).not.toBeNull();
			// A purge with no target record removes nothing else.
			expect(await s.purgeUser('ta', '42')).toBe(0);
			expect(await s.purgeUser('tc', '42')).toBe(0);
			for (const token of ['b', 'n', 'o']) expect(await s.get(token), token).not.toBeNull();
			expect(await s.purgeUser(null, '42')).toBe(1);
			expect(await s.get('n')).toBeNull();
			for (const token of ['b', 'o']) expect(await s.get(token), token).not.toBeNull();
		});

		it('keeps delimiter-like and Unicode user ids apart', async () => {
			const s = sessions();
			const users = ['x:y', 'x', '["ta","x"]', 'Zo' + String.fromCharCode(0xeb), 'zo' + String.fromCharCode(0xeb), 'u' + String.fromCodePoint(0x1f600)];
			for (let i = 0; i < users.length; i++) await s.set('t' + i, owner('ta', users[i]));
			for (let i = 0; i < users.length; i++) {
				expect(await s.purgeUser('ta', users[i]), users[i]).toBe(1);
				for (let j = 0; j < users.length; j++) {
					expect(await s.get('t' + j) === null, users[j]).toBe(j <= i);
				}
			}
		});

		it('stores nothing when the extractor throws or returns a malformed identity', async () => {
			const bad = [
				() => { throw new Error('no owner'); },
				() => ({ userId: 'u' }),
				() => ({ tenantId: 'a.b', userId: 'u' }),
				() => ({ tenantId: 't', userId: '' }),
				() => 'u'
			];
			for (const forgetIdentity of bad) {
				const s = sessions({ forgetIdentity });
				await expect(s.set('t', { v: 1 })).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
				await expect(s.create({ v: 1 })).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
				expect(await record('t')).toBeNull();
			}
			expect(await scanKeys(getClient().redis, getClient().key('sess:*'))).toEqual([]);
		});

		it('stores an explicitly unattributed session without an index entry', async () => {
			const s = sessions();
			await s.set('t', { owner: null, v: 1 });
			expect(await s.get('t')).toEqual({ owner: null, v: 1 });
			expect(await scanKeys(getClient().redis, getClient().key('sess:identity:*'))).toEqual([]);
		});

		it('erases lifecycle sessions by the identity in their data', async () => {
			const s = sessions();
			const token = await s.create(owner('ta', '42'));
			const kept = await s.create(owner('tb', '42'));
			expect(await s.purgeUser('ta', '42')).toBe(1);
			expect(await record(token)).toBeNull();
			expect(await record(kept)).not.toBeNull();
		});

		it('moves a replaced token to the new identity', async () => {
			const s = sessions();
			await s.set('t', owner('ta', '42'));
			await s.set('t', owner('tb', '7'));
			const previous = getClient().key('sess:identity:v1:byuser:' + Buffer.from('["ta","42"]').toString('base64url'));
			expect(await getClient().redis.hexists(previous, 't')).toBe(0);
			expect(await s.purgeUser('ta', '42')).toBe(0);
			expect(await s.get('t')).toEqual(owner('tb', '7'));
			expect(await s.purgeUser('tb', '7')).toBe(1);
			expect(await record('t')).toBeNull();
		});

		it('keeps the index field of a previous owner who wrote again during the replacement', async () => {
			const s = sessions();
			await s.set('t', owner('ta', '42'));
			// The replacement's release of the old field runs after the old owner
			// has written the token again.
			const restore = beforeFirstHdel(getClient().redis, () => s.set('t', owner('ta', '42')));
			try {
				await s.set('t', owner('tb', '7'));
			} finally {
				restore();
			}
			expect(await s.get('t')).toEqual(owner('ta', '42'));
			expect(await s.purgeUser('ta', '42')).toBe(1);
			expect(await record('t')).toBeNull();
		});

		it('deletes a record whose write raced an erasure of its identity', async () => {
			const s = sessions();
			const eraser = sessions();
			// The erasure runs after the index field is written and before the
			// record is: it finds no record and drops the field.
			const restore = afterNthMulti(getClient().redis, 1, async () => {
				expect(await eraser.purgeUser('ta', '42')).toBe(0);
			});
			try {
				await expect(s.set('t', owner('ta', '42'))).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
			} finally {
				restore();
			}
			expect(await record('t')).toBeNull();
			// Erasure is not a ban: a later write is stored and erasable.
			await s.set('t', owner('ta', '42'));
			expect(await s.get('t')).toEqual(owner('ta', '42'));
			expect(await s.purgeUser('ta', '42')).toBe(1);
		});

		it('validates the purge identity the same way directly and through createForgetStore', async () => {
			const s = sessions();
			const store = createForgetStore({ sessions: s });
			await s.set('a', owner('ta', '42'));
			await s.set('b', owner('tb', '42'));
			await expect(s.purgeUser('a.b', '42')).rejects.toThrow(TypeError);
			await expect(store.purgeUser('a.b', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE' });
			await expect(s.purgeUser(undefined, '42')).rejects.toThrow(/tenantId/);
			await expect(store.purgeUser(undefined, '42')).rejects.toThrow(/tenant id is required/);
			expect(await s.get('a')).not.toBeNull();
			expect(await store.purgeUser('ta', '42')).toEqual({ sessions: 1 });
			expect(await s.purgeUser('tb', '42')).toBe(1);
		});

		it('reports a session written before qualified mode as incomplete and keeps it', async () => {
			const legacy = createDistributedSession(getClient(), { forgetUserId: (data) => data?.legacyUser, refreshOnGet: false, ttlMs: 30_000 });
			const s = sessions();
			await legacy.set('old', { legacyUser: '42' });
			await s.set('new', owner('ta', '42'));
			await s.set('other', owner('ta', '43'));
			// The composed store keeps the committed count.
			await expect(createForgetStore({ sessions: s }).purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', partialCounts: { sessions: 1 } });
			expect(await record('new')).toBeNull();
			expect(await record('old')).not.toBeNull();
			await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true, erasureIncomplete: true, removed: 0 });
			expect(await record('old')).not.toBeNull();
			expect(await s.purgeUser('ta', '43')).toBe(1);
			// Once the token is rewritten in the qualified layout it is no residue.
			await s.set('old', owner('tb', '42'));
			expect(await s.purgeUser('ta', '42')).toBe(0);
			expect(await s.get('old')).toEqual(owner('tb', '42'));
		});

		it('reads a session written before qualified mode without extending it', async () => {
			const legacy = createDistributedSession(getClient(), { forgetUserId: (data) => data?.legacyUser, refreshOnGet: false, ttlMs: 30_000 });
			const s = sessions({ refreshOnGet: true });
			await legacy.set('old', { legacyUser: '42' });
			await s.set('new', owner('ta', '42'));
			const expiries = recordExpiries(getClient().redis);
			try {
				expect(await s.get('old')).toEqual({ legacyUser: '42' });
				expect(await s.touch('old')).toBe(false);
				expect(await s.get('new')).toEqual(owner('ta', '42'));
			} finally {
				expiries.restore();
			}
			expect(expiries.keys).not.toContain(getClient().key('sess:old'));
			expect(expiries.keys).toContain(getClient().key('sess:new'));
		});

		it('reserves the identity: token prefix with and without an extractor', async () => {
			for (const s of [sessions(), createDistributedSession(getClient(), {})]) {
				await expect(s.set('identity:v1:byuser:x', { v: 1 })).rejects.toThrow('reserved prefix identity:');
				await expect(s.get('identity:erasure')).rejects.toThrow('reserved prefix identity:');
			}
		});
	});
}
