import { describe, it, expect } from 'vitest';
import { createDistributedSession } from '../../src/redis/session.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { scanKeys } from '../../src/shared/redis-scan.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

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

// Route every node's SCAN through `handler(scan, args, first)`, where `scan`
// is that node's own SCAN and `first` marks the first call. Covers each
// master of a Cluster.
function patchScan(redis, handler) {
	const nodes = typeof redis.nodes === 'function' ? redis.nodes('master') : [redis];
	const originals = nodes.map((node) => node.scan);
	let calls = 0;
	nodes.forEach((node, i) => {
		node.scan = function (...args) {
			return handler((...a) => originals[i].apply(node, a), args, ++calls === 1);
		};
	});
	return () => nodes.forEach((node, i) => { node.scan = originals[i]; });
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

	const markerKey = () => getClient().key('sess:identity:v1:legacy');
	// For cases about something else than the switch window: record that the
	// keyspace's legacy sessions were found long enough ago to have expired.
	const coverageKey = () => getClient().key('sess:identity:v1:legacy-active');
	// For cases about something else than the switch window: record that the
	// keyspace's legacy sessions were found long enough ago to have expired,
	// and that no store without forgetIdentity has written one since.
	const switchedLongAgo = async () => {
		await getClient().redis.set(markerKey(), JSON.stringify({ since: 0, state: 'residue' }));
		await getClient().redis.del(coverageKey());
	};
	// Sessions a store without forgetIdentity wrote stay as if an earlier
	// release, which keeps no coverage, had written them.
	const writtenByEarlierRelease = () => getClient().redis.del(coverageKey());

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
			expect(await scanKeys(getClient().redis, getClient().key('sess:identity:v1:byuser:*'))).toEqual([]);
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
			await switchedLongAgo();
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

		it('adopts a session written before qualified mode where a legacy store would slide it', async () => {
			const legacy = createDistributedSession(getClient(), { refreshOnGet: false, ttlMs: 5_000 });
			const s = sessions({ refreshOnGet: true, identify: (ctx) => ctx.cookies.sid });
			const data = (userId) => ({ ...owner('ta', userId), theme: 'dark' });
			await legacy.set('touched', data('1'));
			await legacy.set('read', data('2'));
			const loaded = await legacy.create(data('3'));
			expect(await s.touch('touched')).toBe(true);
			expect(await s.get('read')).toEqual(data('2'));
			const userData = await s.withHooks().upgrade({ cookies: { sid: loaded }, headers: {}, url: '/ws' });
			expect(s.of({ getUserData: () => userData })).toEqual(data('3'));
			// Each record now has the qualified layout and the qualified store's
			// TTL, and its user's erasure reaches it.
			for (const token of ['touched', 'read', loaded]) {
				expect(JSON.parse(await record(token))).toHaveProperty(['identity:v1']);
				expect(await getClient().redis.pttl(getClient().key('sess:' + token))).toBeGreaterThan(10_000);
			}
			expect(await s.get('read')).toEqual(data('2'));
			await writtenByEarlierRelease();
			for (const userId of ['1', '2', '3']) expect(await s.purgeUser('ta', userId)).toBe(1);
			for (const token of ['touched', 'read', loaded]) expect(await record(token)).toBeNull();
		});

		it('leaves a legacy session whose identity cannot be extracted as it is', async () => {
			const legacy = createDistributedSession(getClient(), { refreshOnGet: false, ttlMs: 30_000 });
			const s = sessions({ refreshOnGet: true });
			await legacy.set('old', { owner: 'bare' });
			const before = await record('old');
			const expiries = recordExpiries(getClient().redis);
			try {
				expect(await s.touch('old')).toBe(false);
				expect(await s.get('old')).toEqual({ owner: 'bare' });
			} finally {
				expiries.restore();
			}
			expect(await record('old')).toBe(before);
			expect(expiries.keys).not.toContain(getClient().key('sess:old'));
		});

		it('opens no residue window for a keyspace that never held a legacy session', async () => {
			const s = sessions();
			await s.set('a', owner('ta', '42'));
			expect(await s.purgeUser('ta', '42')).toBe(1);
			expect(await s.purgeUser('ta', 'nobody')).toBe(0);
			expect(await createForgetStore({ sessions: s }).purgeUser('ta', 'nobody')).toEqual({ sessions: 0 });
			expect(JSON.parse(await getClient().redis.get(markerKey()))).toMatchObject({ state: 'clean' });
		});

		it('reports residue for ttlMs while a legacy session found at the switch may remain', async () => {
			const legacy = createDistributedSession(getClient(), { refreshOnGet: false, ttlMs: 200 });
			await legacy.set('old', owner('ta', '42'));
			await writtenByEarlierRelease();
			const s = sessions({ ttlMs: 400 });
			await s.set('a', owner('ta', '42'));
			await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true, removed: 1 });
			expect(await record('a')).toBeNull();
			expect(await record('old')).not.toBeNull();
			expect(JSON.parse(await getClient().redis.get(markerKey()))).toMatchObject({ state: 'residue' });
			await new Promise((resolve) => setTimeout(resolve, 450));
			await s.set('b', owner('ta', '43'));
			expect(await s.purgeUser('ta', '43')).toBe(1);
		});

		it('scans again after a period of legacy writes', async () => {
			const s = sessions();
			expect(await s.purgeUser('ta', '42')).toBe(0);
			expect(JSON.parse(await getClient().redis.get(markerKey()))).toMatchObject({ state: 'clean' });
			// forgetIdentity switched off: writes without an extractor.
			const legacy = createDistributedSession(getClient(), {});
			await legacy.set('old', owner('ta', '42'));
			// Switched on again, in a running and in a new instance.
			for (const store of [s, sessions()]) {
				await expect(store.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true });
			}
			expect(await record('old')).not.toBeNull();
		});

		it('reports residue after a legacy write that landed during the scan', async () => {
			const legacy = createDistributedSession(getClient(), {});
			const s = sessions();
			const restore = patchScan(getClient().redis, async (scan, args, first) => {
				const result = await scan(...args);
				if (first) await legacy.set('late', owner('ta', '42'));
				return result;
			});
			try {
				// The write was in flight while this purge ran: either answer.
				await s.purgeUser('ta', '42').catch(() => {});
			} finally {
				restore();
			}
			await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true });
			expect(await record('late')).not.toBeNull();
		});

		it('adopts a legacy session without bringing back one logged out meanwhile', async () => {
			const legacy = createDistributedSession(getClient(), {});
			await legacy.set('tok', owner('ta', '42'));
			const s = sessions();
			const restore = afterNthMulti(getClient().redis, 1, () => legacy.delete('tok'));
			try {
				expect(await s.touch('tok')).toBe(false);
			} finally {
				restore();
			}
			expect(await record('tok')).toBeNull();
		});

		it('adopts a legacy session without overwriting a replacement written meanwhile', async () => {
			const legacy = createDistributedSession(getClient(), {});
			await legacy.set('tok', owner('ta', '42'));
			const s = sessions();
			const restore = afterNthMulti(getClient().redis, 1, () => s.set('tok', owner('tb', '7')));
			try {
				expect(await s.touch('tok')).toBe(false);
			} finally {
				restore();
			}
			expect(await s.get('tok')).toEqual(owner('tb', '7'));
			await writtenByEarlierRelease();
			expect(await s.purgeUser('tb', '7')).toBe(1);
		});

		it('deletes a legacy session adopted while its identity was erased', async () => {
			const legacy = createDistributedSession(getClient(), {});
			await legacy.set('tok', owner('ta', '42'));
			const s = sessions();
			const eraser = sessions();
			// The erasure runs after adoption indexed the session and before it
			// rewrote the record: it sees a legacy record and reports residue.
			const restore = afterNthMulti(getClient().redis, 1, async () => {
				await expect(eraser.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true });
			});
			try {
				await expect(s.touch('tok')).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
			} finally {
				restore();
			}
			expect(await record('tok')).toBeNull();
		});

		it('reports residue while a store without forgetIdentity that already wrote keeps writing', async () => {
			const legacy = createDistributedSession(getClient(), {});
			await legacy.set('warm', owner('tb', '1'));
			await legacy.delete('warm');
			const s = sessions();
			await expect(s.purgeUser('ta', 'x')).rejects.toMatchObject({ legacyResidue: true });
			await legacy.set('old', owner('ta', '42'));
			await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true });
			expect(await record('old')).not.toBeNull();
		});

		it('reports residue when a store without forgetIdentity starts writing after a clean scan', async () => {
			const s = sessions();
			expect(await s.purgeUser('ta', 'x')).toBe(0);
			expect(JSON.parse(await getClient().redis.get(markerKey()))).toMatchObject({ state: 'clean' });
			await createDistributedSession(getClient(), {}).set('old', owner('ta', '42'));
			await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true });
		});

		it('ends the coverage after the sessions it covers', async () => {
			const legacy = createDistributedSession(getClient(), { ttlMs: 60 });
			await legacy.set('a', owner('ta', '42'));
			const s = sessions({ ttlMs: 60 });
			await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true });
			const left = await getClient().redis.pttl(coverageKey());
			expect(left).toBeGreaterThan(60);
			expect(left).toBeLessThanOrEqual(90);
			await new Promise((resolve) => setTimeout(resolve, 120));
			expect(await record('a')).toBeNull();
			expect(await s.purgeUser('ta', '42')).toBe(0);
			// A later write covers itself again.
			await legacy.set('b', owner('ta', '42'));
			await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true });
		});

		it('renews the coverage when a store without forgetIdentity slides a session', async () => {
			for (const slideIt of [(legacy) => legacy.touch('old'), (legacy) => legacy.get('old')]) {
				const legacy = createDistributedSession(getClient(), { refreshOnGet: true, ttlMs: 300 });
				await legacy.set('old', owner('ta', '42'));
				await getClient().redis.del(coverageKey());
				// Less than ttlMs of the coverage this store knows of is left.
				await new Promise((resolve) => setTimeout(resolve, 200));
				await slideIt(legacy);
				expect(await getClient().redis.pttl(coverageKey())).toBeGreaterThan(300);
			}
		});

		it('sends a legacy write only once its coverage holds', async () => {
			const legacy = createDistributedSession(getClient(), {});
			const redis = getClient().redis;
			const set = redis.set;
			const seen = [];
			redis.set = async function (key, ...args) {
				if (key === getClient().key('sess:old')) seen.push(await redis.exists(coverageKey()));
				return set.call(this, key, ...args);
			};
			try {
				await legacy.set('old', owner('ta', '42'));
			} finally {
				redis.set = set;
			}
			expect(seen).toEqual([1]);
		});

		it('renews the coverage once for many concurrent slides, with plain commands', async () => {
			const writer = createDistributedSession(getClient(), {});
			for (let i = 0; i < 80; i++) await writer.set('k' + i, { v: i });
			const legacy = createDistributedSession(getClient(), { refreshOnGet: true });
			const redis = getClient().redis;
			const counts = { eval: 0, batch: 0 };
			const wrapped = {};
			// A real client sends scripts, defined commands included, through
			// sendCommand; the mock runs them through eval.
			if (typeof redis.sendCommand === 'function') {
				wrapped.sendCommand = redis.sendCommand;
				redis.sendCommand = function (command, ...rest) {
					if (/^eval/i.test(String(command?.name))) counts.eval++;
					return wrapped.sendCommand.call(this, command, ...rest);
				};
			} else {
				wrapped.eval = redis.eval;
				redis.eval = function (...args) { counts.eval++; return wrapped.eval.apply(this, args); };
			}
			for (const method of ['pipeline', 'multi']) {
				wrapped[method] = redis[method];
				redis[method] = function (...args) { counts.batch++; return wrapped[method].apply(this, args); };
			}
			try {
				await Promise.all(Array.from({ length: 80 }, (_, i) => legacy.get('k' + i)));
				const renewals = counts.eval;
				// Within the coverage the store knows of, slides send no script.
				for (let i = 0; i < 10; i++) await legacy.touch('k' + i);
				await Promise.all(Array.from({ length: 80 }, (_, i) => legacy.touch('k' + i)));
				expect(counts.eval).toBe(renewals);
			} finally {
				for (const method of Object.keys(wrapped)) redis[method] = wrapped[method];
			}
			// One renewal; a real server may answer the first EVALSHA with
			// NOSCRIPT, and the client then retries it as EVAL.
			expect(counts.batch).toBe(0);
			expect(counts.eval).toBeGreaterThanOrEqual(1);
			expect(counts.eval).toBeLessThanOrEqual(2);
		});

		it('keeps the coverage through clear()', async () => {
			const legacy = createDistributedSession(getClient(), {});
			await legacy.set('old', owner('ta', '42'));
			await sessions().clear();
			expect(await getClient().redis.pttl(coverageKey())).toBeGreaterThan(0);
			await legacy.set('new', owner('ta', '42'));
			await expect(sessions().purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true });
		});

		it('gives the marker back when the scan fails, and leases it while it runs', async () => {
			const s = sessions();
			await s.set('a', owner('ta', '42'));
			let lease;
			const restore = patchScan(getClient().redis, async (scan, args, first) => {
				if (first) {
					lease = await getClient().redis.pttl(markerKey());
					throw new Error('scan timed out');
				}
				return scan(...args);
			});
			try {
				await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE', removed: 1 });
			} finally {
				restore();
			}
			expect(lease).toBeGreaterThan(0);
			expect(lease).toBeLessThanOrEqual(15_000);
			expect(await getClient().redis.get(markerKey())).toBeNull();
			await s.set('b', owner('ta', '43'));
			expect(await s.purgeUser('ta', '43')).toBe(1);
			expect(JSON.parse(await getClient().redis.get(markerKey()))).toMatchObject({ state: 'clean' });
		});

		it('reports residue when the scan runs past its time budget', async () => {
			const s = sessions();
			await s.set('a', owner('ta', '42'));
			let offset = 0;
			const base = performance.now();
			setRuntimeEnv({ clock: { monotonic: () => base + offset } });
			const restore = patchScan(getClient().redis, async (scan, args) => {
				offset += 6_000;
				return scan(...args);
			});
			try {
				await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true, removed: 1 });
			} finally {
				restore();
				resetRuntimeEnv();
			}
			expect(JSON.parse(await getClient().redis.get(markerKey()))).toMatchObject({ state: 'residue' });
		});

		it('reports residue when the Cluster topology changes during the scan', async () => {
			const redis = getClient().redis;
			if (typeof redis.nodes !== 'function') return;
			const s = sessions();
			await s.set('a', owner('ta', '42'));
			// The second reading of every master shows one more master.
			const masters = redis.nodes('master');
			const originals = masters.map((node) => node.cluster);
			let calls = 0;
			masters.forEach((node, i) => {
				node.cluster = async function (...args) {
					const reply = await originals[i].apply(node, args);
					return ++calls <= masters.length ? reply : String(reply) + 'deadbeef 127.0.0.1:1@2 master - 0 0 99 connected 0-1';
				};
			});
			try {
				await expect(s.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true });
			} finally {
				masters.forEach((node, i) => { node.cluster = originals[i]; });
			}
		});

		it('does not read a nested store\'s marker, index or sessions as legacy sessions', async () => {
			const nested = sessions({ keyPrefix: 'sess:admin:' });
			await nested.set('t', owner('ta', '7'));
			expect(await nested.purgeUser('ta', 'x')).toBe(0);
			const s = sessions();
			await s.set('t', owner('ta', '42'));
			expect(await s.purgeUser('ta', '42')).toBe(1);
			expect(JSON.parse(await getClient().redis.get(markerKey()))).toMatchObject({ state: 'clean' });
			expect(await nested.purgeUser('ta', '7')).toBe(1);
		});

		it('reserves the identity: token prefix with and without an extractor', async () => {
			for (const s of [sessions(), createDistributedSession(getClient(), {})]) {
				await expect(s.set('identity:v1:byuser:x', { v: 1 })).rejects.toThrow('reserved prefix identity:');
				await expect(s.get('identity:erasure')).rejects.toThrow('reserved prefix identity:');
			}
		});
	});
}
