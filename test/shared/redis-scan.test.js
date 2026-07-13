import { describe, it, expect } from 'vitest';
import { scanAndUnlink, scanUnlinkExcept } from '../../src/shared/redis-scan.js';
import { mockRedisClient } from '../helpers/mock-redis.js';

describe('scanAndUnlink', () => {
	describe('standalone path', () => {
		it('removes every key matching the pattern via batched UNLINK', async () => {
			const client = mockRedisClient();
			const r = client.redis;

			await r.set('foo:1', 'a');
			await r.set('foo:2', 'b');
			await r.set('foo:3', 'c');
			await r.set('bar:1', 'd');

			let unlinkCalls = 0;
			const origUnlink = r.unlink.bind(r);
			r.unlink = async (...keys) => { unlinkCalls++; return origUnlink(...keys); };

			await scanAndUnlink(r, 'foo:*');

			expect(await r.get('foo:1')).toBeNull();
			expect(await r.get('foo:2')).toBeNull();
			expect(await r.get('foo:3')).toBeNull();
			// Non-matching key untouched.
			expect(await r.get('bar:1')).toBe('d');
			// Standalone batches in one UNLINK call.
			expect(unlinkCalls).toBe(1);
		});

		it('no-ops when there are no matching keys', async () => {
			const client = mockRedisClient();
			const r = client.redis;

			let unlinkCalls = 0;
			const origUnlink = r.unlink.bind(r);
			r.unlink = async (...keys) => { unlinkCalls++; return origUnlink(...keys); };

			await scanAndUnlink(r, 'nothing:*');

			expect(unlinkCalls).toBe(0);
		});
	});

	describe('Cluster path (mocked)', () => {
		// A Cluster mock that exposes the canonical `nodes('master')` API
		// surface. Three independent shards, each with its own keyspace.
		// scanAndUnlink should iterate every master and use per-key UNLINK
		// to avoid CROSSSLOT.
		function clusterMock() {
			function node(initial) {
				const store = new Map(Object.entries(initial));
				const calls = [];
				return {
					async scan(cursor, ...args) {
						const matchIdx = args.indexOf('MATCH');
						const pattern = matchIdx !== -1 ? args[matchIdx + 1] : '*';
						const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');
						return ['0', [...store.keys()].filter((k) => regex.test(k))];
					},
					async unlink(...keys) {
						calls.push(['unlink', keys.length]);
						if (keys.length > 1) {
							throw new Error("CROSSSLOT Keys in request don't hash to the same slot");
						}
						let deleted = 0;
						for (const k of keys) {
							if (store.delete(k)) deleted++;
						}
						return deleted;
					},
					_store: store,
					_calls: calls
				};
			}

			const masters = [
				node({ 'foo:1': 'a', 'bar:1': 'd' }),
				node({ 'foo:2': 'b', 'baz:1': 'e' }),
				node({ 'foo:3': 'c' })
			];

			return {
				nodes(role) {
					if (role !== 'master') throw new Error('test mock only handles master');
					return masters;
				},
				_masters: masters
			};
		}

		it('iterates every master node and removes all matching keys across the cluster', async () => {
			const cluster = clusterMock();

			await scanAndUnlink(cluster, 'foo:*');

			// All foo:* keys gone from every shard.
			expect(cluster._masters[0]._store.has('foo:1')).toBe(false);
			expect(cluster._masters[1]._store.has('foo:2')).toBe(false);
			expect(cluster._masters[2]._store.has('foo:3')).toBe(false);

			// Non-matching keys preserved.
			expect(cluster._masters[0]._store.get('bar:1')).toBe('d');
			expect(cluster._masters[1]._store.get('baz:1')).toBe('e');
		});

		it('uses per-key UNLINK on Cluster (never throws CROSSSLOT even when scan returns multiple keys)', async () => {
			const cluster = clusterMock();

			await scanAndUnlink(cluster, 'foo:*');

			// Every unlink call took exactly one key. If the fix regressed
			// to batched UNLINK, the Cluster mock above would throw CROSSSLOT.
			for (const master of cluster._masters) {
				for (const call of master._calls) {
					expect(call).toEqual(['unlink', 1]);
				}
			}
		});

		it('skips masters that have no matching keys without erroring', async () => {
			const cluster = clusterMock();

			await scanAndUnlink(cluster, 'nothing:*');

			// No unlink calls fired anywhere.
			for (const master of cluster._masters) {
				expect(master._calls).toHaveLength(0);
			}
		});
	});
});

describe('scanUnlinkExcept', () => {
	const keepEpoch = (k) => k.startsWith('replay:epoch:{');
	const bumpEpoch = (node, k) => node.incr(k);

	describe('standalone path', () => {
		it('INCRs kept keys, unlinks the rest, and leaves out-of-pattern keys alone', async () => {
			const client = mockRedisClient();
			const r = client.redis;

			await r.set('replay:epoch:{a}', '1');
			await r.set('replay:epoch:{b}', '5');
			await r.set('replay:seq:{a}', '10');
			await r.set('replay:buf:{a}', 'x');
			await r.set('other:1', 'keep'); // outside the scan pattern

			await scanUnlinkExcept(r, 'replay:*', keepEpoch, bumpEpoch);

			// Kept keys are rotated (INCR) and preserved, not deleted.
			expect(await r.get('replay:epoch:{a}')).toBe('2');
			expect(await r.get('replay:epoch:{b}')).toBe('6');
			// Everything else matching the pattern is unlinked.
			expect(await r.get('replay:seq:{a}')).toBeNull();
			expect(await r.get('replay:buf:{a}')).toBeNull();
			// A key outside the pattern is never touched.
			expect(await r.get('other:1')).toBe('keep');
		});

		it('runs every kept-key action before any unlink (two-pass ordering)', async () => {
			const client = mockRedisClient();
			const r = client.redis;
			await r.set('replay:epoch:{a}', '1');
			await r.set('replay:seq:{a}', '10');
			await r.set('replay:buf:{a}', 'x');

			const order = [];
			const origIncr = r.incr.bind(r);
			r.incr = async (k) => { order.push('incr'); return origIncr(k); };
			const origUnlink = r.unlink.bind(r);
			r.unlink = async (...ks) => { order.push('unlink'); return origUnlink(...ks); };

			await scanUnlinkExcept(r, 'replay:*', keepEpoch, bumpEpoch);

			// A resume landing between the two passes must never see seq/buf gone
			// while the epoch still reads the pre-reset value, so every INCR must
			// precede every UNLINK.
			expect(order.lastIndexOf('incr')).toBeLessThan(order.indexOf('unlink'));
		});

		it('no-ops when nothing matches', async () => {
			const client = mockRedisClient();
			const r = client.redis;
			let incrs = 0, unlinks = 0;
			const origIncr = r.incr.bind(r); r.incr = async (k) => { incrs++; return origIncr(k); };
			const origUnlink = r.unlink.bind(r); r.unlink = async (...ks) => { unlinks++; return origUnlink(...ks); };

			await scanUnlinkExcept(r, 'nothing:*', keepEpoch, bumpEpoch);

			expect(incrs).toBe(0);
			expect(unlinks).toBe(0);
		});
	});

	describe('Cluster path (mocked)', () => {
		// Same shape as the scanAndUnlink cluster mock, plus INCR so a kept key
		// can be rotated on the master that owns it.
		function clusterMock() {
			function node(initial) {
				const store = new Map(Object.entries(initial));
				const calls = [];
				return {
					async scan(cursor, ...args) {
						const matchIdx = args.indexOf('MATCH');
						const pattern = matchIdx !== -1 ? args[matchIdx + 1] : '*';
						const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');
						return ['0', [...store.keys()].filter((k) => regex.test(k))];
					},
					async incr(k) {
						calls.push(['incr', k]);
						const next = (parseInt(store.get(k) || '0', 10) || 0) + 1;
						store.set(k, String(next));
						return next;
					},
					async unlink(...keys) {
						calls.push(['unlink', keys.length]);
						if (keys.length > 1) {
							throw new Error("CROSSSLOT Keys in request don't hash to the same slot");
						}
						let deleted = 0;
						for (const k of keys) if (store.delete(k)) deleted++;
						return deleted;
					},
					_store: store,
					_calls: calls
				};
			}

			const masters = [
				node({ 'replay:epoch:{a}': '1', 'replay:seq:{a}': '9' }),
				node({ 'replay:epoch:{b}': '4', 'replay:buf:{b}': 'y', 'other:1': 'keep' }),
				node({ 'replay:seq:{c}': '3' })
			];

			return {
				nodes(role) {
					if (role !== 'master') throw new Error('test mock only handles master');
					return masters;
				},
				_masters: masters
			};
		}

		it('rotates each epoch on its owning node and per-key unlinks the rest', async () => {
			const cluster = clusterMock();

			await scanUnlinkExcept(cluster, 'replay:*', keepEpoch, bumpEpoch);

			// Epoch keys rotated and preserved on whichever shard owns them.
			expect(cluster._masters[0]._store.get('replay:epoch:{a}')).toBe('2');
			expect(cluster._masters[1]._store.get('replay:epoch:{b}')).toBe('5');
			// Non-epoch replay keys unlinked across shards.
			expect(cluster._masters[0]._store.has('replay:seq:{a}')).toBe(false);
			expect(cluster._masters[1]._store.has('replay:buf:{b}')).toBe(false);
			expect(cluster._masters[2]._store.has('replay:seq:{c}')).toBe(false);
			// Out-of-pattern key untouched.
			expect(cluster._masters[1]._store.get('other:1')).toBe('keep');
			// Every INCR ran on the node that owns the key (recorded in its own calls).
			expect(cluster._masters[0]._calls).toContainEqual(['incr', 'replay:epoch:{a}']);
			expect(cluster._masters[1]._calls).toContainEqual(['incr', 'replay:epoch:{b}']);
		});

		it('never batches UNLINK on Cluster (would throw CROSSSLOT)', async () => {
			const cluster = clusterMock();

			await scanUnlinkExcept(cluster, 'replay:*', keepEpoch, bumpEpoch);

			for (const master of cluster._masters) {
				for (const call of master._calls) {
					if (call[0] === 'unlink') expect(call).toEqual(['unlink', 1]);
				}
			}
		});
	});
});
