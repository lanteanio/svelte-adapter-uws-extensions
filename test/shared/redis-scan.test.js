import { describe, it, expect } from 'vitest';
import { scanAndUnlink } from '../../shared/redis-scan.js';
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
