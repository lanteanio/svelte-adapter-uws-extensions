import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { countRedisIO } from '../helpers/redis-io-counter.js';
import { createReplay } from '../../src/redis/replay.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { setOperationalEventSink } from '../../src/shared/diagnostic.js';
import { EXTENSIONS_ERROR_IDS, extensionsErrorDefinition } from '../../src/error-registry.js';
import { createMetrics } from '../../src/prometheus/index.js';

const FIRST_REPORTS = Symbol.for('svelte-adapter-uws-extensions.first-reports.v1');

const BACKENDS = [
	{ name: 'sorted-set', options: {}, buf: 'buf' },
	{ name: 'stream', options: { storage: 'stream' }, buf: 'streambuf' }
];

let clock;
beforeEach(() => {
	clock = 1_700_000_000_000;
	setRuntimeEnv({ clock: { now: () => clock, wallEpoch: () => clock } });
});
afterEach(() => {
	resetRuntimeEnv();
});

for (const backend of BACKENDS) {
	describe(`redis replay maxTopics and generations (${backend.name})`, () => {
		let client;
		let platform;

		beforeEach(() => {
			client = mockRedisClient('t:');
			platform = mockPlatform();
		});

		const create = (options = {}) => createReplay(client, { ...backend.options, ...options });
		const key = (kind, topic) => client.key('replay:' + kind + ':{' + topic + '}');
		const registryKey = () => client.key('replay:topics:{replay-topics}');
		async function publishAt(replay, ms, topic, n = 1) {
			clock = ms;
			for (let i = 0; i < n; i++) await replay.publish(platform, topic, 'msg', { i });
		}

		it('validates maxTopics like the in-memory plugin', () => {
			for (const maxTopics of [0, -1, 1.5, '5', NaN]) {
				expect(() => create({ maxTopics })).toThrow('maxTopics must be a positive integer');
			}
			expect(() => create({ maxTopics: 1 })).not.toThrow();
		});

		it('defaults to 100 topics and evicts the least recently published past it', async () => {
			const replay = create();
			for (let i = 0; i <= 100; i++) await publishAt(replay, clock + 1000, 'room-' + i);
			expect(await replay.seq('room-0')).toBe(0);
			expect(await replay.since('room-0', 0)).toEqual([]);
			expect(await replay.currentEpoch('room-0')).toBe(0);
			for (const i of [1, 50, 100]) expect(await replay.seq('room-' + i)).toBe(1);
			expect(await client.redis.zcard(registryKey())).toBe(100);
		});

		it('evicts by last publish, not by creation order', async () => {
			const replay = create({ maxTopics: 2 });
			await publishAt(replay, 1000, 'a');
			await publishAt(replay, 2000, 'b');
			// A steady publish touches only a's own slot; eviction still sees it.
			await publishAt(replay, 3000, 'a');
			await publishAt(replay, 4000, 'c');
			expect(await replay.seq('a')).toBe(2);
			expect(await replay.seq('b')).toBe(0);
			expect(await replay.seq('c')).toBe(1);
			expect(await client.redis.zcard(registryKey())).toBe(2);
		});

		it('evicts when the created topic ties the oldest score and sorts first', async () => {
			// Same clock second, and 'other' < 'room': a candidate list that
			// counts the creating topic itself would evict nothing.
			const replay = create({ maxTopics: 1 });
			await publishAt(replay, 1000, 'room');
			await publishAt(replay, 1000, 'other');
			expect(await replay.seq('room')).toBe(0);
			expect(await replay.seq('other')).toBe(1);
			expect(await client.redis.zcard(registryKey())).toBe(1);
		});

		it('evicts when the created topic scores below the others (a clock behind)', async () => {
			const replay = create({ maxTopics: 1 });
			await publishAt(replay, 5000, 'room');
			await publishAt(replay, 1000, 'late');
			expect(await replay.seq('room')).toBe(0);
			expect(await replay.seq('late')).toBe(1);
		});

		if (backend.name === 'stream') {
			it('republishes an idempotent request into the generation that follows an eviction', async () => {
				const replay = create({ maxTopics: 1 });
				clock = 1000;
				const opts = { producerId: 'p', requestId: 'r1' };
				expect(await replay.publishIdempotent(platform, 'room', 'msg', { id: 1 }, opts)).toEqual({ seq: 1, isDuplicate: false });
				expect(await replay.publishIdempotent(platform, 'room', 'msg', { id: 1 }, opts)).toEqual({ seq: 1, isDuplicate: true });
				await replay.publish(platform, 'other', 'msg', { id: 1 });
				expect(await replay.publishIdempotent(platform, 'room', 'msg', { id: 1 }, opts)).toEqual({ seq: 1, isDuplicate: false });
			});
		}

		it('draws an opaque epoch from the runtime RNG, not from time or a count', async () => {
			setRuntimeEnv({ clock: { now: () => clock, wallEpoch: () => clock }, rng: { u32: () => 41 } });
			const replay = create({ maxTopics: 10 });
			await publishAt(replay, 1000, 'a');
			expect(await replay.currentEpoch('a')).toBe(42);
		});

		it('a recreated topic never takes the epoch still stored for it', async () => {
			setRuntimeEnv({ clock: { now: () => clock, wallEpoch: () => clock }, rng: { u32: () => 41 } });
			const replay = create({ maxTopics: 10 });
			await publishAt(replay, 1000, 'a');
			expect(await replay.currentEpoch('a')).toBe(42);
			// The counter is lost but the epoch survives; the same draw comes up.
			await client.redis.del(key('seq', 'a'));
			await publishAt(replay, 2000, 'a');
			expect(await replay.currentEpoch('a')).toBe(43);
			await replay.clearTopic('a');
			expect(await replay.currentEpoch('a')).toBe(42);
		});

		it('wraps a drawn epoch within 1 .. 2^32 - 1', async () => {
			setRuntimeEnv({ clock: { now: () => clock, wallEpoch: () => clock }, rng: { u32: () => 4294967295 } });
			const replay = create({ maxTopics: 10 });
			await publishAt(replay, 1000, 'a');
			expect(await replay.currentEpoch('a')).toBe(1);
			await replay.clearTopic('a');
			expect(await replay.currentEpoch('a')).toBe(2);
		});

		it('scans for existing topics in one instance only', async () => {
			let scans = 0;
			const scan = client.redis.scan;
			client.redis.scan = (...args) => { scans++; return scan.apply(client.redis, args); };
			try {
				await create({ maxTopics: 10 }).publish(platform, 'a', 'msg', 1);
				const first = scans;
				expect(first).toBeGreaterThan(0);
				await create({ maxTopics: 10 }).publish(platform, 'b', 'msg', 1);
				expect(scans).toBe(first);
			} finally {
				client.redis.scan = scan;
			}
		});

		it('creates a topic within a bounded wait while another instance holds the seed lease', async () => {
			clock = Date.now();
			const seed = client.key('replay:seed:{replay-topics}');
			await client.redis.hset(seed, 'lease', 'other', 'leaseUntil', String(clock + 600_000));
			await client.redis.set(key('seq', 'legacy'), '4');
			const started = Date.now();
			const replay = create({ maxTopics: 10 });
			await replay.publish(platform, 'fresh', 'msg', 1);
			expect(Date.now() - started).toBeLessThan(5000);
			expect(await replay.currentEpoch('fresh')).toBeGreaterThan(0);
			// The scan belongs to the lease holder: nothing was enrolled here.
			expect(await client.redis.zrange(registryKey(), 0, -1)).not.toContain('legacy');
		}, 10_000);

		it('warns once and counts evictions when existing topics exceed the cap', async () => {
			delete globalThis[FIRST_REPORTS];
			const records = [];
			const dispose = setOperationalEventSink((record) => { records.push(record); });
			try {
				for (const topic of ['l1', 'l2', 'l3']) await client.redis.set(key('seq', topic), '1');
				const metrics = createMetrics();
				const replay = create({ maxTopics: 2, metrics });
				await publishAt(replay, 1000, 'fresh');
				const { event } = extensionsErrorDefinition(EXTENSIONS_ERROR_IDS.REDIS_REPLAY_TOPICS_OVER_CAP);
				const warnings = records.filter((r) => r.event === event);
				expect(warnings).toHaveLength(1);
				expect(warnings[0].attributes).toEqual({ topics: 4, maxTopics: 2 });
				expect(await metrics.serialize()).toMatch(/replay_topic_evictions_total 2/);
			} finally {
				dispose();
				delete globalThis[FIRST_REPORTS];
			}
		});

		it('removes every per-topic key of an evicted topic', async () => {
			const replay = create({ maxTopics: 1 });
			await publishAt(replay, 1000, 'gone', 3);
			await publishAt(replay, 2000, 'kept');
			for (const kind of ['seq', backend.buf, 'epoch', 'used']) {
				expect(await client.redis.exists(key(kind, 'gone'))).toBe(0);
			}
		});

		it('never reuses an evicted epoch; a client holding it rehydrates', async () => {
			const replay = create({ maxTopics: 1 });
			await publishAt(replay, 1000, 'room', 3);
			const evictedEpoch = await replay.currentEpoch('room');
			await publishAt(replay, 2000, 'other');

			// Evicted and not yet recreated: a client that only knows the
			// baseline epoch matches 0, so the counter of 0 must truncate it.
			const hook = replay.resumeHook();
			platform.reset();
			await hook(mockWs({}), { lastSeenSeqs: { room: 3 }, lastSeenEpochs: { room: 0 }, platform });
			expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'truncated')).toBe(true);

			await publishAt(replay, 3000, 'room', 2);
			const recreated = await replay.currentEpoch('room');
			expect(recreated).not.toBe(evictedEpoch);
			expect(recreated).toBeGreaterThan(0);

			platform.reset();
			await hook(mockWs({}), { lastSeenSeqs: { room: 1 }, lastSeenEpochs: { room: evictedEpoch }, platform });
			expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'rehydrate')).toBe(true);
			expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'msg')).toBe(false);
		});

		it('enrolls topics stored before the upgrade and recreates them under a new epoch', async () => {
			// A deployment upgraded from a release without the registry: a room at
			// seq 50 under epoch 2, and an idle topic whose epoch reached 40.
			await client.redis.set(key('seq', 'room'), '50');
			await client.redis.set(key('epoch', 'room'), '2');
			await client.redis.set(key('epoch', 'idle'), '40');
			const replay = create({ maxTopics: 1 });

			await publishAt(replay, 1000, 'fresh');
			// Both legacy topics were enrolled and, never published since, are
			// the least recently used: the room is evicted.
			expect(await replay.seq('room')).toBe(0);

			await publishAt(replay, 2000, 'room');
			expect(await replay.currentEpoch('room')).not.toBe(2);
			platform.reset();
			await replay.resumeHook()(mockWs({}), { lastSeenSeqs: { room: 50 }, lastSeenEpochs: { room: 2 }, platform });
			expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'rehydrate')).toBe(true);
			expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'msg')).toBe(false);
		});

		it('keeps a topic stored before the upgrade that is published after it', async () => {
			await client.redis.set(key('seq', 'a'), '5');
			await client.redis.set(key('seq', 'b'), '5');
			const replay = create({ maxTopics: 2 });
			await publishAt(replay, 1000, 'seeder');
			// The enrollment pass registered a and b at score 0; seeder made it
			// three, so one went. Publishing to the survivor stamps it as used.
			const survivor = (await replay.seq('a')) > 0 ? 'a' : 'b';
			await publishAt(replay, 2000, survivor);
			await publishAt(replay, 3000, 'later');
			expect(await replay.seq(survivor)).toBe(6);
			expect(await replay.seq('seeder')).toBe(0);
			expect(await replay.seq('later')).toBe(1);
		});

		it('publishes to an existing topic in one round trip that touches no shared key', async () => {
			const meter = countRedisIO(client);
			const replay = createReplay(meter.client, { ...backend.options, maxTopics: 1 });
			await publishAt(replay, 1000, 'room');
			const registry = await client.redis.zrange(registryKey(), 0, -1);
			meter.reset();
			await publishAt(replay, 2000, 'room', 5);
			expect(meter.read()).toBe(5);
			expect(await client.redis.zrange(registryKey(), 0, -1)).toEqual(registry);
		});

		it('creates a topic in three round trips while the registry is under the cap', async () => {
			const meter = countRedisIO(client);
			const replay = createReplay(meter.client, { ...backend.options, maxTopics: 10 });
			await publishAt(replay, 1000, 'first');
			meter.reset();
			await publishAt(replay, 2000, 'second');
			expect(meter.read()).toBe(3);
		});

		for (const kind of ['seq', 'since', 'gap', 'replay']) {
			it(`${kind}() counts as use at most once a minute per topic, in its own round trip`, async () => {
				const meter = countRedisIO(client);
				const replay = createReplay(meter.client, { ...backend.options, maxTopics: 5 });
				const read = {
					seq: () => replay.seq('room'),
					since: () => replay.since('room', 0),
					gap: () => replay.gap('room', 1),
					replay: () => replay.replay(mockWs({}), 'room', 1, platform)
				}[kind];
				await publishAt(replay, 1000, 'room', 3);
				const registry = await client.redis.zrange(registryKey(), 0, -1);
				clock = 2000;
				meter.reset();
				await read();
				const trips = meter.read();
				expect(await client.redis.get(key('used', 'room'))).toBe('2000');
				clock = 30_000;
				meter.reset();
				await read();
				expect(meter.read()).toBe(trips);
				// Coalesced: the stamp did not move within the minute.
				expect(await client.redis.get(key('used', 'room'))).toBe('2000');
				clock = 70_000;
				meter.reset();
				await read();
				expect(meter.read()).toBe(trips);
				expect(await client.redis.get(key('used', 'room'))).toBe('70000');
				expect(await client.redis.zrange(registryKey(), 0, -1)).toEqual(registry);
			});
		}

		it('keeps a topic that is only read over one published earlier', async () => {
			const replay = create({ maxTopics: 2 });
			await publishAt(replay, 1000, 'readonly');
			await publishAt(replay, 2000, 'written');
			clock = 70_000;
			await replay.since('readonly', 0);
			await publishAt(replay, 71_000, 'third');
			expect(await replay.seq('readonly')).toBe(1);
			expect(await replay.seq('written')).toBe(0);
		});

		it('a read never creates keys for an unknown topic', async () => {
			const replay = create({ maxTopics: 5 });
			clock = 70_000;
			await replay.seq('ghost');
			await replay.since('ghost', 0);
			expect(await client.redis.exists(key('used', 'ghost'))).toBe(0);
		});

		it('still stores and broadcasts when eviction fails', async () => {
			const replay = create({ maxTopics: 1 });
			await publishAt(replay, 1000, 'old');
			const original = client.redis.eval;
			client.redis.eval = async (script, ...args) => {
				if (script.includes('REPLAY_TOPIC_EVICT')) throw new Error('evict down');
				return original.call(client.redis, script, ...args);
			};
			try {
				platform.reset();
				await publishAt(replay, 2000, 'new');
			} finally {
				client.redis.eval = original;
			}
			expect(await replay.seq('new')).toBe(1);
			expect(platform.published.filter((p) => p.topic === 'new')).toHaveLength(1);
			// Over the cap until the next creation evicts.
			expect(await replay.seq('old')).toBe(1);
			await publishAt(replay, 3000, 'third');
			expect(await replay.seq('old')).toBe(0);
		});

		it('gives a topic created by two instances at once one seq space and one epoch', async () => {
			const one = create({ maxTopics: 10 });
			const two = create({ maxTopics: 10 });
			await Promise.all([
				one.publish(platform, 'room', 'msg', { n: 1 }),
				two.publish(platform, 'room', 'msg', { n: 2 })
			]);
			expect(await one.seq('room')).toBe(2);
			expect((await one.since('room', 0)).map((m) => m.seq)).toEqual([1, 2]);
			expect(await one.currentEpoch('room')).toBeGreaterThan(0);
		});

		it('clear keeps only the seed state', async () => {
			const replay = create({ maxTopics: 5 });
			await publishAt(replay, 1000, 'a');
			await publishAt(replay, 2000, 'b');
			const before = await replay.currentEpoch('a');
			await replay.clear();
			const [, left] = await client.redis.scan('0', 'MATCH', client.key('replay:*'));
			expect(left).toEqual([client.key('replay:seed:{replay-topics}')]);
			await publishAt(replay, 3000, 'a');
			expect(await replay.currentEpoch('a')).not.toBe(before);
		});

		it('clearTopic counts as a use and registers the topic', async () => {
			const replay = create({ maxTopics: 2 });
			await publishAt(replay, 1000, 'a');
			await publishAt(replay, 2000, 'b');
			clock = 3000;
			await replay.clearTopic('a');
			await publishAt(replay, 4000, 'c');
			expect(await replay.currentEpoch('a')).toBeGreaterThan(0);
			expect(await replay.seq('b')).toBe(0);
			expect(await replay.currentEpoch('b')).toBe(0);
		});
	});
}
