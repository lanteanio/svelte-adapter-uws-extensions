import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createReplay } from '../../src/postgres/replay.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

let clock;
let ticks;
beforeEach(() => {
	clock = 1_700_000_000_000;
	ticks = [];
	setRuntimeEnv({
		clock: { now: () => clock, wallEpoch: () => clock },
		rng: { float: () => 1 },
		timers: { set(cb, ms) { const handle = { cb, ms }; ticks.push(handle); return handle; }, clear() {} }
	});
});
afterEach(() => {
	resetRuntimeEnv();
});

describe('postgres replay maxTopics and generations', () => {
	let client;
	let platform;

	beforeEach(() => {
		client = mockPgClient();
		platform = mockPlatform();
	});

	const create = (options = {}) => createReplay(client, { cleanupInterval: 0, ...options });
	async function publishAt(replay, ms, topic, n = 1) {
		clock = ms;
		for (let i = 0; i < n; i++) await replay.publish(platform, topic, 'msg', { i });
	}
	function recordStatements() {
		const seen = [];
		const original = client.query;
		client.query = async (q, values) => {
			seen.push(String(typeof q === 'object' ? q.text : q).trim());
			return original.call(client, q, values);
		};
		return { seen, restore: () => { client.query = original; } };
	}

	it('validates maxTopics like the in-memory plugin', () => {
		for (const maxTopics of [0, -1, 1.5, '5', NaN]) {
			expect(() => create({ maxTopics })).toThrow('maxTopics must be a positive integer');
		}
		expect(() => create({ maxTopics: 1 }).destroy()).not.toThrow();
	});

	it('defaults to 100 topics and evicts the least recently published past it', async () => {
		const replay = create();
		for (let i = 0; i <= 100; i++) await publishAt(replay, clock + 1000, 'room-' + i);
		expect(await replay.seq('room-0')).toBe(0);
		expect(await replay.since('room-0', 0)).toEqual([]);
		expect(await replay.currentEpoch('room-0')).toBe(0);
		for (const i of [1, 50, 100]) expect(await replay.seq('room-' + i)).toBe(1);
	});

	it('evicts by last publish, not by creation order', async () => {
		const replay = create({ maxTopics: 2 });
		await publishAt(replay, 1000, 'a');
		await publishAt(replay, 2000, 'b');
		await publishAt(replay, 3000, 'a');
		await publishAt(replay, 4000, 'c');
		expect(await replay.seq('a')).toBe(2);
		expect(await replay.seq('b')).toBe(0);
		expect(await replay.since('b', 0)).toEqual([]);
		expect(await replay.seq('c')).toBe(1);
	});

	it('evicts past the cap when a batch creates topics', async () => {
		const replay = create({ maxTopics: 2 });
		await publishAt(replay, 1000, 'old');
		clock = 2000;
		await replay.publishBatch(platform, [
			{ topic: 'x', event: 'msg', data: 1 },
			{ topic: 'y', event: 'msg', data: 2 }
		]);
		expect(await replay.seq('old')).toBe(0);
		expect(await replay.seq('x')).toBe(1);
		expect(await replay.seq('y')).toBe(1);
	});

	it('never reuses an evicted epoch; a client holding it rehydrates', async () => {
		const replay = create({ maxTopics: 1 });
		await publishAt(replay, 1000, 'room', 3);
		const evictedEpoch = await replay.currentEpoch('room');
		await publishAt(replay, 2000, 'other');

		const hook = replay.resumeHook();
		platform.reset();
		await hook(mockWs({}), { lastSeenSeqs: { room: 3 }, lastSeenEpochs: { room: 0 }, platform });
		expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'truncated')).toBe(true);

		await publishAt(replay, 3000, 'room', 2);
		const recreated = await replay.currentEpoch('room');
		expect(recreated).toBeGreaterThan(evictedEpoch);

		platform.reset();
		await hook(mockWs({}), { lastSeenSeqs: { room: 1 }, lastSeenEpochs: { room: evictedEpoch }, platform });
		expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'rehydrate')).toBe(true);
		expect(platform.sent.some((s) => s.topic === '__replay:room' && s.event === 'msg')).toBe(false);
	});

	it('publishes to an existing topic in one statement and creates one in two', async () => {
		const replay = create({ maxTopics: 10, size: 100 });
		await publishAt(replay, 1000, 'room');
		const log = recordStatements();
		try {
			await publishAt(replay, 2000, 'room', 3);
			expect(log.seen).toHaveLength(3);
			log.seen.length = 0;
			await publishAt(replay, 3000, 'fresh');
			expect(log.seen).toHaveLength(2);
			expect(log.seen[1]).toMatch(/^WITH victims AS/);
		} finally {
			log.restore();
		}
	});

	it('reads issue no writes', async () => {
		const replay = create({ maxTopics: 5 });
		await publishAt(replay, 1000, 'room', 3);
		const log = recordStatements();
		try {
			await replay.seq('room');
			await replay.since('room', 0);
			await replay.gap('room', 1);
			await replay.currentEpoch('room');
			await replay.replay(mockWs({}), 'room', 1, platform);
			await replay.resumeHook()(mockWs({}), { lastSeenSeqs: { room: 1 }, lastSeenEpochs: { room: await replay.currentEpoch('room') }, platform });
		} finally {
			log.restore();
		}
		expect(log.seen.length).toBeGreaterThan(0);
		for (const text of log.seen) expect(text).toMatch(/^SELECT/);
	});

	it('still stores and broadcasts when eviction fails', async () => {
		const replay = create({ maxTopics: 1 });
		await publishAt(replay, 1000, 'old');
		const original = client.query;
		client.query = async (q, values) => {
			if (String(q?.text ?? q).includes('WITH victims AS')) throw new Error('evict down');
			return original.call(client, q, values);
		};
		try {
			platform.reset();
			await publishAt(replay, 2000, 'new');
		} finally {
			client.query = original;
		}
		expect(await replay.seq('new')).toBe(1);
		expect(platform.published.filter((p) => p.topic === 'new')).toHaveLength(1);
		expect(await replay.seq('old')).toBe(1);
	});

	it('cleanup removes topics idle past the ttl with their rows', async () => {
		const replay = createReplay(client, { ttl: 60, maxTopics: 10, cleanupInterval: 1000, forgetUserId: () => null });
		const tick = ticks.at(-1);
		try {
			await publishAt(replay, 1000, 'idle');
			await publishAt(replay, 50_000, 'warm');
			const idleEpoch = await replay.currentEpoch('idle');
			expect(idleEpoch).toBeGreaterThan(0);

			// idle was last published 69 s ago with a 60 s ttl.
			clock = 70_000;
			await tick.cb();
			expect(await replay.currentEpoch('idle')).toBe(0);
			expect(await replay.seq('idle')).toBe(0);
			expect(await replay.seq('warm')).toBe(1);

			await publishAt(replay, 71_000, 'idle');
			expect(await replay.currentEpoch('idle')).toBeGreaterThan(idleEpoch);
		} finally {
			replay.destroy();
		}
	});

	it('cleanup drains topics over the cap without waiting for a creation', async () => {
		const writer = create({ maxTopics: 100 });
		for (let i = 1; i <= 5; i++) await publishAt(writer, i * 1000, 'room-' + i);
		const replay = createReplay(client, { maxTopics: 2, cleanupInterval: 1000 });
		const tick = ticks.at(-1);
		try {
			await tick.cb();
			for (const i of [1, 2, 3]) expect(await replay.seq('room-' + i)).toBe(0);
			for (const i of [4, 5]) expect(await replay.seq('room-' + i)).toBe(1);
		} finally {
			replay.destroy();
		}
	});
});
