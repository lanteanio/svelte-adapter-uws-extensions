import { describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

describe('presence relayed joins', () => {
	it.each([false, true])('restores a surviving tab identity after its original TTL (closeAll=%s)', async closeAll => {
		vi.useFakeTimers();
		const client = mockRedisClient();
		const options = { binary: false, select: u => ({ id: u.id, name: u.name }), heartbeat: 100, ttl: 1 };
		const a = createPresence(client, options), b = createPresence(client, options);
		const pa = mockPlatform(), pb = mockPlatform();
		const old = mockWs({ id: 'alice', name: 'Old' }), recent = mockWs({ id: 'alice', name: 'Recent' });
		try {
			await b.join(mockWs({ id: 'bob' }), 'room', pb);
			await a.join(old, 'room', pa);
			await a.join(recent, 'room', pa);
			await vi.advanceTimersByTimeAsync(1500);
			await a.leave(recent, pa, closeAll ? undefined : 'room');
			a.flushDiffs(); b.flushDiffs();
			for (const tracker of [a, b]) expect(await tracker.list('room')).toContainEqual({ id: 'alice', name: 'Old' });
			for (const platform of [pa, pb]) expect(platform.published.findLast(frame => frame.event === 'diff' && frame.data.joins.alice)?.data.joins.alice).toEqual({ id: 'alice', name: 'Old' });
		} finally { a.destroy(); b.destroy(); vi.useRealTimers(); }
	});
	it('keeps transient fields out of same-turn join rosters on both instances', async () => {
		const client = mockRedisClient();
		const options = { binary: false, transient: ['typing'] };
		const a = createPresence(client, options);
		const b = createPresence(client, options);
		try {
			const pa = mockPlatform();
			const pb = mockPlatform();
			await b.join(mockWs({ id: 'bob' }), 'room', pb);
			b.flushDiffs();
			pb.reset();
			const alice = mockWs({ id: 'alice' });
			await a.join(alice, 'room', pa);
			await a.update(alice, 'room', { typing: true, color: 'blue' }, pa);
			a.flushDiffs();
			b.flushDiffs();
			for (const platform of [pa, pb]) {
				const join = platform.published.find((f) => f.event === 'diff' && f.data.joins.alice);
				expect(join.data.joins.alice).toEqual({ id: 'alice', color: 'blue' });
			}
			expect(await b.list('room')).toContainEqual({ id: 'alice', color: 'blue' });
		} finally { a.destroy(); b.destroy(); }
	});
	it('refreshes an existing identity and its durable fields on every instance', async () => {
		vi.useFakeTimers();
		const client = mockRedisClient();
		const options = { binary: false, select: (u) => ({ id: u.id, name: u.name }), heartbeat: 100 };
		const a = createPresence(client, options);
		const b = createPresence(client, options);
		try {
			const pa = mockPlatform();
			const pb = mockPlatform();
			const alice = mockWs({ id: 'alice', name: 'Old' });
			await a.join(alice, 'room', pa);
			await a.update(alice, 'room', { color: 'blue' }, pa);
			a.flushDiffs();
			pa.reset();
			vi.setSystemTime(Date.now() + 10);
			await b.join(mockWs({ id: 'alice', name: 'New' }), 'room', pb);
			a.flushDiffs();
			b.flushDiffs();
			const expected = { id: 'alice', name: 'New', color: 'blue' };
			for (const platform of [pa, pb]) {
				expect(platform.published.find((f) => f.event === 'diff').data.joins.alice).toEqual(expected);
				platform.reset();
			}
			await vi.advanceTimersByTimeAsync(100);
			for (const platform of [pa, pb]) {
				expect(platform.published.find((f) => f.event === 'heartbeat').data.alice).toEqual(expected);
			}
			expect(await b.list('room')).toEqual([expected]);
		} finally { a.destroy(); b.destroy(); vi.useRealTimers(); }
	});

	for (const closeAll of [false, true]) it('keeps the peer identity when an older local tab leaves, closeAll=' + closeAll, async () => {
		const client = mockRedisClient();
		const options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
		const a = createPresence(client, options), b = createPresence(client, options);
		const pa = mockPlatform(), pb = mockPlatform();
		const old1 = mockWs({ id: 'alice', name: 'Old' }), old2 = mockWs({ id: 'alice', name: 'Old' });
		try {
			await a.join(old1, 'room', pa); await a.join(old2, 'room', pa);
			await a.update(old1, 'room', { color: 'blue' }, pa);
			await b.join(mockWs({ id: 'alice', name: 'New' }), 'room', pb);
			a.flushDiffs(); b.flushDiffs(); pa.reset(); pb.reset();
			const publish = vi.spyOn(client.redis, 'publish');
			await a.leave(old1, pa, closeAll ? undefined : 'room');
			a.flushDiffs(); b.flushDiffs();
			expect(await a.list('room')).toEqual([{ id: 'alice', name: 'New', color: 'blue' }]);
			expect(await b.list('room')).toEqual([{ id: 'alice', name: 'New', color: 'blue' }]);
			expect(publish.mock.calls.filter(([, raw]) => JSON.parse(raw).event === 'updated')).toHaveLength(0);
			expect(pa.published).toEqual([]); expect(pb.published).toEqual([]);
		} finally { a.destroy(); b.destroy(); }
	});

	for (const legacy of [false, true]) it('keeps selected identity separate from shadowing fields, legacy=' + legacy, async () => {
		vi.useFakeTimers();
		const client = mockRedisClient();
		const options = { binary: false, select: u => ({ id: u.id, name: u.name }), heartbeat: 100 };
		const a = createPresence(client, options), b = createPresence(client, options);
		const pa = mockPlatform(), pb = mockPlatform();
		const old1 = mockWs({ id: 'alice', name: 'Old' }), old2 = mockWs({ id: 'alice', name: 'Old' });
		const publish = client.redis.publish.bind(client.redis);
		const sent = [];
		client.redis.publish = (channel, raw) => {
			const payload = JSON.parse(raw);
			if (legacy && ['join', 'updated'].includes(payload.event)) {
				delete payload.payload.identity; delete payload.payload.fields;
			}
			sent.push(payload);
			return publish(channel, JSON.stringify(payload));
		};
		try {
			await a.join(old1, 'room', pa); await a.join(old2, 'room', pa);
			await a.update(old1, 'room', { name: 'Badge' }, pa);
			await b.join(mockWs({ id: 'alice', name: 'New' }), 'room', pb);
			if (!legacy) {
				const event = sent.findLast(entry => entry.event === 'updated');
				expect(event.payload.identity).toEqual({ id: 'alice', name: 'New' });
				expect(event.payload.fields).toEqual({ name: 'Badge' });
			}
			sent.length = 0;
			await a.leave(old1, pa, 'room');
			await a.join(mockWs({ id: 'alice', name: 'New' }), 'room', pa);
			expect(sent.filter(entry => entry.event === 'updated')).toEqual([]);
			expect(await a.list('room')).toEqual([{ id: 'alice', name: 'Badge' }]);
			pa.reset(); pb.reset();
			await vi.advanceTimersByTimeAsync(100);
			for (const platform of [pa, pb]) expect(platform.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice', name: 'Badge' });
			const stored = JSON.parse(await client.redis.hget(client.key('presence:topic:{room}'), 'alice'));
			expect(stored.data).toEqual({ id: 'alice', name: 'New' });
		} finally { a.destroy(); b.destroy(); vi.useRealTimers(); }
	});

	it('repairs durable fields missed before an identity relay', async () => {
		vi.useFakeTimers();
		const client = mockRedisClient();
		const options = { binary: false, select: u => ({ id: u.id, name: u.name }), heartbeat: 100 };
		const a = createPresence(client, options), b = createPresence(client, options);
		const pa = mockPlatform(), pb = mockPlatform();
		const first = mockWs({ id: 'alice', name: 'Old' });
		try {
			await a.join(mockWs({ id: 'alice', name: 'Old' }), 'room', pa);
			await b.join(first, 'room', pb);
			const publish = client.redis.publish.bind(client.redis);
			client.redis.publish = (channel, raw) => JSON.parse(raw).event === 'fields' ? Promise.resolve(0) : publish(channel, raw);
			await b.update(first, 'room', { name: 'Badge', color: 'blue' }, pb);
			client.redis.publish = publish;
			await b.join(mockWs({ id: 'alice', name: 'New' }), 'room', pb);
			pa.reset();
			await vi.advanceTimersByTimeAsync(100);
			expect(pa.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice', name: 'Badge', color: 'blue' });
			const spy = vi.spyOn(client.redis, 'publish');
			await a.join(mockWs({ id: 'alice', name: 'New' }), 'room', pa);
			expect(spy.mock.calls.filter(([, raw]) => JSON.parse(raw).event === 'updated')).toEqual([]);
		} finally { a.destroy(); b.destroy(); vi.useRealTimers(); }
	});

	it('merges a durable snapshot without erasing locally cached values', async () => {
		vi.useFakeTimers();
		const client = mockRedisClient();
		const a = createPresence(client, { binary: false, transient: ['typing'], heartbeat: 100 });
		const pa = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		try {
			await a.join(ws, 'room', pa);
			await a.update(ws, 'room', { color: 'blue', typing: true }, pa);
			await client.redis.publish(client.key('presence:events:room'), JSON.stringify({
				instanceId: 'peer', topic: 'room', event: 'updated',
				payload: { key: 'alice', data: { id: 'alice' }, identity: true }
			}));
			a.flushDiffs(); pa.reset();
			const publish = vi.spyOn(client.redis, 'publish');
			await a.update(ws, 'room', { typing: true }, pa);
			await a.join(mockWs({ id: 'alice' }), 'room', pa);
			expect(publish).not.toHaveBeenCalled();
			await vi.advanceTimersByTimeAsync(100);
			expect(pa.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice', color: 'blue' });
		} finally { a.destroy(); vi.useRealTimers(); }
	});

	it('recounts durable field bytes after an identical local join reads missed fields', async () => {
		const client = mockRedisClient();
		const a = createPresence(client, { maxTotalFieldsBytes: 60 });
		const b = createPresence(client);
		const pa = mockPlatform(), pb = mockPlatform();
		const wa = mockWs({ id: 'alice' }), wb = mockWs({ id: 'alice' });
		try {
			await a.join(wa, 'room', pa);
			await b.join(wb, 'room', pb);
			await a.update(wa, 'room', { color: 'blue' }, pa);
			const publish = client.redis.publish.bind(client.redis);
			client.redis.publish = (channel, raw) => JSON.parse(raw).event === 'fields' ? Promise.resolve(0) : publish(channel, raw);
			await b.update(wb, 'room', { color: 'x'.repeat(40) }, pb);
			client.redis.publish = publish;
			await a.join(mockWs({ id: 'alice' }), 'room', pa);
			await a.update(wa, 'room', { shape: 'circle' }, pa);
			expect(await a.list('room')).toEqual([{ id: 'alice', color: 'x'.repeat(40) }]);
		} finally { a.destroy(); b.destroy(); }
	});

});

for (const closeAll of [false, true]) for (const sameInstance of [false, true]) it('preserves a pending join through a partial leave, closeAll=' + closeAll + ', sameInstance=' + sameInstance, async () => {
	vi.useFakeTimers();
	const client = mockRedisClient();
	const options = { binary: false, heartbeat: 100, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform();
	const old = mockWs({ id: 'alice', name: 'Old' }), recent = mockWs({ id: 'alice', name: 'Recent' });
	const read = client.redis.hgetall.bind(client.redis);
	try {
		await a.join(old, 'room', pa); await a.join(recent, 'room', pa);
		await a.update(old, 'room', { color: 'blue' }, pa);
		let release, sawRead;
		const reading = new Promise(resolve => { sawRead = resolve; });
		let hold = true;
		client.redis.hgetall = key => {
			if (hold && key === client.key('presence:topic:{room}')) {
				hold = false; sawRead();
				return new Promise(resolve => { release = () => resolve(read(key)); });
			}
			return read(key);
		};
		const target = sameInstance ? a : b, targetPlatform = sameInstance ? pa : pb;
		const joining = target.join(mockWs({ id: 'alice', name: 'Newest' }), 'room', targetPlatform);
		await reading;
		await a.leave(recent, pa, closeAll ? undefined : 'room');
		release();
		await joining;
		expect(await a.list('room')).toEqual([{ id: 'alice', name: 'Newest', color: 'blue' }]);
		pa.reset(); pb.reset();
		await vi.advanceTimersByTimeAsync(100);
		for (const p of sameInstance ? [pa] : [pa, pb]) expect(p.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice', name: 'Newest', color: 'blue' });
	} finally { client.redis.hgetall = read; a.destroy(); b.destroy(); vi.useRealTimers(); }
});

for (const closeAll of [false, true]) for (const afterWrite of [false, true]) it('preserves a peer identity that commits during a partial leave, closeAll=' + closeAll + ', afterWrite=' + afterWrite, async () => {
	vi.useFakeTimers();
	const client = mockRedisClient();
	const options = { binary: false, heartbeat: 100, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform();
	const old = mockWs({ id: 'alice', name: 'Old' }), recent = mockWs({ id: 'alice', name: 'Recent' });
	const evaluate = client.redis.eval.bind(client.redis);
	try {
		await a.join(old, 'room', pa); await a.join(recent, 'room', pa);
		let raced = false;
		client.redis.eval = async (script, ...args) => {
			if (!raced && (script.includes('PRESENCE_RESTORE') || script.includes('PRESENCE_JOIN'))) {
				raced = true;
				const result = afterWrite ? await evaluate(script, ...args) : undefined;
				await b.join(mockWs({ id: 'alice', name: 'Newest' }), 'room', pb);
				return afterWrite ? result : evaluate(script, ...args);
			}
			return evaluate(script, ...args);
		};
		await a.leave(recent, pa, closeAll ? undefined : 'room');
		expect(raced).toBe(true);
		expect(await a.list('room')).toEqual([{ id: 'alice', name: 'Newest' }]);
		pa.reset(); pb.reset();
		await vi.advanceTimersByTimeAsync(100);
		for (const p of [pa, pb]) expect(p.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice', name: 'Newest' });
	} finally { client.redis.eval = evaluate; a.destroy(); b.destroy(); vi.useRealTimers(); }
});

it('retains durable updates made while a peer identity relay is in flight', async () => {
	vi.useFakeTimers();
	const client = mockRedisClient();
	const options = { binary: false, heartbeat: 100, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform();
	const first = mockWs({ id: 'alice', name: 'Old' });
	const publish = client.redis.publish.bind(client.redis);
	try {
		await a.join(mockWs({ id: 'alice', name: 'Old' }), 'room', pa);
		await b.join(first, 'room', pb);
		await b.update(first, 'room', { color: 'blue' }, pb);
		let held;
		client.redis.publish = (channel, raw) => {
			if (JSON.parse(raw).event === 'updated') { held = [channel, raw]; return Promise.resolve(0); }
			return publish(channel, raw);
		};
		await a.join(mockWs({ id: 'alice', name: 'New' }), 'room', pa);
		await b.update(first, 'room', { color: 'red' }, pb);
		await publish(...held);
		b.flushDiffs();
		expect(pb.published.findLast(frame => frame.event === 'diff').data.joins.alice).toEqual({ id: 'alice', name: 'New', color: 'red' });
		pa.reset(); pb.reset();
		await vi.advanceTimersByTimeAsync(100);
		for (const p of [pa, pb]) expect(p.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice', name: 'New', color: 'red' });
		expect(await a.list('room')).toEqual([{ id: 'alice', name: 'New', color: 'red' }]);
	} finally { client.redis.publish = publish; a.destroy(); b.destroy(); vi.useRealTimers(); }
});

it('keeps local durable updates newer than a partial-leave reply', async () => {
	vi.useFakeTimers();
	const client = mockRedisClient();
	const a = createPresence(client, { binary: false, heartbeat: 100, select: u => ({ id: u.id, name: u.name }) });
	const pa = mockPlatform(), old = mockWs({ id: 'alice', name: 'Old' }), recent = mockWs({ id: 'alice', name: 'Recent' });
	const evaluate = client.redis.eval.bind(client.redis);
	try {
		await a.join(old, 'room', pa); await a.join(recent, 'room', pa);
		await a.update(old, 'room', { color: 'blue' }, pa);
		let raced = false;
		client.redis.eval = async (script, ...args) => {
			const result = await evaluate(script, ...args);
			if (!raced && script.includes('PRESENCE_RESTORE')) {
				raced = true;
				await a.update(old, 'room', { color: 'red', shape: 'circle' }, pa);
			}
			return result;
		};
		await a.leave(recent, pa, 'room');
		expect(raced).toBe(true);
		expect(await a.list('room')).toEqual([{ id: 'alice', name: 'Old', color: 'red', shape: 'circle' }]);
		pa.reset();
		await vi.advanceTimersByTimeAsync(100);
		expect(pa.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice', name: 'Old', color: 'red', shape: 'circle' });
	} finally { client.redis.eval = evaluate; a.destroy(); vi.useRealTimers(); }
});

it('does not restore over a newer peer generation with equal selected data', async () => {
	const client = mockRedisClient();
	const options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options), pa = mockPlatform(), pb = mockPlatform();
	const old = mockWs({ id: 'alice', name: 'Old' }), recent = mockWs({ id: 'alice', name: 'Recent' });
	const publish = client.redis.publish.bind(client.redis);
	try {
		await a.join(old, 'room', pa); await a.join(recent, 'room', pa);
		client.redis.publish = () => Promise.resolve(0);
		await b.join(mockWs({ id: 'alice', name: 'Recent' }), 'room', pb);
		await a.leave(recent, pa, 'room');
		expect(await a.list('room')).toEqual([{ id: 'alice', name: 'Recent' }]);
	} finally { client.redis.publish = publish; a.destroy(); b.destroy(); }
});

for (const scenario of ['repair', 'local-update', 'peer-update', 'leave', 'destroy', 'failure', 'coalesce']) it('reconciles conflicting identity fields against Redis: ' + scenario, async () => {
	vi.useFakeTimers();
	const client = mockRedisClient(), options = { binary: false, heartbeat: 100, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options), pa = mockPlatform(), pb = mockPlatform();
	const wa = mockWs({ id: 'alice', name: 'Old' }), wb = mockWs({ id: 'alice', name: 'Old' });
	const read = client.redis.hget.bind(client.redis), publish = client.redis.publish.bind(client.redis);
	let release, rawRead, calls = 0;
	try {
		await a.join(wa, 'room', pa); await b.join(wb, 'room', pb);
		await a.update(wa, 'room', { color: 'blue' }, pa);
		client.redis.publish = (channel, raw) => JSON.parse(raw).event === 'fields' ? Promise.resolve(0) : publish(channel, raw);
		await b.update(wb, 'room', { color: 'red' }, pb);
		client.redis.publish = publish;
		let sawRead;
		const reading = new Promise(resolve => { sawRead = resolve; });
		client.redis.hget = async (...args) => {
			if (args[0] === client.key('presence:topic:{room}') && ++calls === 1) {
				rawRead = await read(...args); sawRead();
				return new Promise((resolve, reject) => { release = () => scenario === 'failure' ? reject(new Error('read unavailable')) : resolve(rawRead); });
			}
			return read(...args);
		};
		const sent = vi.spyOn(client.redis, 'publish');
		await b.join(mockWs({ id: 'alice', name: 'New' }), 'room', pb);
		await reading;
		if (scenario === 'local-update' || scenario === 'peer-update') {
			const tracker = scenario === 'local-update' ? a : b, ws = scenario === 'local-update' ? wa : wb, platform = scenario === 'local-update' ? pa : pb;
			await tracker.update(ws, 'room', { color: 'green' }, platform);
			await tracker.update(ws, 'room', { color: 'blue' }, platform);
		}
		if (scenario === 'leave') await a.leave(wa, pa, 'room');
		if (scenario === 'destroy') a.destroy();
		if (scenario === 'coalesce') {
			const envelope = sent.mock.calls.findLast(([, raw]) => JSON.parse(raw).event === 'updated');
			for (let i = 0; i < 20; i++) await publish(...envelope);
			expect(calls).toBe(1);
		}
		a.flushDiffs(); pa.reset();
		release();
		await vi.advanceTimersByTimeAsync(100);
		if (scenario === 'leave' || scenario === 'destroy') expect(pa.published).toEqual([]);
		else expect(pa.published.findLast(frame => frame.event === 'heartbeat').data.alice.color).toBe(['repair', 'coalesce'].includes(scenario) ? 'red' : 'blue');
		if (scenario === 'coalesce') expect(calls).toBe(2);
	} finally { release?.(); client.redis.hget = read; client.redis.publish = publish; a.destroy(); b.destroy(); vi.useRealTimers(); }
});
