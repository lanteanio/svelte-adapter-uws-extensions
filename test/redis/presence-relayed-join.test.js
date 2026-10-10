import { describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

describe('presence relayed joins', () => {
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

	it('replaces a durable snapshot without erasing locally cached transient values', async () => {
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
			expect(publish).not.toHaveBeenCalled();
			await vi.advanceTimersByTimeAsync(100);
			expect(pa.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual({ id: 'alice' });
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
