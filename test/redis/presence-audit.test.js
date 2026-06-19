// Tests for the per-instance presence consistency auditor: the bounded snapshot
// builder, the persistence-gated escalation of a desync through the auditor, and
// the live wiring into createPresence (no false-positive on healthy state or an
// in-flight join, plus the opt-out).

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';
import { buildPresenceAuditSnapshot } from '../../src/redis/presence/audit-snapshot.js';
import { createConsistencyAuditor } from '../../src/shared/auditor.js';
import { checkRedisPresenceLocalIndex } from '../../src/shared/invariants.js';
import { createLocalIndex } from '../../src/redis/presence/local-index.js';

function countsMap(entries) {
	const m = new Map();
	for (const [topic, keys] of entries) {
		const inner = new Map();
		for (const k of keys) inner.set(k, 1);
		m.set(topic, inner);
	}
	return m;
}
// A topicKeyCount accessor that agrees with the count map (the healthy case).
function matchingIndex(localCounts) {
	return (topic) => localCounts.get(topic)?.size ?? 0;
}

describe('buildPresenceAuditSnapshot', () => {
	it('reports per-topic count and local-index distinct-key cardinalities and the total', () => {
		const localCounts = countsMap([['room', ['u1', 'u2']], ['lobby', ['u3']]]);
		const snap = buildPresenceAuditSnapshot({ localCounts, topicKeyCount: matchingIndex(localCounts), mt: (t) => t, offset: 0, limit: 10 });
		expect(snap.total).toBe(2);
		expect(snap.presenceTopics).toEqual([
			{ topic: 'room', countKeys: 2, indexKeys: 2 },
			{ topic: 'lobby', countKeys: 1, indexKeys: 1 }
		]);
	});

	it('advances a bounded round-robin window without materializing the population', () => {
		const localCounts = countsMap([['a', ['u1']], ['b', ['u1']], ['c', ['u1']]]);
		const idx = matchingIndex(localCounts);
		const win = (offset) => buildPresenceAuditSnapshot({ localCounts, topicKeyCount: idx, mt: (t) => t, offset, limit: 2 })
			.presenceTopics.map((r) => r.topic);
		expect(win(0)).toEqual(['a', 'b']);
		expect(win(2)).toEqual(['c']);
	});

	it('records a desync when the local index lost a key the count map still tracks', () => {
		const localCounts = countsMap([['room', ['u1', 'u2']]]);
		const snap = buildPresenceAuditSnapshot({ localCounts, topicKeyCount: () => 1, mt: (t) => t, offset: 0, limit: 10 });
		expect(snap.presenceTopics).toEqual([{ topic: 'room', countKeys: 2, indexKeys: 1 }]);
		expect(checkRedisPresenceLocalIndex(snap)).toEqual({
			category: 'redis.presence.local-index-desync',
			context: { topic: 'room', countKeys: 2, indexKeys: 1 }
		});
	});

	it('does NOT flag an in-flight join: the count and index move together, only the data commit lags', () => {
		// During a fresh-user join the count map and the local index are both
		// incremented synchronously; only localData is deferred. The snapshot reads
		// the index (not the data), so a join suspended before its data commit -
		// count=1, index=1, data=0 - is clean. (Comparing against data would have
		// false-fired here.)
		const localCounts = countsMap([['room', ['u1']]]);
		const snap = buildPresenceAuditSnapshot({ localCounts, topicKeyCount: () => 1, mt: (t) => t, offset: 0, limit: 10 });
		expect(checkRedisPresenceLocalIndex(snap)).toBeNull();
	});

	it('routes the topic through the metric-safe transform (no raw topic in the snapshot)', () => {
		const localCounts = countsMap([['secret-room', ['u1']]]);
		const snap = buildPresenceAuditSnapshot({ localCounts, topicKeyCount: () => 1, mt: () => '<redacted>', offset: 0, limit: 10 });
		expect(snap.presenceTopics[0].topic).toBe('<redacted>');
	});
});

describe('createLocalIndex topicKeyCount', () => {
	it('counts distinct user keys per topic, decrementing only when a key loses its last ws', () => {
		const idx = createLocalIndex(new Map());
		const a = {}, b = {};
		expect(idx.topicKeyCount('room')).toBe(0);
		idx.indexAdd('room', 'u1', a);
		expect(idx.topicKeyCount('room')).toBe(1);
		idx.indexAdd('room', 'u1', b); // same user, second ws: still one distinct key
		expect(idx.topicKeyCount('room')).toBe(1);
		idx.indexAdd('room', 'u2', a); // distinct user
		expect(idx.topicKeyCount('room')).toBe(2);
		idx.indexRemove('room', 'u1', a); // u1 still has ws b
		expect(idx.topicKeyCount('room')).toBe(2);
		idx.indexRemove('room', 'u1', b); // u1 last ws gone
		expect(idx.topicKeyCount('room')).toBe(1);
		idx.indexRemove('room', 'u2', a); // topic now empty
		expect(idx.topicKeyCount('room')).toBe(0);
	});

	it('clear() drops every per-topic key count', () => {
		const idx = createLocalIndex(new Map());
		idx.indexAdd('room', 'u1', {});
		idx.indexAdd('lobby', 'u2', {});
		expect(idx.topicKeyCount('room')).toBe(1);
		idx.clear();
		expect(idx.topicKeyCount('room')).toBe(0);
		expect(idx.topicKeyCount('lobby')).toBe(0);
	});

	it('mirrors the per-topic distinct-key count of a parallel member-count map', () => {
		const idx = createLocalIndex(new Map());
		const counts = new Map(); // topic -> Map<key, refcount>, the presence localCounts shape
		const bump = (topic, key, ws) => {
			idx.indexAdd(topic, key, ws);
			let c = counts.get(topic);
			if (!c) { c = new Map(); counts.set(topic, c); }
			c.set(key, (c.get(key) || 0) + 1);
		};
		const drop = (topic, key, ws) => {
			idx.indexRemove(topic, key, ws);
			const c = counts.get(topic);
			const n = (c.get(key) || 0) - 1;
			if (n > 0) c.set(key, n); else { c.delete(key); if (c.size === 0) counts.delete(topic); }
		};
		const wss = [{}, {}, {}];
		bump('room', 'u1', wss[0]);
		bump('room', 'u1', wss[1]);
		bump('room', 'u2', wss[2]);
		bump('lobby', 'u1', wss[0]);
		drop('room', 'u1', wss[0]);
		for (const topic of ['room', 'lobby']) {
			expect(idx.topicKeyCount(topic)).toBe(counts.get(topic)?.size ?? 0);
		}
	});
});

describe('presence consistency auditor (persistence gate)', () => {
	it('keeps a desync soft on the first audit and escalates only when it persists', () => {
		const softAssert = vi.fn();
		const fatalFn = vi.fn();
		const auditor = createConsistencyAuditor({
			snapshot: () => ({ presenceTopics: [{ topic: 'room', countKeys: 1, indexKeys: 0 }], total: 1 }),
			assert: softAssert,
			fatal: fatalFn,
			predicates: [checkRedisPresenceLocalIndex],
			hardCategories: ['redis.presence.local-index-desync']
		});
		auditor.runOnce();
		expect(softAssert).toHaveBeenCalledWith(false, 'redis.presence.local-index-desync', { topic: 'room', countKeys: 1, indexKeys: 0 });
		expect(fatalFn).not.toHaveBeenCalled();
		auditor.runOnce();
		expect(fatalFn).toHaveBeenCalledWith(false, 'redis.presence.local-index-desync', { topic: 'room', countKeys: 1, indexKeys: 0 });
		expect(auditor.stats.fatals).toBe(1);
	});
});

describe('presence consistency auditor (wired into createPresence)', () => {
	let client;
	let platform;
	let presence;

	beforeEach(() => {
		client = mockRedisClient('test:');
		platform = mockPlatform();
		presence = createPresence(client, {
			key: 'id',
			select: (userData) => ({ id: userData.id, name: userData.name }),
			heartbeat: 60000,
			ttl: 180
		});
	});

	afterEach(() => {
		presence.destroy();
	});

	it('finds no violation auditing healthy live state after join/leave churn', async () => {
		const a = mockWs({ id: '1', name: 'Alice' });
		const b = mockWs({ id: '2', name: 'Bob' });
		await presence.join(a, 'room', platform);
		await presence.join(b, 'room', platform);
		await presence.leave(a, platform, 'room');
		// runOnce builds a snapshot from the live count map and the live index and
		// runs the predicate; a clean instance yields no violations (and would
		// throw via the real test-mode assert if the two had drifted).
		expect(presence._consistencyAuditor.runOnce()).toEqual([]);
	});

	it('does not flag a phantom desync after clear() resets the index in lockstep with the count map', async () => {
		const a = mockWs({ id: '1', name: 'Alice' });
		const b = mockWs({ id: '2', name: 'Bob' });
		await presence.join(a, 'room', platform);
		await presence.join(b, 'room', platform);
		await presence.clear();
		// Only one of the two prior users rejoins the same topic. clear() must
		// have dropped the local index alongside the count map, or the index would
		// still report two keys for 'room' while the count map rebuilt to one - a
		// phantom divergence the auditor would (in test mode) throw on.
		const a2 = mockWs({ id: '1', name: 'Alice' });
		await presence.join(a2, 'room', platform);
		expect(presence._consistencyAuditor.runOnce()).toEqual([]);
	});

	it('is opt-out: consistencyAuditIntervalMs 0 starts no auditor', () => {
		const off = createPresence(client, {
			key: 'id',
			select: (userData) => ({ id: userData.id }),
			heartbeat: 60000,
			ttl: 180,
			consistencyAuditIntervalMs: 0
		});
		expect(off._consistencyAuditor).toBeNull();
		off.destroy();
	});
});
