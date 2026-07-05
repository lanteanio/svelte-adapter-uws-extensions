// The mass-disconnect self-preservation guard. A heartbeat tick that finds a
// large fraction of the tracked sockets simultaneously dead treats it as a
// network event: evictions are HELD (no leave broadcast, cluster TTLs kept
// alive) for up to holdMaxMs so reconnecting clients land on unbroken roster
// entries instead of a leave/join flap. Individual deaths, small
// populations, and the opt-out keep the previous evict-immediately behavior.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';

function leavesOf(platform) {
	const out = [];
	for (const p of platform.published) {
		if (p.event === 'diff' && p.data && p.data.leaves) {
			out.push(...Object.keys(p.data.leaves));
		}
	}
	return out;
}

describe('presence self-preservation guard', () => {
	let client;
	let platform;
	/** @type {ReturnType<typeof createPresence> | null} */
	let presence;

	beforeEach(() => {
		vi.useFakeTimers();
		client = mockRedisClient('test:');
		platform = mockPlatform();
		presence = null;
	});

	afterEach(() => {
		presence?.destroy();
		vi.useRealTimers();
	});

	function makePresence(extra = {}) {
		presence = createPresence(client, {
			key: 'id',
			select: (userData) => ({ id: userData.id }),
			heartbeat: 1000,
			ttl: 90,
			...extra
		});
		return presence;
	}

	async function joinMany(p, n, topic = 'room') {
		const sockets = [];
		for (let i = 0; i < n; i++) {
			const ws = mockWs({ id: 'u' + i });
			await p.join(ws, topic, platform);
			sockets.push(ws);
		}
		p.flushDiffs();
		platform.published.length = 0;
		return sockets;
	}

	describe('option validation', () => {
		it('accepts true/false/object and rejects malformed shapes', () => {
			expect(() => makePresence({ selfPreservation: true })).not.toThrow();
			presence.destroy();
			expect(() => makePresence({ selfPreservation: false })).not.toThrow();
			presence.destroy();
			expect(() => makePresence({ selfPreservation: { threshold: 0.5, minPopulation: 4, holdMaxMs: 10000 } })).not.toThrow();
			presence.destroy();
			expect(() => makePresence({ selfPreservation: 'yes' })).toThrow('selfPreservation');
			expect(() => makePresence({ selfPreservation: { threshold: 0 } })).toThrow('threshold');
			expect(() => makePresence({ selfPreservation: { threshold: 2 } })).toThrow('threshold');
			expect(() => makePresence({ selfPreservation: { minPopulation: 1 } })).toThrow('minPopulation');
			expect(() => makePresence({ selfPreservation: { holdMaxMs: 0 } })).toThrow('holdMaxMs');
			expect(() => makePresence({ selfPreservation: { onChange: 'cb' } })).toThrow('onChange');
			presence = null;
		});

		it('exposes the accessor, inactive by default', () => {
			makePresence();
			expect(presence.selfPreservation()).toEqual({ enabled: true, active: false, since: null, held: 0 });
			presence.destroy();
			makePresence({ selfPreservation: false });
			expect(presence.selfPreservation().enabled).toBe(false);
			presence.destroy();
			presence = null;
		});
	});

	describe('mass-disconnect hold', () => {
		it('holds a correlated mass death: no leaves, roster intact, guard active', async () => {
			const p = makePresence({ selfPreservation: { threshold: 0.15, minPopulation: 8, holdMaxMs: 5000 } });
			const sockets = await joinMany(p, 10);
			for (let i = 0; i < 5; i++) sockets[i].close();

			await vi.advanceTimersByTimeAsync(1000);
			p.flushDiffs();

			expect(leavesOf(platform)).toEqual([]);
			expect(p.selfPreservation()).toMatchObject({ active: true, held: 5 });
			expect(p.metrics().totalOnline).toBe(10);
			// The cluster roster still carries every user (TTLs kept refreshed).
			expect(await p.count('room')).toBe(10);
		});

		it('keeps publishing the full heartbeat roster while holding', async () => {
			const p = makePresence({ selfPreservation: { holdMaxMs: 5000 } });
			const sockets = await joinMany(p, 10);
			for (let i = 0; i < 5; i++) sockets[i].close();

			await vi.advanceTimersByTimeAsync(1000);
			const hb = platform.published.filter((x) => x.event === 'heartbeat').at(-1);
			expect(hb).toBeDefined();
			expect(Object.keys(hb.data)).toHaveLength(10);
		});

		it('evicts held sockets for real once the hold window elapses', async () => {
			const p = makePresence({ selfPreservation: { holdMaxMs: 3000 } });
			const sockets = await joinMany(p, 10);
			for (let i = 0; i < 5; i++) sockets[i].close();

			await vi.advanceTimersByTimeAsync(1000); // tick 1: hold
			expect(p.selfPreservation().active).toBe(true);
			await vi.advanceTimersByTimeAsync(4000); // past holdMaxMs
			p.flushDiffs();

			expect(p.selfPreservation()).toMatchObject({ active: false, held: 0 });
			expect(p.metrics().totalOnline).toBe(5);
			expect(leavesOf(platform).sort()).toEqual(['u0', 'u1', 'u2', 'u3', 'u4']);
		});

		it('new deaths during an active hold join the hold instead of evicting', async () => {
			const p = makePresence({ selfPreservation: { holdMaxMs: 10000 } });
			const sockets = await joinMany(p, 10);
			for (let i = 0; i < 3; i++) sockets[i].close();
			await vi.advanceTimersByTimeAsync(1000);
			expect(p.selfPreservation()).toMatchObject({ active: true, held: 3 });

			sockets[3].close(); // an individually-late casualty of the same event
			await vi.advanceTimersByTimeAsync(1000);
			p.flushDiffs();
			expect(p.selfPreservation()).toMatchObject({ active: true, held: 4 });
			expect(leavesOf(platform)).toEqual([]);
		});

		it('fires onChange on activation and on release', async () => {
			const changes = [];
			const p = makePresence({
				selfPreservation: { holdMaxMs: 2000, onChange: (active, info) => changes.push({ active, ...info }) }
			});
			const sockets = await joinMany(p, 10);
			for (let i = 0; i < 5; i++) sockets[i].close();
			await vi.advanceTimersByTimeAsync(1000);
			await vi.advanceTimersByTimeAsync(3000);

			expect(changes[0]).toMatchObject({ active: true, held: 5, total: 10 });
			expect(changes.at(-1)).toMatchObject({ active: false, held: 0 });
		});

		it('a held socket whose close handler runs late leaves the hold without a second eviction', async () => {
			const p = makePresence({ selfPreservation: { holdMaxMs: 10000 } });
			const sockets = await joinMany(p, 10);
			for (let i = 0; i < 5; i++) sockets[i].close();
			await vi.advanceTimersByTimeAsync(1000);
			expect(p.selfPreservation().held).toBe(5);

			// The runtime delivers one of the close events after all: the app's
			// close hook runs a normal leave.
			await p.leave(sockets[0], platform);
			await vi.advanceTimersByTimeAsync(1000);
			expect(p.selfPreservation().held).toBe(4);
		});
	});

	describe('non-mass behavior is unchanged', () => {
		it('an individual death below the threshold evicts immediately', async () => {
			const p = makePresence({ selfPreservation: { threshold: 0.5, minPopulation: 8, holdMaxMs: 5000 } });
			const sockets = await joinMany(p, 10);
			sockets[0].close();

			await vi.advanceTimersByTimeAsync(1000);
			p.flushDiffs();

			expect(p.selfPreservation().active).toBe(false);
			expect(p.metrics().totalOnline).toBe(9);
			expect(leavesOf(platform)).toEqual(['u0']);
		});

		it('a small population never reads as a network event', async () => {
			const p = makePresence({ selfPreservation: { threshold: 0.15, minPopulation: 8, holdMaxMs: 5000 } });
			const sockets = await joinMany(p, 4);
			for (let i = 0; i < 3; i++) sockets[i].close();

			await vi.advanceTimersByTimeAsync(1000);
			p.flushDiffs();

			expect(p.selfPreservation().active).toBe(false);
			expect(p.metrics().totalOnline).toBe(1);
		});

		it('selfPreservation: false restores unconditional immediate eviction', async () => {
			const p = makePresence({ selfPreservation: false });
			const sockets = await joinMany(p, 10);
			for (let i = 0; i < 5; i++) sockets[i].close();

			await vi.advanceTimersByTimeAsync(1000);
			p.flushDiffs();

			expect(p.selfPreservation()).toMatchObject({ enabled: false, active: false, held: 0 });
			expect(p.metrics().totalOnline).toBe(5);
			expect(leavesOf(platform)).toHaveLength(5);
		});
	});
});
