/**
 * Integration tests for redis/registry against a real Redis 7 server.
 *
 * The high-value scenario the in-memory mock cannot fully prove is the
 * real round-trip: origin instance publishes a request envelope on the
 * owning instance's push channel, ioredis routes it through Redis, the
 * owning instance's subscriber receives it, calls platform.request, and
 * publishes the reply back on the origin's push channel. Real TCP +
 * channel routing + JSON encoding all in play. The mock-based suite at
 * test/redis/registry.test.js stays as the exhaustive surface; this file
 * pins what only a real server can prove.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys, isClusterBackend } from '../helpers/backend.js';
import { waitRedisMs } from '../helpers/backend-clock.js';
import { createConnectionRegistry } from '../../../src/redis/registry.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

function wsWithSession(userData, sessionId) {
	const ws = mockWs(userData);
	ws.getUserData()[WS_SESSION_ID] = sessionId;
	return ws;
}

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('redis connection registry (integration)', () => {
	let client;
	const registries = [];

	beforeAll(() => {
		client = createBackendClient({
			keyPrefix: 'inttest-registry:'
		});
	});

	beforeEach(async () => {
		// Wipe under our prefix so each test starts clean.
		await resetBackendKeys(client);
	});

	afterEach(async () => {
		while (registries.length) {
			const r = registries.pop();
			await r.destroy().catch(() => {});
		}
	});

	afterAll(async () => {
		await client.quit();
	});

	function makeRegistry(opts = {}) {
		const r = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId,
			heartbeat: 60000,
			ttl: 90,
			...opts
		});
		registries.push(r);
		return r;
	}

	it('writes the entry shape Redis sees on registration', async () => {
		const registry = makeRegistry();
		const platform = mockPlatform();
		const ws = wsWithSession({ userId: 'alice' }, 's-alice');
		await registry.hooks.open(ws, { platform });

		const fields = await client.redis.hgetall(client.key('conns:alice'));
		expect(fields.instanceId).toBe(registry.instanceId);
		expect(fields.sessionId).toBe('s-alice');
		expect(Number(fields.ts)).toBeGreaterThan(0);

		const ttl = await client.redis.ttl(client.key('conns:alice'));
		expect(ttl).toBeGreaterThan(0);
		expect(ttl).toBeLessThanOrEqual(90);
	});

	it('heartbeat refreshes TTL on every locally-owned user (multi-slot expire pipeline)', async () => {
		const platform = mockPlatform();
		const registry = makeRegistry({ ttl: 2, heartbeat: 250 });
		const userIds = ['hb1', 'hb2', 'hb3', 'hb4'];
		for (const userId of userIds) {
			await registry.hooks.open(wsWithSession({ userId }, 's-' + userId), { platform });
		}

		// ttl is 2s but the heartbeat refreshes every 250ms; wait past the ttl
		// on Redis's OWN clock (TIME), where the key TTLs decay, so the window
		// really elapsed despite host/VM clock drift. On a cluster the
		// per-user keys span slots, so a single pipeline would silently drop
		// the other-node EXPIREs and those users would expire.
		await waitRedisMs(client, 2400);

		for (const userId of userIds) {
			expect(await client.redis.exists(client.key('conns:' + userId))).toBe(1);
			expect(await client.redis.ttl(client.key('conns:' + userId))).toBeGreaterThan(0);
		}
	});

	it('cross-instance request: origin -> Redis -> owner -> Redis -> origin', async () => {
		const platformA = mockPlatform();
		const platformB = mockPlatform();
		const regOrigin = makeRegistry();
		const regOwner = makeRegistry();

		const ws = wsWithSession({ userId: 'bob' }, 's-bob');
		await regOwner.hooks.open(ws, { platform: platformB });
		// Origin needs an active subscriber to receive the reply; touch its
		// hooks once to bring the subscriber up.
		const filler = wsWithSession({ userId: 'origin-only' }, 's-origin');
		await regOrigin.hooks.open(filler, { platform: platformA });

		platformB.request = async (targetWs, event, data) => {
			expect(targetWs).toBe(ws);
			return { ok: true, event, echo: data };
		};

		const reply = await regOrigin.request('bob', 'confirm', { id: 42 }, { timeoutMs: 2000 });
		expect(reply).toEqual({ ok: true, event: 'confirm', echo: { id: 42 } });
	});

	it('compare-and-delete: close on origin does not remove an entry owned by another instance', async () => {
		const platformA = mockPlatform();
		const platformB = mockPlatform();
		const regA = makeRegistry();
		const regB = makeRegistry();

		const wsA = wsWithSession({ userId: 'carol' }, 's-A');
		const wsB = wsWithSession({ userId: 'carol' }, 's-B');

		await regA.hooks.open(wsA, { platform: platformA });
		// Migration: B writes second so most-recent-wins.
		await regB.hooks.open(wsB, { platform: platformB });

		// Now A's stale close hook fires.
		await regA.hooks.close(wsA, { platform: platformA });

		// The Redis entry must still point at B.
		const entry = await regB.lookup('carol');
		expect(entry.instanceId).toBe(regB.instanceId);
		expect(entry.sessionId).toBe('s-B');
	});

	it('timeout: request rejects when the owning instance has no live subscriber', async () => {
		const regOrigin = makeRegistry();
		const platformA = mockPlatform();
		const filler = wsWithSession({ userId: 'origin-only' }, 's-origin');
		await regOrigin.hooks.open(filler, { platform: platformA });

		// Plant a phantom entry pointing at an instanceId nobody is listening on.
		await client.redis.hset(
			client.key('conns:ghost'),
			'instanceId', 'phantom-xyz',
			'sessionId', 's-ghost',
			'ts', Date.now()
		);

		await expect(
			regOrigin.request('ghost', 'event', null, { timeoutMs: 50 })
		).rejects.toThrow(/timed out/);
	});

	it('self-targeting still works alongside cross-instance', async () => {
		const platform = mockPlatform();
		const registry = makeRegistry();

		const ws = wsWithSession({ userId: 'dave' }, 's-dave');
		await registry.hooks.open(ws, { platform });

		platform.request = async (_, event, data) => ({ event, data, here: true });

		const reply = await registry.request('dave', 'ping', { n: 1 });
		expect(reply).toEqual({ event: 'ping', data: { n: 1 }, here: true });
	});

	it('cross-instance sendCoalesced reaches the owning instance via the push channel', async () => {
		const platformA = mockPlatform();
		const platformB = mockPlatform();
		const regOrigin = makeRegistry();
		const regOwner = makeRegistry();

		const ws = wsWithSession({ userId: 'frank' }, 's-frank');
		await regOwner.hooks.open(ws, { platform: platformB });
		// Origin needs an active subscriber to publish too; touch its hooks once.
		const filler = wsWithSession({ userId: 'origin-only' }, 's-origin');
		await regOrigin.hooks.open(filler, { platform: platformA });

		await regOrigin.sendCoalesced('frank', {
			key: 'cursor:doc-7',
			topic: 'doc:7',
			event: 'cursor',
			data: { x: 410, y: 220 }
		});

		// Allow the SPUBLISH + subscriber dispatch + microtask to land.
		await wait(50);

		expect(platformB.sentCoalesced).toHaveLength(1);
		expect(platformB.sentCoalesced[0]).toMatchObject({
			ws,
			key: 'cursor:doc-7',
			topic: 'doc:7',
			event: 'cursor',
			data: { x: 410, y: 220 }
		});
	});

	it('cross-instance send reaches the owning instance via the push channel', async () => {
		const platformA = mockPlatform();
		const platformB = mockPlatform();
		const regOrigin = makeRegistry();
		const regOwner = makeRegistry();

		const ws = wsWithSession({ userId: 'gina' }, 's-gina');
		await regOwner.hooks.open(ws, { platform: platformB });
		const filler = wsWithSession({ userId: 'origin-only' }, 's-origin');
		await regOrigin.hooks.open(filler, { platform: platformA });

		await regOrigin.send('gina', 'notifications', 'incoming', { id: 99 });
		await wait(50);

		expect(platformB.sent).toHaveLength(1);
		expect(platformB.sent[0]).toMatchObject({
			ws,
			topic: 'notifications',
			event: 'incoming',
			data: { id: 99 }
		});
	});

	it('heartbeat refreshes TTL on locally-owned entries', async () => {
		const platform = mockPlatform();
		// Short ttl + short heartbeat so we can observe the refresh in real time.
		const registry = makeRegistry({ ttl: 5, heartbeat: 1000 });

		const ws = wsWithSession({ userId: 'erin' }, 's-erin');
		await registry.hooks.open(ws, { platform });

		const ttlBefore = await client.redis.ttl(client.key('conns:erin'));
		expect(ttlBefore).toBeGreaterThan(0);

		// Wait past one heartbeat; TTL should be refreshed (not below ttlBefore - 1.5).
		await wait(1200);
		const ttlAfter = await client.redis.ttl(client.key('conns:erin'));
		expect(ttlAfter).toBeGreaterThanOrEqual(ttlBefore - 2);
	});

	describe('sendTo (cross-instance attribute filter)', () => {
		function makeRegistryWithAttrs(opts = {}) {
			const r = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ws.getUserData()?.attrs,
				heartbeat: 60000,
				ttl: 90,
				...opts
			});
			registries.push(r);
			return r;
		}

		function wsWithAttrs(userId, sessionId, attrs) {
			return wsWithSession({ userId, attrs }, sessionId);
		}

		it('reaches a remote user via the secondary index after the events channel propagates', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const regA = makeRegistryWithAttrs();
			const regB = makeRegistryWithAttrs();

			// Both instances need an active subscriber + bootstrapped index
			// before B's open broadcasts. Touch both registries with a noop
			// open so subscribe(...) and bootstrapIndex run before the events
			// fly.
			const fillerA = wsWithAttrs('filler-a', 's-fa', { role: 'noop' });
			const fillerB = wsWithAttrs('filler-b', 's-fb', { role: 'noop' });
			await regA.hooks.open(fillerA, { platform: platformA });
			await regB.hooks.open(fillerB, { platform: platformB });
			// Drain any in-flight events propagation between the two instances
			// before the targeted registration below.
			await wait(50);

			const wsTarget = wsWithAttrs('alice', 's-alice', { tenantId: 't1', role: 'admin' });
			await regB.hooks.open(wsTarget, { platform: platformB });
			// Allow the open event to reach A and update its secondary index.
			await wait(50);

			await regA.sendTo({ tenantId: 't1', role: 'admin' }, 'alerts', 'warning', { id: 1 });
			await wait(50);

			const matchingSends = platformB.sent.filter(
				(s) => s.ws === wsTarget && s.topic === 'alerts' && s.event === 'warning'
			);
			expect(matchingSends).toHaveLength(1);
			expect(matchingSends[0].data).toEqual({ id: 1 });
			// Origin's own connection (attrs.role='noop') must NOT have been hit.
			expect(platformA.sent.find((s) => s.topic === 'alerts')).toBeUndefined();
		});

		it('bootstraps the secondary index via SCAN against pre-existing entries', async () => {
			const platformExisting = mockPlatform();
			const regExisting = makeRegistryWithAttrs();

			// Pre-existing registration on regExisting; the events broadcast
			// went out, but a brand-new instance starting later cannot
			// retroactively receive it.
			const ws = wsWithAttrs('bob', 's-bob', { tenantId: 't2' });
			await regExisting.hooks.open(ws, { platform: platformExisting });

			// New instance comes up after the registration. ensureSubscriber
			// fires bootstrapIndex which SCANs and pulls bob's attrs into the
			// fresh index without seeing the original `open` event.
			const platformLate = mockPlatform();
			const regLate = makeRegistryWithAttrs();
			const fillerLate = wsWithAttrs('filler-late', 's-fl', { role: 'noop' });
			await regLate.hooks.open(fillerLate, { platform: platformLate });
			// Bootstrap is async (best-effort); allow SCAN + hgetall round trips.
			await wait(150);

			await regLate.sendTo({ tenantId: 't2' }, 'broadcasts', 'hello', { ok: true });
			await wait(50);

			const hits = platformExisting.sent.filter(
				(s) => s.ws === ws && s.topic === 'broadcasts' && s.event === 'hello'
			);
			expect(hits).toHaveLength(1);
			expect(hits[0].data).toEqual({ ok: true });
		});

		it('compound criteria intersect across keys (AND semantics) over the wire', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const regA = makeRegistryWithAttrs();
			const regB = makeRegistryWithAttrs();

			// Subscriber bring-up + drain.
			const fillerA = wsWithAttrs('filler-a', 's-fa', { role: 'noop' });
			const fillerB = wsWithAttrs('filler-b', 's-fb', { role: 'noop' });
			await regA.hooks.open(fillerA, { platform: platformA });
			await regB.hooks.open(fillerB, { platform: platformB });
			await wait(50);

			const wsAdmin = wsWithAttrs('admin-1', 's-adm', { tenantId: 't3', role: 'admin' });
			const wsUser = wsWithAttrs('user-1', 's-usr', { tenantId: 't3', role: 'user' });
			await regB.hooks.open(wsAdmin, { platform: platformB });
			await regB.hooks.open(wsUser, { platform: platformB });
			await wait(50);

			await regA.sendTo({ tenantId: 't3', role: 'admin' }, 'alerts', 'critical', { x: 1 });
			await wait(50);

			const adminHits = platformB.sent.filter((s) => s.ws === wsAdmin && s.event === 'critical');
			const userHits = platformB.sent.filter((s) => s.ws === wsUser && s.event === 'critical');
			expect(adminHits).toHaveLength(1);
			expect(userHits).toHaveLength(0);
		});

		it('self-targeting bucket short-circuits without a Redis hop', async () => {
			const platform = mockPlatform();
			const registry = makeRegistryWithAttrs();

			const wsLocal = wsWithAttrs('local-1', 's-loc', { tenantId: 'self' });
			await registry.hooks.open(wsLocal, { platform });
			// No wait needed: applyOpenEvent runs synchronously inside the open
			// hook for the local instance, so the index is hot before sendTo
			// returns. This is the exact path that exists to keep self-targeted
			// sendTo deliverable without a round trip.

			await registry.sendTo({ tenantId: 'self' }, 't', 'e', { v: 1 });
			// No `await wait(...)` - the local branch fans out synchronously
			// against the local map.
			const hits = platform.sent.filter((s) => s.ws === wsLocal && s.event === 'e');
			expect(hits).toHaveLength(1);
			expect(hits[0].data).toEqual({ v: 1 });
		});

		it('sendTo with no matches no-ops without throwing', async () => {
			const platform = mockPlatform();
			const registry = makeRegistryWithAttrs();
			const ws = wsWithAttrs('alone', 's-alone', { tenantId: 'X' });
			await registry.hooks.open(ws, { platform });

			await registry.sendTo({ tenantId: 'no-such-tenant' }, 't', 'e', { v: 1 });
			expect(platform.sent.find((s) => s.event === 'e')).toBeUndefined();
		});
	});

	// Redis Cluster gap: this composes the registry with presence.join, whose
	// JOIN_SCRIPT is a 2-key eval without a shared {hash-tag} and CROSSSLOTs on the
	// cluster (see the presence cluster gap). The registry's own single-key state +
	// pub/sub routing is cluster-safe; only the presence-composition block is skipped
	// on the cluster backend.
	(isClusterBackend() ? describe.skip : describe)('composition with presence (no key / channel collision)', () => {
		it('registry attrs route a sendTo to a user who is also in a presence room on the owning instance', async () => {
			// Both modules share one RedisClient (same keyPrefix). Registry
			// owns conns:* and __push:* / __registry-events; presence owns
			// presence:* and presence:events:*. Pin that the two namespaces
			// do not conflict by running both end-to-end over the same
			// client and verifying both deliver as expected.
			const { createPresence } = await import('../../../src/redis/presence.js');

			const platformA = mockPlatform();
			const platformB = mockPlatform();

			const regA = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ws.getUserData()?.attrs,
				heartbeat: 60000,
				ttl: 90
			});
			const regB = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ws.getUserData()?.attrs,
				heartbeat: 60000,
				ttl: 90
			});
			registries.push(regA, regB);

			const presenceA = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.userId, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});
			const presenceB = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.userId, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});

			try {
				// Bring both instances' registry subscribers up first.
				const fillerA = wsWithSession(
					{ userId: 'filler-a', name: 'FillerA', attrs: { role: 'noop' } }, 's-fa'
				);
				const fillerB = wsWithSession(
					{ userId: 'filler-b', name: 'FillerB', attrs: { role: 'noop' } }, 's-fb'
				);
				await regA.hooks.open(fillerA, { platform: platformA });
				await regB.hooks.open(fillerB, { platform: platformB });

				// Target user on instance A: registered with attrs AND in a
				// presence room.
				const wsTarget = wsWithSession(
					{ userId: 'alice', name: 'Alice', attrs: { tenantId: 't1' } }, 's-alice'
				);
				await regA.hooks.open(wsTarget, { platform: platformA });
				await presenceA.join(wsTarget, 'team:t1', platformA);

				// Allow the registry events channel propagation.
				await wait(50);

				// From B: registry.sendTo by attrs reaches the target on A
				// via the push channel.
				await regB.sendTo({ tenantId: 't1' }, 'alerts', 'urgent', { id: 1 });
				await wait(50);

				const sentToTarget = platformA.sent.filter(
					(s) => s.ws === wsTarget && s.topic === 'alerts' && s.event === 'urgent'
				);
				expect(sentToTarget).toHaveLength(1);

				// From B: presence.list('team:t1') sees the target via the
				// shared presence hash.
				const list = await presenceB.list('team:t1');
				const aliceEntry = list.find((e) => e.id === 'alice');
				expect(aliceEntry).toEqual({ id: 'alice', name: 'Alice' });

				// Sanity: registry hash AND presence hashes all exist under
				// distinct prefixed keyspaces (no overwrite from collision).
				// presence/createPresence (Design G) stores TWO hashes per
				// topic: presence:topic:{topic} for list/count, and one
				// presence:user:{topic}:{userKey} per user for the leave
				// HLEN check. The userKey here resolves to "__conn:<n>" via
				// presence's fallback because the test's userData uses
				// userId rather than id; the precise key shape does not
				// matter for this collision-sanity check - what matters is
				// that the per-user hash exists under the presence:user:
				// namespace.
				const regHashExists = await client.redis.exists(client.key('conns:alice'));
				const presenceTopicHashExists = await client.redis.exists(client.key('presence:topic:{team:t1}'));
				const presenceUserKeys = await client.redis.keys(client.key('presence:user:{team:t1}:*'));
				expect(regHashExists).toBe(1);
				expect(presenceTopicHashExists).toBe(1);
				expect(presenceUserKeys).toHaveLength(1);
			} finally {
				presenceA.destroy();
				presenceB.destroy();
			}
		});
	});

	describe('send / sendCoalesced edge cases', () => {
		it('sendCoalesced no-ops at the receiver after a fast migration to a third party', async () => {
			// A user appears on instance B, then migrates to instance C.
			// An origin's `sendCoalesced` issued mid-flight using a stale
			// lookup that still pointed at B must hit B's authoritative
			// session-shadow check and decline to deliver.
			const platformOrigin = mockPlatform();
			const platformB = mockPlatform();
			const platformC = mockPlatform();
			const regOrigin = makeRegistry();
			const regB = makeRegistry();
			const regC = makeRegistry();

			const fillerO = wsWithSession({ userId: 'origin-only' }, 's-origin');
			await regOrigin.hooks.open(fillerO, { platform: platformOrigin });

			const wsOnB = wsWithSession({ userId: 'henry' }, 's-B');
			await regB.hooks.open(wsOnB, { platform: platformB });

			// Migration: C registers second so most-recent-wins on the hash;
			// B's local map still holds 'henry' until B's close hook fires
			// (which, in a real reconnect race, hasn't happened yet).
			const wsOnC = wsWithSession({ userId: 'henry' }, 's-C');
			await regC.hooks.open(wsOnC, { platform: platformC });

			// Origin's sendCoalesced lookup happens NOW: hash points at C, so
			// the envelope lands on C's push channel, not B's. B's local map
			// is the stale one, but it never receives the envelope.
			await regOrigin.sendCoalesced('henry', {
				key: 'cursor',
				topic: 'doc',
				event: 'pos',
				data: { x: 1 }
			});
			await wait(50);

			expect(platformC.sentCoalesced).toHaveLength(1);
			expect(platformB.sentCoalesced).toHaveLength(0);
		});
	});
});
