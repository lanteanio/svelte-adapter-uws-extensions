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
import { createRedisClient } from '../../../redis/index.js';
import { createConnectionRegistry } from '../../../redis/registry.js';
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
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-registry:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Wipe under our prefix so each test starts clean.
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');
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
});
