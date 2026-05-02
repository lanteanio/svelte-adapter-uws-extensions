import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createConnectionRegistry } from '../../redis/registry.js';
import { createMetrics } from '../../prometheus/index.js';
import { createCircuitBreaker } from '../../shared/breaker.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

function wsWithSession(userData, sessionId) {
	const ws = mockWs(userData);
	ws.getUserData()[WS_SESSION_ID] = sessionId;
	return ws;
}

describe('redis connection registry', () => {
	let client;
	let platform;
	let registry;

	beforeEach(() => {
		client = mockRedisClient('app:');
		platform = mockPlatform();
		registry = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId,
			heartbeat: 60000,
			ttl: 90
		});
	});

	afterEach(async () => {
		await registry.destroy();
	});

	describe('construction', () => {
		it('requires options with identify', () => {
			expect(() => createConnectionRegistry(client)).toThrow();
			expect(() => createConnectionRegistry(client, {})).toThrow('identify must be a function');
		});

		it('rejects invalid ttl', () => {
			expect(() => createConnectionRegistry(client, {
				identify: () => 'x',
				ttl: 0
			})).toThrow();
		});

		it('rejects invalid heartbeat', () => {
			expect(() => createConnectionRegistry(client, {
				identify: () => 'x',
				heartbeat: -1
			})).toThrow();
		});

		it('rejects invalid requestTimeoutMs', () => {
			expect(() => createConnectionRegistry(client, {
				identify: () => 'x',
				requestTimeoutMs: 'soon'
			})).toThrow();
		});

		it('exposes a stable instanceId per registry', () => {
			expect(typeof registry.instanceId).toBe('string');
			expect(registry.instanceId.length).toBeGreaterThan(0);

			const r2 = createConnectionRegistry(client, { identify: () => 'x' });
			expect(r2.instanceId).not.toBe(registry.instanceId);
			r2.destroy();
		});
	});

	describe('hooks.open / close', () => {
		it('registers a user with userId + sessionId', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });

			const entry = await registry.lookup('u-1');
			expect(entry).toMatchObject({
				instanceId: registry.instanceId,
				sessionId: 's-1'
			});
			expect(entry.ts).toBeGreaterThan(0);
		});

		it('skips anonymous connections (identify returns null)', async () => {
			const ws = wsWithSession({}, 's-1');
			await registry.hooks.open(ws, { platform });
			expect(registry.size()).toBe(0);
		});

		it('skips when sessionId is not stamped', async () => {
			const ws = mockWs({ userId: 'u-1' });
			await registry.hooks.open(ws, { platform });
			expect(registry.size()).toBe(0);
		});

		it('most-recent-wins: a second device replaces the first', async () => {
			const wsA = wsWithSession({ userId: 'u-1' }, 's-laptop');
			const wsB = wsWithSession({ userId: 'u-1' }, 's-phone');
			await registry.hooks.open(wsA, { platform });
			await registry.hooks.open(wsB, { platform });

			expect(registry.size()).toBe(1);
			const entry = await registry.lookup('u-1');
			expect(entry.sessionId).toBe('s-phone');
		});

		it('close removes the user from the registry', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });
			await registry.hooks.close(ws, { platform });

			expect(registry.size()).toBe(0);
			expect(await registry.lookup('u-1')).toBeNull();
		});

		it('close is safe for anonymous connections', async () => {
			const ws = wsWithSession({}, 's-1');
			await expect(registry.hooks.close(ws, { platform })).resolves.toBeUndefined();
		});

		it('close does NOT delete entries owned by another instance', async () => {
			// Pre-populate Redis as if another instance had registered the user.
			const otherInstanceId = 'other-instance-id-XYZ';
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', otherInstanceId,
				'sessionId', 's-other',
				'ts', Date.now()
			);

			// Local close hook with the same userId fires (e.g. user just
			// migrated away). The compare-and-delete must leave the other
			// instance's record intact.
			const ws = wsWithSession({ userId: 'u-1' }, 's-local');
			await registry.hooks.close(ws, { platform });

			const entry = await registry.lookup('u-1');
			expect(entry.instanceId).toBe(otherInstanceId);
			expect(entry.sessionId).toBe('s-other');
		});
	});

	describe('lookup', () => {
		it('returns null for unknown users', async () => {
			expect(await registry.lookup('nobody')).toBeNull();
		});

		it('returns the entry for a registered user', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });
			const entry = await registry.lookup('u-1');
			expect(entry).toMatchObject({ instanceId: registry.instanceId, sessionId: 's-1' });
		});
	});

	describe('request: self-targeting (origin owns the user)', () => {
		it('short-circuits to local platform.request without a Redis hop', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });

			platform.request = async (target, event, data) => {
				return { received: { target: target === ws, event, data } };
			};

			const reply = await registry.request('u-1', 'confirm', { op: 'go' });
			expect(reply.received.event).toBe('confirm');
			expect(reply.received.data).toEqual({ op: 'go' });
			expect(reply.received.target).toBe(true);
		});

		it('rejects when the user is offline', async () => {
			await expect(registry.request('nobody', 'event'))
				.rejects.toThrow(/offline/);
		});

		it('rejects when the local sessionId map has no ws (race after disconnect)', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });

			// Force the entry to look like ours but drop the local sessionToWs binding
			// by closing through the hook (which deletes both local maps and Redis).
			await registry.hooks.close(ws, { platform });
			await expect(registry.request('u-1', 'event')).rejects.toThrow(/offline/);
		});

		it('propagates handler errors from local platform.request', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });
			platform.request = async () => { throw new Error('rejected'); };

			await expect(registry.request('u-1', 'confirm')).rejects.toThrow('rejected');
		});
	});

	describe('request: cross-instance routing', () => {
		it('routes to the owning instance and resolves with the reply', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });

			platformOwner.request = async (targetWs, event, data) => {
				return { ok: true, event, data, sameWs: targetWs === ws };
			};

			// Origin instance does NOT own the user; uses cross-instance path.
			const reply = await registry.request('u-1', 'confirm', { id: 42 });
			expect(reply.ok).toBe(true);
			expect(reply.event).toBe('confirm');
			expect(reply.data).toEqual({ id: 42 });
			expect(reply.sameWs).toBe(true);

			await owner.destroy();
		});

		it('rejects with a handler error when the owning instance throws', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });
			platformOwner.request = async () => { throw new Error('handler kaboom'); };

			await expect(registry.request('u-1', 'event')).rejects.toThrow(/handler kaboom/);
			await owner.destroy();
		});

		it('rejects with offline when the owning instance no longer has the ws', async () => {
			// Owner registers, then drops the ws locally without removing the
			// Redis entry (simulates a crash mid-flight).
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });

			// Simulate the owner crashing and losing its local sessionToWs map
			// without dropping the Redis registry entry.
			await owner.destroy();

			// Re-register the same userId in Redis under a fresh phantom
			// instanceId so origin's request finds an "owning" instance whose
			// subscriber is gone. The request must time out (no reply path).
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', 'phantom-instance',
				'sessionId', 's-1',
				'ts', Date.now()
			);

			await expect(
				registry.request('u-1', 'event', null, { timeoutMs: 50 })
			).rejects.toThrow(/timed out/);
		});
	});

	describe('request: timeout handling', () => {
		it('rejects with a timeout error when no reply arrives', async () => {
			// Plant an entry with no live owning subscriber.
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', 'no-listener',
				'sessionId', 's-1',
				'ts', Date.now()
			);

			await expect(
				registry.request('u-1', 'event', null, { timeoutMs: 30 })
			).rejects.toThrow(/timed out/);
		});

		it('uses the configured default timeoutMs when no per-call override is passed', async () => {
			const fast = createConnectionRegistry(client, {
				identify: () => 'x',
				requestTimeoutMs: 30
			});
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', 'no-listener',
				'sessionId', 's-1',
				'ts', Date.now()
			);
			await expect(fast.request('u-1', 'event')).rejects.toThrow(/timed out/);
			await fast.destroy();
		});
	});

	describe('late replies (mid-flight migration)', () => {
		it('drops a reply whose request already timed out and increments the counter', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics,
				requestTimeoutMs: 30
			});

			// Set up an owner whose handler delays past the timeout.
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });
			platformOwner.request = async () => {
				await new Promise((r) => setTimeout(r, 60));
				return { tooLate: true };
			};

			await expect(
				local.request('u-1', 'event', null, { timeoutMs: 30 })
			).rejects.toThrow(/timed out/);

			// Wait for the owner's reply to arrive after timeout.
			await new Promise((r) => setTimeout(r, 80));

			const out = await metrics.serialize();
			expect(out).toMatch(/push_late_replies_total\s+1/);
			expect(out).toMatch(/push_requests_total\{result="timeout"\}\s+1/);

			await owner.destroy();
			await local.destroy();
		});
	});

	describe('inbound: no local ws (offline at receive)', () => {
		it('replies with offline error when the sessionId is not local', async () => {
			// Owner registers user but local sessionToWs is empty by manipulating
			// state through a separate registry that wrote to Redis but lost ws.
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });

			// Drop the ws locally (simulate connection close that didn't propagate
			// to the close hook yet). We do this by mutating the sessionToWs map
			// indirectly: trigger close hook then re-write the Redis entry to
			// point back to the same instance + same session.
			await owner.hooks.close(ws, { platform: platformOwner });
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', owner.instanceId,
				'sessionId', 's-1',
				'ts', Date.now()
			);

			await expect(
				registry.request('u-1', 'event', null, { timeoutMs: 100 })
			).rejects.toThrow(/offline/);

			await owner.destroy();
		});
	});

	describe('metrics', () => {
		it('counts request results by outcome', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics
			});

			// Self-target ok
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await local.hooks.open(ws, { platform });
			platform.request = async () => ({ ok: true });
			await local.request('u-1', 'event');

			// Offline
			await expect(local.request('nobody', 'event')).rejects.toThrow();

			const out = await metrics.serialize();
			expect(out).toMatch(/push_requests_total\{result="ok"\}\s+1/);
			expect(out).toMatch(/push_requests_total\{result="offline"\}\s+1/);
			await local.destroy();
		});

		it('records reply latency on success', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await local.hooks.open(ws, { platform });
			platform.request = async () => ({ ok: true });
			await local.request('u-1', 'event');

			const out = await metrics.serialize();
			expect(out).toMatch(/push_reply_latency_ms_count\s+1/);
			await local.destroy();
		});

		it('exposes registry size as a gauge that scrapes from local map', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics
			});
			const ws1 = wsWithSession({ userId: 'u-1' }, 's-1');
			const ws2 = wsWithSession({ userId: 'u-2' }, 's-2');
			await local.hooks.open(ws1, { platform });
			await local.hooks.open(ws2, { platform });

			const out = await metrics.serialize();
			expect(out).toMatch(/push_registry_size\s+2/);
			await local.destroy();
		});
	});

	describe('breaker integration', () => {
		it('lookup returns null without throwing when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			breaker.failure(new Error('down'));
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				breaker
			});
			expect(await local.lookup('u-1')).toBeNull();
			await local.destroy();
		});

		it('request rejects with offline when breaker is broken (lookup short-circuits)', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			breaker.failure(new Error('down'));
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				breaker
			});
			await expect(local.request('u-1', 'event')).rejects.toThrow(/offline/);
			await local.destroy();
		});
	});

	describe('sendCoalesced: self-targeting', () => {
		it('short-circuits to local platform.sendCoalesced without a Redis hop', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });

			await registry.sendCoalesced('u-1', {
				key: 'cursor:doc-7',
				topic: 'doc:7',
				event: 'cursor',
				data: { x: 10, y: 20 }
			});

			expect(platform.sentCoalesced).toHaveLength(1);
			expect(platform.sentCoalesced[0]).toMatchObject({
				ws,
				key: 'cursor:doc-7',
				topic: 'doc:7',
				event: 'cursor',
				data: { x: 10, y: 20 }
			});
		});

		it('silently drops when the user is offline', async () => {
			await expect(
				registry.sendCoalesced('nobody', { key: 'k', topic: 't', event: 'e' })
			).resolves.toBeUndefined();
			expect(platform.sentCoalesced).toHaveLength(0);
		});

		it('silently drops when the local sessionToWs has no ws (race after disconnect)', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });
			await registry.hooks.close(ws, { platform });

			await registry.sendCoalesced('u-1', { key: 'k', topic: 't', event: 'e' });
			expect(platform.sentCoalesced).toHaveLength(0);
		});
	});

	describe('sendCoalesced: cross-instance routing', () => {
		it('publishes the coalesced envelope on the owning instance push channel', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });

			await registry.sendCoalesced('u-1', {
				key: 'cursor:doc-7',
				topic: 'doc:7',
				event: 'cursor',
				data: { x: 100, y: 200 }
			});

			// Allow the pubsub dispatch + microtask to process.
			await new Promise((r) => setImmediate(r));

			expect(platformOwner.sentCoalesced).toHaveLength(1);
			expect(platformOwner.sentCoalesced[0]).toMatchObject({
				ws,
				key: 'cursor:doc-7',
				topic: 'doc:7',
				event: 'cursor',
				data: { x: 100, y: 200 }
			});

			await owner.destroy();
		});

		it('drops on the receiver when the local sessionId no longer maps to a ws', async () => {
			const metrics = createMetrics();
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics
			});

			// Owner registers and immediately drops the ws locally without a
			// Redis cleanup, then the remote envelope arrives.
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });
			await owner.hooks.close(ws, { platform: platformOwner });

			// Re-plant the registry entry so origin still routes to owner.
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', owner.instanceId,
				'sessionId', 's-1',
				'ts', Date.now()
			);

			await registry.sendCoalesced('u-1', { key: 'k', topic: 't', event: 'e', data: 1 });
			await new Promise((r) => setImmediate(r));

			expect(platformOwner.sentCoalesced).toHaveLength(0);
			const out = await metrics.serialize();
			expect(out).toMatch(/push_coalesced_total\{result="late"\}\s+1/);

			await owner.destroy();
		});

		it('does not return a reply (fire-and-forget)', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });

			const result = await registry.sendCoalesced('u-1', {
				key: 'k', topic: 't', event: 'e'
			});
			expect(result).toBeUndefined();
			await owner.destroy();
		});
	});

	describe('sendCoalesced: validation', () => {
		it('rejects an empty target', async () => {
			await expect(
				registry.sendCoalesced('', { key: 'k', topic: 't', event: 'e' })
			).rejects.toThrow(/non-empty/);
		});

		it('rejects a non-object message', async () => {
			await expect(registry.sendCoalesced('u', null)).rejects.toThrow();
			await expect(registry.sendCoalesced('u', 'string')).rejects.toThrow();
		});

		it('rejects a missing or non-string key', async () => {
			await expect(
				registry.sendCoalesced('u', { topic: 't', event: 'e' })
			).rejects.toThrow(/key/);
			await expect(
				registry.sendCoalesced('u', { key: '', topic: 't', event: 'e' })
			).rejects.toThrow(/key/);
		});
	});

	describe('sendCoalesced: metrics', () => {
		it('counts outcomes ok | self | offline | late', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics
			});

			// self
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await local.hooks.open(ws, { platform });
			await local.sendCoalesced('u-1', { key: 'k', topic: 't', event: 'e' });

			// offline
			await local.sendCoalesced('nobody', { key: 'k', topic: 't', event: 'e' });

			// ok (cross-instance) -- spin up a sibling owner
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (w) => w.getUserData()?.userId
			});
			const wsB = wsWithSession({ userId: 'u-2' }, 's-2');
			await owner.hooks.open(wsB, { platform: platformOwner });

			await local.sendCoalesced('u-2', { key: 'k', topic: 't', event: 'e' });
			await new Promise((r) => setImmediate(r));

			const out = await metrics.serialize();
			expect(out).toMatch(/push_coalesced_total\{result="self"\}\s+1/);
			expect(out).toMatch(/push_coalesced_total\{result="offline"\}\s+1/);
			expect(out).toMatch(/push_coalesced_total\{result="ok"\}\s+1/);

			await owner.destroy();
			await local.destroy();
		});
	});

	describe('send: self-targeting', () => {
		it('short-circuits to local platform.send without a Redis hop', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });

			await registry.send('u-1', 'notifications', 'incoming', { id: 42 });

			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0]).toMatchObject({
				ws,
				topic: 'notifications',
				event: 'incoming',
				data: { id: 42 }
			});
		});

		it('silently drops when the user is offline', async () => {
			await expect(
				registry.send('nobody', 'notifications', 'incoming')
			).resolves.toBeUndefined();
			expect(platform.sent).toHaveLength(0);
		});

		it('silently drops when the local sessionToWs has no ws', async () => {
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await registry.hooks.open(ws, { platform });
			await registry.hooks.close(ws, { platform });

			await registry.send('u-1', 'notifications', 'incoming');
			expect(platform.sent).toHaveLength(0);
		});
	});

	describe('send: cross-instance routing', () => {
		it('routes the envelope and calls platform.send on the owning instance', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });

			await registry.send('u-1', 'notifications', 'incoming', { id: 42 });
			await new Promise((r) => setImmediate(r));

			expect(platformOwner.sent).toHaveLength(1);
			expect(platformOwner.sent[0]).toMatchObject({
				ws,
				topic: 'notifications',
				event: 'incoming',
				data: { id: 42 }
			});

			await owner.destroy();
		});

		it('drops with a late counter increment when the receiver no longer has the ws', async () => {
			const metrics = createMetrics();
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics
			});

			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });
			await owner.hooks.close(ws, { platform: platformOwner });

			// Re-plant the registry entry so origin still routes to owner.
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', owner.instanceId,
				'sessionId', 's-1',
				'ts', Date.now()
			);

			await registry.send('u-1', 'topic', 'event', null);
			await new Promise((r) => setImmediate(r));

			expect(platformOwner.sent).toHaveLength(0);
			const out = await metrics.serialize();
			expect(out).toMatch(/push_sends_total\{result="late"\}\s+1/);

			await owner.destroy();
		});

		it('does not return a reply (fire-and-forget)', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId
			});
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await owner.hooks.open(ws, { platform: platformOwner });

			const result = await registry.send('u-1', 't', 'e');
			expect(result).toBeUndefined();
			await owner.destroy();
		});
	});

	describe('send: validation', () => {
		it('rejects an empty target', async () => {
			await expect(registry.send('', 't', 'e')).rejects.toThrow(/non-empty/);
		});

		it('rejects a missing topic', async () => {
			await expect(registry.send('u', '', 'e')).rejects.toThrow(/topic/);
		});

		it('rejects a missing event', async () => {
			await expect(registry.send('u', 't', '')).rejects.toThrow(/event/);
		});
	});

	describe('send: metrics', () => {
		it('counts outcomes self | offline | ok', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				metrics
			});

			// self
			const ws = wsWithSession({ userId: 'u-1' }, 's-1');
			await local.hooks.open(ws, { platform });
			await local.send('u-1', 't', 'e', { x: 1 });

			// offline
			await local.send('nobody', 't', 'e');

			// ok (cross-instance) -- spin up a sibling owner
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (w) => w.getUserData()?.userId
			});
			const wsB = wsWithSession({ userId: 'u-2' }, 's-2');
			await owner.hooks.open(wsB, { platform: platformOwner });

			await local.send('u-2', 't', 'e');
			await new Promise((r) => setImmediate(r));

			const out = await metrics.serialize();
			expect(out).toMatch(/push_sends_total\{result="self"\}\s+1/);
			expect(out).toMatch(/push_sends_total\{result="offline"\}\s+1/);
			expect(out).toMatch(/push_sends_total\{result="ok"\}\s+1/);

			await owner.destroy();
			await local.destroy();
		});
	});

	describe('destroy', () => {
		it('rejects all in-flight requests', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				requestTimeoutMs: 5000
			});
			// Plant an entry pointing at a phantom instance so the request
			// stays in-flight (no listener on the phantom push channel).
			await client.redis.hset(
				client.key('conns:u-1'),
				'instanceId', 'phantom',
				'sessionId', 's-1',
				'ts', Date.now()
			);
			const inflight = local.request('u-1', 'event');
			// Allow microtasks to register the pending entry.
			await new Promise((r) => setImmediate(r));
			await local.destroy();
			await expect(inflight).rejects.toThrow(/destroyed/);
		});

		it('is idempotent', async () => {
			await registry.destroy();
			await expect(registry.destroy()).resolves.toBeUndefined();
		});
	});

	describe('attributes / sendTo: construction', () => {
		it('rejects a non-function attributes option', () => {
			expect(() => createConnectionRegistry(client, {
				identify: () => 'x',
				attributes: 'nope'
			})).toThrow(/attributes/);
			expect(() => createConnectionRegistry(client, {
				identify: () => 'x',
				attributes: {}
			})).toThrow(/attributes/);
		});

		it('accepts a valid attributes function', () => {
			const r = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			expect(typeof r.sendTo).toBe('function');
			r.destroy();
		});

		it('sendTo throws when no attributes option was supplied', async () => {
			await expect(
				registry.sendTo({ tenantId: 't' }, 'topic', 'event')
			).rejects.toThrow(/attributes/);
		});
	});

	describe('attributes / sendTo: storage and registry events', () => {
		it('persists attrs as a JSON-encoded field on the registry hash', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({
					tenantId: ws.getUserData()?.tenantId,
					role: ws.getUserData()?.role
				})
			});
			try {
				const ws = wsWithSession({ userId: 'u-1', tenantId: 't42', role: 'admin' }, 's-1');
				await local.hooks.open(ws, { platform });

				const raw = await client.redis.hgetall(client.key('conns:u-1'));
				expect(raw.attrs).toBeDefined();
				expect(JSON.parse(raw.attrs)).toEqual({ tenantId: 't42', role: 'admin' });

				const entry = await local.lookup('u-1');
				expect(entry.attrs).toEqual({ tenantId: 't42', role: 'admin' });
			} finally {
				await local.destroy();
			}
		});

		it('omits the attrs field when attributes returns an empty object', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: () => ({})
			});
			try {
				const ws = wsWithSession({ userId: 'u-1' }, 's-1');
				await local.hooks.open(ws, { platform });

				const raw = await client.redis.hgetall(client.key('conns:u-1'));
				expect(raw.attrs).toBeUndefined();

				const entry = await local.lookup('u-1');
				expect(entry.attrs).toEqual({});
			} finally {
				await local.destroy();
			}
		});

		it('coerces number / boolean attribute values to strings; drops null / object values', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ws.getUserData()?.attrs
			});
			try {
				const ws = wsWithSession({
					userId: 'u-1',
					attrs: {
						tenantId: 't42',
						seat: 7,
						isAdmin: true,
						preferences: { theme: 'dark' }, // dropped
						deletedAt: null // dropped
					}
				}, 's-1');
				await local.hooks.open(ws, { platform });

				const entry = await local.lookup('u-1');
				expect(entry.attrs).toEqual({ tenantId: 't42', seat: '7', isAdmin: 'true' });
			} finally {
				await local.destroy();
			}
		});

		it('publishes an open event on the registry-events channel after a successful registration', async () => {
			const events = [];
			const sub = client.duplicate({ enableReadyCheck: false });
			sub.on('message', (ch, raw) => {
				if (ch === client.key('__registry-events')) {
					events.push(JSON.parse(raw));
				}
			});
			await sub.subscribe(client.key('__registry-events'));

			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const ws = wsWithSession({ userId: 'u-1', tenantId: 't42' }, 's-1');
				await local.hooks.open(ws, { platform });
				await new Promise((r) => setImmediate(r));

				expect(events).toEqual([{
					type: 'open',
					userId: 'u-1',
					instanceId: local.instanceId,
					attrs: { tenantId: 't42' }
				}]);
			} finally {
				await local.destroy();
				await sub.quit().catch(() => sub.disconnect());
			}
		});

		it('publishes a close event when the close hook fires', async () => {
			const events = [];
			const sub = client.duplicate({ enableReadyCheck: false });
			sub.on('message', (ch, raw) => {
				if (ch === client.key('__registry-events')) {
					events.push(JSON.parse(raw));
				}
			});
			await sub.subscribe(client.key('__registry-events'));

			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const ws = wsWithSession({ userId: 'u-1', tenantId: 't42' }, 's-1');
				await local.hooks.open(ws, { platform });
				await local.hooks.close(ws, { platform });
				await new Promise((r) => setImmediate(r));

				expect(events.map((e) => e.type)).toEqual(['open', 'close']);
				expect(events[1]).toEqual({
					type: 'close',
					userId: 'u-1',
					instanceId: local.instanceId
				});
			} finally {
				await local.destroy();
				await sub.quit().catch(() => sub.disconnect());
			}
		});
	});

	describe('attributes / sendTo: secondary index from registry events', () => {
		it('builds the index from sibling open events', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				// Trigger ensureSubscriber so the events listener is wired.
				const lws = wsWithSession({ userId: 'self', tenantId: 't1' }, 'self-s');
				await local.hooks.open(lws, { platform });
				await new Promise((r) => setImmediate(r));

				// Inject a sibling open event manually.
				await client.redis.publish(
					client.key('__registry-events'),
					JSON.stringify({
						type: 'open',
						userId: 'remote-u',
						instanceId: 'remote-inst',
						attrs: { tenantId: 't42' }
					})
				);
				await new Promise((r) => setImmediate(r));

				// resolveMatches isn't public, so exercise via sendTo against a
				// fake remote to confirm the index registered the user.
				let captured = null;
				const handlers = client._pubsubHandlers;
				const probe = client.duplicate({ enableReadyCheck: false });
				probe.on('message', (ch, raw) => {
					if (ch === client.key('__push:remote-inst')) captured = JSON.parse(raw);
				});
				await probe.subscribe(client.key('__push:remote-inst'));

				await local.sendTo({ tenantId: 't42' }, 'announcements', 'created', { id: 1 });
				await new Promise((r) => setImmediate(r));

				expect(captured).toMatchObject({
					type: 'sendTo',
					criteria: { tenantId: 't42' },
					topic: 'announcements',
					event: 'created'
				});

				await probe.quit().catch(() => probe.disconnect());
			} finally {
				await local.destroy();
			}
		});

		it('removes from the index on a sibling close event', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const lws = wsWithSession({ userId: 'self', tenantId: 't1' }, 'self-s');
				await local.hooks.open(lws, { platform });
				await new Promise((r) => setImmediate(r));

				await client.redis.publish(
					client.key('__registry-events'),
					JSON.stringify({
						type: 'open',
						userId: 'remote-u',
						instanceId: 'remote-inst',
						attrs: { tenantId: 't42' }
					})
				);
				await new Promise((r) => setImmediate(r));

				await client.redis.publish(
					client.key('__registry-events'),
					JSON.stringify({
						type: 'close',
						userId: 'remote-u',
						instanceId: 'remote-inst'
					})
				);
				await new Promise((r) => setImmediate(r));

				const metrics = createMetrics();
				const localM = createConnectionRegistry(client, {
					identify: (ws) => ws.getUserData()?.userId,
					attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId }),
					metrics
				});
				const lws2 = wsWithSession({ userId: 'self2', tenantId: 't1' }, 'self2-s');
				await localM.hooks.open(lws2, { platform });
				await new Promise((r) => setImmediate(r));

				// Now sendTo for tenant t42 from a fresh registry should resolve empty.
				await localM.sendTo({ tenantId: 't42' }, 'topic', 'event');
				const out = await metrics.serialize();
				expect(out).toMatch(/push_sendto_total\{result="empty"\}\s+1/);

				await localM.destroy();
			} finally {
				await local.destroy();
			}
		});

		it('updates the index when a re-registration changes attrs', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId }),
				metrics
			});
			try {
				const lws = wsWithSession({ userId: 'self', tenantId: 't1' }, 'self-s');
				await local.hooks.open(lws, { platform });
				await new Promise((r) => setImmediate(r));

				const captured = [];
				const probe = client.duplicate({ enableReadyCheck: false });
				probe.on('message', (ch, raw) => captured.push({ ch, env: JSON.parse(raw) }));
				await probe.subscribe(
					client.key('__push:inst-A'),
					client.key('__push:inst-B')
				);

				// Initial registration on tenant 't42' / inst-A.
				await client.redis.publish(
					client.key('__registry-events'),
					JSON.stringify({
						type: 'open',
						userId: 'mover',
						instanceId: 'inst-A',
						attrs: { tenantId: 't42' }
					})
				);
				await new Promise((r) => setImmediate(r));

				// Re-registration on tenant 't99' / inst-B.
				await client.redis.publish(
					client.key('__registry-events'),
					JSON.stringify({
						type: 'open',
						userId: 'mover',
						instanceId: 'inst-B',
						attrs: { tenantId: 't99' }
					})
				);
				await new Promise((r) => setImmediate(r));

				// t42 should be empty (mover was reindexed under t99).
				captured.length = 0;
				await local.sendTo({ tenantId: 't42' }, 'topic', 'event');
				await new Promise((r) => setImmediate(r));
				expect(captured).toHaveLength(0);

				// t99 should resolve to one envelope on inst-B's push channel.
				await local.sendTo({ tenantId: 't99' }, 'topic', 'event');
				await new Promise((r) => setImmediate(r));
				expect(captured).toHaveLength(1);
				expect(captured[0].ch).toBe(client.key('__push:inst-B'));

				const out = await metrics.serialize();
				expect(out).toMatch(/push_sendto_total\{result="empty"\}\s+1/);
				expect(out).toMatch(/push_sendto_total\{result="ok"\}\s+1/);

				await probe.quit().catch(() => probe.disconnect());
			} finally {
				await local.destroy();
			}
		});
	});

	describe('attributes / sendTo: bootstrap from existing Redis state', () => {
		it('populates the index from existing hash entries on activate', async () => {
			// Plant entries before any registry boots up.
			await client.redis.hset(
				client.key('conns:pre-1'),
				'instanceId', 'inst-A',
				'sessionId', 'pre-1-s',
				'ts', Date.now(),
				'attrs', JSON.stringify({ tenantId: 't42', role: 'admin' })
			);
			await client.redis.hset(
				client.key('conns:pre-2'),
				'instanceId', 'inst-A',
				'sessionId', 'pre-2-s',
				'ts', Date.now(),
				'attrs', JSON.stringify({ tenantId: 't42', role: 'member' })
			);
			await client.redis.hset(
				client.key('conns:pre-3'),
				'instanceId', 'inst-B',
				'sessionId', 'pre-3-s',
				'ts', Date.now(),
				'attrs', JSON.stringify({ tenantId: 't99' })
			);

			let captured = [];
			const probe = client.duplicate({ enableReadyCheck: false });
			probe.on('message', (ch, raw) => {
				if (ch.startsWith(client.key('__push:'))) {
					captured.push({ ch, env: JSON.parse(raw) });
				}
			});
			await probe.subscribe(client.key('__push:inst-A'), client.key('__push:inst-B'));

			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				// Trigger activate via a no-op open to ensure the bootstrap fires.
				const ws = wsWithSession({ userId: 'self', tenantId: 't1' }, 'self-s');
				await local.hooks.open(ws, { platform });
				// Bootstrap is async; let the event loop drain.
				await new Promise((r) => setImmediate(r));
				await new Promise((r) => setImmediate(r));

				captured = []; // ignore the open event from self
				await local.sendTo({ tenantId: 't42' }, 'topic', 'event');
				await new Promise((r) => setImmediate(r));

				// Both pre-1 and pre-2 are on inst-A, so one envelope on inst-A.
				const aEnvs = captured.filter((c) => c.ch === client.key('__push:inst-A'));
				expect(aEnvs).toHaveLength(1);
				expect(aEnvs[0].env).toMatchObject({
					type: 'sendTo',
					criteria: { tenantId: 't42' },
					topic: 'topic',
					event: 'event'
				});
			} finally {
				await local.destroy();
				await probe.quit().catch(() => probe.disconnect());
			}
		});
	});

	describe('attributes / sendTo: routing', () => {
		it('self-targeting matches deliver via local platform.send without a Redis hop', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const ws1 = wsWithSession({ userId: 'u-1', tenantId: 't42' }, 's-1');
				const ws2 = wsWithSession({ userId: 'u-2', tenantId: 't42' }, 's-2');
				const ws3 = wsWithSession({ userId: 'u-3', tenantId: 't99' }, 's-3');
				await local.hooks.open(ws1, { platform });
				await local.hooks.open(ws2, { platform });
				await local.hooks.open(ws3, { platform });
				await new Promise((r) => setImmediate(r));

				platform.sent.length = 0;
				await local.sendTo({ tenantId: 't42' }, 'announcements', 'created', { id: 1 });
				await new Promise((r) => setImmediate(r));

				// Both u-1 and u-2 received; u-3 (different tenant) did not.
				expect(platform.sent).toHaveLength(2);
				const wssReached = new Set(platform.sent.map((s) => s.ws));
				expect(wssReached).toEqual(new Set([ws1, ws2]));
				expect(platform.sent[0]).toMatchObject({
					topic: 'announcements',
					event: 'created',
					data: { id: 1 }
				});
			} finally {
				await local.destroy();
			}
		});

		it('compound criteria intersect attributes (AND semantics)', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => {
					const ud = ws.getUserData();
					return { tenantId: ud.tenantId, role: ud.role };
				}
			});
			try {
				const wsAdmin = wsWithSession({ userId: 'u-1', tenantId: 't42', role: 'admin' }, 's-1');
				const wsMember = wsWithSession({ userId: 'u-2', tenantId: 't42', role: 'member' }, 's-2');
				const wsOtherTenant = wsWithSession({ userId: 'u-3', tenantId: 't99', role: 'admin' }, 's-3');
				await local.hooks.open(wsAdmin, { platform });
				await local.hooks.open(wsMember, { platform });
				await local.hooks.open(wsOtherTenant, { platform });
				await new Promise((r) => setImmediate(r));

				platform.sent.length = 0;
				await local.sendTo({ tenantId: 't42', role: 'admin' }, 'audit', 'created', { e: 1 });
				await new Promise((r) => setImmediate(r));

				expect(platform.sent).toHaveLength(1);
				expect(platform.sent[0].ws).toBe(wsAdmin);
			} finally {
				await local.destroy();
			}
		});

		it('publishes one envelope per owning instance for cross-instance matches', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const lws = wsWithSession({ userId: 'self', tenantId: 't1' }, 'self-s');
				await local.hooks.open(lws, { platform });
				await new Promise((r) => setImmediate(r));

				// Two remote users on instance A, one remote user on instance B.
				for (const env of [
					{ userId: 'r-1', instanceId: 'inst-A', attrs: { tenantId: 't42' } },
					{ userId: 'r-2', instanceId: 'inst-A', attrs: { tenantId: 't42' } },
					{ userId: 'r-3', instanceId: 'inst-B', attrs: { tenantId: 't42' } }
				]) {
					await client.redis.publish(
						client.key('__registry-events'),
						JSON.stringify({ type: 'open', ...env })
					);
				}
				await new Promise((r) => setImmediate(r));

				const captured = [];
				const probe = client.duplicate({ enableReadyCheck: false });
				probe.on('message', (ch, raw) => captured.push({ ch, env: JSON.parse(raw) }));
				await probe.subscribe(
					client.key('__push:inst-A'),
					client.key('__push:inst-B')
				);

				await local.sendTo({ tenantId: 't42' }, 'topic', 'event');
				await new Promise((r) => setImmediate(r));

				// Two distinct push channels → two envelopes (not three, since
				// inst-A coalesces r-1 + r-2 into one).
				expect(captured).toHaveLength(2);
				const channels = new Set(captured.map((c) => c.ch));
				expect(channels).toEqual(new Set([
					client.key('__push:inst-A'),
					client.key('__push:inst-B')
				]));

				await probe.quit().catch(() => probe.disconnect());
			} finally {
				await local.destroy();
			}
		});

		it('mixed local + remote: local fires immediately, remote ships an envelope', async () => {
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const lws = wsWithSession({ userId: 'local-u', tenantId: 't42' }, 'local-s');
				await local.hooks.open(lws, { platform });
				await new Promise((r) => setImmediate(r));

				await client.redis.publish(
					client.key('__registry-events'),
					JSON.stringify({
						type: 'open',
						userId: 'remote-u',
						instanceId: 'inst-B',
						attrs: { tenantId: 't42' }
					})
				);
				await new Promise((r) => setImmediate(r));

				const captured = [];
				const probe = client.duplicate({ enableReadyCheck: false });
				probe.on('message', (ch, raw) => captured.push({ ch, env: JSON.parse(raw) }));
				await probe.subscribe(client.key('__push:inst-B'));

				platform.sent.length = 0;
				await local.sendTo({ tenantId: 't42' }, 'topic', 'event', { x: 1 });
				await new Promise((r) => setImmediate(r));

				expect(platform.sent).toHaveLength(1);
				expect(platform.sent[0].ws).toBe(lws);
				expect(captured).toHaveLength(1);
				expect(captured[0].env).toMatchObject({
					type: 'sendTo',
					criteria: { tenantId: 't42' },
					topic: 'topic',
					event: 'event',
					data: { x: 1 }
				});

				await probe.quit().catch(() => probe.disconnect());
			} finally {
				await local.destroy();
			}
		});

		it('empty-match returns without publishing or sending', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId }),
				metrics
			});
			try {
				const lws = wsWithSession({ userId: 'self', tenantId: 't1' }, 'self-s');
				await local.hooks.open(lws, { platform });
				await new Promise((r) => setImmediate(r));

				platform.sent.length = 0;
				await local.sendTo({ tenantId: 'unknown-tenant' }, 'topic', 'event');
				expect(platform.sent).toHaveLength(0);

				const out = await metrics.serialize();
				expect(out).toMatch(/push_sendto_total\{result="empty"\}\s+1/);
			} finally {
				await local.destroy();
			}
		});
	});

	describe('attributes / sendTo: receiver-side', () => {
		it('iterates the receiver own local matches at receive time', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const ws1 = wsWithSession({ userId: 'u-1', tenantId: 't42' }, 's-1');
				const ws2 = wsWithSession({ userId: 'u-2', tenantId: 't42' }, 's-2');
				const ws3 = wsWithSession({ userId: 'u-3', tenantId: 't99' }, 's-3');
				await owner.hooks.open(ws1, { platform: platformOwner });
				await owner.hooks.open(ws2, { platform: platformOwner });
				await owner.hooks.open(ws3, { platform: platformOwner });
				await new Promise((r) => setImmediate(r));

				// Synthesize an inbound sendTo envelope on owner's push channel.
				platformOwner.sent.length = 0;
				await client.redis.publish(
					client.key('__push:' + owner.instanceId),
					JSON.stringify({
						type: 'sendTo',
						criteria: { tenantId: 't42' },
						topic: 'announcements',
						event: 'created',
						data: { id: 1 }
					})
				);
				await new Promise((r) => setImmediate(r));

				// u-1 and u-2 should receive; u-3 (different tenant) should not.
				expect(platformOwner.sent).toHaveLength(2);
				const reached = new Set(platformOwner.sent.map((s) => s.ws));
				expect(reached).toEqual(new Set([ws1, ws2]));
			} finally {
				await owner.destroy();
			}
		});

		it('no-ops cleanly when the receiver has no local matches', async () => {
			const platformOwner = mockPlatform();
			const owner = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
			try {
				const ws1 = wsWithSession({ userId: 'u-1', tenantId: 't1' }, 's-1');
				await owner.hooks.open(ws1, { platform: platformOwner });
				await new Promise((r) => setImmediate(r));

				platformOwner.sent.length = 0;
				await client.redis.publish(
					client.key('__push:' + owner.instanceId),
					JSON.stringify({
						type: 'sendTo',
						criteria: { tenantId: 't42' },
						topic: 'announcements',
						event: 'created'
					})
				);
				await new Promise((r) => setImmediate(r));

				expect(platformOwner.sent).toHaveLength(0);
			} finally {
				await owner.destroy();
			}
		});
	});

	describe('attributes / sendTo: validation', () => {
		let withAttrs;
		beforeEach(() => {
			withAttrs = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId })
			});
		});
		afterEach(async () => {
			await withAttrs.destroy();
		});

		it('rejects null criteria', async () => {
			await expect(withAttrs.sendTo(null, 't', 'e')).rejects.toThrow(/criteria/);
		});

		it('rejects an empty object after normalization', async () => {
			// All values get dropped (null / object) so the result is empty.
			await expect(
				withAttrs.sendTo({ ignored: { nested: 'obj' } }, 't', 'e')
			).rejects.toThrow(/at least one/);
		});

		it('rejects a missing topic / event', async () => {
			await expect(withAttrs.sendTo({ tenantId: 't' }, '', 'e')).rejects.toThrow(/topic/);
			await expect(withAttrs.sendTo({ tenantId: 't' }, 't', '')).rejects.toThrow(/event/);
		});
	});

	describe('attributes / sendTo: metrics', () => {
		it('counts ok / empty / error outcomes on the push_sendto_total counter', async () => {
			const metrics = createMetrics();
			const local = createConnectionRegistry(client, {
				identify: (ws) => ws.getUserData()?.userId,
				attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId }),
				metrics
			});
			try {
				const ws = wsWithSession({ userId: 'self', tenantId: 't42' }, 's-self');
				await local.hooks.open(ws, { platform });
				await new Promise((r) => setImmediate(r));

				// Empty path
				await local.sendTo({ tenantId: 'nope' }, 'topic', 'event');
				// Self-only path -- sender doesn't publish anywhere remotely; ok still recorded
				// because there were matches (one local) and no publish error.
				await local.sendTo({ tenantId: 't42' }, 'topic', 'event');

				await new Promise((r) => setImmediate(r));

				const out = await metrics.serialize();
				expect(out).toMatch(/push_sendto_total\{result="empty"\}\s+1/);
				expect(out).toMatch(/push_sendto_total\{result="ok"\}\s+1/);
			} finally {
				await local.destroy();
			}
		});
	});
});
