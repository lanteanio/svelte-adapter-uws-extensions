import { afterEach, describe, expect, it } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

// A graceful shutdown does not wait for an async close hook: whatever the
// hook issued before its first await reaches Redis, and nothing after it
// does. `cutAfterSync` models that on the in-memory double: every command
// method called from the next microtask on hangs forever, as it would on a
// process that already exited.
function cutAfterSync(redis) {
	const saved = new Map();
	for (const name of Object.keys(redis)) {
		if (typeof redis[name] === 'function' && !name.startsWith('_') && name !== 'on' && name !== 'key') saved.set(name, redis[name]);
	}
	return {
		cut() {
			for (const name of saved.keys()) redis[name] = () => new Promise(() => {});
		},
		restore() {
			for (const [name, fn] of saved) redis[name] = fn;
		}
	};
}

const settle = () => new Promise((r) => setTimeout(r, 20));
const cleanups = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c(); });

describe('close hooks put their Redis writes on the wire before awaiting', () => {
	it('presence: the last connection leaving removes the entry even when the hook is cut short', async () => {
		const client = mockRedisClient('shut-p:');
		const presence = createPresence(client, { key: 'id', consistencyAuditIntervalMs: 0 });
		cleanups.push(() => presence.destroy());
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await presence.join(ws, 'room', platform);
		expect(await presence.list('room')).toHaveLength(1);

		const wire = cutAfterSync(client.redis);
		const closing = presence.hooks.close(ws, { platform });
		await Promise.resolve();
		wire.cut();
		await settle();
		wire.restore();

		expect(await presence.list('room')).toEqual([]);
		void closing;
	});

	it('presence: a single-topic leave of the last connection does the same', async () => {
		const client = mockRedisClient('shut-pt:');
		const presence = createPresence(client, { key: 'id', consistencyAuditIntervalMs: 0 });
		cleanups.push(() => presence.destroy());
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await presence.join(ws, 'room', platform);

		const wire = cutAfterSync(client.redis);
		const leaving = presence.leave(ws, platform, 'room');
		await Promise.resolve();
		wire.cut();
		await settle();
		wire.restore();

		expect(await presence.list('room')).toEqual([]);
		void leaving;
	});

	it('registry: the row, the app-session row and the close event all go out', async () => {
		const client = mockRedisClient('shut-r:');
		const registry = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId,
			attributes: (ws) => ({ tenantId: ws.getUserData()?.tenantId }),
			sessionIdentify: (ws) => ws.getUserData()?.sid
		});
		cleanups.push(() => registry.destroy());
		const platform = mockPlatform();
		const ws = mockWs({ userId: 'u1', tenantId: 't', sid: 'app-1' });
		ws.getUserData()[WS_SESSION_ID] = 's1';
		await registry.hooks.open(ws, { platform });

		const events = [];
		const probe = client.duplicate();
		probe.on('message', (ch, raw) => { if (ch === client.key('__registry-events')) events.push(JSON.parse(raw)); });
		await probe.subscribe(client.key('__registry-events'));
		cleanups.push(() => probe.quit());

		expect(await client.redis.exists(client.key('conns:u1'))).toBe(1);
		expect(await client.redis.exists(client.key('sess:app-1'))).toBe(1);

		const wire = cutAfterSync(client.redis);
		const closing = registry.hooks.close(ws, { platform });
		await Promise.resolve();
		wire.cut();
		await settle();
		wire.restore();

		expect(await client.redis.exists(client.key('conns:u1'))).toBe(0);
		expect(await client.redis.exists(client.key('sess:app-1'))).toBe(0);
		expect(events).toEqual([expect.objectContaining({ type: 'close', userId: 'u1', instanceId: registry.instanceId })]);
		void closing;
	});
});
