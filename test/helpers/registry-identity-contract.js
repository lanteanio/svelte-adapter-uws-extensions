import { describe, it, expect, afterEach } from 'vitest';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { scanKeys } from '../../src/shared/redis-scan.js';
import { mockPlatform } from './mock-platform.js';
import { mockWs } from './mock-ws.js';

const identityOf = (ws) => ws.getUserData()?.identity;
const b64 = (tenantId, userId) => Buffer.from(JSON.stringify([tenantId, userId])).toString('base64url');

function socket(identity, sessionId, extra = {}) {
	const ws = mockWs({ identity, ...extra });
	ws.getUserData()[WS_SESSION_ID] = sessionId;
	return ws;
}

// Poll until `check` passes or two seconds elapse; the real server delivers
// pub/sub asynchronously.
async function until(check) {
	const deadline = Date.now() + 2000;
	for (;;) {
		try { return await check(); } catch (err) {
			if (Date.now() > deadline) throw err;
			await new Promise((resolve) => setTimeout(resolve, 10));
		}
	}
}

/**
 * Tenant-qualified connection registry, run against the in-memory mock and a
 * real server. `getClient` returns a client whose keyspace is empty per test.
 * @param {() => any} getClient
 */
export function registryIdentityContract(getClient) {
	const registries = [];
	const make = (options = {}) => {
		const registry = createConnectionRegistry(getClient(), { identifyIdentity: identityOf, heartbeat: 60000, ...options });
		registries.push(registry);
		return registry;
	};
	afterEach(async () => {
		while (registries.length) await registries.pop().destroy().catch(() => {});
	});

	describe('registry tenant-qualified identities', () => {
		it('refuses identify together with identifyIdentity and a non-function extractor', () => {
			expect(() => createConnectionRegistry(getClient(), { identify: () => 'u', identifyIdentity: identityOf })).toThrow('mutually exclusive');
			expect(() => createConnectionRegistry(getClient(), { identifyIdentity: 'identity' })).toThrow('identifyIdentity must be a function');
		});

		it('keeps equal user ids in two tenants and the null tenant apart', async () => {
			const platform = mockPlatform();
			const registry = make();
			for (const [tenantId, sid] of [['ta', 's-a'], ['tb', 's-b'], [null, 's-n']]) {
				await registry.hooks.open(socket({ tenantId, userId: '42' }, sid), { platform });
			}
			expect(await registry.lookup({ tenantId: 'ta', userId: '42' })).toMatchObject({ sessionId: 's-a' });
			expect(await registry.lookup({ tenantId: 'tb', userId: '42' })).toMatchObject({ sessionId: 's-b' });
			expect(await registry.lookup({ tenantId: null, userId: '42' })).toMatchObject({ sessionId: 's-n' });
			expect(await registry.lookup({ tenantId: 'tc', userId: '42' })).toBeNull();
			const rows = await scanKeys(getClient().redis, getClient().key('*conns:*'));
			expect(rows.sort()).toEqual([b64('ta', '42'), b64('tb', '42'), b64(null, '42')].map((key) => getClient().key('identity:v1:conns:' + key)).sort());
		});

		it('rejects bare string and malformed targets', async () => {
			const registry = make();
			for (const target of ['42', { userId: '42' }, { tenantId: 'a.b', userId: '42' }, { tenantId: 'ta', userId: '' }, null]) {
				await expect(registry.lookup(target), JSON.stringify(target)).rejects.toThrow(TypeError);
				await expect(registry.send(target, 'topic', 'event'), JSON.stringify(target)).rejects.toThrow(TypeError);
				await expect(registry.sendCoalesced(target, { key: 'k', topic: 't', event: 'e' })).rejects.toThrow(TypeError);
				await expect(registry.request(target, 'event')).rejects.toThrow(TypeError);
			}
		});

		it('registers nothing for a throwing or malformed extractor and closes without throwing', async () => {
			const platform = mockPlatform();
			const registry = make({ identifyIdentity: (ws) => { const v = ws.getUserData().identity; if (v === 'throw') throw new Error('no'); return v; } });
			for (const identity of ['throw', 'bare', { userId: 'u' }, { tenantId: 'a:b', userId: 'u' }]) {
				const ws = socket(identity, 's-x');
				await registry.hooks.open(ws, { platform });
				await expect(registry.hooks.close(ws, { platform })).resolves.toBeUndefined();
			}
			expect(registry.size()).toBe(0);
			expect(await scanKeys(getClient().redis, getClient().key('*conns:*'))).toEqual([]);
		});

		it('releases the identity a socket opened under even when its data changed', async () => {
			const platform = mockPlatform();
			const registry = make();
			const ws = socket({ tenantId: 'ta', userId: '42' }, 's-a');
			await registry.hooks.open(ws, { platform });
			ws.getUserData().identity = 'no longer valid';
			await expect(registry.hooks.close(ws, { platform })).resolves.toBeUndefined();
			expect(await registry.lookup({ tenantId: 'ta', userId: '42' })).toBeNull();
		});

		it('routes send, sendCoalesced and request to the exact identity on another instance', async () => {
			const platformOwner = mockPlatform();
			const owner = make();
			const origin = make();
			const a = socket({ tenantId: 'ta', userId: '42' }, 's-a');
			const b = socket({ tenantId: 'tb', userId: '42' }, 's-b');
			await owner.hooks.open(a, { platform: platformOwner });
			await owner.hooks.open(b, { platform: platformOwner });
			await origin.hooks.open(socket({ tenantId: 'tz', userId: 'origin' }, 's-o'), { platform: mockPlatform() });
			platformOwner.request = async (ws, event) => ({ sameWs: ws === b, event });

			await origin.send({ tenantId: 'ta', userId: '42' }, 'notes', 'new', { id: 1 });
			await origin.sendCoalesced({ tenantId: 'tb', userId: '42' }, { key: 'k', topic: 'cursor', event: 'move', data: { x: 1 } });
			await until(() => {
				expect(platformOwner.sent).toEqual([expect.objectContaining({ ws: a, topic: 'notes', event: 'new' })]);
				expect(platformOwner.sentCoalesced).toHaveLength(1);
				expect(platformOwner.sentCoalesced[0].ws).toBe(b);
			});
			expect(await origin.request({ tenantId: 'tb', userId: '42' }, 'confirm')).toEqual({ sameWs: true, event: 'confirm' });
		});

		it('drops a push envelope whose identity does not own the session', async () => {
			const platformOwner = mockPlatform();
			const owner = make();
			const a = socket({ tenantId: 'ta', userId: '42' }, 's-a');
			await owner.hooks.open(a, { platform: platformOwner });
			const channel = getClient().key('identity:v1:push:' + owner.instanceId);
			const base = { type: 'send', sessionId: 's-a', topic: 't', event: 'e' };
			for (const envelope of [base, { ...base, v: 1, identity: { tenantId: 'tb', userId: '42' } }, { ...base, v: 1, identity: { userId: '42' } }]) {
				await getClient().redis.publish(channel, JSON.stringify(envelope));
			}
			await getClient().redis.publish(channel, JSON.stringify({ ...base, v: 1, identity: { tenantId: 'ta', userId: '42' }, event: 'ok' }));
			await until(() => expect(platformOwner.sent.map((entry) => entry.event)).toEqual(['ok']));
		});

		it('announces versioned events with the identity on the versioned channel only', async () => {
			const platform = mockPlatform();
			const registry = make({ attributes: (ws) => ({ role: ws.getUserData().role }) });
			const probe = getClient().redis.duplicate();
			// A real connection refuses commands until it is ready.
			if (probe.status === 'wait') await probe.connect();
			else if (probe.status !== undefined && probe.status !== 'ready') await new Promise((resolve) => probe.once('ready', resolve));
			const seen = [];
			probe.on('message', (channel, raw) => seen.push({ channel, envelope: JSON.parse(raw) }));
			const versioned = getClient().key('identity:v1:registry-events');
			const legacy = getClient().key('__registry-events');
			await probe.subscribe(versioned, legacy);
			try {
				const ws = socket({ tenantId: null, userId: 'a:b' }, 's-n', { role: 'admin' });
				await registry.hooks.open(ws, { platform });
				await registry.hooks.close(ws, { platform });
				await until(() => expect(seen.map((entry) => entry.envelope.type)).toEqual(['open', 'close']));
				for (const { channel, envelope } of seen) {
					expect(channel).toBe(versioned);
					expect(envelope).toMatchObject({ v: 1, identity: { tenantId: null, userId: 'a:b' }, instanceId: registry.instanceId });
					expect(envelope).not.toHaveProperty('userId');
				}
			} finally {
				await probe.quit();
			}
		});

		it('bootstraps attribute cohorts from versioned rows and skips foreign keys', async () => {
			const platformOwner = mockPlatform();
			const owner = make({ attributes: (ws) => ({ role: ws.getUserData().role }) });
			const a = socket({ tenantId: 'ta', userId: '42' }, 's-a', { role: 'admin' });
			const b = socket({ tenantId: 'tb', userId: '42' }, 's-b', { role: 'admin' });
			await owner.hooks.open(a, { platform: platformOwner });
			await owner.hooks.open(b, { platform: platformOwner });
			await getClient().redis.hset(getClient().key('identity:v1:conns:not-an-identity'), 'instanceId', owner.instanceId, 'sessionId', 's-z', 'ts', '1', 'attr:role', 'admin');
			const late = make({ attributes: () => ({}) });
			await late.hooks.open(socket({ tenantId: 'tz', userId: 'x' }, 's-l'), { platform: mockPlatform() });
			await until(async () => expect(await late.sendTo({ role: 'admin' }, 't', 'e')).toBe(2));
			await until(() => {
				const delivered = platformOwner.sent.map((entry) => entry.ws);
				expect(delivered).toHaveLength(2);
				expect(delivered.includes(a) && delivered.includes(b)).toBe(true);
			});
		});

		it('erases exactly one identity across instances', async () => {
			const platformOwner = mockPlatform();
			const owner = make({ attributes: () => ({ role: 'r' }) });
			const eraser = make({ attributes: () => ({ role: 'r' }) });
			await eraser.hooks.open(socket({ tenantId: 'tz', userId: 'e' }, 's-e'), { platform: mockPlatform() });
			for (const [tenantId, sid] of [['ta', 's-a'], ['tb', 's-b'], [null, 's-n']]) {
				await owner.hooks.open(socket({ tenantId, userId: '42' }, sid), { platform: platformOwner });
			}
			await until(async () => expect(await eraser.sendTo({ role: 'r' }, 't', 'before')).toBe(4));
			expect(await eraser.purgeUser('ta', '42')).toBe(1);
			expect(await eraser.lookup({ tenantId: 'ta', userId: '42' })).toBeNull();
			expect(await eraser.lookup({ tenantId: 'tb', userId: '42' })).not.toBeNull();
			expect(await eraser.lookup({ tenantId: null, userId: '42' })).not.toBeNull();
			// The owner dropped the erased identity from its local state.
			await until(() => expect(owner.size()).toBe(2));
			await until(async () => expect(await eraser.sendTo({ role: 'r' }, 't', 'after')).toBe(3));
			// A second purge, and one for an identity with no row, remove nothing.
			expect(await eraser.purgeUser('ta', '42')).toBe(0);
			expect(await eraser.purgeUser('tc', '42')).toBe(0);
			expect(await owner.lookup({ tenantId: 'tb', userId: '42' })).not.toBeNull();
		});

		it('validates the purge identity the same way directly and through createForgetStore', async () => {
			const platform = mockPlatform();
			const registry = make();
			const store = createForgetStore({ registry });
			await registry.hooks.open(socket({ tenantId: 'ta', userId: '42' }, 's-a'), { platform });
			await expect(registry.purgeUser('a.b', '42')).rejects.toThrow(TypeError);
			await expect(store.purgeUser('a.b', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE' });
			await expect(registry.purgeUser(undefined, '42')).rejects.toThrow(/tenantId/);
			await expect(store.purgeUser(undefined, '42')).rejects.toThrow(/tenant id is required/);
			expect(await store.purgeUser('ta', '42')).toEqual({ registry: 1 });
		});

		it('reports a registration written by identify as incomplete and keeps it', async () => {
			const platform = mockPlatform();
			const legacy = createConnectionRegistry(getClient(), { identify: (ws) => ws.getUserData().userId, heartbeat: 60000 });
			registries.push(legacy);
			const registry = make();
			await legacy.hooks.open(socket(undefined, 's-old', { userId: '42' }), { platform });
			await registry.hooks.open(socket({ tenantId: 'ta', userId: '42' }, 's-a'), { platform });
			await expect(createForgetStore({ registry }).purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', partialCounts: { registry: 1 } });
			await expect(registry.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true, removed: 0 });
			expect(await legacy.lookup('42')).toMatchObject({ sessionId: 's-old' });
			expect(await registry.purgeUser('ta', '43')).toBe(0);
		});
	});
}
