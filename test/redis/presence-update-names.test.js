// Which dynamic field names a presence entry accepts, at each of the three
// ways one reaches it: update() (from a client frame or a server call), a
// peer instance's relayed update, and the fields read back from Redis. The
// rule is the in-memory plugin's: a client frame writes nothing unless the
// app lists the name in clientUpdateFields, and a server call cannot write
// the identity peers see or a credential, personal-data or transport name.

import { describe, it, expect, vi, afterEach } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const select = (u) => ({ id: u.id });

/** Wait until the join diff has flushed, so a later update is not folded into it. */
async function waitForDiffFlush(platform, timeoutMs = 2000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (platform.published.some((f) => f.event === 'diff')) return;
		await sleep(5);
	}
	throw new Error('join diff never flushed');
}

/** One joined user on 'room', with the join diff flushed and the platform reset. */
async function setup(prefix, options = {}) {
	const client = mockRedisClient(prefix);
	const platform = mockPlatform();
	const presence = createPresence(client, { select, ...options });
	const alice = mockWs({ id: 'alice' });
	await presence.join(alice, 'room', platform);
	await waitForDiffFlush(platform);
	platform.reset();
	const relayed = vi.spyOn(client.redis, 'publish');
	const stored = async () => {
		const raw = await client.redis.hget(prefix + 'presence:topic:{room}', 'alice');
		return JSON.parse(raw).fields ?? {};
	};
	const updates = () => platform.published
		.filter((f) => f.event === 'diff' && f.data.updates)
		.map((f) => f.data.updates.alice);
	const relayedFields = () => relayed.mock.calls
		.map(([, msg]) => JSON.parse(msg))
		.filter((m) => m.event === 'fields')
		.map((m) => ({ ...m.payload.durable, ...m.payload.transient }));
	return { client, platform, presence, alice, stored, updates, relayedFields };
}

/** Send a frame through the tracker's own message hook, as the adapter does. */
async function frame(t, fields) {
	t.presence.hooks.message(t.alice, {
		data: { type: 'presence-update', topic: 'room', fields },
		platform: t.platform
	});
	await sleep(20);
	t.presence.flushDiffs();
}

const IDENTITY_AND_PRIVATE = {
	email: 'a@b.test',
	apiKey: 'secret',
	sessionToken: 'secret',
	ip: '203.0.113.10',
	rawHeaders: ['authorization', 'Bearer secret'],
	id: 'victim-id',
	ID: 'victim-id',
	user_id: 'victim-id',
	role: 'admin',
	Roles: ['admin'],
	userRole: 'admin'
};

describe('presence-update frames from a client', () => {
	afterEach(() => vi.restoreAllMocks());

	it('write nothing when clientUpdateFields is not set', async () => {
		const t = await setup('cf1:');
		const getter = vi.fn(() => true);
		const fields = { ...IDENTITY_AND_PRIVATE, typing: true };
		Object.defineProperty(fields, 'color', { enumerable: true, get: getter });
		await frame(t, fields);

		expect(t.updates()).toEqual([]);
		expect(t.relayedFields()).toEqual([]);
		expect(await t.stored()).toEqual({});
		// The frame is refused before any value is read.
		expect(getter).not.toHaveBeenCalled();
		t.presence.destroy();
	});

	it('are not even enumerated when clientUpdateFields is not set', async () => {
		const t = await setup('cf1b:');
		let enumerated = 0;
		const fields = new Proxy({ typing: true }, {
			ownKeys(target) { enumerated++; return Reflect.ownKeys(target); }
		});
		await frame(t, fields);
		t.presence.destroy();
		expect(enumerated).toBe(0);
	});

	it('write exactly the listed names', async () => {
		const t = await setup('cf2:', { clientUpdateFields: ['typing', 'role'] });
		const unlisted = vi.fn(() => 'secret');
		const fields = { ...IDENTITY_AND_PRIVATE, typing: true, color: 'red' };
		Object.defineProperty(fields, 'note', { enumerable: true, get: unlisted });
		await frame(t, fields);
		// An unlisted name is never read, so its getter or toJSON never runs.
		expect(unlisted).not.toHaveBeenCalled();

		expect(t.updates()).toEqual([{ typing: true, role: 'admin' }]);
		expect(t.relayedFields()).toEqual([{ typing: true, role: 'admin' }]);
		expect(await t.stored()).toEqual({ typing: true, role: 'admin' });
		t.presence.destroy();
	});

	it('never write a prototype or reserved name, even when it is listed', async () => {
		const t = await setup('cf3:', { clientUpdateFields: ['__proto__', 'constructor', 'prototype', '__internal', 'typing'] });
		await frame(t, JSON.parse('{"__proto__":{"isAdmin":true},"constructor":1,"prototype":2,"__internal":3,"typing":true}'));

		expect(t.updates()).toEqual([{ typing: true }]);
		expect(t.relayedFields()).toEqual([{ typing: true }]);
		expect(await t.stored()).toEqual({ typing: true });
		expect(({}).isAdmin).toBeUndefined();
		t.presence.destroy();
	});

	it('refuses a clientUpdateFields that is not an array', () => {
		const client = mockRedisClient('cf4:');
		for (const value of ['typing', { typing: true }, 7, null]) {
			expect(() => createPresence(client, { select, clientUpdateFields: value }))
				.toThrow('redis presence: clientUpdateFields must be an array of field names');
		}
	});
});

describe('update() called by the server', () => {
	afterEach(() => vi.restoreAllMocks());

	it('drops the identity peers see and private names, keeps ordinary ones', async () => {
		const t = await setup('sf1:');
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		await t.presence.update(t.alice, 'room', {
			...IDENTITY_AND_PRIVATE,
			typing: true,
			authorName: 'Ada',
			microphoneOn: true
		}, t.platform);
		t.presence.flushDiffs();

		const kept = { typing: true, authorName: 'Ada', microphoneOn: true };
		expect(t.updates()).toEqual([kept]);
		expect(t.relayedFields()).toEqual([kept]);
		expect(await t.stored()).toEqual(kept);
		t.presence.destroy();
	});

	it('lets a server-owned value that changes mid-session ride under its own name', async () => {
		const t = await setup('sf8:');
		await t.presence.update(t.alice, 'room', { isHost: true }, t.platform);
		t.presence.flushDiffs();
		expect(t.updates()).toEqual([{ isHost: true }]);
		// ...which a client frame cannot write, because no list is set.
		await frame(t, { isHost: false });
		expect(await t.stored()).toEqual({ isHost: true });
		t.presence.destroy();
	});

	it('refuses the configured key field in every spelling', async () => {
		const client = mockRedisClient('sf2:');
		const platform = mockPlatform();
		const presence = createPresence(client, { key: 'userKey', select: (u) => ({ userKey: u.userKey }) });
		const ws = mockWs({ userKey: 'k1' });
		await presence.join(ws, 'room', platform);
		await waitForDiffFlush(platform);
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		await presence.update(ws, 'room', { userKey: 'k2', user_key: 'k2', USERKEY: 'k2', color: 'red' }, platform);
		const raw = await client.redis.hget('sf2:presence:topic:{room}', 'k1');
		presence.destroy();
		expect(JSON.parse(raw).fields).toEqual({ color: 'red' });
	});

	it('refuses a name longer than the private-name rule reads', async () => {
		const t = await setup('sf6:');
		const long = 'a'.repeat(65);
		await t.presence.update(t.alice, 'room', { [long]: 1, ['b'.repeat(64)]: 2 }, t.platform);
		t.presence.flushDiffs();
		expect(t.updates()).toEqual([{ ['b'.repeat(64)]: 2 }]);
		expect(await t.stored()).toEqual({ ['b'.repeat(64)]: 2 });
		t.presence.destroy();
	});

	it('writes nothing when clientUpdateFields is an empty list', async () => {
		const t = await setup('sf7:', { clientUpdateFields: [] });
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		await t.presence.update(t.alice, 'room', { typing: true, color: 'red' }, t.platform);
		t.presence.flushDiffs();
		expect(t.updates()).toEqual([]);
		expect(await t.stored()).toEqual({});
		t.presence.destroy();
	});

	it('takes the allowlist instead of the guard when clientUpdateFields is set', async () => {
		const t = await setup('sf3:', { clientUpdateFields: ['role'] });
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		await t.presence.update(t.alice, 'room', { role: 'host', typing: true, id: 'x' }, t.platform);
		t.presence.flushDiffs();
		// With a list, a refused name is one the app did not list: no warning.
		expect(warn).not.toHaveBeenCalled();

		expect(t.updates()).toEqual([{ role: 'host' }]);
		expect(await t.stored()).toEqual({ role: 'host' });
		t.presence.destroy();
	});

	it('warns once per reserved identity name outside production', async () => {
		const t = await setup('sf4:');
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		await t.presence.update(t.alice, 'room', { id: 'x', ID: 'x', Id: 'x', email: 'a@b.test' }, t.platform);
		await t.presence.update(t.alice, 'room', { id: 'y', role: 'admin' }, t.platform);
		const lines = warn.mock.calls.map((c) => String(c[0]));
		t.presence.destroy();

		// `id` in any spelling once, `role` once; a credential-shaped name is
		// an unbounded space, so it is never reported.
		expect(lines).toHaveLength(2);
		expect(lines[0]).toContain("field 'id' is reserved");
		expect(lines[1]).toContain("field 'role' is reserved");
		expect(lines.join('\n')).not.toContain('email');
	});

	it('does not warn about prototype names, which no option admits', async () => {
		const t = await setup('sf4b:');
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		await t.presence.update(t.alice, 'room', JSON.parse('{"constructor":1,"prototype":2}'), t.platform);
		const calls = warn.mock.calls.length;
		t.presence.destroy();
		expect(calls).toBe(0);
	});

	it('prints a refused name on one line', async () => {
		const t = await setup('sf4c:');
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		await t.presence.update(t.alice, 'room', { 'I\nD': 'x' }, t.platform);
		const lines = warn.mock.calls.map((c) => String(c[0]));
		t.presence.destroy();
		expect(lines).toHaveLength(1);
		expect(lines[0]).toContain("field 'I?D' is reserved");
		expect(lines[0]).not.toContain('\n');
	});

	it('does not warn in production', async () => {
		const prev = process.env.NODE_ENV;
		process.env.NODE_ENV = 'production';
		let t;
		try {
			t = await setup('sf5:');
		} finally {
			process.env.NODE_ENV = prev;
		}
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		await t.presence.update(t.alice, 'room', { id: 'x', role: 'admin' }, t.platform);
		const calls = warn.mock.calls.length;
		t.presence.destroy();
		expect(calls).toBe(0);
	});
});

describe('fields relayed by a peer instance', () => {
	afterEach(() => vi.restoreAllMocks());

	it('get the same rule as a server call', async () => {
		const t = await setup('rf1:', { transient: ['typing'], heartbeat: 25 });
		await t.client.redis.publish('rf1:presence:events:room', JSON.stringify({
			instanceId: 'foreign',
			topic: 'room',
			event: 'fields',
			payload: { key: 'alice', durable: { ...IDENTITY_AND_PRIVATE, color: 'blue' }, transient: { typing: true, email: 'a@b.test' } }
		}));
		await sleep(30);
		t.presence.flushDiffs();
		expect(t.updates()).toEqual([{ color: 'blue', typing: true }]);

		// This instance also presents alice, so it merged the relayed fields
		// into its own entry; its heartbeat shows what that entry took.
		t.platform.reset();
		const start = Date.now();
		while (!t.platform.published.some((f) => f.event === 'heartbeat') && Date.now() - start < 2000) await sleep(5);
		const beat = t.platform.published.find((f) => f.event === 'heartbeat');
		t.presence.destroy();
		expect(beat.data.alice).toEqual({ id: 'alice', color: 'blue' });
	});
});

describe('fields relayed by a peer instance, with clientUpdateFields set', () => {
	afterEach(() => vi.restoreAllMocks());

	it('take only the listed names', async () => {
		const t = await setup('rf2:', { clientUpdateFields: ['role'] });
		await t.client.redis.publish('rf2:presence:events:room', JSON.stringify({
			instanceId: 'foreign',
			topic: 'room',
			event: 'fields',
			payload: { key: 'alice', durable: { role: 'host', color: 'blue' }, transient: { typing: true } }
		}));
		await sleep(30);
		t.presence.flushDiffs();
		expect(t.updates()).toEqual([{ role: 'host' }]);
		t.presence.destroy();
	});
});

describe('fields read back from Redis', () => {
	afterEach(() => vi.restoreAllMocks());

	it('get the same rule as a server call, in the snapshot and in list()', async () => {
		const t = await setup('bf1:');
		const key = 'bf1:presence:topic:{room}';
		const entry = JSON.parse(await t.client.redis.hget(key, 'alice'));
		entry.fields = { ...IDENTITY_AND_PRIVATE, color: 'red' };
		await t.client.redis.hset(key, 'alice', JSON.stringify(entry));

		await t.presence.sync(mockWs({ id: 'observer' }), 'room', t.platform);
		const state = t.platform.sent.find((e) => e.event === 'state').data;
		expect(state.alice).toEqual({ id: 'alice', color: 'red' });
		expect(await t.presence.list('room')).toEqual([{ id: 'alice', color: 'red' }]);
		t.presence.destroy();
	});

	it('take only the listed names when clientUpdateFields is set', async () => {
		const t = await setup('bf3:', { clientUpdateFields: ['role'] });
		const key = 'bf3:presence:topic:{room}';
		const entry = JSON.parse(await t.client.redis.hget(key, 'alice'));
		entry.fields = { role: 'host', color: 'red' };
		await t.client.redis.hset(key, 'alice', JSON.stringify(entry));
		expect(await t.presence.list('room')).toEqual([{ id: 'alice', role: 'host' }]);
		t.presence.destroy();
	});

	it('ignore a stored fields value that is not an object', async () => {
		const t = await setup('bf2:');
		const key = 'bf2:presence:topic:{room}';
		for (const fields of ['abc', [1, 2], 7]) {
			const entry = JSON.parse(await t.client.redis.hget(key, 'alice'));
			entry.fields = fields;
			await t.client.redis.hset(key, 'alice', JSON.stringify(entry));
			expect(await t.presence.list('room')).toEqual([{ id: 'alice' }]);
		}
		t.presence.destroy();
	});
});

describe('list()', () => {
	it('returns what the state snapshot carries', async () => {
		const t = await setup('lf1:', { transient: ['typing'] });
		await t.presence.update(t.alice, 'room', { color: 'red', typing: true }, t.platform);
		const listed = await t.presence.list('room');
		await t.presence.sync(mockWs({ id: 'observer' }), 'room', t.platform);
		const state = t.platform.sent.find((e) => e.event === 'state').data;
		t.presence.destroy();
		expect(listed).toEqual([{ id: 'alice', color: 'red' }]);
		expect(listed).toEqual(Object.values(state));
	});
});
