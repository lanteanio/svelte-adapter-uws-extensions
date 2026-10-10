import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createReservedFieldWarner } from '../../src/redis/presence/field-policy.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';

let uWS;
try { uWS = (await import('uWebSockets.js')).default; } catch { uWS = null; }
if (!uWS && process.env.REQUIRE_UWS === '1') throw new Error('uWebSockets.js is required');
const describeUWS = uWS ? describe : describe.skip;
const ROOM = 'unicode-presence';
const TOPIC = '__presence:' + ROOM;
const cleanups = [];

afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
	vi.restoreAllMocks();
});

async function connect(url) {
	const { WebSocket } = await import('ws');
	const ws = new WebSocket(url);
	const messages = [];
	ws.on('message', (raw, binary) => {
		if (!binary) messages.push(JSON.parse(raw.toString()));
	});
	await new Promise((resolve, reject) => {
		ws.once('open', resolve);
		ws.once('error', reject);
	});
	cleanups.push(() => ws.close());
	return {
		send: (message) => ws.send(JSON.stringify(message)),
		async waitFor(predicate) {
			await expect.poll(() => messages.find(predicate), { timeout: 3000 }).toBeDefined();
			return messages.find(predicate);
		}
	};
}

async function start(redis, key, clientUpdateFields) {
	const { createTestServer } = await import('svelte-adapter-uws/testing');
	const presence = createPresence(redis, { key, clientUpdateFields, topicThrottle: 0, binary: false });
	const server = await createTestServer({
		handler: {
			upgrade({ url }) { return { [key]: new URL('http://test' + url).searchParams.get('id') }; },
			subscribe(_ws, topic) { return topic === ROOM ? null : 'FORBIDDEN'; },
			async message(ws, { data, platform }) {
				const msg = JSON.parse(Buffer.from(data).toString('utf8'));
				if (msg.type === 'server-join') await presence.join(ws, ROOM, platform);
				else if (msg.type === 'server-update') await presence.update(ws, ROOM, msg.fields, platform);
				else await presence.hooks.message(ws, { data, msg, platform });
				if (msg.ref) ws.send(JSON.stringify({ type: 'ack', ref: msg.ref }));
			},
			close(ws, { platform }) { return presence.hooks.close(ws, { platform }); }
		}
	});
	cleanups.push(() => presence.destroy(), () => server.close());
	return server;
}

async function snapshot(server) {
	const client = await connect(server.wsUrl + '?id=observer');
	client.send({ type: 'presence-snapshot', topic: ROOM });
	const state = await client.waitFor((m) => m.topic === TOPIC && m.event === 'state');
	return { client, state: state.data };
}

describeUWS('Redis presence identity names over WebSocket', () => {
	it.each([
		{ label: 'Unicode-only identity', key: '\u7528\u6237', allowed: ['\u72b6\u6001', '...'], aliases: [] },
		{ label: 'Unicode case stays exact', key: '\u00c4', allowed: ['\u00e4', '\u00d6'], aliases: [] },
		{ label: 'punctuation-only identity', key: '---', allowed: ['...', '\u03a9'], aliases: [] },
		{ label: 'Kelvin sign folds to ASCII', key: '\u212aey', allowed: ['\u72b6\u6001', '...'], aliases: ['K-e_y', 'KEY'] },
		{ label: 'ASCII identity keeps aliases', key: 'userKey', allowed: ['\u72b6\u6001', '...'], aliases: ['USER_KEY'] }
	])('$label preserves identity across local and relayed delivery', async ({ key, allowed, aliases }) => {
		const warning = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const redis = mockRedisClient('unicode:');
		const local = await start(redis, key);
		const remote = await start(redis, key);
		const { client: localObserver } = await snapshot(local);
		const { client: remoteObserver } = await snapshot(remote);
		const participant = await connect(local.wsUrl + '?id=participant');
		participant.send({ type: 'server-join' });
		for (const observer of [localObserver, remoteObserver]) {
			await observer.waitFor((m) => m.topic === TOPIC && m.event === 'diff' && m.data.joins?.participant);
		}
		const expected = { ...Object.fromEntries(allowed.map((name) => [name, 'server-value'])), status: 'ready' };
		participant.send({ type: 'server-update', fields: {
			[key]: 'forged', ...Object.fromEntries(aliases.map((name) => [name, 'forged'])),
			...expected, '__\u72b6\u6001': 'private', constructor: 'private', email: 'private'
		} });
		for (const observer of [localObserver, remoteObserver]) {
			const diff = await observer.waitFor((m) => m.topic === TOPIC && m.event === 'diff' && m.data.updates?.participant?.status === 'ready');
			expect(diff.data.updates.participant).toEqual(expected);
		}
		expect((await snapshot(remote)).state.participant).toEqual({ [key]: 'participant', ...expected });
		const lines = warning.mock.calls.map(([message]) => String(message)).filter((m) => m.includes('presence.update():'));
		expect(lines).toHaveLength(1);
		expect(lines[0]).not.toMatch(/[^\x20-\x7e]/);

		participant.send({ type: 'presence-update', topic: ROOM, fields: { [allowed[0]]: 'wire-value', status: 'wire-value' }, ref: 'denied' });
		await participant.waitFor((m) => m.type === 'ack' && m.ref === 'denied');
		expect((await snapshot(remote)).state.participant).toEqual({ [key]: 'participant', ...expected });
	});

	it('keeps explicit Unicode allowlists exact on wire updates', async () => {
		const key = '\u7528\u6237';
		const allowed = '\u00c4';
		const redis = mockRedisClient('unicode-list:');
		const local = await start(redis, key, [allowed]);
		const remote = await start(redis, key, [allowed]);
		const { client: observer } = await snapshot(remote);
		const participant = await connect(local.wsUrl + '?id=participant');
		participant.send({ type: 'server-join' });
		await observer.waitFor((m) => m.event === 'diff' && m.data.joins?.participant);
		participant.send({ type: 'presence-update', topic: ROOM, fields: { [key]: 'forged', [allowed]: 'ready', '\u00e4': 'excluded', status: 'excluded' } });
		const diff = await observer.waitFor((m) => m.event === 'diff' && m.data.updates?.participant?.[allowed] === 'ready');
		expect(diff.data.updates.participant).toEqual({ [allowed]: 'ready' });
		expect((await snapshot(remote)).state.participant).toEqual({ [key]: 'participant', [allowed]: 'ready' });
	});
});

it('warns only for the exact empty-fold identity and deduplicates it', () => {
	vi.stubEnv('NODE_ENV', 'test');
	try {
		const warning = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const warn = createReservedFieldWarner('\u7528\u6237');
		for (const field of ['...', '\u72b6\u6001', '__\u72b6\u6001', '\u7528\u6237', '\u7528\u6237']) warn(field);
		expect(warning).toHaveBeenCalledTimes(1);
		expect(warning.mock.calls[0][0]).toContain("field '??' is reserved");
	} finally { vi.unstubAllEnvs(); }
});
