import { afterEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { WebSocket } from 'ws';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createCursor } from '../../../src/redis/cursor.js';

const sockets = [], servers = [], trackers = [], clients = [];
afterEach(async () => {
	for (const ws of sockets.splice(0)) ws.terminate();
	const results = await Promise.allSettled(servers.splice(0).map(server => server.close()));
	for (const tracker of trackers.splice(0)) tracker.destroy();
	for (const client of clients.splice(0)) {
		results.push(...await Promise.allSettled([resetBackendKeys(client)]));
		results.push(...await Promise.allSettled([client.quit()]));
	}
	const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
	if (errors.length) throw new AggregateError(errors, 'Cursor erasure cleanup failed');
});

async function waitFor(read, label) {
	const deadline = Date.now() + 10000;
	while (!await read()) {
		if (Date.now() >= deadline) throw new Error('Timed out waiting for ' + label);
		await new Promise(resolve => setTimeout(resolve, 5));
	}
}
async function connect(server, id) {
	const ws = server.track(new WebSocket(server.wsUrl, { headers: { 'x-id': id } })); sockets.push(ws);
	const frames = [], state = { error: null };
	const append = frame => { if (frame.type === 'batch') for (const entry of frame.events) append(entry); else frames.push(frame); };
	ws.on('message', (data, binary) => { if (!binary) { try { append(JSON.parse(data.toString())); } catch (error) { state.error = error; } } });
	ws.on('error', error => { state.error = error; });
	await waitFor(() => { if (state.error) throw state.error; return ws.readyState === WebSocket.OPEN; }, 'connection');
	return { ws, frames, state };
}
async function subscribe(client, topic) {
	const ref = randomUUID(); client.ws.send(JSON.stringify({ type: 'subscribe', topic, ref }));
	await waitFor(() => { if (client.state.error) throw client.state.error; return client.frames.some(frame => frame.type === 'subscribed' && frame.ref === ref); }, 'subscription');
}

it('removes an erased cursor from real local and remote WebSocket observers', async () => {
	const id = randomUUID(), topic = 'erase-' + id, prefix = 'cursor-erase:' + id + ':';
	const localClient = createBackendClient({ keyPrefix: prefix }), peerClient = createBackendClient({ keyPrefix: prefix }); clients.push(localClient, peerClient);
	const make = client => { const t = createCursor(client, { binary: false, throttle: 0, topicThrottle: 16, snapshotIntervalMs: 0 }); trackers.push(t); return t; };
	const local = make(localClient), peer = make(peerClient);
	const start = async tracker => {
		const server = await createTestServer({ handler: {
			...tracker.hooks,
			upgrade: ({ headers }) => ({ id: headers['x-id'] }),
			async open(ws, { platform }) { await tracker.attach(ws, topic, platform); },
			subscribe(_ws, name) { return name === topic ? null : 'FORBIDDEN'; }
		} }); servers.push(server); return server;
	};
	const a = await start(local), b = await start(peer);
	const observer = await connect(a, 'observer'), remote = await connect(b, 'remote'), victim = await connect(a, 'erased');
	await Promise.all([subscribe(observer, topic), subscribe(remote, topic), subscribe(victim, topic)]);
	const nativeVictim = [...a.wsConnections].find(ws => ws.getUserData().id === 'erased');
	local.update(nativeVictim, topic, { x: 10 }, a.platform);
	const key = await (async () => { let key; await waitFor(async () => { key = (await local.list(topic)).find(entry => entry.user.id === 'erased')?.key; return !!key; }, 'persisted cursor'); return key; })();
	const moved = client => client.frames.some(frame => frame.topic === '__cursor:' + topic && ((frame.event === 'update' && frame.data.key === key) || (frame.event === 'bulk' && frame.data.some(entry => entry.key === key))));
	await waitFor(() => moved(observer) && moved(remote), 'local and peer cursor delivery');
	expect(await local.purgeUser(null, 'erased')).toBe(1);
	const removed = client => client.frames.filter(frame => frame.topic === '__cursor:' + topic && frame.event === 'remove' && frame.data.key === key);
	await waitFor(() => removed(observer).length && removed(remote).length, 'erasure on both native observers');
	expect(removed(observer)).toHaveLength(1); expect(removed(remote)).toHaveLength(1);
	expect(await localClient.redis.hget(localClient.key('cursor:{' + topic + '}'), key)).toBeNull();
});
