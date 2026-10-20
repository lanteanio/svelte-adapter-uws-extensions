import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createBackendClient, resetBackendKeys, isClusterBackend } from '../helpers/backend.js';
import { createCursor } from '../../../src/redis/cursor.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

const clients = [];
const trackers = [];
let admin;
const makeClient = () => { const client = createBackendClient({ keyPrefix: 'inttest-cursor-recovery:' }); clients.push(client); return client; };
const track = client => { const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 20 }); trackers.push(cursor); return cursor; };
const servers = () => isClusterBackend() ? admin.redis.nodes('all') : [admin.redis];
async function subscriberIds() {
	const result = [];
	for (const server of servers()) {
		const rows = String(await server.call('CLIENT', 'LIST', 'TYPE', 'pubsub')).trim().split('\n');
		for (const row of rows) if (/ sub=[1-9]/.test(row)) result.push({ server, id: /^id=(\d+)/.exec(row)[1] });
	}
	return result;
}
beforeEach(async () => { admin = makeClient(); await admin.redis.ping(); await resetBackendKeys(admin); });
afterEach(async () => {
	for (const tracker of trackers.splice(0)) tracker.destroy();
	await resetBackendKeys(admin);
	await Promise.all(clients.splice(0).map(client => client.quit()));
});

describe('cursor recovery over Redis connections', () => {
	it('removes a departed peer after a lost REMOVE and a real subscriber reconnect', async () => {
		const watcher = track(admin);
		const watcherPlatform = mockPlatform();
		await watcher.attach(mockWs({ id: 'watcher' }), 'board', watcherPlatform);
		const subscriptions = await subscriberIds();
		expect(subscriptions).toHaveLength(1);
		const moverClient = makeClient();
		const mover = track(moverClient);
		const moverPlatform = mockPlatform();
		const ws = mockWs({ id: 'peer' });
		await mover.attach(ws, 'board', moverPlatform);
		mover.update(ws, 'board', { x: 7 }, moverPlatform);
		await expect.poll(() => watcherPlatform.published.some(frame => frame.event === 'join')).toBe(true);
		await expect.poll(async () => Object.keys(await admin.redis.hgetall(admin.key('cursor:{board}'))).length).toBe(1);
		const key = watcherPlatform.published.find(frame => frame.event === 'join').data.key;
		// Drop the outbound departure frame while retaining its real HDEL.
		const publish = moverClient.redis.publish.bind(moverClient.redis);
		moverClient.redis.publish = (channel, value) => JSON.parse(value).event === 'remove' ? Promise.resolve(0) : publish(channel, value);
		await mover.remove(ws, moverPlatform, 'board');
		expect(await admin.redis.hgetall(admin.key('cursor:{board}'))).toEqual({});
		watcherPlatform.reset();
		for (const { server, id } of subscriptions) await server.call('CLIENT', 'KILL', 'ID', id);
		await expect.poll(() => watcherPlatform.sent.some(frame => frame.event === 'catalog')).toBe(true);
		await expect.poll(() => watcherPlatform.published.some(frame => frame.event === 'remove' && frame.data.key === key)).toBe(true);
		watcherPlatform.reset();
		await watcher.attach(mockWs({ id: 'later' }), 'board', watcherPlatform);
		expect(watcherPlatform.sent.filter(frame => frame.event === 'catalog').flatMap(frame => frame.data)).toEqual([]);
	});

	it('restores the current local snapshot after its Redis hash is lost and its subscriber reconnects', async () => {
		const mover = track(admin);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'local' });
		await mover.attach(ws, 'board', platform);
		mover.update(ws, 'board', { x: 11 }, platform);
		const hash = admin.key('cursor:{board}');
		await expect.poll(async () => Object.keys(await admin.redis.hgetall(hash)).length).toBe(1);
		await admin.redis.del(hash);
		for (const { server, id } of await subscriberIds()) await server.call('CLIENT', 'KILL', 'ID', id);
		await expect.poll(async () => Object.values(await admin.redis.hgetall(hash)).map(value => JSON.parse(value).data)).toEqual([{ x: 11 }]);
	});
});
