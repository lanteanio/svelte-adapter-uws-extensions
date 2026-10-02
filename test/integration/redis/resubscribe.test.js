/**
 * Integration tests for subscriber re-subscription after a reconnect, against
 * a real Redis server.
 *
 * Redis disconnects a client that loses access to a channel it is subscribed
 * to, so removing a channel from the user's ACL is a real reconnect whose
 * re-subscribe Redis refuses with NOPERM. Left to ioredis, that refusal is an
 * unhandled rejection and ends the process. These tests pin that it is
 * reported instead, and that the subscription comes back on its own once the
 * ACL grants the channel again, with no reconnect in between.
 *
 * Standalone only: on a Cluster the client's duplicate() returns ioredis's own
 * Cluster subscriber, which the package leaves alone.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
import { createRedisClient } from '../../../src/redis/index.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { createConnectionRegistry } from '../../../src/redis/registry.js';
import { createPresence } from '../../../src/redis/presence.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { mockWs } from '../../helpers/mock-ws.js';
import { EXTENSIONS_ERROR_IDS } from '../../../src/error-registry.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { captureRegistered } from '../../helpers/diagnostic-line.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 5000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(20);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe.runIf(!isClusterBackend())('subscriber re-subscription after a reconnect (integration)', () => {
	const user = 'inttest-resub-' + Math.random().toString(16).slice(2, 10);
	const password = 'pw-' + Math.random().toString(16).slice(2, 10);
	let admin;
	/** @type {Array<() => Promise<unknown>>} */
	const cleanups = [];
	let sink = null;
	const unhandled = vi.fn();

	/**
	 * A client that connects as the test's ACL user. `readies()` counts the
	 * 'ready' events of the subscriber connections the modules open from it.
	 */
	function userClient(keyPrefix) {
		const url = new URL(process.env.INTEGRATION_REDIS_URL);
		url.username = user;
		url.password = password;
		const client = createRedisClient({ url: url.toString(), keyPrefix, autoShutdown: false });
		let readies = 0;
		const duplicate = client.duplicate;
		client.duplicate = (...args) => {
			const conn = duplicate(...args);
			conn.on('ready', () => { readies++; });
			return conn;
		};
		client.readies = () => readies;
		cleanups.push(() => client.quit());
		return client;
	}

	const grantAllChannels = () => admin.redis.call('ACL', 'SETUSER', user, 'allchannels');
	/** Subscribers the server holds on a channel, and its pattern subscriptions. */
	const numsub = async (channel) => Number((await admin.redis.call('PUBSUB', 'NUMSUB', channel))[1]);
	/** Pattern subscriptions held by the test user's connections. */
	const userPatterns = async () => {
		const list = await admin.redis.call('CLIENT', 'LIST', 'TYPE', 'pubsub');
		let total = 0;
		for (const line of String(list).split('\n')) {
			if (!line.includes(' user=' + user + ' ')) continue;
			const m = / psub=(\d+)/.exec(line);
			if (m) total += Number(m[1]);
		}
		return total;
	};
	const channelsMatching = (pattern) => admin.redis.call('PUBSUB', 'CHANNELS', pattern);
	// Redis disconnects the user's subscribed clients when a channel is taken away.
	const revokeChannels = () => admin.redis.call('ACL', 'SETUSER', user, 'resetchannels');

	beforeAll(async () => {
		admin = createBackendClient({ keyPrefix: 'inttest-resub:' });
		await admin.redis.call('ACL', 'SETUSER', user, 'reset', 'on', '>' + password, '~*', '&*', '+@all');
		process.on('unhandledRejection', unhandled);
	});

	afterEach(async () => {
		sink?.stop();
		sink = null;
		while (cleanups.length > 0) await cleanups.pop()().catch(() => {});
		await grantAllChannels();
	});

	afterAll(async () => {
		process.off('unhandledRejection', unhandled);
		await admin.redis.call('ACL', 'DELUSER', user).catch(() => {});
		await admin.quit();
	});

	it('pub/sub bus: a refused re-subscribe is reported, and delivery resumes once the channel is granted again', async () => {
		const channel = 'inttest-resub:pubsub:' + Date.now();
		const client = userClient('inttest-resub:');
		const platform = mockPlatform();
		const bus = createPubSubBus(client, { channel });
		cleanups.push(() => bus.deactivate());
		await bus.activate(platform);
		sink = captureRegistered();
		const resubscribes = () => sink.records.filter((r) => r.event === 'redis.subscriber.resubscribe.failed');

		await revokeChannels();
		await waitFor(() => resubscribes().length > 0);
		expect(sink.ids()).toContain(EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE);
		expect(resubscribes()[0].attributes).toMatchObject({ owner: 'pubsub', kind: 'subscribe', refused: 1 });
		expect(resubscribes()[0].attributes.error.message).toMatch(/^NOPERM/);
		expect(JSON.stringify(sink.records)).not.toContain(channel);

		// The grant does not reconnect anything; the backoff retry restores the channel.
		const readiesBeforeGrant = client.readies();
		await grantAllChannels();
		const envelope = JSON.stringify({ instanceId: 'remote', topic: 'chat', event: 'msg', data: 1 });
		await waitFor(async () => {
			await admin.redis.publish(channel, envelope);
			return platform.published.some((p) => p.event === 'msg');
		});
		expect(client.readies()).toBe(readiesBeforeGrant);
		expect(resubscribes()).toHaveLength(1);
		expect(unhandled).not.toHaveBeenCalled();
	});

	it('sharded bus: a refused shard re-subscribe is reported, and the shard channel comes back', async () => {
		const prefix = 'inttest-resub:sharded:' + Date.now() + ':';
		const client = userClient('inttest-resub:');
		const platform = mockPlatform();
		const bus = createShardedBus(client, { channelPrefix: prefix });
		cleanups.push(() => bus.deactivate());
		await bus.activate(platform);
		await bus.follow('room');
		sink = captureRegistered();
		const resubscribes = () => sink.records.filter((r) => r.event === 'redis.subscriber.resubscribe.failed');

		await revokeChannels();
		await waitFor(() => resubscribes().length > 0);
		expect(resubscribes()[0].attributes).toMatchObject({ owner: 'sharded-pubsub', kind: 'ssubscribe', refused: 1 });
		expect(JSON.stringify(sink.records)).not.toContain(prefix);

		const readiesBeforeGrant = client.readies();
		await grantAllChannels();
		const envelope = JSON.stringify({ instanceId: 'remote', topic: 'room', event: 'msg', data: 1 });
		await waitFor(async () => {
			await admin.redis.spublish(prefix + 'room', envelope);
			return platform.published.some((p) => p.event === 'msg');
		});
		expect(client.readies()).toBe(readiesBeforeGrant);
		expect(unhandled).not.toHaveBeenCalled();
	});

	it('registry: one refused channel of a two-channel subscription does not take the other down', async () => {
		const prefix = 'reg' + Date.now() + ':';
		const client = userClient('inttest-resub:');
		const registry = createConnectionRegistry(client, { keyPrefix: prefix, identify: (ws) => ws.getUserData().userId, heartbeat: 60000 });
		cleanups.push(() => registry.destroy());
		const ws = mockWs({ userId: 'u1' });
		ws.getUserData()[WS_SESSION_ID] = 's1';
		await registry.hooks.open(ws, { platform: mockPlatform() });
		const eventsChannel = 'inttest-resub:' + prefix + '__registry-events';
		const pushPattern = 'inttest-resub:' + prefix + '__push:*';
		expect(await numsub(eventsChannel)).toBe(1);
		expect(await channelsMatching(pushPattern)).toHaveLength(1);
		sink = captureRegistered();
		const resubscribes = () => sink.records.filter((r) => r.event === 'redis.subscriber.resubscribe.failed');

		// Keep only the events channel: Redis drops the connection, and the
		// restore's SUBSCRIBE of both channels is refused as a whole.
		await admin.redis.call('ACL', 'SETUSER', user, 'resetchannels', '&' + eventsChannel);
		await waitFor(() => resubscribes().length > 0);
		expect(resubscribes()[0].attributes).toMatchObject({ owner: 'registry', kind: 'subscribe', refused: 1 });
		expect(await numsub(eventsChannel)).toBe(1);
		expect(await channelsMatching(pushPattern)).toHaveLength(0);

		await grantAllChannels();
		await waitFor(async () => (await channelsMatching(pushPattern)).length === 1);
		expect(await numsub(eventsChannel)).toBe(1);
		expect(unhandled).not.toHaveBeenCalled();
	});

	it('presence: the keyspace pattern subscription comes back with the topic channel', async () => {
		const client = userClient('inttest-resub-presence' + Date.now() + ':');
		const presence = createPresence(client, { key: 'id', select: (ud) => ({ id: ud.id }), heartbeat: 60000, keyspaceNotifications: true });
		cleanups.push(async () => presence.destroy?.());
		await presence.join(mockWs({ id: 'u1' }), 'room', mockPlatform());
		expect(await userPatterns()).toBe(1);
		sink = captureRegistered();
		const resubscribes = () => sink.records.filter((r) => r.event === 'redis.subscriber.resubscribe.failed');

		await revokeChannels();
		await waitFor(() => resubscribes().length > 0);
		expect(resubscribes()[0].attributes).toMatchObject({ owner: 'presence', refused: 2 });
		expect(await userPatterns()).toBe(0);

		await grantAllChannels();
		await waitFor(async () => (await userPatterns()) === 1);
		expect(unhandled).not.toHaveBeenCalled();
	});
});
