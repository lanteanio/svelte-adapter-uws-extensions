/**
 * Integration tests for subscriber re-subscription after a reconnect, against
 * a real Redis server and, under the cluster-mirror tier, a real Redis
 * Cluster.
 *
 * Redis disconnects a client that loses access to a channel it is subscribed
 * to, so removing a channel from the user's ACL is a real reconnect whose
 * re-subscribe Redis refuses with NOPERM. Left to ioredis, that refusal is an
 * unhandled rejection that ends the process on a standalone server, and a
 * silent loss of every channel of the refused command on a Cluster. These
 * tests pin that it is reported instead, and that the subscription comes back
 * on its own once the ACL grants the channel again.
 *
 * ACLs are per node on a Cluster, so every ACL change, channel count and
 * client listing covers every node.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
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

// A cluster retries a refused shard channel or keyspace subscription with a
// backoff that can grow past 5 seconds while the grant is withheld.
const RESTORE_TIMEOUT_MS = isClusterBackend() ? 20000 : 5000;

describe('subscriber re-subscription after a reconnect (integration)', () => {
	const user = 'inttest-resub-' + Math.random().toString(16).slice(2, 10);
	const password = 'pw-' + Math.random().toString(16).slice(2, 10);
	let admin;
	/** @type {Array<() => Promise<unknown>>} */
	const cleanups = [];
	let sink = null;
	const unhandled = vi.fn();

	/** The server connections: every node of a Cluster, or the one server. */
	const servers = () => (isClusterBackend() ? admin.redis.nodes('all') : [admin.redis]);
	const onEveryServer = (...args) => Promise.all(servers().map((s) => s.call(...args)));

	/**
	 * A client that connects as the test's ACL user. `readies()` counts the
	 * 'ready' events of the subscriber connections the modules open from it.
	 */
	function userClient(keyPrefix) {
		const auth = { username: user, password };
		const client = createBackendClient({ keyPrefix, options: isClusterBackend() ? { redisOptions: auth } : auth });
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

	const grantAllChannels = () => onEveryServer('ACL', 'SETUSER', user, 'allchannels');
	/** Subscribers the servers hold on a channel. */
	const numsub = async (channel) => {
		let total = 0;
		for (const reply of await onEveryServer('PUBSUB', 'NUMSUB', channel)) total += Number(reply[1]);
		return total;
	};
	/** Pattern subscriptions held by the test user's connections. */
	const userPatterns = async () => {
		let total = 0;
		for (const list of await onEveryServer('CLIENT', 'LIST', 'TYPE', 'pubsub')) {
			for (const line of String(list).split('\n')) {
				if (!line.includes(' user=' + user + ' ')) continue;
				const m = / psub=(\d+)/.exec(line);
				if (m) total += Number(m[1]);
			}
		}
		return total;
	};
	/** The test user's connections holding a channel, by server address and client id. */
	const userSubscribers = async () => {
		const found = [];
		for (const list of await onEveryServer('CLIENT', 'LIST', 'TYPE', 'pubsub')) {
			for (const line of String(list).split('\n')) {
				if (!line.includes(' user=' + user + ' ') || !/ sub=[1-9]/.test(line)) continue;
				found.push(/ laddr=(\S+)/.exec(line)?.[1] + '#' + /^id=(\d+)/.exec(line)?.[1]);
			}
		}
		return found;
	};
	const channelsMatching = async (pattern) => {
		const all = new Set();
		for (const reply of await onEveryServer('PUBSUB', 'CHANNELS', pattern)) {
			for (const channel of reply) all.add(String(channel));
		}
		return [...all];
	};
	// Redis disconnects the user's subscribed clients when a channel is taken away.
	const revokeChannels = () => onEveryServer('ACL', 'SETUSER', user, 'resetchannels');

	beforeAll(async () => {
		admin = createBackendClient({ keyPrefix: 'inttest-resub:' });
		if (isClusterBackend()) await admin.redis.ping();
		await onEveryServer('ACL', 'SETUSER', user, 'reset', 'on', '>' + password, '~*', '&*', '+@all');
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
		await onEveryServer('ACL', 'DELUSER', user).catch(() => {});
		await admin.quit();
	});

	it('pub/sub bus: a refused re-subscribe is reported, and delivery resumes once the channel is granted again', async () => {
		const channel = 'inttest-resub:pubsub:' + Date.now();
		const client = userClient('inttest-resub:');
		const platform = mockPlatform();
		const bus = createPubSubBus(client, { channel });
		cleanups.push(() => bus.deactivate());
		await bus.activate(platform);
		expect(await numsub(channel)).toBe(1);
		sink = captureRegistered();
		const resubscribes = () => sink.records.filter((r) => r.event === 'redis.subscriber.resubscribe.failed');

		await revokeChannels();
		await waitFor(() => resubscribes().length > 0);
		expect(sink.ids()).toContain(EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE);
		expect(resubscribes()[0].attributes).toMatchObject({ owner: 'pubsub', kind: 'subscribe', refused: 1 });
		expect(resubscribes()[0].attributes.error.message).toMatch(/^NOPERM/);
		expect(JSON.stringify(sink.records)).not.toContain(channel);
		expect(await numsub(channel)).toBe(0);

		// The grant does not reconnect anything; the backoff retry restores the channel.
		const readiesBeforeGrant = client.readies();
		await grantAllChannels();
		const envelope = JSON.stringify({ instanceId: 'remote', topic: 'chat', event: 'msg', data: 1 });
		await waitFor(async () => {
			await admin.redis.publish(channel, envelope);
			return platform.published.some((p) => p.event === 'msg');
		});
		expect(await numsub(channel)).toBe(1);
		expect(client.readies()).toBe(readiesBeforeGrant);
		expect(resubscribes()).toHaveLength(1);
		expect(unhandled).not.toHaveBeenCalled();
	});

	it('pub/sub bus: a subscriber connection the server drops comes back with its channel', async () => {
		const channel = 'inttest-resub:dropped:' + Date.now();
		const client = userClient('inttest-resub:');
		const platform = mockPlatform();
		const bus = createPubSubBus(client, { channel });
		cleanups.push(() => bus.deactivate());
		await bus.activate(platform);
		expect(await numsub(channel)).toBe(1);
		const before = await userSubscribers();
		expect(before).toHaveLength(1);

		await onEveryServer('CLIENT', 'KILL', 'USER', user);
		await waitFor(async () => {
			const now = await userSubscribers();
			return now.length === 1 && now[0] !== before[0] && (await numsub(channel)) === 1;
		});
		const envelope = JSON.stringify({ instanceId: 'remote', topic: 'chat', event: 'back', data: 1 });
		await waitFor(async () => {
			await admin.redis.publish(channel, envelope);
			return platform.published.some((p) => p.event === 'back');
		});
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
		// On a cluster the sharded bus subscribes on each slot's master itself and
		// reports a refused channel from its reconciliation.
		const event = isClusterBackend() ? 'redis.sharded-pubsub.channel-reconcile.failed' : 'redis.subscriber.resubscribe.failed';
		const refusals = () => sink.records.filter((r) => r.event === event);

		await revokeChannels();
		await waitFor(() => refusals().length > 0);
		if (!isClusterBackend()) {
			expect(refusals()[0].attributes).toMatchObject({ owner: 'sharded-pubsub', kind: 'ssubscribe', refused: 1 });
		}
		expect(JSON.stringify(sink.records)).not.toContain(prefix);

		const readiesBeforeGrant = client.readies();
		await grantAllChannels();
		const envelope = JSON.stringify({ instanceId: 'remote', topic: 'room', event: 'msg', data: 1 });
		await waitFor(async () => {
			await admin.redis.spublish(prefix + 'room', envelope);
			return platform.published.some((p) => p.event === 'msg');
		}, RESTORE_TIMEOUT_MS);
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
		await onEveryServer('ACL', 'SETUSER', user, 'resetchannels', '&' + eventsChannel);
		await waitFor(() => resubscribes().length > 0);
		expect(resubscribes()[0].attributes).toMatchObject({ owner: 'registry', kind: 'subscribe', refused: 1 });
		await waitFor(async () => (await numsub(eventsChannel)) === 1);
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
		// One keyspace subscription per master on a cluster, where key deletion
		// events are node-local; on standalone it shares the topic's connection.
		const patterns = isClusterBackend() ? admin.redis.nodes('master').length : 1;
		expect(await userPatterns()).toBe(patterns);
		sink = captureRegistered();
		const resubscribes = () => sink.records.filter((r) => r.event === 'redis.subscriber.resubscribe.failed');
		const keyspaceRefusals = () => sink.records.filter((r) => r.event === 'redis.presence.keyspace-subscribe.failed');

		await revokeChannels();
		if (isClusterBackend()) {
			await waitFor(() => resubscribes().length > 0 && keyspaceRefusals().length === patterns);
			expect(resubscribes()[0].attributes).toMatchObject({ owner: 'presence', kind: 'subscribe', refused: 1 });
			// Past the first retries (1 s, then 2 s), each still refused.
			await wait(3500);
		} else {
			await waitFor(() => resubscribes().length > 0);
			expect(resubscribes()[0].attributes).toMatchObject({ owner: 'presence', refused: 2 });
		}
		expect(await userPatterns()).toBe(0);

		await grantAllChannels();
		await waitFor(async () => (await userPatterns()) === patterns, RESTORE_TIMEOUT_MS);
		// Each refusal streak is reported once, however often it was retried.
		expect(keyspaceRefusals()).toHaveLength(isClusterBackend() ? patterns : 0);
		expect(unhandled).not.toHaveBeenCalled();
	});
});
