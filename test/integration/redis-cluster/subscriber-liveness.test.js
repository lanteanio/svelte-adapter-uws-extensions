/**
 * Cluster subscriber connections whose node stops answering or leaves the
 * cluster, against a real Redis Cluster.
 *
 * CLIENT PAUSE ... ALL makes a node hold every command it receives while its
 * connections stay open, which is what a subscriber sees of a host that died
 * without closing its sockets. A node that is forgotten by the others and
 * reset keeps running and keeps its connections, and answers them, but
 * PUBLISH no longer reaches it. These tests pin that each kind of cluster
 * subscriber notices and delivers again.
 *
 * The PING rounds, and the slot map reads every other one, run on demand
 * through the runtime seam; the 10 second PING timeout is real.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { createPresence } from '../../../src/redis/presence.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';
import { captureRegistered } from '../../helpers/diagnostic-line.js';

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, timeoutMs = 10000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(50);
	}
	throw new Error('waitFor timed out after ' + timeoutMs + 'ms');
}

// Longer than the PING timeout and the step limit of what follows it (10
// seconds each), so the subscriber gives up on the node, and a sweep or a
// topology read that waits on it gives up too, while it still holds every
// command.
const PAUSE_MS = 25000;

describe('cluster subscribers on nodes that stop answering or leave (cluster)', () => {
	/** @type {import('ioredis').Cluster} */
	let ctrl;
	/** @type {Array<() => Promise<unknown>>} */
	const cleanups = [];
	let sink = null;
	const unhandled = vi.fn();
	/** @type {Array<{ cb: () => unknown, cleared: boolean }>} */
	let intervals = [];
	/** Picks the node a cluster subscriber samples; null leaves it to chance. */
	let steer = null;

	/** A RedisClient-shaped wrapper whose duplicates are recorded in `dups`. */
	function wrapper(prefix) {
		const cluster = clusterClient();
		const dups = [];
		const client = {
			redis: cluster,
			keyPrefix: prefix,
			key: (k) => prefix + k,
			duplicate: (o) => {
				const dup = clusterClient(o);
				dups.push(dup);
				return dup;
			},
			dups,
			quit: async () => {
				for (const c of [cluster, ...dups]) await c.quit().catch(() => c.disconnect());
			}
		};
		cleanups.push(() => client.quit());
		return client;
	}

	const nodes = () => ctrl.nodes('all');
	/** The one node connection for which `count(node)` is positive. */
	async function nodeWith(count) {
		const found = [];
		for (const node of nodes()) {
			if ((await count(node)) > 0) found.push(node);
		}
		expect(found).toHaveLength(1);
		return found[0];
	}
	const numsub = async (node, channel) => Number((await node.call('PUBSUB', 'NUMSUB', channel))[1]);
	const shardnumsub = async (node, channel) => Number((await node.call('PUBSUB', 'SHARDNUMSUB', channel))[1]);
	/** Ids of the node's connections holding a subscription of a kind (psub, ssub). */
	const clientsWith = async (node, kind) => String(await node.call('CLIENT', 'LIST', 'TYPE', 'pubsub'))
		.split('\n')
		.filter((line) => new RegExp(' ' + kind + '=[1-9]').test(line))
		.map((line) => /^id=(\d+)/.exec(line)?.[1]);

	/** Hold every command on `node` for PAUSE_MS; the test waits it out before the next one. */
	async function pause(node) {
		await node.call('CLIENT', 'PAUSE', String(PAUSE_MS), 'ALL');
		// Answered once the pause is over.
		cleanups.unshift(() => node.ping());
		return Date.now() + PAUSE_MS;
	}
	/** One PING round of every subscriber, as its 30 second interval would run it. */
	const pingRound = () => {
		for (const t of intervals) if (!t.cleared) t.cb();
	};
	const silences = () => sink.records.filter((r) => r.event === 'redis.subscriber.silent');

	/** The cluster's nodes as CLUSTER NODES lists them, with the host port each is reached on. */
	async function topology() {
		const natMap = JSON.parse(process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP);
		const text = String(await nodes()[0].call('CLUSTER', 'NODES'));
		return text.split('\n').filter(Boolean).map((line) => {
			const [id, addr, flags, master] = line.split(' ');
			const internal = addr.split('@')[0];
			return { id, internal, port: natMap[internal].port, replica: flags.includes('slave'), master };
		});
	}

	/** Every node reports a healthy cluster of all six nodes. */
	async function healthy() {
		for (const node of nodes()) {
			const info = String(await node.cluster('INFO'));
			if (!/cluster_state:ok/.test(info) || !/cluster_known_nodes:6\b/.test(info)) return false;
		}
		return true;
	}

	beforeAll(async () => {
		ctrl = clusterClient();
		await ctrl.cluster('INFO');
		process.on('unhandledRejection', unhandled);
	});

	beforeEach(() => {
		intervals = [];
		steer = null;
		setRuntimeEnv({
			timers: {
				setInterval: (cb, ms) => { const t = { cb, ms, cleared: false, unref() {} }; intervals.push(t); return t; },
				clearInterval: (t) => { if (t) t.cleared = true; }
			},
			rng: { float: () => (steer ? steer() : Math.random()) }
		});
		sink = captureRegistered();
	});

	afterEach(async () => {
		sink?.stop();
		sink = null;
		while (cleanups.length > 0) await cleanups.shift()().catch(() => {});
		resetRuntimeEnv();
	});

	afterAll(async () => {
		process.off('unhandledRejection', unhandled);
		if (ctrl) await ctrl.quit().catch(() => ctrl.disconnect());
	});

	it('pub/sub bus: a subscriber on a node removed from the cluster moves to a member within two PING rounds', async () => {
		const all = await topology();
		const replica = all.find((n) => n.replica);
		const master = all.find((n) => n.id === replica.master);
		const replicaNode = nodes().find((n) => n.options.port === replica.port);
		const client = wrapper('inttest-live-removed:');
		// The subscriber samples the replica.
		steer = () => {
			const known = client.dups[0].nodes('all');
			const i = known.findIndex((n) => n.options.port === replica.port);
			return i < 0 ? 0 : (i + 0.5) / known.length;
		};
		const channel = 'inttest-live-removed:' + Date.now();
		const platform = mockPlatform();
		const bus = createPubSubBus(client, { channel });
		cleanups.push(() => bus.deactivate());
		await bus.activate(platform);
		expect(await numsub(replicaNode, channel)).toBe(1);
		steer = null;

		// Removed as redis-cli --cluster del-node does, except that a hard reset
		// gives the node a new id, which lets it rejoin at once afterwards.
		cleanups.unshift(async () => {
			await replicaNode.call('CLUSTER', 'MEET', master.internal.split(':')[0], master.internal.split(':')[1]);
			await waitFor(async () => String(await replicaNode.call('CLUSTER', 'NODES')).split('\n').filter(Boolean).length === 6, 20000);
			await replicaNode.call('CLUSTER', 'REPLICATE', master.id);
			await waitFor(healthy, 30000);
		});
		for (const node of nodes()) {
			if (node !== replicaNode) await node.call('CLUSTER', 'FORGET', replica.id);
		}
		await replicaNode.call('CLUSTER', 'RESET', 'HARD');

		// Still subscribed there and answering, but a publish no longer reaches it.
		const publisher = nodes().find((n) => n.options.port === master.port);
		const envelope = (event) => JSON.stringify({ instanceId: 'remote', topic: 'chat', event, data: 1 });
		await publisher.publish(channel, envelope('lost'));
		await wait(300);
		expect(platform.published.some((p) => p.event === 'lost')).toBe(false);
		expect(await numsub(replicaNode, channel)).toBe(1);

		pingRound();
		pingRound();
		await waitFor(async () => {
			await publisher.publish(channel, envelope('back'));
			return platform.published.some((p) => p.event === 'back');
		});
		expect(await numsub(replicaNode, channel)).toBe(0);
		// A node that left is no silent one.
		expect(silences()).toEqual([]);
		expect(unhandled).not.toHaveBeenCalled();
	}, 90000);

	it('presence: a master that stops answering holds up no keyspace sweep, and its subscription is replaced once it answers', async () => {
		const client = wrapper('inttest-live-presence' + Date.now() + ':');
		const presence = createPresence(client, { key: 'id', select: (ud) => ({ id: ud.id }), heartbeat: 60000, keyspaceNotifications: true });
		cleanups.push(async () => presence.destroy?.());
		await presence.join(mockWs({ id: 'u1' }), 'room', mockPlatform());
		const [target, other] = ctrl.nodes('master');
		const before = await clientsWith(target, 'psub');
		const otherBefore = await clientsWith(other, 'psub');
		expect(before).toHaveLength(1);
		expect(otherBefore).toHaveLength(1);

		const pausedUntil = await pause(target);
		pingRound();
		await waitFor(() => silences().some((r) => r.attributes.owner === 'presence-keyspace'), 12000);
		// The sweep after it waits on the paused master's CLUSTER MYID. Another
		// master's keyspace connection dropped meanwhile is subscribed again
		// before the pause is over: that sweep gave up on the paused master.
		await other.call('CLIENT', 'KILL', 'ID', otherBefore[0]);
		await waitFor(async () => {
			const now = await clientsWith(other, 'psub');
			return now.length === 1 && now[0] !== otherBefore[0];
		}, 15000);
		expect(Date.now()).toBeLessThan(pausedUntil);
		// The paused master is asked again with a backoff until it answers.
		await waitFor(async () => {
			const now = await clientsWith(target, 'psub');
			return now.length === 1 && now[0] !== before[0];
		}, 30000);
		expect(unhandled).not.toHaveBeenCalled();
	}, 90000);

	it('sharded bus: a master that stops answering holds back only its own shard channels, which come back once it answers', async () => {
		const prefix = 'inttest-live:sharded:' + Date.now() + ':';
		const client = wrapper('');
		const platform = mockPlatform();
		const bus = createShardedBus(client, { channelPrefix: prefix });
		cleanups.push(() => bus.deactivate());
		await bus.activate(platform);
		// Topics until two masters own one each.
		/** @type {Map<object, string>} owner node -> a topic on it */
		const topicOn = new Map();
		for (let i = 0; topicOn.size < 2; i++) {
			const topic = 'room-' + i;
			await bus.follow(topic);
			const owner = await nodeWith((node) => shardnumsub(node, prefix + topic));
			if (!topicOn.has(owner)) topicOn.set(owner, topic);
		}
		const [[paused, pausedTopic], [other, otherTopic]] = topicOn;
		const before = await clientsWith(paused, 'ssub');
		const otherBefore = await clientsWith(other, 'ssub');
		expect(otherBefore).toHaveLength(1);
		const envelope = (topic, event) => JSON.stringify({ instanceId: 'remote', topic, event, data: 1 });
		const deliveredOn = (node, topic, event, timeoutMs) => waitFor(async () => {
			await node.spublish(prefix + topic, envelope(topic, event));
			return platform.published.some((p) => p.event === event);
		}, timeoutMs);

		const pausedUntil = await pause(paused);
		pingRound();
		await waitFor(() => silences().some((r) => r.attributes.owner === 'sharded-pubsub'), 12000);
		// The topology read that follows waits on the paused master's CLUSTER
		// MYID. Another master's shard connection dropped meanwhile delivers
		// again before the pause is over: that read gave up on the paused
		// master, and needed nothing more from it.
		await other.call('CLIENT', 'KILL', 'ID', otherBefore[0]);
		await deliveredOn(other, otherTopic, 'other-back', 14000);
		expect(Date.now()).toBeLessThan(pausedUntil);
		// The paused master's channels come back on a new connection once it answers.
		await deliveredOn(paused, pausedTopic, 'paused-back', 40000);
		const now = await clientsWith(paused, 'ssub');
		expect(now).toHaveLength(1);
		expect(now[0]).not.toBe(before[0]);
		expect(unhandled).not.toHaveBeenCalled();
	}, 120000);
});
