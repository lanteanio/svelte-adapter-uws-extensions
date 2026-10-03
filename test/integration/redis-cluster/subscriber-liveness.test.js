/**
 * Cluster subscriber connections whose node leaves the cluster, against a
 * real Redis Cluster.
 *
 * A node that is forgotten by the others and reset keeps running and keeps
 * its connections, and answers them, but PUBLISH no longer reaches it. This
 * pins that a module subscriber on it moves to a member and delivers again.
 *
 * The PING rounds, and the slot map reads every other one, run on demand
 * through the runtime seam.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
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

describe('cluster subscribers on nodes that leave the cluster (cluster)', () => {
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
	const numsub = async (node, channel) => Number((await node.call('PUBSUB', 'NUMSUB', channel))[1]);
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
});
