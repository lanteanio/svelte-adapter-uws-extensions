/**
 * The sharded bus recovers delivery after a live slot migration.
 *
 * When a hash slot migrates to a new master, Redis drops that slot's shard
 * subscriptions on the old master (ioredis surfaces the server-initiated
 * unsubscribe as a connection error, not a clean event), and a stale slot cache
 * would re-subscribe on the wrong node, where `node.ssubscribe` hangs. This
 * drives a real `CLUSTER SETSLOT` migration of a followed channel's slot and
 * asserts the bus re-resolves the new owner and keeps delivering.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { keySlot } from '../../../src/shared/cluster.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, timeoutMs = 10000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(25);
	}
	throw new Error('waitFor timed out after ' + timeoutMs + 'ms');
}

// Minimal RedisClient-shaped wrapper around a raw cluster client for the bus.
// The cluster path never uses client.duplicate (it duplicates per-master node
// connections itself), and channels are not key-prefixed.
function busClient(cluster) {
	return { redis: cluster, keyPrefix: '', key: (k) => k, duplicate: (o) => clusterClient(o) };
}

function ownerIdOf(slots, slot) {
	for (const e of slots) if (slot >= e[0] && slot <= e[1]) return e[2][2];
	return null;
}

describe('sharded bus survives a live slot migration (cluster)', () => {
	/** @type {import('ioredis').Cluster} */
	let ctrl;
	/** @type {Map<string, import('ioredis').Redis>} nodeId -> master node connection */
	let mastersById;

	beforeAll(async () => {
		ctrl = clusterClient();
		await ctrl.cluster('info');
		mastersById = new Map();
		for (const node of ctrl.nodes('master')) {
			mastersById.set(await node.cluster('MYID'), node);
		}
	});

	afterAll(async () => {
		if (ctrl) await ctrl.quit().catch(() => ctrl.disconnect());
	});

	async function migrateSlot(slot, fromId, toId) {
		const src = mastersById.get(fromId);
		const tgt = mastersById.get(toId);
		await tgt.cluster('SETSLOT', slot, 'IMPORTING', fromId);
		await src.cluster('SETSLOT', slot, 'MIGRATING', toId);
		// A pub/sub channel has no keys to MIGRATE; just hand over ownership, then
		// tell every master so a SLOTS query on any node is immediately fresh.
		for (const [id, node] of mastersById) {
			await node.cluster('SETSLOT', slot, 'NODE', toId).catch(() => {});
		}
		void src;
	}

	it.each([false, true])('re-subscribes after reshard with an outstanding PING: %s', async (queuedPing) => {
		const channelPrefix = 'inttest-reshard:';
		const topic = 'room';
		const slot = keySlot(channelPrefix + topic);

		const clientA = busClient(clusterClient());
		const clientB = busClient(clusterClient());
		const subscribers = new Map();
		await clientB.redis.cluster('info');
		for (const node of clientB.redis.nodes('master')) {
			const id = await node.cluster('MYID');
			const duplicate = node.duplicate.bind(node);
			node.duplicate = (...args) => {
				const conn = duplicate(...args);
				subscribers.set(id, conn);
				return conn;
			};
		}

		const platA = mockPlatform();
		const platB = mockPlatform();
		const busA = createShardedBus(clientA, { channelPrefix });
		const busB = createShardedBus(clientB, { channelPrefix });

		try {
			await busA.activate(platA);
			await busB.activate(platB);
			await busA.follow(topic);
			await busB.follow(topic);

			// Baseline: A -> B delivery works before the reshard.
			busA.wrap(platA).publish(topic, 'msg', { n: 1 });
			await waitFor(() => platB.published.some((p) => p.data && p.data.n === 1));

			// Migrate the followed channel's slot to a different master.
			let slots = await ctrl.cluster('SLOTS');
			const fromId = ownerIdOf(slots, slot);
			const toId = [...mastersById.keys()].find((id) => id !== fromId);
			expect(fromId).toBeTruthy();
			expect(toId).toBeTruthy();
			if (queuedPing) {
				const subscriber = subscribers.get(fromId);
				const write = subscriber.stream.write.bind(subscriber.stream);
				// Keep a real ioredis PING command outstanding by delaying its
				// socket write. The live SUNSUBSCRIBE must update client state while
				// its parser consumes that command instead of emitting an error.
				subscriber.stream.write = (chunk, ...args) => {
					if (chunk.toString().toLowerCase().includes('\r\nping\r\n')) return true;
					return write(chunk, ...args);
				};
				void subscriber.ping().catch(() => {});
				expect(subscriber.commandQueue.length).toBeGreaterThan(0);
			}
			await migrateSlot(slot, fromId, toId);

			slots = await ctrl.cluster('SLOTS');
			expect(ownerIdOf(slots, slot)).toBe(toId); // the slot really moved

			// Recovery: once B reconciles onto the new owner, a publish lands.
			// Publishes are one-shot, so re-emit until B receives (or time out) -
			// this asserts eventual recovery without racing the reconcile.
			await waitFor(async () => {
				busA.wrap(platA).publish(topic, 'msg', { n: 2 });
				await wait(150);
				return platB.published.some((p) => p.data && p.data.n === 2);
			}, 10000);
			expect(platB.published.some((p) => p.data && p.data.n === 2)).toBe(true);

			// Subscribe operations still work on the migrated slot: re-follow from
			// scratch (unsubscribe on the new owner, then re-subscribe) and confirm
			// delivery, so a fresh follow after a reshard resolves the new owner.
			await busB.unfollow(topic);
			await busB.follow(topic);
			await waitFor(async () => {
				busA.wrap(platA).publish(topic, 'msg', { n: 3 });
				await wait(150);
				return platB.published.some((p) => p.data && p.data.n === 3);
			}, 10000);
			expect(platB.published.some((p) => p.data && p.data.n === 3)).toBe(true);
		} finally {
			await busA.deactivate().catch(() => {});
			await busB.deactivate().catch(() => {});
			await clientA.redis.quit().catch(() => clientA.redis.disconnect());
			await clientB.redis.quit().catch(() => clientB.redis.disconnect());
		}
	}, 30000);

	it('gets a followed channel back after its master drops the subscriber connection', async () => {
		const channelPrefix = 'inttest-reconnect:';
		const topic = 'room';
		const slot = keySlot(channelPrefix + topic);

		const clientA = busClient(clusterClient());
		const clientB = busClient(clusterClient());
		const platA = mockPlatform();
		const platB = mockPlatform();
		const busA = createShardedBus(clientA, { channelPrefix });
		const busB = createShardedBus(clientB, { channelPrefix });

		try {
			await busA.activate(platA);
			await busB.activate(platB);
			await busB.follow(topic);

			busA.wrap(platA).publish(topic, 'msg', { n: 1 });
			await waitFor(() => platB.published.some((p) => p.data && p.data.n === 1));

			// The owner closes every pub/sub connection. A cluster node connection
			// never reconnects (ioredis gives it no retry strategy), so the
			// channel only comes back if the bus replaces the connection.
			const ownerId = ownerIdOf(await ctrl.cluster('SLOTS'), slot);
			await mastersById.get(ownerId).client('KILL', 'TYPE', 'pubsub');

			await waitFor(async () => {
				busA.wrap(platA).publish(topic, 'msg', { n: 2 });
				await wait(150);
				return platB.published.some((p) => p.data && p.data.n === 2);
			}, 10000);
			expect(platB.published.some((p) => p.data && p.data.n === 2)).toBe(true);
		} finally {
			await busA.deactivate().catch(() => {});
			await busB.deactivate().catch(() => {});
			await clientA.redis.quit().catch(() => clientA.redis.disconnect());
			await clientB.redis.quit().catch(() => clientB.redis.disconnect());
		}
	}, 30000);
});
