/**
 * A topic's replay epoch survives a clean slot migration and only changes when
 * the seq space actually resets.
 *
 * The epoch key shares the topic's `{hash-tag}` with seq:/buf:, so a CLUSTER
 * SETSLOT handover MIGRATEs all three together: the seq counter and its buffer
 * arrive contiguous on the new owner and the epoch is unchanged, so a resume
 * gap-fills as before. The dangerous case is a handover that drops the keys
 * (the new owner starts seq at 1): the publish Lua's reset edge bumps the epoch
 * atomically with re-populating the space, so a client holding the pre-reset
 * epoch is cold-rehydrated instead of being served the new shard's seq numbering
 * (1, 2, ...) as if it continued the old one.
 *
 * Mirrors the CLUSTER SETSLOT migration drive in sharded-pubsub-reshard.test.js.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { createReplay } from '../../../src/redis/replay.js';
import { keySlot } from '../../../src/shared/cluster.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

// RedisClient-shaped wrapper around a raw cluster client, prefixed so this
// suite's keys are isolated. The replay tracker only needs `redis` + `key`;
// the migration drive talks to the cluster nodes directly.
function clusterReplayClient(cluster, keyPrefix) {
	return {
		redis: cluster,
		keyPrefix,
		key: (k) => keyPrefix + k,
		duplicate: (o) => clusterClient(o),
		async quit() {
			await cluster.quit().catch(() => cluster.disconnect());
		}
	};
}

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, timeoutMs = 10000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(25);
	}
	throw new Error('waitFor timed out after ' + timeoutMs + 'ms');
}

function ownerIdOf(slots, slot) {
	for (const e of slots) if (slot >= e[0] && slot <= e[1]) return e[2][2];
	return null;
}

describe('replay epoch across a topic slot migration (cluster)', () => {
	/** @type {import('ioredis').Cluster} */
	let ctrl;
	/** @type {Map<string, import('ioredis').Redis>} */
	let mastersById;
	/** @type {Map<string, { host: string, port: number }>} nodeId -> cluster-internal address */
	let addrById;
	let client;
	let platform;
	let replay;

	const prefix = 'inttest-epoch-reshard:';

	beforeAll(async () => {
		ctrl = clusterClient();
		await ctrl.cluster('info');
		mastersById = new Map();
		for (const node of ctrl.nodes('master')) {
			mastersById.set(await node.cluster('MYID'), node);
		}
		// Capture each master's cluster-internal address (the bridge-network IP
		// the nodes announce, e.g. 172.30.0.10:6379) so a node-to-node MIGRATE
		// can target it directly - MIGRATE runs inside the cluster network, not
		// through the host NAT the client uses.
		addrById = new Map();
		const slots = await ctrl.cluster('SLOTS');
		for (const e of slots) {
			const [host, port, id] = e[2];
			addrById.set(id, { host, port: Number(port) });
		}
		client = clusterReplayClient(clusterClient(), prefix);
		platform = mockPlatform();
		replay = createReplay(client, { size: 50 });
	});

	afterAll(async () => {
		if (client) await client.quit();
		if (ctrl) await ctrl.quit().catch(() => ctrl.disconnect());
	});

	// Move a slot's ownership to a new master WITHOUT carrying its keys - the
	// lossy handover (operator SETSLOT NODE without MIGRATE). The slot's data is
	// orphaned on the old node, so the new owner sees an empty seq space.
	async function reassignSlot(slot, fromId, toId) {
		const src = mastersById.get(fromId);
		const tgt = mastersById.get(toId);
		await tgt.cluster('SETSLOT', slot, 'IMPORTING', fromId);
		await src.cluster('SETSLOT', slot, 'MIGRATING', toId);
		for (const [, node] of mastersById) {
			await node.cluster('SETSLOT', slot, 'NODE', toId).catch(() => {});
		}
	}

	// Move a slot AND its keys to a new master - the clean handover. The keys
	// travel via a real node-to-node MIGRATE before ownership flips, so the new
	// owner serves the same seq space contiguous.
	async function migrateSlotWithKeys(slot, fromId, toId, keys) {
		const src = mastersById.get(fromId);
		const tgt = mastersById.get(toId);
		const dst = addrById.get(toId);
		await tgt.cluster('SETSLOT', slot, 'IMPORTING', fromId);
		await src.cluster('SETSLOT', slot, 'MIGRATING', toId);
		// MIGRATE the keys node-to-node (COPY off = move; REPLACE so a re-run is
		// idempotent). All keys share the slot via the topic hash tag, so one
		// multi-key MIGRATE moves them together.
		await src.migrate(dst.host, dst.port, '', 0, 5000, 'REPLACE', 'KEYS', ...keys);
		for (const [, node] of mastersById) {
			await node.cluster('SETSLOT', slot, 'NODE', toId).catch(() => {});
		}
	}

	it('leaves the epoch unchanged when a clean migration carries the seq space', async () => {
		const topic = 'lobby';
		const epochKey = client.key('replay:epoch:{' + topic + '}');
		const seqKey = client.key('replay:seq:{' + topic + '}');
		const bufKey = client.key('replay:buf:{' + topic + '}');
		const slot = keySlot(epochKey);

		await replay.publish(platform, topic, 'created', { id: 1 });
		await replay.publish(platform, topic, 'created', { id: 2 });
		const epochBefore = await replay.currentEpoch(topic);
		expect(epochBefore).toBeGreaterThan(0);

		let slots = await ctrl.cluster('SLOTS');
		const fromId = ownerIdOf(slots, slot);
		const toId = [...mastersById.keys()].find((id) => id !== fromId);
		expect(fromId).toBeTruthy();
		expect(toId).toBeTruthy();
		await migrateSlotWithKeys(slot, fromId, toId, [seqKey, bufKey, epochKey]);

		slots = await ctrl.cluster('SLOTS');
		expect(ownerIdOf(slots, slot)).toBe(toId);

		// A clean migration relocates seq:/buf:/epoch: intact. The seq space is
		// contiguous on the new owner and the epoch is unchanged, so a resume
		// presenting the pre-migration epoch gap-fills rather than rehydrating.
		const epochAfter = await replay.currentEpoch(topic);
		expect(epochAfter).toBe(epochBefore);

		platform.reset();
		const ws = mockWs({});
		const hook = replay.resumeHook();
		await hook(ws, {
			lastSeenSeqs: { [topic]: 1 },
			lastSeenEpochs: { [topic]: epochBefore },
			platform
		});

		const gap = platform.sent.find((s) => s.topic === '__replay:' + topic && s.event === 'msg');
		const rehydrate = platform.sent.find((s) => s.topic === '__replay:' + topic && s.event === 'rehydrate');
		expect(gap).toBeTruthy();
		expect(gap.data.seq).toBe(2);
		expect(rehydrate).toBeFalsy();
	}, 30000);

	it('cold-rehydrates a stale-epoch resume after the seq space is dropped and restarted', async () => {
		const topic = 'board';
		const epochKey = client.key('replay:epoch:{' + topic + '}');
		const seqKey = client.key('replay:seq:{' + topic + '}');
		const bufKey = client.key('replay:buf:{' + topic + '}');

		// A client reaches seq 3 under the pre-reset epoch.
		await replay.publish(platform, topic, 'created', { id: 1 });
		await replay.publish(platform, topic, 'created', { id: 2 });
		await replay.publish(platform, topic, 'created', { id: 3 });
		const staleEpoch = await replay.currentEpoch(topic);
		expect(staleEpoch).toBeGreaterThan(0);

		// Model a lossy handover: the seq + buffer keys are lost (an operator
		// SETSLOT NODE without MIGRATE on populated keys, or a wipe). The epoch
		// key has NO ttl and is NOT dropped here, mirroring a real reshard where
		// only the data keys went missing - the point under test is that the
		// next publish on the empty seq space restarts at 1 and the in-Lua reset
		// edge bumps the epoch atomically.
		await client.redis.unlink(seqKey, bufKey);

		// New owner issues seq 1, 2 for the same topic. Their seq numbers sit
		// BELOW the client's lastSeenSeq of 3, so a naive gap-fill would skip
		// them and the client would silently lose state. The epoch bump on the
		// seq==1 edge is what forces a cold rehydrate instead.
		await replay.publish(platform, topic, 'created', { id: 4 });
		await replay.publish(platform, topic, 'created', { id: 5 });
		await waitFor(async () => (await replay.currentEpoch(topic)) !== staleEpoch);
		const freshEpoch = await replay.currentEpoch(topic);
		expect(freshEpoch).not.toBe(staleEpoch);
		expect(await replay.seq(topic)).toBe(2);

		platform.reset();
		const ws = mockWs({});
		const hook = replay.resumeHook();
		await hook(ws, {
			lastSeenSeqs: { [topic]: 3 },
			lastSeenEpochs: { [topic]: staleEpoch },
			platform
		});

		const rehydrate = platform.sent.find((s) => s.topic === '__replay:' + topic && s.event === 'rehydrate');
		const gap = platform.sent.find((s) => s.topic === '__replay:' + topic && s.event === 'msg');
		// Stale epoch: rehydrate, never serve the restarted seq space as contiguous.
		expect(rehydrate).toBeTruthy();
		expect(gap).toBeFalsy();
	}, 30000);

	it('co-locates the epoch key with seq:/buf: on one slot', async () => {
		const topic = 'room';
		const epochKey = client.key('replay:epoch:{' + topic + '}');
		const seqKey = client.key('replay:seq:{' + topic + '}');
		const bufKey = client.key('replay:buf:{' + topic + '}');
		// The shared {topic} hash tag is what lets the publish eval touch all
		// three keys in one slot under Cluster and what makes them migrate
		// together on a reshard.
		expect(keySlot(epochKey)).toBe(keySlot(seqKey));
		expect(keySlot(epochKey)).toBe(keySlot(bufKey));
	});
});
