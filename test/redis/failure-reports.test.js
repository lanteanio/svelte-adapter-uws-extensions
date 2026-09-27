import { describe, it, expect, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createCrdtCluster } from '../../src/redis/crdt.js';
import { createSmoothCluster } from '../../src/redis/smooth-cluster.js';
import { createTopicBroadcast } from '../../src/redis/topic-broadcast.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createGroup } from '../../src/redis/groups.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createPublishRateAggregator } from '../../src/redis/publish-rate.js';
import { createSubscriber } from '../../src/redis/presence/subscriber.js';
import { keySlot } from '../../src/shared/cluster.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

const settle = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));

/** A mock client that remembers every duplicate connection it hands out. */
function recordingClient(prefix) {
	const client = mockRedisClient(prefix);
	const duplicates = [];
	const duplicate = client.duplicate.bind(client);
	client.duplicate = (...args) => {
		const dup = duplicate(...args);
		duplicates.push(dup);
		return dup;
	};
	return { client, duplicates };
}

/** Fire the 'error' listener a module attached to a mock connection. */
function failConnection(conn, message = 'read ECONNRESET') {
	const onError = conn._listeners.get('error');
	expect(typeof onError).toBe('function');
	onError(Object.assign(new Error(message), { code: 'ECONNRESET' }));
}

/** The text ioredis emits when a reply arrives that matches no queued command. */
const unmatchedReply = (reply) =>
	'Command queue state error. If you can reproduce this, please report it. Last reply: ' + reply;

/** What Redis and Valkey answer an ACL refusal with: the key or channel is not named. */
const noperm = (what) => new Error('NOPERM No permissions to access a ' + what);

/** A backend, or a proxy in front of it, that repeats what it refused. */
const echoing = (name) => new Error("backend refused '" + name + "'");

let sink = null;
afterEach(() => {
	sink?.stop();
	sink = null;
	resetRuntimeEnv();
});

describe('subscriber connection errors are reported through the registry', () => {
	it('pub/sub bus', async () => {
		const { client, duplicates } = recordingClient('fr-pubsub:');
		const bus = createPubSubBus(client);
		await bus.activate(mockPlatform());
		sink = captureRegistered();
		failConnection(duplicates[0]);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PUBSUB_SUBSCRIBER]);
		expect(sink.records[0].attributes.error).toEqual({ name: 'Error', code: 'ECONNRESET', message: 'read ECONNRESET' });
		await bus.deactivate();
	});

	it('pub/sub bus: an unmatched reply is reported by its kind, without the channel or the payload it carried', async () => {
		const { client, duplicates } = recordingClient('fr-pubsub-late:');
		const bus = createPubSubBus(client);
		await bus.activate(mockPlatform());
		sink = captureRegistered();
		failConnection(duplicates[0], unmatchedReply('message,uws:pubsub,{"topic":"user:42:inbox","data":"secret"}'));
		expect(sink.records[0].attributes.error.message).toBe(
			'Command queue state error. If you can reproduce this, please report it. Last reply: message <redacted>'
		);
		expect(JSON.stringify(sink.records)).not.toMatch(/user:42|secret/);
		await bus.deactivate();
	});

	it('CRDT relay', () => {
		const { client, duplicates } = recordingClient('fr-crdt:');
		const crdt = createCrdtCluster(client);
		crdt.onMessage({ onUpdate() {} });
		sink = captureRegistered();
		failConnection(duplicates[0]);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.CRDT_SUBSCRIBER]);
		crdt.destroy?.();
	});

	it('smooth relay', () => {
		const { client, duplicates } = recordingClient('fr-smooth:');
		const smooth = createSmoothCluster(client);
		smooth.onMessage({ onCommand() {} });
		sink = captureRegistered();
		failConnection(duplicates[0]);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.SMOOTH_SUBSCRIBER]);
		smooth.destroy?.();
	});

	it('topic broadcast', () => {
		const { client, duplicates } = recordingClient('fr-broadcast:');
		const broadcast = createTopicBroadcast(client);
		broadcast.onRequest(async () => []);
		sink = captureRegistered();
		failConnection(duplicates[0]);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.TOPIC_BROADCAST_SUBSCRIBER]);
		broadcast.destroy?.();
	});

	it('connection registry', async () => {
		const { client, duplicates } = recordingClient('fr-registry:');
		const registry = createConnectionRegistry(client, { identify: (ws) => ws.getUserData().userId, heartbeat: 60000 });
		try {
			const ws = mockWs({ userId: 'u1' });
			ws.getUserData()[WS_SESSION_ID] = 's1';
			await registry.hooks.open(ws, { platform: mockPlatform() });
			sink = captureRegistered();
			failConnection(duplicates[0]);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REGISTRY_SUBSCRIBER]);
		} finally {
			await registry.destroy();
		}
	});

	it('group, named by the group', async () => {
		captureIntervals();
		const { client, duplicates } = recordingClient('fr-group-sub:');
		const group = createGroup(client, 'lobby');
		try {
			await group.join(mockWs({ id: 'member-1' }), mockPlatform());
			sink = captureRegistered();
			failConnection(duplicates[0]);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.GROUPS_SUBSCRIBER]);
			expect(sink.records[0].attributes).toEqual({
				group: 'lobby',
				error: { name: 'Error', code: 'ECONNRESET', message: 'read ECONNRESET' }
			});
		} finally {
			group.destroy();
		}
	});

	it('cursor', () => {
		captureIntervals();
		const { client, duplicates } = recordingClient('fr-cursor-sub:');
		const cursor = createCursor(client);
		try {
			cursor.update(mockWs({ id: 'c1' }), 'user:42:board', { x: 1, y: 2 }, mockPlatform());
			sink = captureRegistered();
			failConnection(duplicates[0]);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.CURSOR_SUBSCRIBER]);
			expect(JSON.stringify(sink.records)).not.toContain('user:42');
		} finally {
			cursor.destroy?.();
		}
	});

	it('publish-rate aggregator', async () => {
		const { client, duplicates } = recordingClient('fr-rate:');
		const aggregator = createPublishRateAggregator(client);
		await aggregator.activate(mockPlatform());
		try {
			sink = captureRegistered();
			failConnection(duplicates[0]);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PUBLISH_RATE_SUBSCRIBER]);
		} finally {
			await aggregator.deactivate();
		}
	});

	for (const [label, id, make, trigger] of [
		['CRDT relay', 'CRDT_SUBSCRIBE', (client) => createCrdtCluster(client), (relay) => relay.onMessage({ onUpdate() {} })],
		['smooth relay', 'SMOOTH_SUBSCRIBE', (client) => createSmoothCluster(client), (relay) => relay.onMessage({ onCommand() {} })],
		['topic broadcast', 'TOPIC_BROADCAST_SUBSCRIBE', (client) => createTopicBroadcast(client), (relay) => relay.onRequest(async () => [])]
	]) {
		it(label + ': a refused subscribe is reported once per failure streak', async () => {
			const { client, duplicates } = recordingClient('fr-sub-' + id + ':');
			const relay = make(client);
			const duplicate = client.duplicate;
			client.duplicate = (...args) => {
				const conn = duplicate(...args);
				conn.subscribe = () => Promise.reject(noperm('channel'));
				return conn;
			};
			sink = captureRegistered();
			try {
				trigger(relay);
				await settle();
				trigger(relay);
				await settle();
				const reports = () => sink.ids().filter((x) => x === EXTENSIONS_ERROR_IDS[id]);
				expect(duplicates.length).toBeGreaterThanOrEqual(2);
				expect(reports()).toHaveLength(1);
				expect(sink.records[sink.ids().indexOf(EXTENSIONS_ERROR_IDS[id])].attributes.error).toEqual({ name: 'Error', code: null, message: 'NOPERM No permissions to access a channel' });
			} finally {
				relay.destroy?.();
			}
		});
	}

	it('cursor relay: a catch-up that fails after a good subscribe is not a subscribe failure', async () => {
		captureIntervals();
		const client = mockRedisClient('fr-cursor-catchup:');
		// Another instance's live position, so the catch-up has something to deliver.
		await client.redis.hset(client.key('cursor:{room}'), 'other:c9', JSON.stringify({ user: { id: 'c9' }, data: { x: 1 }, ts: Date.now() }));
		const platform = mockPlatform();
		const publish = platform.publish.bind(platform);
		let catalogs = 0;
		platform.publish = (topic, event, ...rest) => {
			if (event === 'catalog') {
				catalogs++;
				throw new Error('catalog delivery failed');
			}
			return publish(topic, event, ...rest);
		};
		const cursor = createCursor(client);
		sink = captureRegistered();
		try {
			cursor.update(mockWs({ id: 'c1' }), 'room', { x: 1, y: 2 }, platform);
			await settle();
			expect(catalogs).toBeGreaterThan(0);
			expect(sink.ids()).not.toContain(EXTENSIONS_ERROR_IDS.CURSOR_SUBSCRIBE);
		} finally {
			cursor.destroy?.();
		}
	});

	it('cursor relay: a refused subscribe is reported once per failure streak', async () => {
		captureIntervals();
		const { client } = recordingClient('fr-cursor-subscribe:');
		const duplicate = client.duplicate;
		client.duplicate = (...args) => {
			const conn = duplicate(...args);
			conn.subscribe = () => Promise.reject(noperm('channel'));
			return conn;
		};
		const cursor = createCursor(client);
		sink = captureRegistered();
		try {
			cursor.update(mockWs({ id: 'c1' }), 'user:42:board', { x: 1, y: 2 }, mockPlatform());
			await settle();
			cursor.update(mockWs({ id: 'c1' }), 'user:42:board', { x: 2, y: 2 }, mockPlatform());
			await settle();
			expect(sink.ids().filter((x) => x === EXTENSIONS_ERROR_IDS.CURSOR_SUBSCRIBE)).toHaveLength(1);
		} finally {
			cursor.destroy?.();
		}
	});

	it('presence', async () => {
		const { client, duplicates } = recordingClient('fr-presence:');
		const subscriber = presenceSubscriber(client, false);
		try {
			await subscriber.subscribeToTopic('user:42:room', mockPlatform());
			sink = captureRegistered();
			failConnection(duplicates[0]);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_SUBSCRIBER]);
			expect(JSON.stringify(sink.records)).not.toContain('user:42');
		} finally {
			await subscriber.dispose?.();
		}
	});
});

/** Capture interval callbacks through the runtime seam, so a test runs one tick on demand. */
function captureIntervals() {
	/** @type {Array<{ cb: () => unknown, ms: number }>} */
	const intervals = [];
	setRuntimeEnv({ timers: {
		setInterval: (cb, ms) => { intervals.push({ cb, ms }); return { unref() {} }; },
		clearInterval: () => {}
	} });
	return intervals;
}

/** The presence subscriber the tracker creates, on its own. */
function presenceSubscriber(client, keyspaceNotifications) {
	return createSubscriber({
		client,
		instanceId: 'i1',
		maxEnvelopeBytes: 65536,
		keyspaceNotifications,
		bufferDiff() {},
		bufferUpdate() {},
		localData: new Map(),
		emit() {},
		eventChannel: (topic) => client.key('presence:events:' + topic),
		mKeyspaceCleanups: undefined
	});
}

describe('background sweeps report their failures', () => {
	it('groups heartbeat names the group, once per failure streak', async () => {
		const intervals = captureIntervals();
		const client = mockRedisClient('fr-groups:');
		const group = createGroup(client, 'lobby');
		const pipeline = client.redis.pipeline.bind(client.redis);
		const failingPipeline = () => {
			const pipe = new Proxy({}, { get: (_, prop) => (prop === 'exec' ? () => Promise.reject(new Error('pipeline down')) : () => pipe) });
			return pipe;
		};
		const beat = async () => {
			for (const { cb } of intervals) cb();
			await settle();
		};
		try {
			await group.join(mockWs({ id: 'member-1' }), mockPlatform());
			client.redis.pipeline = failingPipeline;
			sink = captureRegistered();
			await beat();
			await beat();
			const heartbeats = () => sink.records.filter((r) => r.event === 'redis.groups.heartbeat.failed');
			expect(heartbeats()).toHaveLength(1);
			expect(heartbeats()[0].attributes).toEqual({ group: 'lobby', error: { name: 'Error', code: null, message: 'pipeline down' } });

			// A successful beat re-arms the report for the next streak.
			client.redis.pipeline = pipeline;
			await beat();
			client.redis.pipeline = failingPipeline;
			await beat();
			expect(heartbeats()).toHaveLength(2);
		} finally {
			group.destroy();
		}
	});

	it('cursor cleanup reports once per failure streak, with counts and without the topic', async () => {
		const intervals = captureIntervals();
		const client = mockRedisClient('fr-cursor:');
		const cursor = createCursor(client);
		const evalScript = client.redis.eval.bind(client.redis);
		const refuse = async (_script, _numKeys, key) => { throw echoing(key); };
		const sweep = async () => {
			for (const { cb } of intervals) cb();
			await settle();
		};
		try {
			cursor.update(mockWs({ id: 'c1' }), 'user:42:board', { x: 1, y: 2 }, mockPlatform());
			cursor.update(mockWs({ id: 'c2' }), 'user:43:board', { x: 1, y: 2 }, mockPlatform());
			client.redis.eval = refuse;
			sink = captureRegistered();
			await sweep();
			await sweep();
			const cleanups = () => sink.records.filter((r) => r.event === 'redis.cursor.cleanup.failed');
			expect(cleanups()).toHaveLength(1);
			expect(cleanups()[0].attributes).toEqual({
				failed: 2,
				topics: 2,
				error: { name: 'Error', code: null, message: "backend refused '<redacted>'" }
			});
			expect(JSON.stringify(sink.records)).not.toMatch(/user:4[23]/);

			// The streak narrowing from every topic to one is reported once more.
			const refuseOne = async (script, numKeys, key, ...rest) => {
				if (key.includes('user:43')) throw echoing(key);
				return evalScript(script, numKeys, key, ...rest);
			};
			client.redis.eval = refuseOne;
			await sweep();
			await sweep();
			expect(cleanups()).toHaveLength(2);
			expect(cleanups()[1].attributes).toMatchObject({ failed: 1, topics: 2 });

			// A clean sweep re-arms the report, even for the scope reported last.
			client.redis.eval = evalScript;
			await sweep();
			client.redis.eval = refuseOne;
			await sweep();
			expect(cleanups()).toHaveLength(3);
		} finally {
			cursor.destroy?.();
		}
	});
});

/**
 * A cluster-shaped client: `nodes('master')`, `CLUSTER MYID` per node, one
 * `CLUSTER SLOTS` range owned by a switchable node, and per-node duplicate
 * connections whose shard subscribe can be made to refuse or to hang.
 */
function fakeCluster() {
	let owner = 'node-a';
	let slotsDown = false;
	let created = 0;
	/** @type {Promise<void> | null} while set, CLUSTER MYID waits for it */
	let myidGate = null;
	/** @type {Map<string, any>} */
	const conns = new Map();
	const conn = (id) => {
		const listeners = new Map();
		created++;
		const c = {
			refuse: false,
			hang: false,
			/** @type {Promise<void> | null} while set, ssubscribe waits for it */
			gate: null,
			attempts: 0,
			subscribed: new Set(),
			on(event, fn) { listeners.set(event, fn); return c; },
			removeAllListeners() { listeners.clear(); },
			emit(event, ...args) { listeners.get(event)?.(...args); },
			async ssubscribe(channel) {
				c.attempts++;
				if (c.gate) await c.gate;
				if (c.hang) return new Promise(() => {});
				if (c.refuse) throw noperm('channel');
				c.subscribed.add(channel);
			},
			async sunsubscribe(channel) { c.subscribed.delete(channel); },
			async psubscribe() { if (c.refuse) throw noperm('channel'); },
			async quit() {},
			disconnect() {}
		};
		conns.set(id, c);
		return c;
	};
	/** @type {Map<string, string | Error>} what CONFIG GET notify-keyspace-events answers per node */
	const keyspaceFlags = new Map();
	const node = (id) => ({
		cluster: async () => {
			if (myidGate) await myidGate;
			return id;
		},
		duplicate: () => conn(id),
		config: async () => {
			const flags = keyspaceFlags.get(id) ?? 'Eg';
			if (flags instanceof Error) throw flags;
			return ['notify-keyspace-events', flags];
		}
	});
	const masters = [node('node-a'), node('node-b')];
	return {
		conns,
		masters,
		setKeyspaceFlags(id, flags) { keyspaceFlags.set(id, flags); },
		get created() { return created; },
		/** Hold CLUSTER MYID until the returned function is called. */
		holdMyid() {
			let release;
			myidGate = new Promise((resolve) => { release = resolve; });
			return () => { myidGate = null; release(); };
		},
		setOwner(id) { owner = id; },
		setSlotsDown(down) { slotsDown = down; },
		redis: {
			info: async () => '# Server\nredis_version:7.2.0\n',
			nodes: () => masters,
			cluster: async () => {
				if (slotsDown) throw new Error('CLUSTERDOWN The cluster is down');
				// A null owner leaves every slot out of the map.
				return owner === null ? [] : [[0, 16383, ['127.0.0.1', 7000, owner]]];
			},
			spublish: async () => 0,
			on() {}
		}
	};
}

const CHANNEL = 'uws:sharded:user:42:inbox';

async function followedCluster() {
	const cluster = fakeCluster();
	const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
	await bus.activate(mockPlatform());
	await bus.follow('user:42:inbox');
	return { cluster, bus };
}

describe('cluster-only failure paths', () => {
	it('a shard channel refused on its new owner is reported by its slot, once per failure streak', async () => {
		const { cluster, bus } = await followedCluster();
		cluster.setOwner('node-b');
		cluster.conns.get('node-b').refuse = true;
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();

		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER, EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE]);
		expect(sink.records[1].attributes).toEqual({
			slot: keySlot(CHANNEL),
			failed: 1,
			error: { name: 'Error', code: null, message: 'NOPERM No permissions to access a channel' }
		});
		expect(JSON.stringify(sink.records)).not.toContain('user:42');

		// A second failing reconciliation stays quiet; one that completes re-arms it.
		const channelReports = () => sink.ids().filter((id) => id === EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE).length;
		cluster.conns.get('node-b').emit('error', new Error('MOVED'));
		await settle();
		expect(channelReports()).toBe(1);
		cluster.conns.get('node-b').refuse = false;
		cluster.conns.get('node-b').emit('error', new Error('MOVED'));
		await settle();
		cluster.setOwner('node-a');
		cluster.conns.get('node-a').refuse = true;
		cluster.conns.get('node-b').emit('error', new Error('MOVED'));
		await settle();
		expect(channelReports()).toBe(2);
		await bus.deactivate();
	});

	it('a shard channel whose new owner repeats it in an error has it cut', async () => {
		const { cluster, bus } = await followedCluster();
		cluster.setOwner('node-b');
		cluster.conns.get('node-b').ssubscribe = async (channel) => { throw echoing(channel); };
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();
		expect(sink.records[1].attributes.error.message).toBe("backend refused '<redacted>'");
		await bus.deactivate();
	});

	it('a shard channel whose new owner is not a known master names the slot, not the channel', async () => {
		const { cluster, bus } = await followedCluster();
		cluster.setOwner('node-c');
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();

		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER, EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE]);
		expect(sink.records[1].attributes.error.message).toBe('sharded bus: no owning node for slot ' + keySlot(CHANNEL));
		expect(JSON.stringify(sink.records)).not.toContain('user:42');
		await bus.deactivate();
	});

	it('a shard subscribe that hangs on its new owner times out naming the slot, not the channel', async () => {
		const { cluster, bus } = await followedCluster();
		// The 5 s shard-subscribe bound elapses at once; every other timer keeps its delay.
		setRuntimeEnv({ timers: { set: (cb, ms, ...a) => setTimeout(cb, ms >= 5000 ? 1 : ms, ...a) } });
		cluster.setOwner('node-b');
		cluster.conns.get('node-b').hang = true;
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle(60);

		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER, EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE]);
		expect(sink.records[1].attributes.error.message).toBe(
			'sharded bus: ssubscribe on slot ' + keySlot(CHANNEL) + ' (retry) timed out (stale slot owner)'
		);
		expect(JSON.stringify(sink.records)).not.toContain('user:42');
		await bus.deactivate();
	});

	it('the unsubscribe a slot migration pushes is reported without the channel it names', async () => {
		const { cluster, bus } = await followedCluster();
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error(unmatchedReply('sunsubscribe,' + CHANNEL + ',0')));
		await settle();

		expect(sink.ids()[0]).toBe(EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER);
		expect(sink.records[0].attributes.error.message).toBe(
			'Command queue state error. If you can reproduce this, please report it. Last reply: sunsubscribe <redacted>'
		);
		expect(JSON.stringify(sink.records)).not.toContain('user:42');
		await bus.deactivate();
	});

	it('reconciliation that cannot refresh slot ownership is reported', async () => {
		const { cluster, bus } = await followedCluster();
		cluster.setSlotsDown(true);
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();

		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER, EXTENSIONS_ERROR_IDS.SHARDED_RECONCILE]);
		expect(sink.records[1].attributes.error).toEqual({ name: 'Error', code: null, message: 'CLUSTERDOWN The cluster is down' });
		await bus.deactivate();
	});

	it('retries a channel its new owner refused until it subscribes', async () => {
		const { cluster, bus } = await followedCluster();
		// Every retry delay elapses at once; the reconcile debounce keeps its 0.
		setRuntimeEnv({ timers: { set: (cb, ms, ...a) => setTimeout(cb, ms >= 1000 ? 2 : ms, ...a) } });
		cluster.setOwner('node-b');
		const nodeB = cluster.conns.get('node-b');
		nodeB.refuse = true;
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle(60);
		expect(nodeB.attempts).toBeGreaterThan(4);
		expect(sink.ids().filter((id) => id === EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE)).toHaveLength(1);

		nodeB.refuse = false;
		await settle(60);
		expect(nodeB.subscribed.has(CHANNEL)).toBe(true);
		const attempts = nodeB.attempts;
		await settle(30);
		expect(nodeB.attempts).toBe(attempts);
		await bus.deactivate();
	});

	it('does not wait out a pending retry when a subscriber error arrives', async () => {
		const { cluster, bus } = await followedCluster();
		cluster.setOwner('node-b');
		const nodeB = cluster.conns.get('node-b');
		nodeB.refuse = true;
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();
		// The first retry is a second away; the next error reconciles now.
		nodeB.refuse = false;
		nodeB.emit('error', new Error('MOVED'));
		await settle();
		expect(nodeB.subscribed.has(CHANNEL)).toBe(true);
		await bus.deactivate();
	});

	it('retries a channel whose slot has no owner in the map', async () => {
		const { cluster, bus } = await followedCluster();
		setRuntimeEnv({ timers: { set: (cb, ms, ...a) => setTimeout(cb, ms >= 1000 ? 2 : ms, ...a) } });
		cluster.setOwner(null);
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle(40);
		const record = sink.records.find((r) => r.event === 'redis.sharded-pubsub.channel-reconcile.failed');
		expect(record.attributes.error.message).toBe('sharded bus: no owning node for slot ' + keySlot(CHANNEL));
		cluster.setOwner('node-b');
		await settle(40);
		expect(cluster.conns.get('node-b').subscribed.has(CHANNEL)).toBe(true);
		await bus.deactivate();
	});

	it('opens no subscriber connection when the bus closes during a topology refresh', async () => {
		const { cluster, bus } = await followedCluster();
		const release = cluster.holdMyid();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();
		const created = cluster.created;
		await bus.deactivate();
		release();
		await settle();
		expect(cluster.created).toBe(created);
	});

	it('opens no subscriber connection when a resubscribe fails after the bus closed', async () => {
		const { cluster, bus } = await followedCluster();
		cluster.setOwner('node-b');
		const nodeB = cluster.conns.get('node-b');
		let open;
		nodeB.gate = new Promise((resolve) => { open = resolve; });
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();
		const created = cluster.created;
		await bus.deactivate();
		// The closed connection's pending subscribe fails, as ioredis's does.
		nodeB.refuse = true;
		open();
		await settle();
		expect(cluster.created).toBe(created);
	});

	it('opens no subscriber connection when a follow outlives the bus closing', async () => {
		const { cluster, bus } = await followedCluster();
		const release = cluster.holdMyid();
		// The error marks the slot map stale, so the follow waits on a refresh.
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();
		const pending = bus.follow('user:43:inbox');
		await settle();
		const created = cluster.created;
		await bus.deactivate();
		release();
		await pending.catch(() => {});
		await settle();
		expect(cluster.created).toBe(created);
	});

	it('a reconciliation from before a close leaves the next activation\'s reports alone', async () => {
		const { cluster, bus } = await followedCluster();
		// A reconciliation whose resubscribe is still in flight when the bus closes.
		cluster.setOwner('node-b');
		const staleB = cluster.conns.get('node-b');
		let open;
		staleB.gate = new Promise((resolve) => { open = resolve; });
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();
		await bus.deactivate();

		// The bus comes back and starts a failure streak of its own.
		await bus.activate(mockPlatform());
		await bus.follow('user:42:inbox');
		cluster.setOwner('node-a');
		cluster.conns.get('node-a').refuse = true;
		sink = captureRegistered();
		const channelReports = () => sink.ids().filter((id) => id === EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE).length;
		cluster.conns.get('node-b').emit('error', new Error('MOVED'));
		await settle();
		expect(channelReports()).toBe(1);

		// The old reconciliation finishes; the streak must still be latched.
		open();
		await settle();
		cluster.conns.get('node-b').emit('error', new Error('MOVED'));
		await settle();
		expect(channelReports()).toBe(1);
		await bus.deactivate();
	});

	it('leaves a channel unfollowed during followBatch out of later reconciliations', async () => {
		const { cluster, bus } = await followedCluster();
		const nodeA = cluster.conns.get('node-a');
		let open;
		nodeA.gate = new Promise((resolve) => { open = resolve; });
		const batch = bus.followBatch(['user:7:inbox']);
		await settle();
		await bus.unfollow('user:7:inbox');
		open();
		await batch;
		nodeA.gate = null;
		const attempts = nodeA.attempts;
		nodeA.emit('error', new Error('MOVED'));
		await settle();
		expect(nodeA.attempts).toBe(attempts);
		expect(nodeA.subscribed.has('uws:sharded:user:7:inbox')).toBe(false);
		await bus.deactivate();
	});

	it('undoes a resubscribe that completes after its channel was unfollowed', async () => {
		const { cluster, bus } = await followedCluster();
		cluster.setOwner('node-b');
		const nodeB = cluster.conns.get('node-b');
		let open;
		nodeB.gate = new Promise((resolve) => { open = resolve; });
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle();
		await bus.unfollow('user:42:inbox');
		open();
		await settle();
		expect(nodeB.attempts).toBeGreaterThan(0);
		expect(nodeB.subscribed.has(CHANNEL)).toBe(false);
		await bus.deactivate();
	});

	it('stops retrying once the bus is deactivated', async () => {
		const { cluster, bus } = await followedCluster();
		setRuntimeEnv({ timers: { set: (cb, ms, ...a) => setTimeout(cb, ms >= 1000 ? 2 : ms, ...a) } });
		cluster.setOwner('node-b');
		const nodeB = cluster.conns.get('node-b');
		nodeB.refuse = true;
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle(20);
		await bus.deactivate();
		const attempts = nodeB.attempts;
		await settle(30);
		expect(nodeB.attempts).toBe(attempts);
	});

	it('retries a reconciliation whose slot map could not be read', async () => {
		const { cluster, bus } = await followedCluster();
		setRuntimeEnv({ timers: { set: (cb, ms, ...a) => setTimeout(cb, ms >= 1000 ? 2 : ms, ...a) } });
		cluster.setOwner('node-b');
		cluster.setSlotsDown(true);
		sink = captureRegistered();
		cluster.conns.get('node-a').emit('error', new Error('MOVED'));
		await settle(40);
		expect(sink.ids().filter((id) => id === EXTENSIONS_ERROR_IDS.SHARDED_RECONCILE)).toHaveLength(1);
		cluster.setSlotsDown(false);
		await settle(40);
		expect(cluster.conns.get('node-b').subscribed.has(CHANNEL)).toBe(true);
		await bus.deactivate();
	});

	it('a malformed shard envelope is asserted by its slot, not its channel', async () => {
		const { cluster, bus } = await followedCluster();
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			cluster.conns.get('node-a').emit('smessage', CHANNEL, '{"instanceId":7}');
			const line = error.mock.calls.map((call) => String(call[0])).find((text) => text.includes('sharded-bus.envelope.shape'));
			expect(line).toBeDefined();
			expect(JSON.parse(line.slice(line.indexOf('{'))).attributes.context).toEqual({ slot: keySlot(CHANNEL) });
			expect(line).not.toContain('user:42');
		} finally {
			error.mockRestore();
			await bus.deactivate();
		}
	});

	it('a master that refuses the keyspace subscription is reported by its node id', async () => {
		const cluster = fakeCluster();
		cluster.conns.clear();
		const client = mockRedisClient('fr-keyspace:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		// Refuse on node-b only; node-a subscribes normally.
		const duplicateB = cluster.masters[1].duplicate;
		cluster.masters[1].duplicate = () => { const c = duplicateB(); c.refuse = true; return c; };
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBE]);
			expect(sink.records[0].attributes).toEqual({
				master: 'node-b',
				error: { name: 'Error', code: null, message: 'NOPERM No permissions to access a channel' }
			});
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master whose keyspace notification setting cannot fire the cleanup is reported with its flags', async () => {
		const cluster = fakeCluster();
		cluster.setKeyspaceFlags('node-a', 'KEA');
		cluster.setKeyspaceFlags('node-b', 'Kg');
		const client = mockRedisClient('fr-keyspace-flags:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			await settle();
			// KEA includes g; Kg has no keyevent (E) channel at all.
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_EVENTS_OFF]);
			expect(sink.records[0].attributes).toEqual({ master: 'node-b', flags: 'Kg' });
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master that refuses CONFIG is not reported', async () => {
		const cluster = fakeCluster();
		cluster.setKeyspaceFlags('node-a', new Error("ERR unknown command 'CONFIG'"));
		cluster.setKeyspaceFlags('node-b', new Error("NOPERM User default has no permissions to run the 'config|get' command"));
		const client = mockRedisClient('fr-keyspace-noconfig:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			await settle();
			expect(sink.ids()).toEqual([]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a standalone server whose keyspace notification setting cannot fire the cleanup is reported', async () => {
		const client = mockRedisClient('fr-keyspace-solo:');
		client.redis.config = async () => ['notify-keyspace-events', ''];
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			await settle();
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_EVENTS_OFF]);
			expect(sink.records[0].attributes).toEqual({ flags: '' });
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace subscriber error, and a reconciliation that then fails, are both reported', async () => {
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-rec:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			// A master added by a failover whose connection cannot be opened.
			cluster.masters.push({ cluster: async () => 'node-c', duplicate: () => { throw new Error('connect ECONNREFUSED'); } });
			sink = captureRegistered();
			cluster.conns.get('node-a').emit('error', new Error('read ECONNRESET'));
			await settle();

			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBER, EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_RECONCILE]);
			expect(sink.records[1].attributes.error).toEqual({ name: 'Error', code: null, message: 'connect ECONNREFUSED' });
		} finally {
			await subscriber.dispose?.();
		}
	});
});
