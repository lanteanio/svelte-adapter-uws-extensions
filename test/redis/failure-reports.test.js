import { describe, it, expect, afterEach, vi } from 'vitest';
import { EventEmitter } from 'node:events';
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
import { createPresence } from '../../src/redis/presence.js';
import { createRedisIo } from '../../src/redis/cursor/redis-io.js';
import { keySlot } from '../../src/shared/cluster.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

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

/** What a replica answers a write with after a failover. */
const readonly = () => new Error("READONLY You can't write against a read only replica.");

/** A breaker double that records what it was told. */
function recordingBreaker() {
	return {
		state: 'healthy',
		isHealthy: true,
		guard() {},
		success: vi.fn(),
		failure: vi.fn(),
		subscribe() { return () => {}; }
	};
}

/** A metrics registry double that records every counter increment. */
function recordingMetrics() {
	/** @type {Array<{ name: string, labels: any, value: any }>} */
	const incs = [];
	return {
		incs,
		/** Sum of a counter's increments: inc(n), inc(labels, n) and inc(labels) all count. */
		count: (name) => incs
			.filter((inc) => inc.name === name)
			.reduce((n, inc) => n + (typeof inc.labels === 'number' ? inc.labels : (inc.value ?? 1)), 0),
		counter: (name) => ({ inc: (labels, value) => { incs.push({ name, labels, value }); } }),
		histogram: () => ({ observe() {} }),
		gauge: () => ({ set() {}, inc() {}, dec() {} }),
		mapTopic: (topic) => topic
	};
}

/**
 * Run fn outside test mode (no VITEST), with NODE_ENV set to the given mode:
 * production by default, the mode a deployment runs in.
 */
async function inMode(fn, nodeEnv = 'production') {
	const vitest = process.env.VITEST;
	const previous = process.env.NODE_ENV;
	delete process.env.VITEST;
	process.env.NODE_ENV = nodeEnv;
	try {
		return await fn();
	} finally {
		if (vitest === undefined) delete process.env.VITEST;
		else process.env.VITEST = vitest;
		if (previous === undefined) delete process.env.NODE_ENV;
		else process.env.NODE_ENV = previous;
	}
}

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

/** Capture one-shot timers through the runtime seam, so a test fires them on demand. */
function captureTimers() {
	/** @type {Array<{ cb: () => unknown, ms: number, cleared: boolean, fired: boolean }>} */
	const timers = [];
	setRuntimeEnv({ timers: {
		set: (cb, ms) => { const t = { cb, ms, cleared: false, fired: false, unref() {} }; timers.push(t); return t; },
		clear: (t) => { if (t) t.cleared = true; }
	} });
	const pending = () => timers.filter((t) => !t.cleared && !t.fired);
	return {
		pending,
		async fire() {
			for (const t of pending()) {
				t.fired = true;
				t.cb();
			}
			await settle();
		}
	};
}

/**
 * Capture one-shot timers and intervals through the runtime seam: fire() runs
 * the timers due, tick() one round of every interval.
 */
function captureClock() {
	/** @type {Array<{ cb: () => unknown, ms: number, cleared: boolean, fired?: boolean }>} */
	const timers = [];
	/** @type {Array<{ cb: () => unknown, ms: number, cleared: boolean }>} */
	const intervals = [];
	const timer = (list) => (cb, ms) => { const t = { cb, ms, cleared: false, unref() {} }; list.push(t); return t; };
	const clear = (t) => { if (t) t.cleared = true; };
	setRuntimeEnv({ timers: { set: timer(timers), clear, setInterval: timer(intervals), clearInterval: clear } });
	const pending = () => timers.filter((t) => !t.cleared && !t.fired);
	return {
		pending,
		running: () => intervals.filter((t) => !t.cleared),
		async fire() {
			for (const t of pending()) {
				t.fired = true;
				t.cb();
			}
			await settle();
		},
		async tick() {
			for (const t of intervals.filter((t) => !t.cleared)) t.cb();
			await settle();
		}
	};
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
	/** @type {Promise<void> | null} while set, CLUSTER SLOTS waits for it */
	let slotsGate = null;
	/** @type {Promise<void> | null} while set, every psubscribe waits for it */
	let psubscribeGate = null;
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
			on(event, fn) {
				if (!listeners.has(event)) listeners.set(event, []);
				listeners.get(event).push(fn);
				return c;
			},
			removeAllListeners() { listeners.clear(); },
			emit(event, ...args) { for (const fn of listeners.get(event) ?? []) fn(...args); },
			async ssubscribe(channel) {
				c.attempts++;
				if (c.gate) await c.gate;
				if (c.hang) return new Promise(() => {});
				if (c.refuse) throw noperm('channel');
				c.subscribed.add(channel);
			},
			async sunsubscribe(channel) { c.subscribed.delete(channel); },
			patterns: new Set(),
			async psubscribe(pattern) {
				if (psubscribeGate) await psubscribeGate;
				if (c.ended) throw new Error('Connection is closed.');
				if (c.refuse) throw noperm('channel');
				c.patterns.add(pattern);
			},
			quits: 0,
			// Closed either way: a QUIT, or at once.
			closes: 0,
			async quit() { c.quits++; c.closes++; },
			disconnect() { c.closes++; }
		};
		conns.set(id, c);
		return c;
	};
	const slotsReply = async () => {
		if (slotsGate) await slotsGate;
		if (slotsDown) throw new Error('CLUSTERDOWN The cluster is down');
		// A null owner leaves every slot out of the map.
		return owner === null ? [] : [[0, 16383, ['127.0.0.1', 7000, owner]]];
	};
	/** @type {Map<string, string | Error>} what CONFIG GET notify-keyspace-events answers per node */
	const keyspaceFlags = new Map();
	const node = (id) => ({
		options: { host: id, port: 6379 },
		// CLUSTER SLOTS answers the same on every node.
		cluster: async (sub) => {
			if (String(sub).toUpperCase() === 'SLOTS') return slotsReply();
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
		node,
		setKeyspaceFlags(id, flags) { keyspaceFlags.set(id, flags); },
		get created() { return created; },
		/** Hold CLUSTER MYID until the returned function is called. */
		/** Hold CLUSTER SLOTS until the returned function is called. */
		holdSlots() {
			let release;
			slotsGate = new Promise((resolve) => { release = resolve; });
			return () => { slotsGate = null; release(); };
		},
		/** Hold every psubscribe until the returned function is called. */
		holdPsubscribe() {
			let release;
			psubscribeGate = new Promise((resolve) => { release = resolve; });
			return () => { psubscribeGate = null; release(); };
		},
		holdMyid() {
			let release;
			myidGate = new Promise((resolve) => { release = resolve; });
			return () => { myidGate = null; release(); };
		},
		/**
		 * Make master `id` not answer CLUSTER MYID. As in ioredis, whose
		 * connection to it then ends, the master also leaves the node list.
		 * The returned function brings it back as a refreshed slot map would.
		 */
		failMyid(id, err = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' })) {
			const target = masters.find((n) => n.options.host === id);
			const myid = target.cluster;
			target.cluster = async () => {
				const i = masters.indexOf(target);
				if (i !== -1) masters.splice(i, 1);
				throw err;
			};
			return () => {
				target.cluster = myid;
				if (!masters.includes(target)) masters.push(target);
			};
		},
		setOwner(id) { owner = id; },
		setSlotsDown(down) { slotsDown = down; },
		redis: {
			info: async () => '# Server\nredis_version:7.2.0\n',
			// A fresh array per call, as ioredis returns.
			nodes: () => [...masters],
			cluster: () => slotsReply(),
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

	it('a shard envelope without an instance id is counted and dropped without a console line, in every mode', async () => {
		const cluster = fakeCluster();
		const metrics = recordingMetrics();
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' }, { metrics });
		await bus.activate(mockPlatform());
		await bus.follow('user:42:inbox');
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			for (let i = 0; i < 20; i++) cluster.conns.get('node-a').emit('smessage', CHANNEL, '{"instanceId":7}');
			await inMode(() => {
				for (let i = 0; i < 20; i++) cluster.conns.get('node-a').emit('smessage', CHANNEL, '{"instanceId":null}');
			});
			await inMode(() => {
				for (let i = 0; i < 20; i++) cluster.conns.get('node-a').emit('smessage', CHANNEL, '"just a string"');
			}, 'development');
			expect(error).not.toHaveBeenCalled();
			expect(metrics.count('sharded_pubsub_parse_errors_total')).toBe(60);
		} finally {
			error.mockRestore();
			await bus.deactivate();
		}
	});

	it('a master connection that ends is replaced, and its channels come back on the fresh one', async () => {
		const { cluster, bus } = await followedCluster();
		const first = cluster.conns.get('node-a');
		expect(first.subscribed.has(CHANNEL)).toBe(true);
		first.subscribed.clear();
		first.emit('end');
		await settle();
		const fresh = cluster.conns.get('node-a');
		expect(fresh).not.toBe(first);
		expect(fresh.subscribed.has(CHANNEL)).toBe(true);
		// The ended connection is out of the bus: deactivate closes only the live ones.
		await bus.deactivate();
		expect(first.closes).toBe(0);
		expect(fresh.closes).toBe(1);
		// At once: a QUIT waits forever on a master that stopped answering.
		expect(fresh.quits).toBe(0);
	});

	it('a master connection that ends while holding nothing is replaced on the next follow', async () => {
		const cluster = fakeCluster();
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		await bus.activate(mockPlatform());
		const first = cluster.conns.get('node-a');
		first.emit('end');
		await settle();
		expect(cluster.conns.get('node-a')).toBe(first);
		await bus.follow('user:42:inbox');
		const fresh = cluster.conns.get('node-a');
		expect(fresh).not.toBe(first);
		expect(fresh.subscribed.has(CHANNEL)).toBe(true);
		expect(first.attempts).toBe(0);
		await bus.deactivate();
	});

	it('a master connection that ends during a topology refresh is replaced before the next follow uses it', async () => {
		const cluster = fakeCluster();
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		await bus.activate(mockPlatform());
		const first = cluster.conns.get('node-a');
		// A reconciliation's refresh is past CLUSTER MYID and waiting on SLOTS when
		// the connection ends; it must not clear the staleness the end recorded.
		const release = cluster.holdSlots();
		first.emit('error', new Error('read ECONNRESET'));
		await settle();
		first.emit('end');
		release();
		await settle();
		await bus.follow('user:42:inbox');
		const fresh = cluster.conns.get('node-a');
		expect(fresh).not.toBe(first);
		expect(fresh.subscribed.has(CHANNEL)).toBe(true);
		await bus.deactivate();
	});

	it('a presence keyspace connection that ends while a sweep is running is still replaced', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-race:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			const release = cluster.holdPsubscribe();
			const subscribing = subscriber.subscribeToTopic('room', mockPlatform());
			await settle();
			// The sweep is waiting on the psubscribe when the master drops node-a.
			const first = cluster.conns.get('node-a');
			first.ended = true;
			first.emit('end');
			await settle();
			// The psubscribe fails with the connection, which leaves the master to
			// the retry rather than to an immediate redial: only the time limits of
			// the two psubscribes still out are pending.
			expect(clock.pending().map((t) => t.ms)).toEqual([10000, 10000]);
			release();
			await subscribing;
			await settle();
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
			await clock.fire();
			const fresh = cluster.conns.get('node-a');
			expect(fresh).not.toBe(first);
			expect(fresh.patterns.has('__keyevent@*__:del')).toBe(true);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a subscribed keyspace connection that ends during a running sweep is replaced after it', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-rerun:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			const b = cluster.conns.get('node-b');
			// A failover adds node-c, and the sweep that covers it waits on its
			// psubscribe, already past node-b.
			cluster.masters.push(cluster.node('node-c'));
			const release = cluster.holdPsubscribe();
			cluster.conns.get('node-a').emit('error', new Error('read ECONNRESET'));
			await clock.fire();
			b.emit('end');
			await clock.fire();
			release();
			await settle();
			await clock.fire();
			const fresh = cluster.conns.get('node-b');
			expect(fresh).not.toBe(b);
			expect(fresh.patterns.has('__keyevent@*__:del')).toBe(true);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace connection that fails before it subscribes is reported with its own error and retried after a delay', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-login:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		// node-b refuses the login: the connection raises the error, ends, and
		// the psubscribe queued on it is rejected as closed.
		const duplicateB = cluster.masters[1].duplicate;
		cluster.masters[1].duplicate = () => {
			const c = duplicateB();
			c.psubscribe = async () => {
				c.emit('error', new Error('WRONGPASS invalid username-password pair or user is disabled.'));
				c.emit('end');
				throw new Error('Connection is closed.');
			};
			return c;
		};
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBE]);
			expect(sink.records[0].attributes).toMatchObject({ master: 'node-b', error: { message: 'WRONGPASS invalid username-password pair or user is disabled.' } });
			// No immediate redial: only the backoff retry is pending.
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
			await clock.fire();
			expect(sink.ids()).toHaveLength(1);
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a presence keyspace connection a master drops is replaced and subscribed again', async () => {
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-end:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			const first = cluster.conns.get('node-a');
			expect(first.patterns.has('__keyevent@*__:del')).toBe(true);
			first.emit('end');
			await settle();
			const fresh = cluster.conns.get('node-a');
			expect(fresh).not.toBe(first);
			expect(fresh.patterns.has('__keyevent@*__:del')).toBe(true);
			// node-b's connection is untouched.
			expect(cluster.created).toBe(3);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master that does not answer CLUSTER MYID is missed after 10 seconds instead of holding the sweep, and its retry reads the slot map first', async () => {
		const clock = captureClock();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-myid-silent:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		client.redis.refreshSlotsCache = vi.fn();
		const myid = cluster.masters[1].cluster;
		cluster.masters[1].cluster = () => new Promise(() => {});
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			let done = false;
			const subscribing = subscriber.subscribeToTopic('room', mockPlatform()).then(() => { done = true; });
			await settle();
			expect(done).toBe(false);
			expect(clock.pending().map((t) => t.ms)).toEqual([10000]);
			await clock.fire();
			await subscribing;
			expect(cluster.conns.get('node-a').patterns.has('__keyevent@*__:del')).toBe(true);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_RECONCILE]);
			expect(sink.records[0].attributes.error.message).toBe('CLUSTER MYID was not answered within 10000 ms');
			expect(client.redis.refreshSlotsCache).not.toHaveBeenCalled();
			// Retried with the backoff, reading the slot map first; the master
			// answers now.
			cluster.masters[1].cluster = myid;
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
			await clock.fire();
			expect(client.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
			expect(cluster.conns.get('node-b').patterns.has('__keyevent@*__:del')).toBe(true);
		} finally {
			await subscriber.dispose?.();
		}
	});

	/**
	 * A cluster-shaped presence client whose master node-b refuses connections,
	 * with ioredis's events: CLUSTER MYID on node-b fails and takes it off the
	 * node list, as its ended node connection does, and a slot map read, which
	 * still lists node-b, puts it back and emits '+node' for it, then 'refresh'.
	 */
	function refusingMasterClient(prefix) {
		const cluster = fakeCluster();
		const client = mockRedisClient(prefix);
		const events = new EventEmitter();
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = (event, fn) => { events.on(event, fn); };
		client.redis.removeListener = (event, fn) => { events.removeListener(event, fn); };
		const nodeB = cluster.masters[1];
		client.redis.refreshSlotsCache = vi.fn(() => {
			queueMicrotask(() => {
				if (!cluster.masters.includes(nodeB)) {
					cluster.masters.push(nodeB);
					events.emit('+node', nodeB);
				}
				events.emit('refresh');
			});
		});
		const myid = nodeB.cluster;
		const refuse = (err) => {
			nodeB.cluster = async () => {
				const i = cluster.masters.indexOf(nodeB);
				if (i !== -1) cluster.masters.splice(i, 1);
				throw err;
			};
		};
		refuse(Object.assign(new Error('connect ECONNREFUSED 10.0.0.2:6379'), { code: 'ECONNREFUSED' }));
		return { cluster, client, nodeB, refuse, restore: () => { nodeB.cluster = myid; } };
	}

	it('a master that refuses connections is swept with a growing backoff and one slot map read per retry', async () => {
		const clock = captureClock();
		const { cluster, client } = refusingMasterClient('fr-keyspace-refusing:');
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			const seen = [];
			for (let i = 0; i < 8; i++) {
				seen.push([clock.pending().map((t) => t.ms), client.redis.refreshSlotsCache.mock.calls.length]);
				await clock.fire();
			}
			// Each retry reads the slot map, whose '+node' for node-b starts one
			// more sweep; both miss node-b, so the backoff keeps growing.
			expect(seen).toEqual([
				[[1000], 0], [[0], 1],
				[[4000], 1], [[0], 2],
				[[16000], 2], [[0], 3],
				[[30000], 3], [[0], 4]
			]);
			expect(cluster.conns.has('node-b')).toBe(false);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace PSUBSCRIBE that ioredis gave up on (commandTimeout) closes its connection at once, not with a QUIT', async () => {
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-cmd-timeout:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const duplicateB = cluster.masters[1].duplicate;
		let conn = null;
		cluster.masters[1].duplicate = () => {
			conn = duplicateB();
			conn.psubscribe = async () => { throw new Error('Command timed out'); };
			// A QUIT to a master that stopped answering is never answered.
			conn.quit = () => { conn.quits++; return new Promise(() => {}); };
			return conn;
		};
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(conn.quits).toBe(0);
			expect(conn.closes).toBe(1);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBE]);
			expect(sink.records[0].attributes).toMatchObject({ master: 'node-b', error: { message: 'Command timed out' } });
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a subscribe after the tracker was disposed starts no PING rounds', async () => {
		const clock = captureClock();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-after-dispose:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		await subscriber.subscribeToTopic('room', mockPlatform());
		expect(clock.running()).toHaveLength(1);
		subscriber.dispose();
		expect(clock.running()).toEqual([]);
		// Nothing is opened again either: nothing would close it.
		const duplicate = vi.spyOn(client, 'duplicate');
		await subscriber.subscribeToTopic('room', mockPlatform());
		expect(duplicate).not.toHaveBeenCalled();
		expect(clock.running()).toEqual([]);
	});

	it('a slot map that lists no master prunes no keyspace connection', async () => {
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-empty-map:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			// What a removed node, reset and slotless, answers for the slot map.
			const masters = cluster.masters.splice(0);
			listeners.refresh();
			expect(cluster.conns.get('node-a').closes).toBe(0);
			expect(cluster.conns.get('node-b').closes).toBe(0);
			cluster.masters.push(...masters);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('while sweeps fail, the topology events between two retries start one sweep', async () => {
		const clock = captureClock();
		const { cluster, client, nodeB } = refusingMasterClient('fr-keyspace-node-events:');
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		let asked = 0;
		const refusing = nodeB.cluster;
		nodeB.cluster = (...args) => { asked++; return refusing(...args); };
		/** Run the sweeps asked for at once, not the retry waiting out its backoff. */
		const sweepNow = async () => {
			for (const t of clock.pending().filter((p) => p.ms === 0)) {
				t.fired = true;
				t.cb();
			}
			await settle();
		};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(asked).toBe(1);
			// ioredis creating its connection to node-b again and again for the
			// app's own traffic, while node-b refuses every one.
			for (let i = 0; i < 3; i++) {
				cluster.masters.push(nodeB);
				listeners['+node'](nodeB);
				await sweepNow();
			}
			// The first started a sweep, which missed node-b again; the others
			// wait for the retry.
			expect(asked).toBe(2);
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master the slot map lists that ioredis lost while its keyspace connection stands is not missed', async () => {
		const clock = captureClock();
		const { cluster, client, nodeB, restore } = refusingMasterClient('fr-keyspace-covered:');
		restore();
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			listeners.refresh();
			// ioredis's own idle connection to node-b ended; the keyspace one did not.
			cluster.masters.splice(cluster.masters.indexOf(nodeB), 1);
			listeners['+node']();
			await clock.fire();
			expect(clock.pending()).toEqual([]);
			expect(client.redis.refreshSlotsCache).not.toHaveBeenCalled();
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master whose keyspace subscription stands is not asked for its id again', async () => {
		const clock = captureClock();
		const { cluster, client, restore } = refusingMasterClient('fr-keyspace-covered-myid:');
		restore();
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			// ioredis's own connection to node-a no longer answers; the keyspace
			// one does.
			let asked = 0;
			cluster.masters[0].cluster = () => { asked++; return new Promise(() => {}); };
			listeners['+node']();
			await clock.fire();
			expect(asked).toBe(0);
			expect(clock.pending()).toEqual([]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master that refuses CLUSTER MYID itself needs no slot map read', async () => {
		const clock = captureClock();
		const { client, nodeB } = refusingMasterClient('fr-keyspace-myid-noperm:');
		// It answers, with an error: the node list is not in doubt.
		nodeB.cluster = async () => { throw Object.assign(new Error('NOPERM this user has no permissions to run the \'cluster|myid\' command'), { name: 'ReplyError' }); };
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			for (let i = 0; i < 3; i++) await clock.fire();
			expect(client.redis.refreshSlotsCache).not.toHaveBeenCalled();
			expect(clock.pending().map((t) => t.ms)).toEqual([8000]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master the slot map lists but ioredis lost, with its keyspace connection gone, is missed until a slot map read brings it back', async () => {
		const clock = captureClock();
		const { cluster, client, nodeB, restore } = refusingMasterClient('fr-keyspace-lost:');
		restore();
		const subscriber = presenceSubscriber(client, true);
		try {
			// No slot map was read since: the node list at setup stands for it.
			await subscriber.subscribeToTopic('room', mockPlatform());
			const before = cluster.conns.get('node-b');
			// node-b restarted: ioredis's connection to it ended, which took it off
			// the node list, and so did its keyspace connection.
			cluster.masters.splice(cluster.masters.indexOf(nodeB), 1);
			before.emit('end');
			await clock.fire();
			// The sweep does not see node-b; it is missed, not done with.
			expect(client.redis.refreshSlotsCache).not.toHaveBeenCalled();
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
			await clock.fire();
			await clock.fire();
			expect(client.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
			const fresh = cluster.conns.get('node-b');
			expect(fresh).not.toBe(before);
			expect(fresh.patterns.has('__keyevent@*__:del')).toBe(true);
			expect(clock.pending()).toEqual([]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace PSUBSCRIBE that is not answered within 10 seconds closes its connection and is reported as such', async () => {
		const clock = captureClock();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-psub-silent:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const duplicateB = cluster.masters[1].duplicate;
		/** @type {any[]} */
		const silent = [];
		cluster.masters[1].duplicate = () => {
			const c = duplicateB();
			let fail;
			c.psubscribe = () => new Promise((_, reject) => { fail = reject; });
			c.disconnects = 0;
			// As ioredis: closing it rejects what is out as closed, then ends it.
			c.disconnect = () => {
				c.disconnects++;
				fail(new Error('Connection is closed.'));
				c.emit('end');
			};
			silent.push(c);
			return c;
		};
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			const subscribing = subscriber.subscribeToTopic('room', mockPlatform());
			await settle();
			expect(clock.pending().map((t) => t.ms)).toEqual([10000]);
			// A connection still subscribing is left to that limit, not asked for
			// a PING as well.
			silent[0].status = 'ready';
			silent[0].ping = vi.fn(() => new Promise(() => {}));
			await clock.tick();
			expect(silent[0].ping).not.toHaveBeenCalled();
			await clock.fire();
			await subscribing;
			expect(silent[0].disconnects).toBeGreaterThan(0);
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBE]);
			expect(sink.records[0].attributes).toMatchObject({
				master: 'node-b',
				error: { message: 'PSUBSCRIBE was not answered within 10000 ms' }
			});
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);

			// One that raised an error before it stalled is reported with that error.
			cluster.masters[1].duplicate = () => {
				const c = duplicateB();
				let fail;
				c.psubscribe = () => {
					queueMicrotask(() => c.emit('error', Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })));
					return new Promise((_, reject) => { fail = reject; });
				};
				c.disconnect = () => {
					fail(new Error('Connection is closed.'));
					c.emit('end');
				};
				return c;
			};
			await clock.fire();
			await clock.fire();
			expect(sink.ids()).toHaveLength(2);
			expect(sink.records[1].attributes).toMatchObject({ master: 'node-b', error: { code: 'ECONNRESET' } });
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace connection that stops answering PING is closed, reported by its master, and replaced', async () => {
		const clock = captureClock();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-ping:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		client.redis.refreshSlotsCache = vi.fn();
		const duplicateA = cluster.masters[0].duplicate;
		/** @type {any[]} */
		const opened = [];
		cluster.masters[0].duplicate = () => {
			const c = duplicateA();
			c.status = 'ready';
			// The first goes silent; the one that replaces it answers.
			c.ping = opened.length === 0 ? () => new Promise(() => {}) : async () => ['pong', ''];
			c.disconnect = () => {
				c.status = 'end';
				c.emit('end');
			};
			opened.push(c);
			return c;
		};
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(clock.running().map((t) => t.ms)).toEqual([30000]);
			await clock.tick();
			expect(clock.pending().map((t) => t.ms)).toEqual([10000]);
			await clock.fire();
			expect(opened[0].status).toBe('end');
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
			expect(sink.records[0].attributes).toEqual({ owner: 'presence-keyspace', master: 'node-a', timeoutMs: 10000 });
			// ioredis still lists the master, so the sweep needs no slot map.
			expect(client.redis.refreshSlotsCache).not.toHaveBeenCalled();
			// Its 'end' sweeps at once, which opens a new connection to the master.
			await clock.fire();
			expect(opened).toHaveLength(2);
			expect(opened[1].patterns.has('__keyevent@*__:del')).toBe(true);
			await clock.tick();
			expect(clock.pending()).toEqual([]);
			expect(opened[1].status).toBe('ready');
		} finally {
			await subscriber.dispose?.();
		}
		expect(clock.running()).toEqual([]);
	});

	it('an activation that cannot read the slot map closes the subscriber connections it opened', async () => {
		const cluster = fakeCluster();
		cluster.setSlotsDown(true);
		const breaker = recordingBreaker();
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' }, { breaker });
		await expect(bus.activate(mockPlatform())).rejects.toThrow('CLUSTERDOWN');
		const opened = [...cluster.conns.values()];
		expect(opened).toHaveLength(2);
		for (const conn of opened) expect(conn.closes).toBe(1);
		expect(breaker.failure).toHaveBeenCalledTimes(1);
		// Nothing of the failed activation survives into the next one.
		cluster.setSlotsDown(false);
		await bus.activate(mockPlatform());
		await bus.follow('user:42:inbox');
		const conn = cluster.conns.get('node-a');
		expect(opened).not.toContain(conn);
		expect(conn.subscribed.has(CHANNEL)).toBe(true);
		await bus.deactivate();
		expect(conn.closes).toBe(1);
	});

	it('a master that does not answer CLUSTER MYID within 5 seconds holds back only the channels on its slots', async () => {
		const clock = captureClock();
		const cluster = fakeCluster();
		cluster.redis.refreshSlotsCache = vi.fn();
		cluster.masters[1].cluster = () => new Promise(() => {});
		// Sent to any node, the slot map read could land on the silent one.
		cluster.redis.cluster = () => new Promise(() => {});
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		try {
			const activating = bus.activate(mockPlatform());
			await settle();
			// node-a answered; node-b's time limit is running.
			expect(clock.pending().map((t) => t.ms)).toEqual([5000]);
			await clock.fire();
			await activating;
			// The master that did not answer may have moved or left.
			expect(cluster.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
			// node-a owns the channel's slot: subscribed although node-b is silent.
			await bus.follow('user:42:inbox');
			expect(cluster.conns.get('node-a').subscribed.has(CHANNEL)).toBe(true);
			// The slot moves to node-b, which has no connection here: the
			// reconciliation reports its channel, mentioning node-b's error.
			sink = captureRegistered();
			cluster.setOwner('node-b');
			cluster.conns.get('node-a').emit('error', new Error('read ECONNRESET'));
			for (let i = 0; i < 4; i++) await clock.fire();
			const failures = sink.records.filter((r) => r.event === 'redis.sharded-pubsub.channel-reconcile.failed');
			expect(failures).toHaveLength(1);
			expect(failures[0].attributes.error.message).toBe(
				'sharded bus: no owning node for slot ' + keySlot(CHANNEL) +
				' (a master did not give its id: sharded bus: CLUSTER MYID timed out (the master did not answer))'
			);
			// Slot map reads are at least 5 seconds apart.
			expect(cluster.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
		} finally {
			await bus.deactivate();
		}
		expect(clock.running()).toEqual([]);
	});

	it('a slot map read that is not answered within 5 seconds, there or through the client, fails the sharded topology read', async () => {
		const clock = captureClock();
		const cluster = fakeCluster();
		for (const node of cluster.masters) {
			const myid = node.cluster;
			node.cluster = (sub) => (String(sub).toUpperCase() === 'SLOTS' ? new Promise(() => {}) : myid(sub));
		}
		cluster.redis.cluster = () => new Promise(() => {});
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		const activating = bus.activate(mockPlatform());
		activating.catch(() => {});
		await settle();
		expect(clock.pending().map((t) => t.ms)).toEqual([5000]);
		await clock.fire();
		// Then once more through the client, which picks a node itself.
		expect(clock.pending().map((t) => t.ms)).toEqual([5000]);
		await clock.fire();
		await expect(activating).rejects.toThrow('sharded bus: CLUSTER SLOTS timed out (the node did not answer)');
		expect(clock.running()).toEqual([]);
	});

	it('a slot map read whose node connection closed since it gave its id goes through the client instead', async () => {
		const cluster = fakeCluster();
		for (const node of cluster.masters) {
			const myid = node.cluster;
			node.cluster = (sub) => (String(sub).toUpperCase() === 'SLOTS' ? Promise.reject(new Error('Connection is closed.')) : myid(sub));
		}
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		try {
			await bus.activate(mockPlatform());
			await bus.follow('user:42:inbox');
			expect(cluster.conns.get('node-a').subscribed.has(CHANNEL)).toBe(true);
		} finally {
			await bus.deactivate();
		}
	});

	it('a topology read where no master gives its id fails, and with it the activation', async () => {
		const cluster = fakeCluster();
		for (const node of cluster.masters) {
			node.cluster = async () => { throw Object.assign(new Error('NOPERM this user has no permissions to run the \'cluster|myid\' command'), { name: 'ReplyError' }); };
		}
		const breaker = recordingBreaker();
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' }, { breaker });
		await expect(bus.activate(mockPlatform())).rejects.toThrow(/^NOPERM/);
		expect(breaker.failure).toHaveBeenCalledTimes(1);
	});

	it('slot map reads the sharded bus asks for are at least 5 seconds apart, and go on after', async () => {
		let clockNow = 0;
		setRuntimeEnv({
			clock: { monotonic: () => clockNow },
			timers: { set: () => ({ unref() {} }), clear: () => {} }
		});
		const cluster = fakeCluster();
		cluster.redis.refreshSlotsCache = vi.fn();
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		try {
			await bus.activate(mockPlatform());
			const endNodeA = async (topic) => {
				// A follow opens node-a's connection again; its end asks for a read.
				await bus.follow(topic);
				cluster.conns.get('node-a').emit('end');
			};
			await endNodeA('t1');
			expect(cluster.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
			clockNow = 4999;
			await endNodeA('t2');
			expect(cluster.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
			clockNow = 5000;
			await endNodeA('t3');
			expect(cluster.redis.refreshSlotsCache).toHaveBeenCalledTimes(2);
		} finally {
			await bus.deactivate();
		}
	});

	it('a slot owner ioredis does not list as a master fails its channel and has the slot map read', async () => {
		const cluster = fakeCluster();
		cluster.redis.refreshSlotsCache = vi.fn();
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		cluster.setOwner('node-x');
		try {
			await bus.activate(mockPlatform());
			await expect(bus.follow('user:42:inbox')).rejects.toThrow(/^sharded bus: no owning node for slot \d+$/);
			expect(cluster.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
		} finally {
			await bus.deactivate();
		}
	});

	it('a channel whose slot owner has no connection fails its follow, which counts against the breaker, its probe included', async () => {
		const clock = captureClock();
		const cluster = fakeCluster();
		cluster.masters[1].cluster = async () => { throw Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' }); };
		cluster.setOwner('node-b');
		const breaker = createCircuitBreaker({ failureThreshold: 2, resetTimeout: 1000 });
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' }, { breaker });
		try {
			await bus.activate(mockPlatform());
			breaker.reset();
			for (let i = 0; i < 2; i++) await expect(bus.follow('room-' + i)).rejects.toThrow(/no owning node/);
			expect(breaker.state).toBe('broken');
			await clock.fire();
			expect(breaker.state).toBe('probing');
			// The probe it lets through fails the same way: broken again with the
			// next probe scheduled, not probing with no probe left.
			await expect(bus.follow('room-2')).rejects.toThrow(/no owning node/);
			expect(breaker.state).toBe('broken');
			await clock.fire();
			expect(breaker.state).toBe('probing');
			// A batch follow the same.
			await expect(bus.followBatch(['room-3', 'room-4'])).rejects.toThrow(/no owning node/);
			expect(breaker.state).toBe('broken');
		} finally {
			await bus.deactivate();
			breaker.destroy();
		}
	});

	it('a master that refuses CLUSTER MYID holds back its channels without a slot map read', async () => {
		const cluster = fakeCluster();
		cluster.redis.refreshSlotsCache = vi.fn();
		cluster.masters[1].cluster = async () => { throw Object.assign(new Error('NOPERM this user has no permissions to run the \'cluster|myid\' command'), { name: 'ReplyError' }); };
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' });
		try {
			await bus.activate(mockPlatform());
			await bus.follow('user:42:inbox');
			expect(cluster.conns.get('node-a').subscribed.has(CHANNEL)).toBe(true);
			// It answered: the slot map is not in doubt.
			expect(cluster.redis.refreshSlotsCache).not.toHaveBeenCalled();
		} finally {
			await bus.deactivate();
		}
	});

	it('a shard connection that stops answering PING is closed, its channels subscribed again, and the slot map read again', async () => {
		const clock = captureClock();
		const { cluster, bus } = await followedCluster();
		cluster.redis.refreshSlotsCache = vi.fn();
		sink = captureRegistered();
		try {
			const owner = cluster.conns.get('node-a');
			expect(owner.subscribed.has(CHANNEL)).toBe(true);
			owner.status = 'ready';
			owner.ping = () => new Promise(() => {});
			owner.disconnect = () => {
				owner.status = 'end';
				owner.emit('end');
			};
			expect(clock.running().map((t) => t.ms)).toEqual([30000]);
			await clock.tick();
			expect(clock.pending().map((t) => t.ms)).toEqual([10000]);
			await clock.fire();
			expect(owner.status).toBe('end');
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
			expect(sink.records[0].attributes).toEqual({ owner: 'sharded-pubsub', master: 'node-a', timeoutMs: 10000 });
			expect(cluster.redis.refreshSlotsCache).toHaveBeenCalledTimes(1);
			// The reconciliation its end scheduled opens a new connection to the
			// slot's owner and subscribes the channel there.
			await clock.fire();
			const fresh = cluster.conns.get('node-a');
			expect(fresh).not.toBe(owner);
			expect(fresh.subscribed.has(CHANNEL)).toBe(true);
		} finally {
			await bus.deactivate();
		}
		expect(clock.running()).toEqual([]);
	});

	it('a shard relay batch with a refused SPUBLISH fails the breaker and counts only what went out', async () => {
		const cluster = fakeCluster();
		const breaker = recordingBreaker();
		const metrics = recordingMetrics();
		cluster.redis.spublish = async (channel) => {
			if (channel.includes('secret')) throw noperm('channel');
			return 1;
		};
		const bus = createShardedBus({ redis: cluster.redis, key: (k) => k, keyPrefix: '' }, { breaker, metrics });
		await bus.activate(mockPlatform());
		try {
			breaker.success.mockClear();
			const platform = bus.wrap(mockPlatform());
			platform.publish('room:open', 'msg', 1);
			platform.publish('room:secret', 'msg', 2);
			await settle();
			expect(breaker.failure).toHaveBeenCalledTimes(1);
			expect(breaker.success).not.toHaveBeenCalled();
			const relayed = metrics.incs.filter((inc) => inc.name === 'sharded_pubsub_messages_relayed_total');
			expect(relayed.map((inc) => inc.labels)).toEqual([{ topic: 'room:open' }]);
		} finally {
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

	it('a master that refuses the keyspace subscription is asked again with a backoff, reported once per streak', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-retry:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		let refuseB = true;
		const duplicateB = cluster.masters[1].duplicate;
		cluster.masters[1].duplicate = () => { const c = duplicateB(); c.refuse = refuseB; return c; };
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		const refusals = () => sink.records.filter((r) => r.event === 'redis.presence.keyspace-subscribe.failed');
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(refusals()).toHaveLength(1);
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);

			// Still refused: asked again after a doubled delay, not reported again.
			await clock.fire();
			expect(refusals()).toHaveLength(1);
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);

			// The grant is back: the next sweep subscribes, and the retries stop.
			refuseB = false;
			await clock.fire();
			expect(cluster.conns.get('node-b').patterns.has('__keyevent@*__:del')).toBe(true);
			expect(clock.pending()).toEqual([]);

			// A later refusal is a new streak, reported again from the first delay.
			refuseB = true;
			cluster.conns.get('node-b').emit('end');
			await clock.fire();
			expect(refusals()).toHaveLength(2);
			expect(refusals()[1].attributes).toMatchObject({ master: 'node-b' });
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace refusal whose cause changes, or whose master left and came back, is reported again', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-cause:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		let failure = 'down';
		const duplicateB = cluster.masters[1].duplicate;
		cluster.masters[1].duplicate = () => {
			const c = duplicateB();
			c.psubscribe = async () => {
				if (failure === 'down') {
					c.emit('error', Object.assign(new Error('connect ECONNREFUSED 10.0.0.2:6379'), { code: 'ECONNREFUSED' }));
					throw new Error('Connection is closed.');
				}
				throw noperm('channel');
			};
			return c;
		};
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		const refusals = () => sink.records.filter((r) => r.event === 'redis.presence.keyspace-subscribe.failed');
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			await clock.fire();
			expect(refusals()).toHaveLength(1);
			expect(refusals()[0].attributes.error.code).toBe('ECONNREFUSED');

			// The master is back, now refusing the pattern: a new cause.
			failure = 'refused';
			await clock.fire();
			await clock.fire();
			expect(refusals()).toHaveLength(2);
			expect(refusals()[1].attributes.error.message).toMatch(/^NOPERM/);

			// A failure that keeps switching between the two kinds says nothing new.
			for (const next of ['down', 'refused', 'down']) {
				failure = next;
				await clock.fire();
			}
			expect(refusals()).toHaveLength(2);

			// node-b leaves the topology, as a refreshed slot map shows, then
			// returns still refusing.
			const nodeB = cluster.masters.splice(1, 1)[0];
			listeners.refresh();
			await clock.fire();
			cluster.masters.push(nodeB);
			listeners['+node']();
			await clock.fire();
			expect(refusals()).toHaveLength(3);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a topology change does not wait out a keyspace retry', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-topology:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const duplicateB = cluster.masters[1].duplicate;
		cluster.masters[1].duplicate = () => { const c = duplicateB(); c.refuse = true; return c; };
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
			listeners['+node']();
			expect(clock.pending().map((t) => t.ms)).toEqual([0]);
			// A sweep asked for meanwhile does not push it back.
			listeners.refresh();
			expect(clock.pending().map((t) => t.ms)).toEqual([0]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a refreshed slot map drops the keyspace connection of a master that left, whatever the sweep after it reads', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-prune-refresh:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			const a = cluster.conns.get('node-a');
			const b = cluster.conns.get('node-b');
			cluster.masters.splice(1, 1);
			cluster.failMyid('node-a');
			listeners.refresh();
			expect(b.closes).toBe(1);
			expect(b.quits).toBe(0);
			await clock.fire();
			// node-a stays listed in the refreshed map, so its working
			// connection stays, although its id could not be read since.
			expect(a.closes).toBe(0);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master list that cannot be read is reported', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-nodes:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			sink = captureRegistered();
			client.redis.nodes = () => { throw new Error('Cluster is disconnected.'); };
			listeners['+node']();
			await clock.fire();
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_RECONCILE]);
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master missing from the node list keeps its keyspace connection until a refreshed slot map drops it', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-prune:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			const b = cluster.conns.get('node-b');
			// ioredis's own connection to node-b ended (an idle timeout): node-b
			// is out of nodes('master') until the next refresh, and a node joining
			// asks for a sweep meanwhile.
			const nodeB = cluster.masters.splice(1, 1)[0];
			listeners['+node']();
			await clock.fire();
			expect(b.closes).toBe(0);
			expect(cluster.conns.get('node-b')).toBe(b);

			// A refreshed slot map without node-b: it left.
			listeners.refresh();
			await clock.fire();
			expect(b.closes).toBe(1);
			cluster.masters.push(nodeB);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a master whose id cannot be read is covered once the cluster lists it again, and reported once until then', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-myid:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const restore = cluster.failMyid('node-b');
		const subscriber = presenceSubscriber(client, true);
		sink = captureRegistered();
		const failures = () => sink.records.filter((r) => r.event === 'redis.presence.keyspace-reconcile.failed');
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(cluster.conns.has('node-b')).toBe(false);
			expect(failures()).toHaveLength(1);
			expect(failures()[0].attributes.error.code).toBe('ECONNREFUSED');
			// The retry finds node-b gone from the list, as ioredis drops it, but
			// the cluster still lists it: it is still missed, and retried.
			await clock.fire();
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);

			// A refreshed slot map lists it again, still not answering.
			restore();
			const restoreAgain = cluster.failMyid('node-b');
			listeners.refresh();
			await clock.fire();
			expect(failures()).toHaveLength(1);

			// It answers.
			restoreAgain();
			listeners.refresh();
			await clock.fire();
			expect(cluster.conns.get('node-b').patterns.has('__keyevent@*__:del')).toBe(true);

			// A later outage is reported again.
			const restoreLater = cluster.failMyid('node-b');
			cluster.conns.get('node-b').emit('end');
			await clock.fire();
			expect(failures()).toHaveLength(2);

			// So is one of a master that left the slot map and came back.
			restoreLater();
			const nodeB = cluster.masters.find((n) => n.options.host === 'node-b');
			cluster.masters.splice(cluster.masters.indexOf(nodeB), 1);
			listeners.refresh();
			await clock.fire();
			cluster.masters.push(nodeB);
			const restoreBack = cluster.failMyid('node-b');
			listeners.refresh();
			await clock.fire();
			restoreBack();
			expect(failures()).toHaveLength(3);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a node a refresh found no master while its id was being read gets no keyspace connection', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-demoted:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			// A reconnect lists node-c as a master until the slot map says it is
			// a replica.
			const nodeC = cluster.node('node-c');
			cluster.masters.push(nodeC);
			const release = cluster.holdMyid();
			listeners['+node']();
			await clock.fire();
			cluster.masters.splice(cluster.masters.indexOf(nodeC), 1);
			listeners.refresh();
			release();
			await clock.fire();
			expect(cluster.conns.has('node-c')).toBe(false);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace connection pruned while subscribing is no failure', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-pruned-subscribing:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			listeners.refresh();
			await clock.fire();
			const nodeC = cluster.node('node-c');
			const duplicateC = nodeC.duplicate;
			nodeC.duplicate = () => { const c = duplicateC(); c.refuse = true; return c; };
			cluster.masters.push(nodeC);
			const release = cluster.holdPsubscribe();
			listeners['+node']();
			await clock.fire();
			sink = captureRegistered();
			// node-c leaves while its subscription is pending; the refusal that
			// answers the quit connection's psubscribe is not node-c's.
			cluster.masters.splice(cluster.masters.indexOf(nodeC), 1);
			listeners.refresh();
			expect(cluster.conns.get('node-c').closes).toBe(1);
			release();
			await clock.fire();
			expect(sink.ids()).toEqual([]);
			expect(clock.pending()).toEqual([]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace subscription that stands after its master was pruned is not checked or reported', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-pruned-subscribed:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			listeners.refresh();
			await clock.fire();
			// node-c's notification setting could not fire the cleanup, which
			// would be reported for a master that is still one.
			cluster.setKeyspaceFlags('node-c', 'Kg');
			const nodeC = cluster.node('node-c');
			cluster.masters.push(nodeC);
			const release = cluster.holdPsubscribe();
			listeners['+node']();
			await clock.fire();
			sink = captureRegistered();
			cluster.masters.splice(cluster.masters.indexOf(nodeC), 1);
			listeners.refresh();
			release();
			await clock.fire();
			expect(sink.ids()).toEqual([]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a node a refresh dropped while its id was being read is not reported', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-dropped-unread:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			listeners.refresh();
			await clock.fire();
			let failMyid;
			const nodeC = cluster.node('node-c');
			nodeC.cluster = () => new Promise((_, reject) => { failMyid = reject; });
			cluster.masters.push(nodeC);
			listeners['+node']();
			await clock.fire();
			sink = captureRegistered();
			// A refresh drops node-c and closes ioredis's connection to it.
			cluster.masters.splice(cluster.masters.indexOf(nodeC), 1);
			listeners.refresh();
			failMyid(new Error('Connection is closed.'));
			await clock.fire();
			expect(sink.ids()).toEqual([]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a refresh that leaves the masters as they were does not cut a retry short', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-same-refresh:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const duplicateB = cluster.masters[1].duplicate;
		cluster.masters[1].duplicate = () => { const c = duplicateB(); c.refuse = true; return c; };
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			// The first refresh is the first view of the masters.
			listeners.refresh();
			await clock.fire();
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);
			// A MOVED or a refresh interval tick: same masters.
			listeners.refresh();
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);
			// A replica promoted in place joins the masters without a '+node'.
			cluster.masters.push(cluster.node('node-c'));
			listeners.refresh();
			expect(clock.pending().map((t) => t.ms)).toEqual([0]);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('nothing is reported for a sweep that dispose() overtook', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-disposed:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		await subscriber.subscribeToTopic('room', mockPlatform());
		// node-c's id is still being read, and node-d's subscription is pending.
		let failMyid;
		const nodeC = cluster.node('node-c');
		nodeC.cluster = () => new Promise((_, reject) => { failMyid = reject; });
		const nodeD = cluster.node('node-d');
		const duplicateD = nodeD.duplicate;
		nodeD.duplicate = () => { const c = duplicateD(); c.refuse = true; return c; };
		cluster.masters.push(nodeC, nodeD);
		const release = cluster.holdPsubscribe();
		listeners['+node']();
		await clock.fire();
		sink = captureRegistered();
		await subscriber.dispose?.();
		failMyid(new Error('Connection is closed.'));
		release();
		await settle();
		expect(sink.ids()).toEqual([]);
	});

	it('the topology listeners are attached once across an idle shutdown', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-listeners:');
		client.redis.nodes = cluster.redis.nodes;
		const attached = [];
		client.redis.on = (event) => { attached.push(event); };
		client.redis.removeListener = () => {};
		// Every master refuses, so nothing keeps the subscriber from idling.
		for (const n of cluster.masters) {
			const duplicate = n.duplicate;
			n.duplicate = () => { const c = duplicate(); c.refuse = true; return c; };
		}
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			await subscriber.unsubscribeFromTopic('room');
			const idle = clock.pending().find((t) => t.ms === 30000);
			idle.fired = true;
			idle.cb();
			await subscriber.subscribeToTopic('room', mockPlatform());
			expect(attached.filter((e) => e === '+node')).toHaveLength(1);
			expect(attached.filter((e) => e === 'refresh')).toHaveLength(1);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a covered master whose id cannot be read keeps its keyspace connection', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-unread:');
		client.redis.nodes = cluster.redis.nodes;
		const listeners = {};
		client.redis.on = (event, fn) => { listeners[event] = fn; };
		client.redis.removeListener = () => {};
		const subscriber = presenceSubscriber(client, true);
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			const a = cluster.conns.get('node-a');
			const restore = cluster.failMyid('node-a');
			// A sweep asked for by node-b's connection error cannot read node-a's
			// id, and the one after no longer sees node-a at all.
			cluster.conns.get('node-b').emit('error', new Error('read ECONNRESET'));
			await clock.fire();
			await clock.fire();
			expect(a.closes).toBe(0);
			expect(cluster.conns.get('node-a')).toBe(a);

			// The slot map still lists node-a: its connection stays.
			restore();
			listeners.refresh();
			await clock.fire();
			expect(a.closes).toBe(0);
			expect(cluster.conns.get('node-a')).toBe(a);
		} finally {
			await subscriber.dispose?.();
		}
	});

	it('a keyspace sweep that fails is retried and reported once per streak', async () => {
		const clock = captureTimers();
		const cluster = fakeCluster();
		const client = mockRedisClient('fr-keyspace-sweep:');
		client.redis.nodes = cluster.redis.nodes;
		client.redis.on = () => {};
		const subscriber = presenceSubscriber(client, true);
		const failures = () => sink.records.filter((r) => r.event === 'redis.presence.keyspace-reconcile.failed');
		try {
			await subscriber.subscribeToTopic('room', mockPlatform());
			// A master added by a failover whose connection cannot be opened.
			const nodeC = cluster.node('node-c');
			const duplicateC = nodeC.duplicate;
			nodeC.duplicate = () => { throw new Error('connect ECONNREFUSED'); };
			cluster.masters.push(nodeC);
			sink = captureRegistered();
			cluster.conns.get('node-a').emit('error', new Error('read ECONNRESET'));
			await clock.fire();
			expect(failures()).toHaveLength(1);
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
			await clock.fire();
			expect(failures()).toHaveLength(1);
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);

			nodeC.duplicate = duplicateC;
			await clock.fire();
			expect(cluster.conns.get('node-c').patterns.has('__keyevent@*__:del')).toBe(true);
			expect(clock.pending()).toEqual([]);

			// A clean sweep re-arms the report.
			cluster.conns.get('node-c').emit('end');
			nodeC.duplicate = () => { throw new Error('connect ECONNREFUSED'); };
			await clock.fire();
			expect(failures()).toHaveLength(2);
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

describe('a command Redis refuses inside a batch is a failed command', () => {
	it('groups heartbeat: a refused command fails the beat, and only a clean beat re-arms the report', async () => {
		const intervals = captureIntervals();
		const client = mockRedisClient('fr-groups-refused:');
		const group = createGroup(client, 'lobby');
		const pipeline = client.redis.pipeline.bind(client.redis);
		// exec() resolves; the refusal is an [err, result] entry, as ioredis reports it.
		const refusingPipeline = () => {
			const pipe = new Proxy({}, { get: (_, prop) => (prop === 'exec' ? () => Promise.resolve([[null, 1], [readonly(), null], [null, 1], [null, 1]]) : () => pipe) });
			return pipe;
		};
		const rejectingPipeline = () => {
			const pipe = new Proxy({}, { get: (_, prop) => (prop === 'exec' ? () => Promise.reject(new Error('pipeline down')) : () => pipe) });
			return pipe;
		};
		const beat = async () => {
			for (const { cb } of intervals) cb();
			await settle();
		};
		try {
			await group.join(mockWs({ id: 'member-1' }), mockPlatform());
			sink = captureRegistered();
			const heartbeats = () => sink.records.filter((r) => r.event === 'redis.groups.heartbeat.failed');
			client.redis.pipeline = refusingPipeline;
			await beat();
			await beat();
			expect(heartbeats()).toHaveLength(1);
			expect(heartbeats()[0].attributes).toEqual({ group: 'lobby', error: { name: 'Error', code: null, message: "READONLY You can't write against a read only replica." } });

			// A rejected beat and then a refused one are the same streak.
			client.redis.pipeline = rejectingPipeline;
			await beat();
			client.redis.pipeline = refusingPipeline;
			await beat();
			expect(heartbeats()).toHaveLength(1);

			client.redis.pipeline = pipeline;
			await beat();
			client.redis.pipeline = refusingPipeline;
			await beat();
			expect(heartbeats()).toHaveLength(2);
		} finally {
			group.destroy();
		}
	});

	it('pub/sub relay: a refused PUBLISH in a batch fails the breaker and is not counted as relayed', async () => {
		const client = mockRedisClient('fr-pubsub-refused:');
		const breaker = recordingBreaker();
		const metrics = recordingMetrics();
		const bus = createPubSubBus(client, { breaker, metrics });
		await bus.activate(mockPlatform());
		try {
			breaker.success.mockClear();
			let publishes = 0;
			const publish = client.redis.publish.bind(client.redis);
			client.redis.publish = async (...args) => {
				if (++publishes === 2) throw noperm('channel');
				return publish(...args);
			};
			const platform = bus.wrap(mockPlatform());
			platform.publish('a', 'e', 1);
			platform.publish('b', 'e', 2);
			platform.publish('c', 'e', 3);
			await settle();
			expect(publishes).toBe(3);
			expect(breaker.failure).toHaveBeenCalledTimes(1);
			expect(breaker.success).not.toHaveBeenCalled();
			expect(metrics.count('pubsub_messages_relayed_total')).toBe(2);
		} finally {
			await bus.deactivate();
		}
	});

	it('cursor: a refused snapshot write fails the breaker, and a refused removal is not reported as done', async () => {
		const client = mockRedisClient('fr-cursor-refused:');
		const breaker = recordingBreaker();
		const make = (snapshotIntervalMs) => createRedisIo({
			client, redis: client.redis, channel: 'cursor-ch', instanceId: 'i1', b: breaker,
			validator: { acceptRaw: () => true, acceptEnvelope: () => true },
			cursorTtl: 30, snapshotIntervalMs, activeTopics: new Set(['room']),
			emit() {}, onMessage() {}, queueRemove() {}
		});
		client.redis.hset = async () => { throw readonly(); };
		client.redis.hdel = async () => { throw readonly(); };

		const direct = make(0);
		direct.queueSnapshot('room', 'i1:c1', { id: 'c1' }, { x: 1 });
		await settle();
		expect(breaker.failure).toHaveBeenCalledTimes(1);
		expect(breaker.success).not.toHaveBeenCalled();

		const batched = make(50);
		batched.queueSnapshot('room', 'i1:c1', { id: 'c1' }, { x: 2 });
		batched.flushSnapshot();
		await settle();
		expect(breaker.failure).toHaveBeenCalledTimes(2);

		expect(await direct.removeKeysBatch(['room'], 'i1:c1')).toBe(false);
		expect(breaker.failure).toHaveBeenCalledTimes(3);
		expect(breaker.success).not.toHaveBeenCalled();
		direct.dispose();
		batched.dispose();
	});

	it('cursor erasure: a cursor whose removal Redis refused is not counted as erased', async () => {
		const client = mockRedisClient('fr-cursor-erase:');
		const cursor = createCursor(client);
		try {
			const entry = JSON.stringify({ user: 'u1', data: { x: 1 }, ts: Date.now() });
			await client.redis.hset(client.key('cursor:{room}'), 'other:c1', entry);
			const hdel = client.redis.hdel.bind(client.redis);
			client.redis.hdel = async () => { throw readonly(); };
			expect(await cursor.purgeUser(null, 'u1')).toBe(0);
			client.redis.hdel = hdel;
			expect(await cursor.purgeUser(null, 'u1')).toBe(1);
		} finally {
			cursor.destroy?.();
		}
	});

	it('cursor erasure: a refused REMOVE publish fails the breaker but the erased cursor still counts', async () => {
		const client = mockRedisClient('fr-cursor-erase-pub:');
		const breaker = recordingBreaker();
		const cursor = createCursor(client, { breaker });
		try {
			const entry = JSON.stringify({ user: 'u1', data: { x: 1 }, ts: Date.now() });
			await client.redis.hset(client.key('cursor:{room}'), 'other:c1', entry);
			client.redis.publish = async () => { throw noperm('channel'); };
			expect(await cursor.purgeUser(null, 'u1')).toBe(1);
			expect(await client.redis.hgetall(client.key('cursor:{room}'))).toEqual({});
			expect(breaker.failure).toHaveBeenCalled();
		} finally {
			cursor.destroy?.();
		}
	});

	it('connection registry heartbeat: a refused EXPIRE fails the breaker', async () => {
		const intervals = captureIntervals();
		const client = mockRedisClient('fr-registry-refused:');
		const breaker = recordingBreaker();
		const registry = createConnectionRegistry(client, { identify: (ws) => ws.getUserData().userId, heartbeat: 60000, breaker });
		try {
			const ws = mockWs({ userId: 'u1' });
			ws.getUserData()[WS_SESSION_ID] = 's1';
			await registry.hooks.open(ws, { platform: mockPlatform() });
			breaker.success.mockClear();
			breaker.failure.mockClear();
			client.redis.expire = async () => { throw readonly(); };
			for (const { cb } of intervals) cb();
			await settle();
			expect(breaker.failure).toHaveBeenCalledTimes(1);
			expect(breaker.success).not.toHaveBeenCalled();
		} finally {
			await registry.destroy();
		}
	});

	it('presence erasure: a topic whose delete Redis refused is not counted as erased', async () => {
		const client = mockRedisClient('fr-presence-refused:');
		const presence = createPresence(client);
		try {
			await client.redis.hset(client.key('presence:user:{room}:u1'), 'i1|w1', '{}');
			await client.redis.hset(client.key('presence:{room}'), 'u1', '{}');
			const del = client.redis.del.bind(client.redis);
			client.redis.del = async () => { throw readonly(); };
			expect(await presence.purgeUser(null, 'u1')).toBe(0);
			client.redis.del = del;
			expect(await presence.purgeUser(null, 'u1')).toBe(1);
		} finally {
			await presence.destroy?.();
		}
	});
});

describe('teardown survives a refused quit', () => {
	it('pub/sub bus: a refused subscribe closes its own connection when quit is refused too', async () => {
		const client = mockRedisClient('fr-pubsub-quit:');
		const duplicate = client.duplicate.bind(client);
		/** @type {any} */
		let conn = null;
		client.duplicate = (...args) => {
			conn = duplicate(...args);
			conn.subscribe = async () => { throw noperm('channel'); };
			conn.quit = async () => { throw new Error('Connection is closed.'); };
			conn.disconnect = vi.fn();
			return conn;
		};
		const unhandled = vi.fn();
		process.on('unhandledRejection', unhandled);
		try {
			const bus = createPubSubBus(client);
			await expect(bus.activate(mockPlatform())).rejects.toThrow('NOPERM');
			await settle();
			expect(conn.disconnect).toHaveBeenCalledTimes(1);
			expect(unhandled).not.toHaveBeenCalled();
		} finally {
			process.off('unhandledRejection', unhandled);
		}
	});

	it('pub/sub bus: a deactivate still closing leaves the connection of a newer activation alone', async () => {
		const { client, duplicates } = recordingClient('fr-pubsub-race:');
		const bus = createPubSubBus(client);
		const platform = mockPlatform();
		await bus.activate(platform);
		const first = duplicates[0];
		let release;
		first.unsubscribe = () => new Promise((resolve) => { release = resolve; });
		first.quit = vi.fn(async () => {});
		const stopping = bus.deactivate();
		await bus.activate(platform);
		const second = duplicates[1];
		second.quit = vi.fn(async () => {});
		release();
		await stopping;
		expect(first.quit).toHaveBeenCalledTimes(1);
		expect(second.quit).not.toHaveBeenCalled();
		await bus.deactivate();
		expect(second.quit).toHaveBeenCalledTimes(1);
	});

	it('cursor: dispose closes its connection when quit is refused', async () => {
		const { client, duplicates } = recordingClient('fr-cursor-quit:');
		const io = createRedisIo({
			client, redis: client.redis, channel: 'cursor-ch', instanceId: 'i1', b: null,
			validator: { acceptRaw: () => true, acceptEnvelope: () => true },
			cursorTtl: 30, snapshotIntervalMs: 0, activeTopics: new Set(),
			emit() {}, onMessage() {}, queueRemove() {}
		});
		io.ensureSubscriber(mockPlatform());
		await settle();
		const conn = duplicates[0];
		conn.quit = async () => { throw new Error('Connection is closed.'); };
		conn.disconnect = vi.fn();
		io.dispose();
		await settle();
		expect(conn.disconnect).toHaveBeenCalledTimes(1);
	});

	it('presence: the idle shutdown closes its connection when quit is refused', async () => {
		/** @type {Array<() => unknown>} */
		const timers = [];
		setRuntimeEnv({ timers: { set: (cb) => { timers.push(cb); return { unref() {} }; }, clear: () => {} } });
		const { client, duplicates } = recordingClient('fr-presence-quit:');
		const subscriber = presenceSubscriber(client, false);
		await subscriber.subscribeToTopic('room', mockPlatform());
		const conn = duplicates[0];
		conn.quit = async () => { throw new Error('Connection is closed.'); };
		conn.disconnect = vi.fn();
		await subscriber.unsubscribeFromTopic('room');
		for (const cb of timers) cb();
		await settle();
		expect(conn.disconnect).toHaveBeenCalledTimes(1);
		await subscriber.dispose?.();
	});
});

describe('a pub/sub envelope without an instance id', () => {
	it('is counted and dropped without a console line, in every mode', async () => {
		const { client, duplicates } = recordingClient('fr-pubsub-shape:');
		const metrics = recordingMetrics();
		const bus = createPubSubBus(client, { metrics });
		await bus.activate(mockPlatform());
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			const deliver = duplicates[0]._listeners.get('message');
			for (let i = 0; i < 20; i++) deliver('uws:pubsub', '{"instanceId":7}');
			await inMode(() => {
				for (let i = 0; i < 20; i++) deliver('uws:pubsub', '[1,2]');
			});
			await inMode(() => {
				for (let i = 0; i < 20; i++) deliver('uws:pubsub', 'null');
			}, 'development');
			expect(error).not.toHaveBeenCalled();
			expect(metrics.count('pubsub_parse_errors_total')).toBe(60);
		} finally {
			error.mockRestore();
			await bus.deactivate();
		}
	});
});

describe('every subscriber connection the package opens takes over its re-subscription', () => {
	/** A recording client whose duplicates carry ioredis options, as real ones do. */
	function optionsClient(prefix) {
		const recorded = recordingClient(prefix);
		const duplicate = recorded.client.duplicate;
		recorded.client.duplicate = (...args) => {
			const dup = duplicate(...args);
			dup.options = { autoResubscribe: true };
			return dup;
		};
		return recorded;
	}

	const sessionWs = () => {
		const ws = mockWs({ userId: 'u1' });
		ws.getUserData()[WS_SESSION_ID] = 's1';
		return ws;
	};

	for (const [owner, start] of [
		['pubsub', async (client) => { const bus = createPubSubBus(client); await bus.activate(mockPlatform()); return () => bus.deactivate(); }],
		['sharded-pubsub', async (client) => { const bus = createShardedBus(client); await bus.activate(mockPlatform()); return () => bus.deactivate(); }],
		['groups', async (client) => { captureIntervals(); const group = createGroup(client, 'lobby'); await group.join(mockWs({ id: 'm1' }), mockPlatform()); return () => group.destroy(); }],
		['cursor', async (client) => { captureIntervals(); const cursor = createCursor(client); cursor.update(mockWs({ id: 'c1' }), 'room', { x: 1, y: 2 }, mockPlatform()); return () => cursor.destroy?.(); }],
		['presence', async (client) => { const subscriber = presenceSubscriber(client, false); await subscriber.subscribeToTopic('room', mockPlatform()); return () => subscriber.dispose?.(); }],
		['crdt', async (client) => { const relay = createCrdtCluster(client); relay.onMessage({ onUpdate() {} }); return () => relay.destroy?.(); }],
		['smooth-cluster', async (client) => { const relay = createSmoothCluster(client); relay.onMessage({ onCommand() {} }); return () => relay.destroy?.(); }],
		['topic-broadcast', async (client) => { const relay = createTopicBroadcast(client); relay.onRequest(async () => []); return () => relay.destroy?.(); }],
		['registry', async (client) => { const registry = createConnectionRegistry(client, { identify: (ws) => ws.getUserData().userId, heartbeat: 60000 }); await registry.hooks.open(sessionWs(), { platform: mockPlatform() }); return () => registry.destroy(); }],
		['publish-rate', async (client) => { const aggregator = createPublishRateAggregator(client); await aggregator.activate(mockPlatform()); return () => aggregator.deactivate(); }]
	]) {
		it(owner, async () => {
			const { client, duplicates } = optionsClient('fr-guard-' + owner + ':');
			const stop = await start(client);
			try {
				await settle();
				expect(duplicates.length).toBeGreaterThan(0);
				for (const dup of duplicates) expect(dup.options.autoResubscribe).toBe(false);
			} finally {
				await stop();
			}
		});
	}
});
