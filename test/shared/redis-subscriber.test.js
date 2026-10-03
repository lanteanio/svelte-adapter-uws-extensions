import { describe, it, expect, afterEach, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { guardSubscriber, openSubscriber } from '../../src/shared/redis-subscriber.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

const flush = () => new Promise((resolve) => setImmediate(resolve));

const noperm = () => Object.assign(new Error('NOPERM No permissions to access a channel'), { name: 'ReplyError' });

/**
 * An ioredis-shaped subscriber connection. `server` is what Redis holds for it;
 * reconnect() drops that state and emits 'ready', as a real reconnect does.
 * `refuse(kind, channels)` decides whether Redis refuses, and `refusal` builds
 * the error it answers with (NOPERM by default).
 */
function fakeConn() {
	const conn = new EventEmitter();
	conn.options = { autoResubscribe: true };
	conn.status = 'ready';
	conn.server = { subscribe: new Set(), psubscribe: new Set(), ssubscribe: new Set() };
	/** @type {Array<string[]>} */
	conn.calls = [];
	conn.refuse = (_kind, _channels) => false;
	conn.refusal = (_channels) => noperm();
	/** @type {Promise<void> | null} while set, every reply waits for it */
	conn.gate = null;
	const channelsOf = (args) => args.flat().filter((arg) => typeof arg !== 'function').map(String);
	const subscribe = (kind) => async function (...args) {
		const channels = channelsOf(args);
		conn.calls.push([kind, ...channels]);
		if (conn.gate) await conn.gate;
		if (conn.refuse(kind, channels)) throw conn.refusal(channels);
		for (const channel of channels) conn.server[kind].add(channel);
		return conn.server[kind].size;
	};
	const unsubscribe = (kind, name) => async function (...args) {
		const channels = channelsOf(args);
		conn.calls.push([name, ...channels]);
		if (conn.gate) await conn.gate;
		if (channels.length === 0) conn.server[kind].clear();
		for (const channel of channels) conn.server[kind].delete(channel);
		return conn.server[kind].size;
	};
	conn.subscribe = subscribe('subscribe');
	conn.unsubscribe = unsubscribe('subscribe', 'unsubscribe');
	conn.psubscribe = subscribe('psubscribe');
	conn.punsubscribe = unsubscribe('psubscribe', 'punsubscribe');
	conn.ssubscribe = subscribe('ssubscribe');
	conn.sunsubscribe = unsubscribe('ssubscribe', 'sunsubscribe');
	conn.reconnect = () => {
		for (const kind of Object.keys(conn.server)) conn.server[kind].clear();
		conn.status = 'ready';
		conn.emit('ready');
	};
	return conn;
}

/** A guarded connection that has completed its first connect. */
function connected(owner = 'pubsub') {
	const conn = guardSubscriber(fakeConn(), owner);
	conn.emit('ready');
	return conn;
}

/** Capture timers set through the runtime seam so a test fires them by hand. */
function captureTimers() {
	/** @type {Array<{ cb: () => unknown, ms: number, cleared: boolean }>} */
	const timers = [];
	setRuntimeEnv({ timers: {
		set: (cb, ms) => { const t = { cb, ms, cleared: false, unref() {} }; timers.push(t); return t; },
		clear: (t) => { if (t) t.cleared = true; }
	} });
	return {
		timers,
		pending: () => timers.filter((t) => !t.cleared && !t.fired),
		async fire() {
			const due = timers.filter((t) => !t.cleared && !t.fired);
			for (const t of due) { t.fired = true; t.cb(); }
			await flush();
		}
	};
}

let sink = null;
afterEach(() => {
	sink?.stop();
	sink = null;
	resetRuntimeEnv();
});

describe('guarded subscriber connections', () => {
	it('switch ioredis re-subscription off and restore every channel after a reconnect', async () => {
		const conn = connected();
		expect(conn.options.autoResubscribe).toBe(false);
		await conn.subscribe('a', 'b');
		await conn.psubscribe('p:*');
		await conn.ssubscribe('s:1');
		await conn.ssubscribe('s:2');
		conn.calls.length = 0;

		conn.reconnect();
		await flush();
		expect(conn.server.subscribe).toEqual(new Set(['a', 'b']));
		expect(conn.server.psubscribe).toEqual(new Set(['p:*']));
		expect(conn.server.ssubscribe).toEqual(new Set(['s:1', 's:2']));
		// One command per kind, except shard channels, which go one per command.
		expect(conn.calls).toEqual([
			['subscribe', 'a', 'b'],
			['psubscribe', 'p:*'],
			['ssubscribe', 's:1'],
			['ssubscribe', 's:2']
		]);
	});

	it('send nothing on the first ready, which flushes what was queued while connecting', async () => {
		const conn = guardSubscriber(fakeConn(), 'pubsub');
		await conn.subscribe('a');
		conn.calls.length = 0;
		conn.emit('ready');
		await flush();
		expect(conn.calls).toEqual([]);
	});

	it('restore the channels Redis accepts when it refuses one, and report the refusal without the channel', async () => {
		const conn = connected('presence');
		await conn.subscribe('room:public', 'room:secret');
		const unhandled = vi.fn();
		process.on('unhandledRejection', unhandled);
		sink = captureRegistered();
		try {
			conn.refuse = (_kind, channels) => channels.includes('room:secret');
			conn.reconnect();
			await flush();
			await flush();
			expect(conn.server.subscribe).toEqual(new Set(['room:public']));
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE]);
			expect(sink.records[0].attributes).toEqual({
				owner: 'presence',
				kind: 'subscribe',
				refused: 1,
				error: { name: 'ReplyError', code: null, message: 'NOPERM No permissions to access a channel' }
			});
			expect(JSON.stringify(sink.records)).not.toContain('room:');
			expect(unhandled).not.toHaveBeenCalled();
		} finally {
			process.off('unhandledRejection', unhandled);
		}
	});

	it('cut the channel from a refusal that repeats it', async () => {
		const conn = connected('pubsub');
		await conn.subscribe('tenant:7:chat');
		sink = captureRegistered();
		conn.refuse = () => true;
		conn.refusal = (channels) => new Error("backend refused '" + channels[0] + "'");
		conn.reconnect();
		await flush();
		await flush();
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE]);
		expect(JSON.stringify(sink.records)).not.toContain('tenant:7');
	});

	it('retry a refused channel with a growing backoff while connected, and re-arm the report once it is restored', async () => {
		const clock = captureTimers();
		const conn = connected();
		await conn.subscribe('a');
		sink = captureRegistered();
		conn.refuse = () => true;
		conn.reconnect();
		await flush();
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE]);
		expect(clock.pending().map((t) => t.ms)).toEqual([1000]);

		// Still refused: retried, not reported again, and the delay doubles.
		await clock.fire();
		expect(sink.ids()).toHaveLength(1);
		expect(clock.pending().map((t) => t.ms)).toEqual([2000]);

		// The ACL is fixed: the next retry restores the channel and stops.
		conn.refuse = () => false;
		await clock.fire();
		expect(conn.server.subscribe).toEqual(new Set(['a']));
		expect(clock.pending()).toEqual([]);

		// A later streak is reported again, starting from the first delay.
		conn.refuse = () => true;
		conn.reconnect();
		await flush();
		expect(sink.ids()).toHaveLength(2);
		expect(clock.pending().map((t) => t.ms)).toEqual([1000]);
	});

	it('cap the retry delay at 30 seconds', async () => {
		const clock = captureTimers();
		const conn = connected();
		await conn.subscribe('a');
		conn.refuse = () => true;
		conn.reconnect();
		await flush();
		const delays = [];
		for (let i = 0; i < 8; i++) {
			delays.push(clock.pending()[0].ms);
			await clock.fire();
		}
		expect(delays).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
	});

	it('skip a retry while the connection is down and leave it to the next reconnect', async () => {
		const clock = captureTimers();
		const conn = connected();
		await conn.subscribe('a');
		conn.refuse = () => true;
		conn.reconnect();
		await flush();
		conn.status = 'reconnecting';
		conn.calls.length = 0;
		await clock.fire();
		expect(conn.calls).toEqual([]);
		conn.refuse = () => false;
		conn.reconnect();
		await flush();
		expect(conn.server.subscribe).toEqual(new Set(['a']));
	});

	it('stop retrying once the connection ends', async () => {
		const clock = captureTimers();
		const conn = connected();
		await conn.subscribe('a');
		conn.refuse = () => true;
		conn.reconnect();
		await flush();
		expect(clock.pending()).toHaveLength(1);
		conn.emit('end');
		expect(clock.pending()).toEqual([]);
	});

	it('do not retry a refused channel the owner released meanwhile', async () => {
		const clock = captureTimers();
		const conn = connected();
		await conn.subscribe('a');
		conn.refuse = () => true;
		conn.reconnect();
		await flush();
		await conn.unsubscribe('a');
		conn.calls.length = 0;
		await clock.fire();
		expect(conn.calls).toEqual([]);
		expect(clock.pending()).toEqual([]);
	});

	it('neither report nor retry a channel the owner released while Redis was refusing its restore', async () => {
		const clock = captureTimers();
		const conn = connected();
		await conn.subscribe('a');
		sink = captureRegistered();
		let open;
		conn.gate = new Promise((resolve) => { open = resolve; });
		conn.refuse = (kind) => kind === 'subscribe';
		conn.reconnect();
		const released = conn.unsubscribe('a');
		open();
		await released;
		await flush();
		await flush();
		expect(sink.ids()).toEqual([]);
		expect(clock.pending()).toEqual([]);
	});

	it('hand the owner its own rejection unchanged', async () => {
		const conn = connected();
		const refusal = noperm();
		conn.refuse = () => true;
		conn.refusal = () => refusal;
		let caught = null;
		try { await conn.subscribe('a'); } catch (err) { caught = err; }
		expect(caught).toBe(refusal);
		conn.refuse = () => false;
		conn.reconnect();
		await flush();
		// A subscribe Redis refused was never held, so nothing is restored.
		expect(conn.server.subscribe.size).toBe(0);
	});

	it('ask only for the channels the owner still holds when a refused batch falls back to one per channel', async () => {
		const conn = connected();
		await conn.subscribe('keep', 'drop');
		let open;
		conn.gate = new Promise((resolve) => { open = resolve; });
		conn.refuse = (_kind, channels) => channels.includes('keep') && channels.includes('drop');
		conn.reconnect();
		// The owner releases one channel while Redis is answering the batch.
		const released = conn.unsubscribe('drop');
		conn.calls.length = 0;
		open();
		await released;
		conn.gate = null;
		await flush();
		await flush();
		expect(conn.calls).toEqual([['subscribe', 'keep']]);
		expect(conn.server.subscribe).toEqual(new Set(['keep']));
	});

	it('do not restore a channel unsubscribed while its subscribe was still awaiting a reply', async () => {
		const conn = connected();
		let open;
		conn.gate = new Promise((resolve) => { open = resolve; });
		const subscribed = conn.subscribe('a');
		const unsubscribed = conn.unsubscribe('a');
		open();
		await subscribed;
		await unsubscribed;
		conn.gate = null;
		conn.calls.length = 0;
		conn.reconnect();
		await flush();
		expect(conn.calls).toEqual([]);
	});

	it('drop every channel of a kind on an unsubscribe without arguments', async () => {
		const conn = connected();
		await conn.subscribe('a', 'b');
		await conn.psubscribe('p:*');
		await conn.unsubscribe();
		conn.calls.length = 0;
		conn.reconnect();
		await flush();
		expect(conn.calls).toEqual([['psubscribe', 'p:*']]);
	});

	it('ignore the results of a restore that a newer reconnect superseded', async () => {
		const clock = captureTimers();
		const conn = connected();
		await conn.subscribe('a');
		sink = captureRegistered();
		let open;
		conn.gate = new Promise((resolve) => { open = resolve; });
		conn.refuse = () => true;
		conn.reconnect();
		// Reconnects again before Redis answered; this round is accepted.
		conn.gate = null;
		conn.refuse = () => false;
		conn.reconnect();
		await flush();
		conn.refuse = () => true;
		open();
		await flush();
		await flush();
		expect(sink.ids()).toEqual([]);
		expect(clock.pending()).toEqual([]);
	});

	it('leave a connection whose re-subscription cannot be switched off untouched', () => {
		const conn = fakeConn();
		delete conn.options;
		const subscribe = conn.subscribe;
		expect(guardSubscriber(conn, 'pubsub')).toBe(conn);
		expect(conn.subscribe).toBe(subscribe);
		expect(conn.listenerCount('ready')).toBe(0);
	});

	it('openSubscriber duplicates without a ready check and guards the result', () => {
		const conn = fakeConn();
		const source = { duplicate: vi.fn(() => conn) };
		expect(openSubscriber(source, 'groups')).toBe(conn);
		expect(source.duplicate).toHaveBeenCalledWith({ enableReadyCheck: false });
		expect(conn.options.autoResubscribe).toBe(false);
	});
});

/**
 * An ioredis-shaped Cluster. Its own (un)subscribe methods only record the
 * call, since a guarded Cluster must not use them for what it takes over.
 * Each node's duplicate() opens a node connection that is connecting until
 * connect() runs (at once unless `autoConnect` is off), holds its commands
 * until then, and on end() rejects them and ends for good, as a cluster node
 * connection without a retry strategy does. `refuse` applies to every node.
 * An end() inside a test body runs from a microtask, where ioredis's
 * rejections are handled before its 'end' event.
 */
function fakeCluster() {
	const cluster = new EventEmitter();
	cluster.options = {};
	cluster.status = 'ready';
	/** @type {Array<unknown[]>} */
	cluster.ownCalls = [];
	for (const name of ['subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe', 'ssubscribe', 'sunsubscribe']) {
		cluster[name] = async (...args) => { cluster.ownCalls.push([name, ...args]); };
	}
	cluster.quits = 0;
	cluster.quit = async () => { cluster.quits++; return 'OK'; };
	/** @type {unknown[]} */
	cluster.disconnects = [];
	// Reconnecting resets the node pool, so every node leaves, as in ioredis.
	cluster.disconnect = (reconnect) => {
		cluster.disconnects.push(reconnect);
		if (!reconnect) return;
		cluster.status = 'reconnecting';
		for (const n of cluster.nodeList) cluster.emit('-node', n);
	};
	cluster.connects = 0;
	cluster.connect = async () => {
		cluster.connects++;
		cluster.status = 'ready';
		cluster.emit('ready');
	};
	cluster.autoConnect = true;
	// While set, a node connection becomes 'ready' as ioredis reports one
	// whose login was refused, then rejects what it holds and ends.
	cluster.loginRefused = false;
	cluster.refuse = (_kind, _channels) => false;
	/** @type {any[]} every node connection opened, in order */
	cluster.opened = [];
	cluster.roles = [];
	const node = (port) => ({
		options: { host: '10.0.0.1', port },
		duplicate(overrides) {
			const conn = fakeConn();
			conn.options = { host: '10.0.0.1', port, lazyConnect: true, ...overrides };
			conn.status = 'connecting';
			conn.refuse = (kind, channels) => cluster.refuse(kind, channels);
			let open;
			let fail;
			conn.gate = new Promise((resolve, reject) => { open = resolve; fail = reject; });
			conn.gate.catch(() => {});
			conn.connect = () => {
				conn.status = 'ready';
				conn.gate = null;
				open();
				conn.emit('ready');
			};
			// As ioredis does: the status and the rejected commands at once, the
			// event on the next tick, so what awaited a command can run first.
			conn.end = () => {
				if (conn.status === 'end') return;
				conn.status = 'end';
				fail(new Error('Connection is closed.'));
				process.nextTick(() => conn.emit('end'));
			};
			// As ioredis without a ready check: 'ready' although the login was
			// refused, the queued commands rejected with that refusal, the 'error'
			// at once, and the close after.
			conn.refuseLogin = () => {
				conn.status = 'ready';
				conn.emit('ready');
				const refusal = new Error('WRONGPASS invalid username-password pair or user is disabled.');
				fail(refusal);
				conn.emit('error', refusal);
				process.nextTick(() => {
					conn.status = 'end';
					conn.emit('end');
				});
			};
			// A socket close as ioredis handles it outside a microtask: 'end'
			// runs before what awaited a command learns it was rejected.
			conn.drop = () => {
				conn.status = 'end';
				conn.emit('end');
				fail(new Error('Connection is closed.'));
			};
			// Holds every reply until the connection ends, which rejects them.
			conn.hold = () => {
				conn.gate = new Promise((_, reject) => { fail = reject; });
				conn.gate.catch(() => {});
			};
			// ioredis rejects a command on an ended connection without sending it.
			for (const name of ['subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe']) {
				const command = conn[name];
				conn[name] = (...args) => (conn.status === 'end' ? Promise.reject(new Error('Connection is closed.')) : command(...args));
			}
			conn.quits = 0;
			conn.quit = async () => { conn.quits++; conn.end(); return 'OK'; };
			conn.disconnects = 0;
			conn.disconnect = () => { conn.disconnects++; conn.end(); };
			cluster.opened.push(conn);
			if (cluster.autoConnect) {
				queueMicrotask(() => {
					if (conn.status !== 'connecting') return;
					if (cluster.loginRefused) conn.refuseLogin();
					else conn.connect();
				});
			}
			return conn;
		}
	});
	cluster.nodeList = [node(7000), node(7001)];
	cluster.nodes = (role) => {
		cluster.roles.push(role);
		return cluster.nodeList;
	};
	cluster.node = node;
	return cluster;
}

describe('guarded Cluster subscribers', () => {
	it('carry subscriptions on a node connection of their own and re-emit what it receives', async () => {
		const cluster = fakeCluster();
		expect(guardSubscriber(cluster, 'pubsub')).toBe(cluster);
		await cluster.subscribe('a', 'b');
		await cluster.psubscribe('p:*');
		expect(cluster.opened).toHaveLength(1);
		const conn = cluster.opened[0];
		expect(conn.server.subscribe).toEqual(new Set(['a', 'b']));
		expect(conn.server.psubscribe).toEqual(new Set(['p:*']));
		expect(conn.options).toMatchObject({ enableReadyCheck: false, autoResubscribe: false });
		expect(cluster.roles).toEqual(['all']);
		// ioredis's cluster subscriber is never given a channel.
		expect(cluster.ownCalls).toEqual([]);

		const received = [];
		cluster.on('message', (...args) => received.push(['message', ...args]));
		cluster.on('pmessage', (...args) => received.push(['pmessage', ...args]));
		conn.emit('message', 'a', 'hello');
		conn.emit('pmessage', 'p:*', 'p:1', 'hi');
		expect(received).toEqual([['message', 'a', 'hello'], ['pmessage', 'p:*', 'p:1', 'hi']]);
	});

	it('leave shard subscriptions to the Cluster, which routes them to the slot owner', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.ssubscribe('s:1');
		expect(cluster.ownCalls).toEqual([['ssubscribe', 's:1']]);
		expect(cluster.opened).toEqual([]);
	});

	it('pick the node through the runtime RNG', async () => {
		setRuntimeEnv({ rng: { float: () => 0.99 } });
		const last = guardSubscriber(fakeCluster(), 'pubsub');
		await last.subscribe('a');
		expect(last.opened[0].options.port).toBe(7001);
		setRuntimeEnv({ rng: { float: () => 0 } });
		const first = guardSubscriber(fakeCluster(), 'pubsub');
		await first.subscribe('a');
		expect(first.opened[0].options.port).toBe(7000);
	});

	it('move the subscriptions to a new node connection when the connection ends, one command per kind', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a', 'b');
		await cluster.psubscribe('p:*');
		cluster.opened[0].end();
		await flush();
		expect(cluster.opened).toHaveLength(2);
		const next = cluster.opened[1];
		expect(next.calls).toEqual([['subscribe', 'a', 'b'], ['psubscribe', 'p:*']]);
		expect(next.server.subscribe).toEqual(new Set(['a', 'b']));
		expect(next.server.psubscribe).toEqual(new Set(['p:*']));
		expect(cluster.ownCalls).toEqual([]);
	});

	it('restore what the new node accepts, report a refusal once, and retry it there until the grant is back', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'registry');
		await cluster.subscribe('room:public', 'room:secret');
		sink = captureRegistered();
		const unhandled = vi.fn();
		process.on('unhandledRejection', unhandled);
		try {
			cluster.refuse = (_kind, channels) => channels.includes('room:secret');
			cluster.opened[0].end();
			await flush();
			await flush();
			const next = cluster.opened[1];
			expect(next.server.subscribe).toEqual(new Set(['room:public']));
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE]);
			expect(sink.records[0].attributes).toMatchObject({ owner: 'registry', kind: 'subscribe', refused: 1 });
			expect(JSON.stringify(sink.records)).not.toContain('room:');
			expect(clock.pending().map((t) => t.ms)).toEqual([1000]);

			await clock.fire();
			expect(sink.ids()).toHaveLength(1);
			expect(clock.pending().map((t) => t.ms)).toEqual([2000]);

			cluster.refuse = () => false;
			await clock.fire();
			expect(next.server.subscribe).toEqual(new Set(['room:public', 'room:secret']));
			expect(clock.pending()).toEqual([]);
			// Restored on the same node connection, without another move.
			expect(cluster.opened).toHaveLength(2);
			expect(unhandled).not.toHaveBeenCalled();
		} finally {
			process.off('unhandledRejection', unhandled);
		}
	});

	it('keep one failure streak across node connections', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		sink = captureRegistered();
		cluster.refuse = () => true;
		cluster.opened[0].end();
		await flush();
		// Nothing succeeded on the second connection, so the third waits a moment.
		cluster.opened[1].end();
		await flush();
		await clock.fire();
		expect(cluster.opened).toHaveLength(3);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE]);
	});

	it('ignore what a restore reports once its connection has ended', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		sink = captureRegistered();
		cluster.autoConnect = false;
		cluster.opened[0].end();
		await flush();
		// The restore waits on a connection that ends before it is ready.
		cluster.opened[1].end();
		await flush();
		await flush();
		expect(sink.ids()).toEqual([]);
		// Replaced after a delay, and the channel restored there.
		expect(clock.pending().map((t) => t.ms)).toEqual([100]);
		await clock.fire();
		cluster.opened[2].connect();
		await flush();
		expect(cluster.opened[2].server.subscribe).toEqual(new Set(['a']));
	});

	it('replace a connection on which nothing succeeded after a growing delay, and at once after one that answered', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		cluster.autoConnect = false;
		cluster.opened[0].end();
		await flush();
		expect(cluster.opened).toHaveLength(2);
		const delays = [];
		for (let i = 0; i < 22; i++) {
			cluster.opened.at(-1).end();
			await flush();
			delays.push(clock.pending()[0].ms);
			await clock.fire();
		}
		expect(delays.slice(0, 3)).toEqual([100, 200, 300]);
		expect(delays.slice(-3)).toEqual([2000, 2000, 2000]);

		cluster.opened.at(-1).connect();
		await flush();
		const opened = cluster.opened.length;
		cluster.opened.at(-1).end();
		await flush();
		expect(cluster.opened).toHaveLength(opened + 1);
		expect(clock.pending()).toEqual([]);
	});

	it('back off from a node that reports ready but refused the login', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		sink = captureRegistered();
		cluster.loginRefused = true;
		cluster.opened[0].end();
		await flush();
		await flush();
		expect(cluster.opened).toHaveLength(2);
		expect(clock.pending().map((t) => t.ms)).toEqual([100]);
		await clock.fire();
		await flush();
		expect(clock.pending().map((t) => t.ms)).toEqual([200]);
		// The refused login is not a refused channel.
		expect(sink.ids()).toEqual([]);

		cluster.loginRefused = false;
		await clock.fire();
		await flush();
		expect(cluster.opened.at(-1).server.subscribe).toEqual(new Set(['a']));
		expect(clock.pending()).toEqual([]);
	});

	it('send an owner call cut off by the end of its connection again on the next one', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		cluster.autoConnect = false;
		let done = false;
		const subscribed = cluster.subscribe('a').then(() => { done = true; });
		await flush();
		cluster.opened[0].end();
		await flush();
		expect(done).toBe(false);
		await clock.fire();
		expect(cluster.opened).toHaveLength(2);
		cluster.opened[1].connect();
		await subscribed;
		expect(cluster.opened[1].server.subscribe).toEqual(new Set(['a']));
		// Held, so it is restored on the next connection too.
		cluster.autoConnect = true;
		cluster.opened[1].end();
		await flush();
		expect(cluster.opened[2].server.subscribe).toEqual(new Set(['a']));
	});

	it('wait out the delay before resending a first call whose connection closed unanswered', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		cluster.autoConnect = false;
		const subscribed = cluster.subscribe('a');
		await flush();
		cluster.opened[0].drop();
		await flush();
		expect(cluster.opened).toHaveLength(1);
		expect(clock.pending().map((t) => t.ms)).toEqual([100]);
		await clock.fire();
		cluster.opened[1].connect();
		await subscribed;
		expect(cluster.opened[1].server.subscribe).toEqual(new Set(['a']));
	});

	it('settle an undo at once while no node connection holds anything', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		// The Cluster is reconnecting: the subscriptions died with the connection.
		cluster.status = 'connecting';
		cluster.opened[0].end();
		await flush();
		await cluster.unsubscribe('a');
		cluster.status = 'ready';
		cluster.emit('ready');
		await flush();
		expect(cluster.opened).toHaveLength(1);
	});

	it('settle an undo its connection did not answer before ending, in either event order', async () => {
		for (const close of ['end', 'drop']) {
			const cluster = guardSubscriber(fakeCluster(), 'pubsub');
			await cluster.subscribe('a');
			const conn = cluster.opened[0];
			// The Cluster loses its nodes: nothing reopens until it is ready.
			cluster.status = 'connecting';
			conn.hold();
			const released = cluster.unsubscribe('a');
			conn[close]();
			await released;
			await flush();
			expect(cluster.opened).toHaveLength(1);
		}
	});

	it('keep the order of calls a connection did not answer before ending', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('d');
		const first = cluster.opened[0];
		first.hold();
		const released = cluster.unsubscribe('d');
		first.end();
		// Made before the end is handled, after the unsubscribe.
		const subscribed = cluster.subscribe('d');
		await released;
		await subscribed;
		const next = cluster.opened[1];
		expect(next.calls).toEqual([['subscribe', 'd']]);
		expect(next.server.subscribe).toEqual(new Set(['d']));
		next.end();
		await flush();
		expect(cluster.opened[2].server.subscribe).toEqual(new Set(['d']));
	});

	it('hand a waiting call the error of a node connection that cannot be created', async () => {
		const cluster = fakeCluster();
		const failure = new Error('invalid options');
		for (const n of cluster.nodeList) n.duplicate = () => { throw failure; };
		cluster.status = 'connecting';
		guardSubscriber(cluster, 'pubsub');
		const subscribed = cluster.subscribe('a');
		cluster.status = 'ready';
		// Opened from the Cluster's own event: nothing may throw back into it.
		expect(() => cluster.emit('ready')).not.toThrow();
		await expect(subscribed).rejects.toBe(failure);
	});

	it('send an undo in order behind a subscribe still waiting for a connection', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		cluster.status = 'connecting';
		const subscribed = cluster.subscribe('b');
		const released = cluster.unsubscribe('b');
		await flush();
		cluster.status = 'ready';
		cluster.emit('ready');
		await subscribed;
		await released;
		expect(cluster.opened[0].calls).toEqual([['subscribe', 'b'], ['unsubscribe', 'b']]);
		expect(cluster.opened[0].server.subscribe.size).toBe(0);
	});

	it('give up on a cut-off call after as many attempts as the Cluster allows', async () => {
		const clock = captureTimers();
		const raw = fakeCluster();
		raw.options.maxRedirections = 2;
		const cluster = guardSubscriber(raw, 'pubsub');
		cluster.autoConnect = false;
		const subscribed = cluster.subscribe('a');
		let error = null;
		subscribed.catch((err) => { error = err; });
		await flush();
		cluster.opened[0].end();
		await flush();
		await clock.fire();
		cluster.opened[1].end();
		await flush();
		await flush();
		expect(error?.message).toBe('Connection is closed.');
		expect(cluster.opened).toHaveLength(2);
	});

	it('open nothing before the Cluster is ready, and connect one created lazily', async () => {
		const connecting = fakeCluster();
		connecting.status = 'connecting';
		guardSubscriber(connecting, 'pubsub');
		const subscribed = connecting.subscribe('a');
		await flush();
		expect(connecting.opened).toEqual([]);
		expect(connecting.connects).toBe(0);
		connecting.status = 'ready';
		connecting.emit('ready');
		await subscribed;
		expect(connecting.opened[0].server.subscribe).toEqual(new Set(['a']));

		const lazy = fakeCluster();
		lazy.status = 'wait';
		guardSubscriber(lazy, 'pubsub');
		await lazy.subscribe('a');
		expect(lazy.connects).toBe(1);
		expect(lazy.opened[0].server.subscribe).toEqual(new Set(['a']));
	});

	it('report a later refusal streak after the owner released the refused channels', async () => {
		captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		sink = captureRegistered();
		cluster.refuse = () => true;
		cluster.opened[0].end();
		await flush();
		expect(sink.ids()).toHaveLength(1);
		await cluster.unsubscribe('a');
		cluster.opened[1].end();
		await flush();
		cluster.refuse = () => false;
		await cluster.subscribe('b');
		cluster.refuse = () => true;
		cluster.opened.at(-1).end();
		await flush();
		expect(sink.ids()).toHaveLength(2);
	});

	it('open no connection while the owner holds nothing', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		await cluster.unsubscribe('a');
		cluster.opened[0].end();
		await flush();
		expect(cluster.opened).toHaveLength(1);
		await cluster.subscribe('b');
		expect(cluster.opened).toHaveLength(2);
		expect(cluster.opened[1].calls).toEqual([['subscribe', 'b']]);
	});

	it('hold a call until the Cluster knows a node, then send it', async () => {
		const cluster = fakeCluster();
		const nodes = cluster.nodeList;
		cluster.nodeList = [];
		guardSubscriber(cluster, 'pubsub');
		let done = false;
		const subscribed = cluster.subscribe('a').then(() => { done = true; });
		await flush();
		expect(cluster.opened).toEqual([]);
		expect(done).toBe(false);
		cluster.nodeList = nodes;
		cluster.emit('+node', nodes[0]);
		await subscribed;
		expect(cluster.opened).toHaveLength(1);
		expect(cluster.opened[0].server.subscribe).toEqual(new Set(['a']));
	});

	it('move off a node the refreshed slot map no longer lists, and only then', async () => {
		setRuntimeEnv({ rng: { float: () => 0 } });
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		const first = cluster.opened[0];
		// ioredis's own connection to the node ending says nothing about this one.
		cluster.emit('-node', cluster.nodeList[0]);
		// A refresh that still lists the node changes nothing.
		cluster.emit('refresh');
		await flush();
		expect(first.status).toBe('ready');
		expect(cluster.opened).toHaveLength(1);
		cluster.nodeList = cluster.nodeList.slice(1);
		cluster.emit('refresh');
		await flush();
		expect(first.status).toBe('end');
		expect(cluster.opened).toHaveLength(2);
		expect(cluster.opened[1].options.port).toBe(7001);
		expect(cluster.opened[1].server.subscribe).toEqual(new Set(['a']));
	});

	it('stop re-emitting what a replaced connection still receives', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		const first = cluster.opened[0];
		first.end();
		await flush();
		const received = [];
		cluster.on('message', (...args) => received.push(args));
		cluster.on('pmessage', (...args) => received.push(args));
		first.emit('message', 'a', 'late');
		first.emit('pmessage', 'p:*', 'p:1', 'late');
		cluster.opened[1].emit('message', 'a', 'live');
		cluster.opened[1].emit('pmessage', 'p:*', 'p:1', 'live');
		expect(received).toEqual([['a', 'live'], ['p:*', 'p:1', 'live']]);
	});

	it('close the node connection when the Cluster is quit, and send nothing after', async () => {
		const clock = captureTimers();
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		const conn = cluster.opened[0];
		const received = [];
		cluster.on('message', (...args) => received.push(args));
		await cluster.quit();
		expect(cluster.quits).toBe(1);
		// Closed at once, as ioredis stops its own subscriber.
		expect(conn.disconnects).toBe(1);
		// What the closing connection still receives is not re-emitted.
		conn.emit('message', 'a', 'late');
		expect(received).toEqual([]);
		await flush();
		expect(cluster.opened).toHaveLength(1);
		expect(clock.pending()).toEqual([]);
		await expect(cluster.subscribe('b')).rejects.toThrow('Connection is closed.');
		expect(cluster.listenerCount('+node')).toBe(0);
		expect(cluster.listenerCount('-node')).toBe(0);
	});

	it('reject a call still waiting for a node when the Cluster is quit', async () => {
		const cluster = fakeCluster();
		cluster.nodeList = [];
		guardSubscriber(cluster, 'pubsub');
		const subscribed = cluster.subscribe('a');
		await cluster.quit();
		await expect(subscribed).rejects.toThrow('Connection is closed.');
	});

	it('keep the node connection through a reconnect of the Cluster, and close it on a disconnect', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		const first = cluster.opened[0];
		// Reconnecting resets ioredis's own node connections, not this one.
		cluster.disconnect(true);
		cluster.status = 'ready';
		cluster.emit('ready');
		cluster.emit('refresh');
		await flush();
		expect(first.status).toBe('ready');
		expect(cluster.opened).toHaveLength(1);

		cluster.disconnect();
		expect(first.disconnects).toBe(1);
		expect(cluster.disconnects).toEqual([true, undefined]);
		await expect(cluster.subscribe('b')).rejects.toThrow('Connection is closed.');
	});

	it('reject a sent call whose closing was set aside when the Cluster is quit', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		const conn = cluster.opened[0];
		conn.hold();
		const subscribed = cluster.subscribe('x');
		// The rejection reaches the call before the connection's 'end' event,
		// and the Cluster is quit in between.
		conn.end();
		await null;
		await null;
		await null;
		cluster.quit();
		await expect(subscribed).rejects.toThrow('Connection is closed.');
	});

	it('close the node connection when the Cluster ends on its own', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		const conn = cluster.opened[0];
		cluster.emit('end');
		expect(conn.disconnects).toBe(1);
		await flush();
		expect(cluster.opened).toHaveLength(1);
	});

	it('openSubscriber guards a Cluster the client duplicates', async () => {
		const cluster = fakeCluster();
		const source = { duplicate: vi.fn(() => cluster) };
		expect(openSubscriber(source, 'groups')).toBe(cluster);
		await cluster.subscribe('a');
		expect(cluster.ownCalls).toEqual([]);
		expect(cluster.opened[0].server.subscribe).toEqual(new Set(['a']));
	});
});
