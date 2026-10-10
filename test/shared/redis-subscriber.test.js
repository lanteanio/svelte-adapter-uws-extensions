import { describe, it, expect, afterEach, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { guardSubscriber, openSubscriber, watchLiveness } from '../../src/shared/redis-subscriber.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';
import Redis from 'ioredis';
import { TimeoutError } from '../../src/shared/errors.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

const flush = () => new Promise((resolve) => setImmediate(resolve));

const noperm = () => Object.assign(new Error('NOPERM No permissions to access a channel'), { name: 'ReplyError' });

// ioredis's class for an error the server replied with; one a bundler renamed
// is told by the class alone.
const { ReplyError } = Redis;
class RenamedReplyError extends ReplyError {}

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
	// While `silent` is set a PING is never answered, as on a host that died
	// without closing the socket; `pingReply` is what an answered one returns.
	conn.silent = false;
	conn.pings = 0;
	conn.pingReply = () => Promise.resolve(['pong', '']);
	conn.ping = () => {
		conn.pings++;
		return conn.silent ? new Promise(() => {}) : conn.pingReply();
	};
	// Each connection has a socket of its own. Destroying it closes the
	// connection, which `socketClosed` then handles as ioredis does.
	conn.sockets = [];
	const openSocket = () => {
		const socket = Object.assign(new EventEmitter(), { destroyed: false, destroy() { this.destroyed = true; conn.socketClosed(); } });
		conn.sockets.push(socket);
		conn.stream = socket;
	};
	openSocket();
	conn.reconnect = () => {
		for (const kind of Object.keys(conn.server)) conn.server[kind].clear();
		openSocket();
		conn.status = 'ready';
		conn.emit('ready');
	};
	conn.socketClosed = () => conn.reconnect();
	return conn;
}

/** A guarded connection that has completed its first connect. */
function connected(owner = 'pubsub') {
	const conn = guardSubscriber(fakeConn(), owner);
	conn.emit('ready');
	return conn;
}

/**
 * Capture timers set through the runtime seam so a test fires them by hand:
 * fire() runs the one-shot timers due, tick() one round of every interval.
 * `env` is installed with them, a seeded RNG for example.
 */
function captureTimers(env = {}) {
	/** @type {Array<{ cb: () => unknown, ms: number, cleared: boolean }>} */
	const timers = [];
	/** @type {Array<{ cb: () => unknown, ms: number, cleared: boolean }>} */
	const intervals = [];
	const timer = (list) => (cb, ms) => { const t = { cb, ms, cleared: false, unref() {} }; list.push(t); return t; };
	const clear = (t) => { if (t) t.cleared = true; };
	setRuntimeEnv({ ...env, timers: { set: timer(timers), clear, setInterval: timer(intervals), clearInterval: clear } });
	return {
		timers,
		intervals,
		pending: () => timers.filter((t) => !t.cleared && !t.fired),
		running: () => intervals.filter((t) => !t.cleared),
		async fire() {
			const due = timers.filter((t) => !t.cleared && !t.fired);
			for (const t of due) { t.fired = true; t.cb(); }
			await flush();
		},
		async tick() {
			for (const t of intervals.filter((t) => !t.cleared)) t.cb();
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

describe('subscriber connections that stop answering', () => {
	it('ask for a PING every 30 seconds and reconnect one that does not answer within 10, restoring its channels', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		await conn.subscribe('a');
		await conn.psubscribe('p:*');
		sink = captureRegistered();
		expect(clock.running().map((t) => t.ms)).toEqual([30000]);
		await clock.tick();
		expect(conn.pings).toBe(1);
		// Answered: its timeout is cleared, nothing is closed, and nothing is
		// left listening on the socket, which an idle connection would pile up.
		expect(clock.pending()).toEqual([]);
		expect(conn.stream.listenerCount('data')).toBe(0);
		expect(conn.sockets).toHaveLength(1);

		conn.silent = true;
		await clock.tick();
		expect(conn.pings).toBe(2);
		expect(clock.pending().map((t) => t.ms)).toEqual([10000]);
		await clock.fire();
		// Its socket is destroyed, not ended: ending waits on a peer that may
		// never answer. ioredis then reconnects and the channels come back.
		expect(conn.sockets[0].destroyed).toBe(true);
		expect(conn.sockets).toHaveLength(2);
		expect(conn.server.subscribe).toEqual(new Set(['a']));
		expect(conn.server.psubscribe).toEqual(new Set(['p:*']));
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
		expect(sink.records[0].attributes).toEqual({ owner: 'pubsub', timeoutMs: 10000 });
	});

	it('report the first silent connection of a streak, and again once a PING was answered in between', async () => {
		const clock = captureTimers();
		const conn = connected('registry');
		await conn.subscribe('a');
		sink = captureRegistered();
		conn.silent = true;
		await clock.tick();
		await clock.fire();
		await clock.tick();
		await clock.fire();
		expect(conn.sockets).toHaveLength(3);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);

		conn.silent = false;
		await clock.tick();
		conn.silent = true;
		await clock.tick();
		await clock.fire();
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT, EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
	});

	it('take any reply for an answer, a refusal of the PING included', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		await conn.subscribe('a');
		conn.pingReply = () => Promise.reject(Object.assign(new Error('NOPERM this user has no permissions to run the \'ping\' command'), { name: 'ReplyError' }));
		await clock.tick();
		expect(conn.pings).toBe(1);
		expect(clock.pending()).toEqual([]);
		expect(conn.sockets).toHaveLength(1);
		// It ends a silence streak like any answer.
		sink = captureRegistered();
		conn.silent = true;
		await clock.tick();
		await clock.fire();
		conn.silent = false;
		await clock.tick();
		conn.silent = true;
		await clock.tick();
		await clock.fire();
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT, EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
	});

	it('send no second PING while one is out, and none on a connection that is not ready', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		conn.silent = true;
		await clock.tick();
		await clock.tick();
		expect(conn.pings).toBe(1);
		await clock.fire();
		conn.status = 'reconnecting';
		await clock.tick();
		expect(conn.pings).toBe(1);
	});

	it.each([new Error('Command timed out'), new TimeoutError('redis', 'command', 5000)])('keeps the silence deadline after a command timeout (%s)', async error => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		await conn.subscribe('a');
		sink = captureRegistered();
		conn.pingReply = () => Promise.reject(error);
		await clock.tick();
		expect(conn.sockets[0].destroyed).toBe(false);
		expect(clock.pending().map((t) => t.ms)).toEqual([10000]);
		await clock.fire();
		expect(conn.sockets[0].destroyed).toBe(true);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
		expect(conn.server.subscribe).toEqual(new Set(['a']));
	});

	it('take anything coming in over the socket for an answer, as on a busy connection whose reply waits behind its messages', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		conn.silent = true;
		await clock.tick();
		conn.stream.emit('data', Buffer.from('*3\r\n$7\r\nmessage\r\n'));
		await flush();
		expect(clock.pending()).toEqual([]);
		expect(conn.stream.listenerCount('data')).toBe(0);
		await clock.fire();
		expect(conn.sockets[0].destroyed).toBe(false);
	});

	it('take what came in before the 10 seconds were up for an answer, though the timer runs ahead of the socket read', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		sink = captureRegistered();
		conn.silent = true;
		await clock.tick();
		// A timer runs before the socket is read in the same turn of the event
		// loop, so what arrived meanwhile is read after it.
		const [timeout] = clock.pending();
		timeout.fired = true;
		timeout.cb();
		conn.stream.emit('data', Buffer.from('+PONG\r\n'));
		await flush();
		expect(conn.sockets[0].destroyed).toBe(false);
		expect(sink.ids()).toEqual([]);
	});

	it('take a reply of ioredis\'s error class for an answer, renamed or not', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		sink = captureRegistered();
		for (const Reply of [ReplyError, RenamedReplyError]) {
			conn.silent = true;
			await clock.tick();
			await clock.fire();
			conn.silent = false;
			conn.pingReply = () => Promise.reject(new Reply('NOPERM this user has no permissions to run the \'ping\' command'));
			await clock.tick();
		}
		// Each refusal ended the streak, so each silence before it was reported.
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT, EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
		conn.silent = true;
		await clock.tick();
		await clock.fire();
		expect(sink.ids()).toHaveLength(3);
	});

	it('count nothing for a PING whose connection reconnected or closed meanwhile', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		await conn.subscribe('a');
		sink = captureRegistered();
		// Out when the connection dropped, and still unanswered when the
		// reconnect is up: whatever that one says comes next round.
		conn.silent = true;
		await clock.tick();
		conn.reconnect();
		await clock.fire();
		expect(conn.sockets.map((s) => s.destroyed)).toEqual([false, false]);
		// Rejected because the connection closed: its end is handled elsewhere.
		conn.silent = false;
		conn.pingReply = () => Promise.reject(new Error('Connection is closed.'));
		await clock.tick();
		expect(conn.sockets.map((s) => s.destroyed)).toEqual([false, false]);
		expect(sink.ids()).toEqual([]);
	});

	it('keep a silence streak through a PING rejected because its connection closed', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		sink = captureRegistered();
		conn.silent = true;
		await clock.tick();
		await clock.fire();
		expect(sink.ids()).toHaveLength(1);
		// No answer: the streak goes on, and the next silence is not reported.
		conn.silent = false;
		conn.pingReply = () => Promise.reject(new Error('Connection is closed.'));
		await clock.tick();
		conn.silent = true;
		await clock.tick();
		await clock.fire();
		expect(conn.sockets.filter((s) => s.destroyed)).toHaveLength(2);
		expect(sink.ids()).toHaveLength(1);
	});

	it('count nothing for a PING still out while its connection is reconnecting on the same socket object', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		sink = captureRegistered();
		conn.silent = true;
		const socket = conn.stream;
		await clock.tick();
		// ioredis keeps the old stream until the reconnect connects.
		conn.status = 'reconnecting';
		await clock.fire();
		expect(socket.destroyed).toBe(false);
		expect(sink.ids()).toEqual([]);
		// And nothing is left listening on it.
		expect(socket.listenerCount('data')).toBe(0);
	});

	it('close a connection whose handshake is still unanswered on the same socket a round later', async () => {
		const clock = captureTimers();
		const conn = fakeConn();
		const closed = [];
		watchLiveness('cursor', () => [conn], (c) => { closed.push(c.stream); });
		sink = captureRegistered();
		// TCP is up, the server does not answer: ioredis has no time limit here.
		conn.status = 'connect';
		await clock.tick();
		expect(closed).toEqual([]);
		await clock.tick();
		expect(closed).toEqual([conn.stream]);
		expect(sink.records[0].attributes).toEqual({ owner: 'cursor', timeoutMs: 30000 });
		// One that gets through its handshake between two rounds is left alone,
		// and so is a handshake started again on a new socket.
		closed.length = 0;
		await clock.tick();
		conn.status = 'ready';
		await clock.tick();
		conn.status = 'connect';
		await clock.tick();
		conn.reconnect();
		conn.status = 'connect';
		await clock.tick();
		expect(closed).toEqual([]);
	});

	it('hold owner calls during a stuck handshake and send only the latest intent when ready', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		sink = captureRegistered();
		// Held during the handshake instead of writing ahead of older queues.
		conn.status = 'connect';
		conn.gate = new Promise(() => {});
		const a = conn.subscribe('a');
		const b = conn.subscribe('b');
		const undoA = conn.unsubscribe('a');
		conn.gate = null;
		await clock.tick();
		expect(conn.sockets[0].destroyed).toBe(false);
		await clock.tick();
		expect(conn.sockets[0].destroyed).toBe(true);
		expect(sink.records[0].attributes).toEqual({ owner: 'pubsub', timeoutMs: 30000 });
		// Ready again: sent in order, for the channels a later call did
		// not decide.
		await Promise.all([a, b, undoA]);
		expect(conn.calls).toEqual([['subscribe', 'b'], ['unsubscribe', 'a']]);
		expect(conn.server.subscribe).toEqual(new Set(['b']));
		// Held from then on.
		conn.reconnect();
		expect(conn.server.subscribe).toEqual(new Set(['b']));
	});

	it('hold a call through the first handshake until a ready connection', async () => {
		const clock = captureTimers();
		const conn = guardSubscriber(fakeConn(), 'pubsub');
		conn.status = 'connect';
		conn.gate = new Promise(() => {});
		const a = conn.subscribe('a');
		conn.gate = null;
		await clock.tick();
		await clock.tick();
		expect(conn.sockets[0].destroyed).toBe(true);
		await a;
		expect(conn.calls).toEqual([['subscribe', 'a']]);
		expect(conn.server.subscribe).toEqual(new Set(['a']));
	});

	it('skip a held handshake call for a channel a later owner call decided', async () => {
		const conn = connected('pubsub');
		conn.status = 'connect';
		conn.gate = new Promise(() => {});
		const a = conn.subscribe('a');
		conn.gate = null;
		// The socket closes; while reconnecting the owner releases the channel,
		// which ioredis sends from its offline queue before the 'ready' event.
		conn.status = 'reconnecting';
		const undo = conn.unsubscribe('a');
		conn.reconnect();
		await Promise.all([a, undo]);
		expect(conn.calls).toEqual([['unsubscribe', 'a']]);
		expect(conn.server.subscribe).toEqual(new Set());
	});

	it('flush a held unsubscribe-all for channels the owner held', async () => {
		const conn = connected('pubsub');
		await conn.subscribe('a');
		// ioredis sends again on the next ready what a ready connection left
		// unanswered, a restore of 'a' for example: the lost unsubscribe must
		// name it.
		conn.status = 'connect';
		conn.gate = new Promise(() => {});
		const undo = conn.unsubscribe();
		conn.gate = null;
		conn.reconnect();
		await undo;
		expect(conn.calls.slice(-1)).toEqual([['unsubscribe', 'a']]);
	});

	it('wait for the reply of a held call after the handshake completes', async () => {
		const conn = connected('pubsub');
		conn.status = 'connect';
		let answer;
		conn.gate = new Promise((resolve) => { answer = resolve; });
		const a = conn.subscribe('a');
		conn.gate = null;
		conn.status = 'ready';
		conn.emit('ready');
		answer();
		await a;
		expect(conn.calls).toEqual([['subscribe', 'a']]);
		conn.reconnect();
		expect(conn.server.subscribe).toEqual(new Set(['a']));
	});

	it('reject a held handshake call when the connection ends or disconnects', async () => {
		const conn = connected('pubsub');
		conn.status = 'connect';
		conn.gate = new Promise(() => {});
		const a = conn.subscribe('a');
		conn.gate = null;
		conn.status = 'end';
		conn.emit('end');
		await expect(a).rejects.toThrow('Connection is closed.');

		// ioredis emits no 'end' for a connection closed between two attempts.
		const other = fakeConn();
		other.disconnect = () => {};
		guardSubscriber(other, 'pubsub');
		other.emit('ready');
		other.status = 'connect';
		other.gate = new Promise(() => {});
		const b = other.subscribe('b');
		other.gate = null;
		other.status = 'reconnecting';
		other.disconnect();
		await expect(b).rejects.toThrow('Connection is closed.');
	});

	it('close a standalone subscriber stuck in its handshake while a call waits for it in the offline queue', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		sink = captureRegistered();
		// Sent while reconnecting: ioredis queues it and sends it once ready,
		// so closing the socket loses nothing.
		conn.status = 'reconnecting';
		conn.gate = new Promise(() => {});
		conn.subscribe('a');
		conn.status = 'connect';
		await clock.tick();
		expect(conn.sockets[0].destroyed).toBe(false);
		await clock.tick();
		expect(conn.sockets[0].destroyed).toBe(true);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
	});

	it('keep a streak per master when the watched connections name one', async () => {
		const clock = captureTimers();
		const a = fakeConn();
		const b = fakeConn();
		const closed = [];
		const names = new Map([[a, 'id-a'], [b, 'id-b']]);
		watchLiveness('sharded-pubsub', () => [a, b], (conn) => { closed.push(names.get(conn)); }, { identify: (conn) => names.get(conn) });
		sink = captureRegistered();
		a.silent = true;
		b.silent = true;
		await clock.tick();
		await clock.fire();
		expect(closed).toEqual(['id-a', 'id-b']);
		expect(sink.records.map((r) => r.attributes)).toEqual([
			{ owner: 'sharded-pubsub', master: 'id-a', timeoutMs: 10000 },
			{ owner: 'sharded-pubsub', master: 'id-b', timeoutMs: 10000 }
		]);
		// b answering ends b's streak, not a's.
		b.silent = false;
		await clock.tick();
		b.silent = true;
		await clock.tick();
		await clock.fire();
		expect(sink.records.map((r) => r.attributes.master)).toEqual(['id-a', 'id-b', 'id-b']);
		expect(closed).toEqual(['id-a', 'id-b', 'id-a', 'id-b']);
	});

	it('stop asking once the owner quits or disconnects the connection, and keep asking through a reconnect', async () => {
		const clock = captureTimers();
		for (const close of ['quit', 'disconnect']) {
			const conn = fakeConn();
			conn.quit = async () => 'OK';
			conn.disconnect = () => {};
			guardSubscriber(conn, 'pubsub');
			conn.emit('ready');
			expect(clock.running()).toHaveLength(1);
			await conn[close]();
			expect(clock.running()).toEqual([]);
			clock.intervals.length = 0;
		}
		// What ioredis does itself on an error or a reconnect.
		const conn = fakeConn();
		conn.disconnect = () => {};
		guardSubscriber(conn, 'pubsub');
		conn.emit('ready');
		conn.disconnect(true);
		expect(clock.running()).toHaveLength(1);
	});

	it('close a connection its QUIT has not closed within 10 seconds, as on a host that died', async () => {
		const clock = captureTimers();
		const conn = fakeConn();
		const disconnects = [];
		conn.quit = () => new Promise(() => {});
		conn.disconnect = (reconnect) => { disconnects.push(reconnect); };
		// Closed on purpose, as ioredis does after a QUIT: no reconnect.
		conn.socketClosed = () => { conn.status = 'end'; conn.emit('end'); };
		guardSubscriber(conn, 'pubsub');
		conn.emit('ready');
		conn.quit();
		expect(clock.pending().map((t) => t.ms)).toEqual([10000]);
		await clock.fire();
		expect(disconnects).toEqual([undefined]);
		expect(conn.sockets[0].destroyed).toBe(true);
		expect(conn.status).toBe('end');

		// One ioredis queued after all (its socket was already closing) and that
		// is between two attempts when the time is up: closing it now would
		// leave it neither reconnecting nor ended.
		const queued = fakeConn();
		const queuedDisconnects = [];
		queued.quit = () => new Promise(() => {});
		queued.disconnect = (reconnect) => { queuedDisconnects.push(reconnect); };
		guardSubscriber(queued, 'pubsub');
		queued.emit('ready');
		queued.quit();
		queued.status = 'reconnecting';
		await clock.fire();
		expect(queuedDisconnects).toEqual([]);

		// One sent during a handshake the server does not answer: the same.
		const stuck = fakeConn();
		const stuckDisconnects = [];
		stuck.quit = () => new Promise(() => {});
		stuck.disconnect = (reconnect) => { stuckDisconnects.push(reconnect); };
		stuck.socketClosed = () => { stuck.status = 'end'; stuck.emit('end'); };
		guardSubscriber(stuck, 'pubsub');
		stuck.emit('ready');
		stuck.status = 'connect';
		stuck.quit();
		await clock.fire();
		expect(stuckDisconnects).toEqual([undefined]);
		expect(stuck.status).toBe('end');

		// One sent between two attempts waits in ioredis's offline queue for the
		// next connection, and closing the connection now would leave it neither
		// reconnecting nor ended: no time limit, and the PINGs go on until it
		// ends, as they would close a stuck handshake on the next socket.
		const waiting = fakeConn();
		const waitingDisconnects = [];
		waiting.quit = () => new Promise(() => {});
		waiting.disconnect = (reconnect) => { waitingDisconnects.push(reconnect); };
		guardSubscriber(waiting, 'pubsub');
		waiting.emit('ready');
		waiting.status = 'reconnecting';
		waiting.quit();
		expect(clock.pending()).toEqual([]);
		expect(clock.running()).toHaveLength(1);
		await clock.fire();
		expect(waitingDisconnects).toEqual([]);
		waiting.status = 'end';
		waiting.emit('end');
		expect(clock.running()).toEqual([]);

		// One its QUIT closed is left alone.
		const done = fakeConn();
		done.quit = async () => { done.status = 'end'; done.emit('end'); return 'OK'; };
		const doneDisconnect = vi.fn();
		done.disconnect = doneDisconnect;
		guardSubscriber(done, 'pubsub');
		done.emit('ready');
		await done.quit();
		expect(clock.pending()).toEqual([]);
		expect(doneDisconnect).not.toHaveBeenCalled();
		expect(done.sockets[0].destroyed).toBe(false);
	});

	it('stop asking once the connection ends', async () => {
		const clock = captureTimers();
		const conn = connected('pubsub');
		conn.silent = true;
		await clock.tick();
		conn.status = 'end';
		conn.emit('end');
		expect(clock.running()).toEqual([]);
		// A PING out when it ended does not close anything.
		await clock.fire();
		expect(conn.sockets[0].destroyed).toBe(false);
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
	// As ioredis: the slot map is read from one known node after another, and
	// 'refresh' is emitted before the callback. While `unreadable` is set no
	// node answers, which ioredis reports as a ClusterAllFailedError.
	cluster.refreshes = 0;
	cluster.unreadable = false;
	cluster.refreshSlotsCache = (callback) => {
		cluster.refreshes++;
		queueMicrotask(() => {
			if (cluster.unreadable) {
				// Told by its message, as ioredis does: a bundler can rename the class.
				callback?.(new Error('Failed to refresh slots cache.'));
				return;
			}
			cluster.emit('refresh');
			callback?.();
		});
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
			// ioredis gives node connections no retry strategy.
			conn.socketClosed = () => conn.end();
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
		const fake = fakeCluster();
		// More nodes than connections fail here, so every one goes to a node
		// that has not failed yet.
		fake.nodeList = Array.from({ length: 30 }, (_, i) => fake.node(7000 + i));
		const cluster = guardSubscriber(fake, 'pubsub');
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
		const fake = fakeCluster();
		fake.nodeList = [fake.node(7000), fake.node(7001), fake.node(7002)];
		const cluster = guardSubscriber(fake, 'pubsub');
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
		// Closed at once, as ioredis stops its own subscriber, and its socket
		// destroyed, which a host that no longer answers would never close.
		expect(conn.disconnects).toBe(1);
		expect(conn.sockets[0].destroyed).toBe(true);
		expect(clock.running()).toEqual([]);
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

describe('Cluster subscribers on nodes that go silent, leave or cannot be reached', () => {
	it('move the channels off a node connection that stops answering, to a node that has not failed', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const cluster = guardSubscriber(fakeCluster(), 'cursor');
		await cluster.subscribe('a');
		const first = cluster.opened[0];
		expect(first.options.port).toBe(7000);
		sink = captureRegistered();
		first.silent = true;
		await clock.tick();
		await clock.fire();
		expect(first.status).toBe('end');
		expect(first.sockets[0].destroyed).toBe(true);
		// The RNG names the first node again, but the silent one is not dialled
		// while another has not failed.
		expect(cluster.opened.map((c) => c.options.port)).toEqual([7000, 7001]);
		expect(cluster.opened[1].server.subscribe).toEqual(new Set(['a']));
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
		expect(sink.records[0].attributes).toEqual({ owner: 'cursor', timeoutMs: 10000 });
	});

	it('read the slot map every other round while a node connection is open, and move off a node it no longer lists', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await clock.tick();
		await clock.tick();
		// Nothing is open, so there is nothing to check.
		expect(cluster.refreshes).toBe(0);
		await cluster.subscribe('a');
		await clock.tick();
		await clock.tick();
		expect(cluster.refreshes).toBe(1);
		expect(cluster.opened).toHaveLength(1);
		// Removed from the cluster: the node still answers, but the next read
		// no longer lists it.
		cluster.nodeList = cluster.nodeList.slice(1);
		await clock.tick();
		await clock.tick();
		expect(cluster.refreshes).toBe(2);
		expect(cluster.opened[0].status).toBe('end');
		// Destroyed, so a node that left because it died closes all the same.
		expect(cluster.opened[0].sockets[0].destroyed).toBe(true);
		expect(cluster.opened[1].options.port).toBe(7001);
		expect(cluster.opened[1].server.subscribe).toEqual(new Set(['a']));
	});

	it('replace a node connection still in its handshake a round later', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const fake = fakeCluster();
		fake.autoConnect = false;
		const cluster = guardSubscriber(fake, 'pubsub');
		const subscribed = cluster.subscribe('a');
		await flush();
		const stuck = cluster.opened[0];
		stuck.status = 'connect';
		sink = captureRegistered();
		await clock.tick();
		expect(stuck.status).toBe('connect');
		await clock.tick();
		expect(stuck.status).toBe('end');
		expect(sink.records[0].attributes).toEqual({ owner: 'pubsub', timeoutMs: 30000 });
		// The call moves to a node that has not failed.
		cluster.autoConnect = true;
		await clock.fire();
		cluster.opened[1].connect();
		await subscribed;
		expect(cluster.opened[1].options.port).toBe(7001);
		expect(cluster.opened[1].server.subscribe).toEqual(new Set(['a']));
	});

	it('take a reconnect its own slot map read set off for forced: a slotless node answered it', async () => {
		const clock = captureTimers();
		const fake = fakeCluster();
		fake.options.clusterRetryStrategy = (times) => (times > 2 ? null : 100);
		const cluster = guardSubscriber(fake, 'pubsub');
		const strategy = (times) => cluster.options.clusterRetryStrategy.call(cluster, times);
		await cluster.subscribe('a');
		// A reconnect that starts while the read is out is none of its doing,
		// and a read that finds the slot map as it was leaves the strategy alone.
		let answer = () => {};
		cluster.refreshSlotsCache = (callback) => {
			cluster.refreshes++;
			answer = () => { cluster.emit('refresh'); callback(); };
		};
		await clock.tick();
		await clock.tick();
		expect(cluster.refreshes).toBe(1);
		expect(strategy(3)).toBe(null);
		answer();
		await flush();
		expect(strategy(3)).toBe(null);
		// One a reset node answers with an empty map: ioredis drops every node
		// before the callback, and the Cluster reconnects once the first of them
		// has closed, which can take longer than a turn of the event loop.
		const emptyMap = (callback) => {
			cluster.refreshes++;
			queueMicrotask(() => {
				cluster.nodeList = [];
				cluster.emit('refresh');
				callback();
			});
		};
		cluster.refreshSlotsCache = emptyMap;
		await clock.tick();
		await clock.tick();
		for (let i = 0; i < 3; i++) await flush();
		expect(cluster.refreshes).toBe(2);
		cluster.status = 'reconnecting';
		expect(strategy(3)).toBe(300);
		cluster.status = 'ready';
		cluster.emit('ready');
		expect(strategy(3)).toBe(null);
		// Closed before that reconnect: any reconnect of the Cluster afterwards
		// is the strategy's alone.
		await clock.tick();
		await clock.tick();
		await flush();
		expect(cluster.refreshes).toBe(3);
		expect(strategy(3)).toBe(300);
		await cluster.quit();
		expect(strategy(3)).toBe(null);
	});

	it('leave a Cluster already reconnecting to that reconnect when no known node answers the slot map read', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		cluster.autoConnect = false;
		// ioredis is reconnecting the Cluster by the time the read fails.
		cluster.refreshSlotsCache = (callback) => {
			cluster.refreshes++;
			queueMicrotask(() => {
				cluster.status = 'connecting';
				callback(new Error('Failed to refresh slots cache.'));
			});
		};
		cluster.opened[0].end();
		await flush();
		cluster.opened[1].end();
		await flush();
		await clock.fire();
		cluster.opened[2].end();
		await flush();
		await clock.fire();
		expect(cluster.refreshes).toBe(1);
		expect(cluster.disconnects).toEqual([]);
		// Its 'ready' leads to the next connection.
		cluster.autoConnect = true;
		cluster.status = 'ready';
		cluster.emit('ready');
		await clock.fire();
		expect(cluster.opened).toHaveLength(4);
		expect(cluster.opened[3].server.subscribe).toEqual(new Set(['a']));
	});

	it('keep the node connection through a slot map that lists no node', async () => {
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		// What a removed node, reset and slotless, answers for the slot map.
		cluster.nodeList = [];
		cluster.emit('refresh');
		await flush();
		expect(cluster.opened[0].status).toBe('ready');
		expect(cluster.opened).toHaveLength(1);
	});

	it('report no unreachable cluster when the one node it knows only went silent, and open it again', async () => {
		const clock = captureTimers();
		const fake = fakeCluster();
		fake.nodeList = [fake.node(7000)];
		const cluster = guardSubscriber(fake, 'pubsub');
		await cluster.subscribe('a');
		sink = captureRegistered();
		cluster.opened[0].silent = true;
		await clock.tick();
		await clock.fire();
		// Silent, so it is dialled again after the slot map is read again.
		expect(cluster.refreshes).toBe(1);
		expect(cluster.opened).toHaveLength(2);
		expect(cluster.opened[1].server.subscribe).toEqual(new Set(['a']));
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
	});

	it('leave the slot map to a Cluster that reads it at least once a minute itself', async () => {
		const clock = captureTimers();
		const often = fakeCluster();
		often.options.slotsRefreshInterval = 60000;
		guardSubscriber(often, 'pubsub');
		await often.subscribe('a');
		const rarely = fakeCluster();
		rarely.options.slotsRefreshInterval = 120000;
		guardSubscriber(rarely, 'pubsub');
		await rarely.subscribe('a');
		for (let i = 0; i < 4; i++) await clock.tick();
		expect(often.refreshes).toBe(0);
		expect(rarely.refreshes).toBe(2);
	});

	it('dial a failed node again only after every other node failed, then report it once per streak and read the slot map again', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const cluster = guardSubscriber(fakeCluster(), 'groups');
		await cluster.subscribe('a');
		sink = captureRegistered();
		cluster.loginRefused = true;
		const step = async () => {
			await clock.fire();
			await flush();
		};
		cluster.opened[0].end();
		await flush();
		await flush();
		expect(cluster.opened.map((c) => c.options.port)).toEqual([7000, 7000]);
		await step();
		// The node that has not failed, whatever the RNG names.
		expect(cluster.opened.map((c) => c.options.port)).toEqual([7000, 7000, 7001]);
		expect(cluster.refreshes).toBe(0);
		expect(sink.ids()).toEqual([]);

		// Both failed: the slot map is read before either is dialled again.
		await step();
		expect(cluster.opened).toHaveLength(3);
		expect(cluster.refreshes).toBe(1);
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_UNREACHABLE]);
		expect(sink.records[0].attributes).toMatchObject({
			owner: 'groups',
			nodes: 2,
			error: { message: expect.stringContaining('WRONGPASS') }
		});
		// Then each is tried again after the delay the failures earned.
		expect(clock.pending().map((t) => t.ms)).toEqual([200]);
		await step();
		await step();
		await step();
		expect(cluster.opened).toHaveLength(5);
		expect(cluster.refreshes).toBe(2);
		expect(sink.ids()).toHaveLength(1);

		cluster.loginRefused = false;
		await step();
		expect(cluster.opened).toHaveLength(6);
		expect(cluster.opened[5].server.subscribe).toEqual(new Set(['a']));
		// A connection that works ends the streak, so the next is reported.
		cluster.loginRefused = true;
		cluster.opened[5].end();
		await flush();
		await flush();
		await step();
		await step();
		expect(sink.ids()).toEqual([
			EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_UNREACHABLE,
			EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_UNREACHABLE
		]);
	});

	it('forget the nodes a streak failed on once a connection works', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const fake = fakeCluster();
		fake.nodeList = [fake.node(7000), fake.node(7001), fake.node(7002)];
		const cluster = guardSubscriber(fake, 'pubsub');
		await cluster.subscribe('a');
		cluster.autoConnect = false;
		cluster.opened[0].end();
		await flush();
		cluster.opened[1].end();
		await flush();
		cluster.autoConnect = true;
		await clock.fire();
		expect(cluster.opened.map((c) => c.options.port)).toEqual([7000, 7000, 7001]);
		expect(cluster.opened[2].server.subscribe).toEqual(new Set(['a']));
		// 7001 worked, so 7000 is a candidate again, and the RNG names it.
		cluster.opened[2].end();
		await flush();
		expect(cluster.opened[3].options.port).toBe(7000);
	});

	it('reconnect the Cluster through its startup nodes when no node it knows answers the slot map read', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		sink = captureRegistered();
		// Every node moved to a new address: the known ones cannot be reached.
		cluster.autoConnect = false;
		cluster.unreadable = true;
		cluster.opened[0].end();
		await flush();
		cluster.opened[1].end();
		await flush();
		await clock.fire();
		cluster.opened[2].end();
		await flush();
		await clock.fire();
		expect(cluster.refreshes).toBe(1);
		expect(cluster.disconnects).toEqual([true]);
		expect(cluster.opened).toHaveLength(3);

		// The reconnect reads the slot map from the startup nodes, which name the
		// new addresses, and its 'ready' opens a connection there.
		cluster.autoConnect = true;
		cluster.unreadable = false;
		cluster.nodeList = [cluster.node(8000), cluster.node(8001)];
		cluster.status = 'ready';
		cluster.emit('ready');
		await flush();
		expect(cluster.opened.map((c) => c.options.port)).toEqual([7000, 7000, 7001, 8000]);
		expect(cluster.opened[3].server.subscribe).toEqual(new Set(['a']));
	});

	it('space the reconnects of the Cluster it forced, stop none of them, and leave every other reconnect to the strategy', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const fake = fakeCluster();
		const seen = [];
		// ioredis's default, except that it gives up after two attempts.
		fake.options.clusterRetryStrategy = function (times, reason) {
			seen.push([this, times, reason]);
			return times > 2 ? null : Math.min(100 + times * 2, 2000);
		};
		const cluster = guardSubscriber(fake, 'pubsub');
		const strategy = (times) => cluster.options.clusterRetryStrategy.call(cluster, times, 'why');
		// Not forced: the strategy's own answer, giving up included.
		expect([strategy(1), strategy(3)]).toEqual([102, null]);
		expect(seen[0]).toEqual([cluster, 1, 'why']);

		// No node it knows can be opened, and none answers the slot map read.
		sink = captureRegistered();
		await cluster.subscribe('a');
		cluster.autoConnect = false;
		cluster.unreadable = true;
		cluster.opened[0].end();
		await flush();
		cluster.opened[1].end();
		await flush();
		await clock.fire();
		cluster.opened[2].end();
		await flush();
		await clock.fire();
		expect(cluster.disconnects).toEqual([true]);
		// Forced: spaced, the longer delay winning, and never given up.
		expect([1, 2, 5, 20, 40].map(strategy)).toEqual([102, 200, 500, 2000, 2000]);

		// Ready again: the strategy alone.
		cluster.status = 'ready';
		cluster.emit('ready');
		expect(strategy(3)).toBe(null);
	});

	it('open nothing while the slot map is read again, and nothing after a quit meanwhile', async () => {
		const clock = captureTimers({ rng: { float: () => 0 } });
		const cluster = guardSubscriber(fakeCluster(), 'pubsub');
		await cluster.subscribe('a');
		sink = captureRegistered();
		cluster.autoConnect = false;
		cluster.opened[0].end();
		await flush();
		cluster.opened[1].end();
		await flush();
		await clock.fire();
		cluster.opened[2].end();
		await flush();
		let finish;
		cluster.refreshSlotsCache = (callback) => {
			cluster.refreshes++;
			finish = callback;
		};
		await clock.fire();
		expect(cluster.refreshes).toBe(1);
		// A topology event during the read opens nothing.
		cluster.emit('+node', cluster.nodeList[0]);
		await flush();
		expect(cluster.opened).toHaveLength(3);
		// No node answered it, but the Cluster was quit meanwhile: it is not
		// reconnected.
		await cluster.quit();
		const err = new Error('Failed to refresh slots cache.');
		Object.defineProperty(err, 'name', { value: 'ClusterAllFailedError' });
		finish(err);
		await flush();
		expect(cluster.disconnects).toEqual([]);
		expect(cluster.opened).toHaveLength(3);
		expect(clock.pending()).toEqual([]);
	});
});


describe('handshake owner command ordering', () => {
	it.each([['subscribe', 'unsubscribe'], ['psubscribe', 'punsubscribe'], ['ssubscribe', 'sunsubscribe']])('holds %s behind older offline commands', async (subscribe, unsubscribe) => {
		const raw = fakeConn();
		const original = raw[subscribe];
		let flushOffline;
		raw[subscribe] = (...args) => raw.status === 'connecting'
			? new Promise((resolve, reject) => { flushOffline = () => original(...args).then(resolve, reject); })
			: original(...args);
		const conn = guardSubscriber(raw, 'pubsub');
		conn.emit('ready');
		conn.status = 'connecting';
		const first = conn[subscribe]('x');
		conn.status = 'connect';
		const callback = vi.fn();
		const undo = conn[unsubscribe]('x', callback);
		expect(conn.calls).toEqual([]);
		conn.status = 'ready';
		flushOffline();
		conn.emit('ready');
		await Promise.all([first, undo]);
		expect(conn.server[subscribe]).toEqual(new Set());
		expect(callback).toHaveBeenCalledExactlyOnceWith(null, 0);
		conn.reconnect();
		expect(conn.server[subscribe]).toEqual(new Set());
		conn.emit('end');
	});

	it.each(['error', 'close', 'quit'])('settles held promises and callbacks when a handshake fails via %s', async (event) => {
		const raw = fakeConn();
		raw.quit = async () => 'OK';
		const conn = guardSubscriber(raw, 'pubsub');
		conn.emit('ready');
		conn.status = 'connect';
		const callback = vi.fn();
		const waiting = conn.subscribe('x', callback);
		const rejected = expect(waiting).rejects.toThrow(event === 'error' ? 'WRONGPASS' : 'Connection is closed');
		if (event === 'quit') await conn.quit();
		else conn.emit(event, new Error('WRONGPASS'));
		await rejected;
		expect(callback).toHaveBeenCalledTimes(1);
		expect(callback.mock.calls[0][0]).toBeInstanceOf(Error);
		conn.reconnect();
		await flush();
		expect(conn.calls).toEqual([]);
		conn.emit('end');
	});

	it('holds unsubscribe-all after a previously unanswered subscribe is resent', async () => {
		const conn = connected();
		await conn.subscribe('x');
		conn.status = 'connect';
		const undo = conn.unsubscribe();
		expect(conn.calls).toEqual([['subscribe', 'x']]);
		// Model ioredis resending the unanswered older subscribe before ready.
		conn.server.subscribe.add('x');
		conn.status = 'ready';
		conn.emit('ready');
		await undo;
		expect(conn.server.subscribe).toEqual(new Set());
		conn.emit('end');
	});
});
