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

	it('leave a Cluster untouched, whose subscriber re-subscribes on its own', () => {
		const conn = fakeConn();
		conn.nodes = () => [];
		const subscribe = conn.subscribe;
		expect(guardSubscriber(conn, 'pubsub')).toBe(conn);
		expect(conn.options.autoResubscribe).toBe(true);
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
