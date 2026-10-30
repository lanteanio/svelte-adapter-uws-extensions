/**
 * Redis client factory for svelte-adapter-uws-extensions.
 *
 * Wraps ioredis with lifecycle management and graceful shutdown
 * via the SvelteKit `sveltekit:shutdown` event.
 *
 * @module svelte-adapter-uws-extensions/redis
 */

import { registerClusterHealth, redisConnectionState } from '../shared/cluster-dashboard.js';
import { closeConnection, endOnDisconnectBetweenAttempts } from '../shared/redis-subscriber.js';
import Redis from 'ioredis';
import { ConnectionError, TimeoutError } from '../shared/errors.js';
import { MAX_REDIS_DUPLICATES_PER_CLIENT, MAX_REDIS_QUEUED_COMMANDS } from '../shared/caps.js';
import { monotonicNow, setTimer, clearTimer } from '../shared/runtime.js';
import { redactConnectionUrl } from '../shared/sensitive.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure } from '../shared/diagnostic.js';
import { defineOptions, option } from '../shared/options.js';

// Verify the private completion seam once per connection. If ioredis changes
// it, keep its native per-command timers instead of risking lost deadlines.
function supportsSharedDeadlines() {
	const Command = Redis.Command;
	if (typeof Command?.prototype?._clearTimers !== 'function' || typeof Command?.prototype?.setTimeout !== 'function') return false;
	try {
		for (const settle of ['resolve', 'reject']) {
			const command = new Command('ping');
			command.promise.catch(() => {});
			let cleared = false;
			command._clearTimers = () => { cleared = true; };
			command[settle](undefined);
			if (!cleared) return false;
		}
		return true;
	} catch { return false; }
}

// Before ready ioredis writes its own handshake commands and any user command
// of the same name (INFO, SELECT, CLIENT, AUTH). Only the handshake's own
// commands may stay unsettled when that connection drops, so they are told
// apart by when they are sent, never by name. The connect handler resets the
// command queue and then writes AUTH, SELECT, CLIENT SETNAME and CLIENT
// SETINFO LIB-NAME synchronously, before the 'connect' event that ioredis
// emits on the next tick, so no application code runs in that window. The
// window closes when that event is emitted, ahead of every listener,
// including ones an application prepends later. The ready check writes INFO
// synchronously from `_readyCheck`. Its CLIENT SETINFO LIB-VER waits for a file read and can
// follow application commands, so it is not tracked; ioredis ignores its
// outcome. Rejecting it on a pre-ready drop can complete the handshake's
// CLIENT commands and start that connection's ready check after it closed,
// and that check would refuse INFO and fail the client, so a ready check only
// runs while a connection is in its handshake. The sending hook is installed
// when a connection starts its handshake and removed at ready, so commands on
// a ready connection pay nothing for it. Call this before installing a ready
// listener that replaces `sendCommand`. Returns null when those private seams
// are missing, so callers fall back to the command name alone.
function trackHandshake(redis) {
	const reset = redis.resetCommandQueue;
	const readyCheck = redis._readyCheck;
	const setStatus = redis.setStatus;
	if (typeof reset !== 'function' || typeof readyCheck !== 'function' || typeof setStatus !== 'function') return null;
	const own = new WeakSet();
	let inConnect = false;
	let inReadyCheck = false;
	let base = null;
	function sendNoting(command, stream) {
		if (redis.status === 'connect' && (inConnect || (inReadyCheck && command.name === 'info'))) own.add(command);
		return base.call(this, command, stream);
	}
	redis.resetCommandQueue = function () {
		const result = reset.apply(this, arguments);
		inConnect = this.status === 'connect';
		if (this.sendCommand !== sendNoting) {
			base = this.sendCommand;
			this.sendCommand = sendNoting;
		}
		return result;
	};
	// setStatus binds `this.emit` when it queues the status event, so the
	// 'connect' event emitted through this wrapper closes the window before
	// any listener runs. Other events and later emits use `emit` unchanged.
	redis.setStatus = function (status) {
		if (status !== 'connect') return setStatus.apply(this, arguments);
		const hadOwn = Object.hasOwn(this, 'emit');
		const emit = this.emit;
		this.emit = function () {
			inConnect = false;
			return emit.apply(this, arguments);
		};
		try { return setStatus.apply(this, arguments); } finally {
			if (hadOwn) this.emit = emit;
			else delete this.emit;
		}
	};
	redis._readyCheck = function () {
		// Covers the loading retry too, which calls back in here on a timer.
		if (this.status !== 'connect') return;
		inReadyCheck = true;
		try { return readyCheck.apply(this, arguments); } finally { inReadyCheck = false; }
	};
	redis.on('ready', () => {
		if (redis.sendCommand === sendNoting) redis.sendCommand = base;
	});
	return { has(command) { return own.has(command); } };
}

// ioredis rejects timed-out commands without removing them from its offline
// queue. Unsent pipelines must be cancelled as a whole: ioredis only writes
// their buffered bytes after every member has passed through sendCommand.
function boundOfflineQueue(redis, keepOfflineQueue) {
	const rejectedPipelines = new WeakMap();
	const send = redis.sendCommand;
	if (typeof send !== 'function') return;
	// One timer per connection, including pipelines. Native per-command timers
	// materially slow large pipelines; their completion hook removes entries
	// from this scheduler without changing command promises or reply ordering.
	const pending = new Map();
	const sharedDeadlines = supportsSharedDeadlines();
	const nativeClear = Redis.Command?.prototype?._clearTimers;
	const nativeTimeout = Redis.Command?.prototype?.setTimeout;
	let timer = null;
	let due = Infinity;
	function arm(at) {
		if (at >= due) return;
		if (timer !== null) clearTimer(timer);
		due = at;
		timer = setTimer(expire, Math.max(0, at - monotonicNow()));
	}
	function retireConnection(error) {
		// A reply timeout cannot free one protocol slot safely. Retire the socket
		// and its whole queue before reconnecting, so rejected writes and WAIT
		// never re-enter ioredis's unanswered-command resend path.
		redis.flushQueue(error);
		const previous = redis.prevCommandQueue;
		if (previous && previous !== redis.commandQueue) {
			let entry;
			while ((entry = previous.shift())) entry.command.reject(error);
			redis.prevCommandQueue = null;
		}
		// A reconnect delay already has a retry scheduled and no live socket
		// to retire. Preserve that native retry and only close live connections.
		if (redis.status !== 'reconnecting' && redis.stream && !redis.stream.destroyed) redis.disconnect(true);
	}
	function wasSent(command) {
		for (const queue of [redis.commandQueue, redis.prevCommandQueue]) {
			if (!queue) continue;
			for (let i = 0; i < queue.length; i++) if (queue.peekAt(i).command === command) return true;
		}
		return false;
	}
	// Status events are emitted in order, so this tracks whether the
	// connection that just closed had reached ready. Before ready ioredis
	// writes handshake commands (AUTH, SELECT, the INFO ready check) and any
	// command flagged `loading` (PUBLISH, SUBSCRIBE, MULTI, ...); everything
	// else waits unsent in the offline queue. Rejecting a handshake command
	// makes ioredis treat the handshake as fatal and flush that offline queue,
	// so ioredis's own handshake commands stay with its retry. The other sent
	// commands, user commands named like handshake ones included, are
	// uncertain, and ioredis neither resends them nor keeps them once the next
	// connection starts, so they are rejected here even when unanswered-command
	// resend is enabled; otherwise they never settle.
	const Command = Redis.Command;
	const handshake = trackHandshake(redis);
	let connectionReady = false;
	redis.on('ready', () => { connectionReady = true; });
	redis.on('close', () => {
		const wasReady = connectionReady;
		connectionReady = false;
		const error = new ConnectionError('redis', 'connection closed before the command reply; execution is uncertain');
		if (!wasReady) {
			const queue = redis.commandQueue;
			for (let i = queue.length - 1; i >= 0; i--) {
				const entry = queue.peekAt(i);
				if (handshake ? handshake.has(entry.command) : Command?.checkFlag('HANDSHAKE_COMMANDS', entry.command.name)) continue;
				queue.remove(i, 1);
				entry.command.reject(error);
			}
			return;
		}
		if (redis.options.autoResendUnfulfilledCommands) return;
		const previous = redis.prevCommandQueue;
		redis.flushQueue(error, { offlineQueue: false });
		if (previous && previous !== redis.commandQueue) {
			let entry;
			while ((entry = previous.shift())) entry.command.reject(error);
		}
		redis.prevCommandQueue = null;
	});
	function expire() {
		timer = null;
		due = Infinity;
		const now = monotonicNow();
		for (const [command, deadline] of pending) {
			if (deadline > now) continue;
			const error = new TimeoutError('redis', 'command', redis.options.commandTimeout);
			if (wasSent(command)) retireConnection(error);
			else command.reject(error);
		}
		let next = Infinity;
		for (const deadline of pending.values()) next = Math.min(next, deadline);
		if (next !== Infinity) arm(next);
	}

	function setDeadline(ms) {
		if (!Number.isFinite(ms)) return nativeTimeout.call(this, ms);
		if (pending.has(this)) return;
		const deadline = monotonicNow() + ms;
		pending.set(this, deadline);
		arm(deadline);
	}
	function clearDeadline() {
		pending.delete(this);
		if (pending.size === 0 && timer !== null) {
			clearTimer(timer);
			timer = null;
			due = Infinity;
		}
		nativeClear.call(this);
	}
	function sendTimed(command, stream) {
		// A command that brings its own setTimeout keeps it, as the subscriber
		// liveness PING does to opt out of the connection's deadline.
		if (Object.hasOwn(command, 'setTimeout') && command.setTimeout !== setDeadline) return send.call(this, command, stream);
		if (sharedDeadlines && (command._clearTimers === nativeClear || command._clearTimers === clearDeadline)) {
			command.setTimeout = setDeadline;
			command._clearTimers = clearDeadline;
		}
		if (!sharedDeadlines) command.promise.catch(error => {
			if (error?.message === 'Command timed out' && wasSent(command)) retireConnection(error);
		});
		return send.call(this, command, stream);
	}
	if (!keepOfflineQueue) redis.on('ready', () => {
		redis.options.enableOfflineQueue = false;
		// Offline admission no longer needs checking after first ready.
		redis.sendCommand = sendTimed;
	});
	function cancelOffline(command, stream, error) {
		const pipeline = stream?.isPipeline ? stream : null;
		if (pipeline) {
			if (rejectedPipelines.has(pipeline)) return;
			rejectedPipelines.set(pipeline, error);
		}
		const queue = redis.offlineQueue;
		for (let i = queue.length - 1; i >= 0; i--) {
			const entry = queue.peekAt(i);
			if (entry.command !== command && (!pipeline || entry.stream !== pipeline)) continue;
			queue.remove(i, 1);
			entry.command.reject(error);
		}
	}
	redis.sendCommand = function (command, stream) {
		if (stream?.isPipeline && rejectedPipelines.has(stream)) {
			command.reject(rejectedPipelines.get(stream));
			return command.promise;
		}
		const reply = sendTimed.call(this, command, stream);
		const queue = this.offlineQueue;
		if (queue?.peekAt(queue.length - 1)?.command !== command) return reply;
		if (queue.length > MAX_REDIS_QUEUED_COMMANDS) {
			cancelOffline(command, stream, new ConnectionError('redis', 'offline command queue capacity exceeded'));
			return reply;
		}
		command.promise.catch(error => cancelOffline(command, stream, error));
		return reply;
	};
}

/**
 * @typedef {Object} RedisClientOptions
 * @property {string} [url='redis://localhost:6379'] - Redis connection URL
 * @property {string} [keyPrefix=''] - Prefix for all keys written by extensions
 * @property {boolean} [autoShutdown=true] - Listen for `sveltekit:shutdown` and disconnect
 * @property {import('ioredis').RedisOptions} [options] - Override ioredis defaults: one retry, 5s command/connect/socket deadlines, no automatic replay of unanswered commands, bounded startup buffering and no offline buffering after first ready
 */

/**
 * @typedef {Object} RedisClient
 * @property {import('ioredis').Redis} redis - The underlying ioredis instance
 * @property {string} keyPrefix - The key prefix
 * @property {(key: string) => string} key - Prefix a key: `keyPrefix + key`
 * @property {(overrides?: import('ioredis').RedisOptions) => import('ioredis').Redis} duplicate - Create a new connection with the same config (for subscribers)
 * @property {() => Promise<void>} quit - Gracefully disconnect and remove the `sveltekit:shutdown` listener
 */

const validateOptions = defineOptions({
	factory: 'createRedisClient',
	label: 'redis client',
	types: 'RedisClientOptions in redis/index.d.ts'
}, {
	url: option.string,
	keyPrefix: option.string,
	// Only literal false disables the shutdown hook.
	autoShutdown: option.boolean,
	options: option.object
});

/**
 * Create a Redis client.
 *
 * @param {RedisClientOptions} [options]
 * @returns {RedisClient}
 *
 * @example
 * ```js
 * import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';
 *
 * export const redis = createRedisClient({
 *   url: 'redis://localhost:6379',
 *   keyPrefix: 'myapp:'
 * });
 * ```
 */
export function createRedisClient(options = {}) {
	options = validateOptions(options);
	const url = options.url || 'redis://localhost:6379';
	const keyPrefix = options.keyPrefix || '';
	const autoShutdown = options.autoShutdown !== false;

	/** @type {import('ioredis').RedisOptions} */
	const redisOpts = {
		autoResendUnfulfilledCommands: false,
		maxRetriesPerRequest: 1,
		commandTimeout: 5000,
		connectTimeout: 5000,
		socketTimeout: 5000,
		enableReadyCheck: true,
		lazyConnect: false,
		...(options.options || {})
	};

	let redis;
	try {
		redis = new Redis(url, redisOpts);
	} catch (err) {
		throw new ConnectionError('redis', `failed to create client for ${redactConnectionUrl(url)}`, err);
	}

	boundOfflineQueue(redis, options.options?.enableOfflineQueue === true);
	endOnDisconnectBetweenAttempts(redis);

	// Without a listener ioredis prints its own unindexed "Unhandled error
	// event" line, with the connection error unredacted.
	redis.on('error', (err) => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, { error: err }));

	// Track duplicate connections for cleanup
	/** @type {Set<import('ioredis').Redis>} */
	const duplicates = new Set();
	let duplicatesWarnFired = false;

	/** @type {boolean} */
	let shuttingDown = false;
	const stopDashboard = registerClusterHealth('redis', () => ({
		backend: 'redis', connection: redisConnectionState(redis), duplicates: duplicates.size
	}));

	async function quit() {
		if (shuttingDown) return;
		shuttingDown = true;
		stopDashboard();
		// A client released by a development reload must not keep its listener.
		if (typeof process !== 'undefined') process.removeListener('sveltekit:shutdown', quit);
		const all = [redis, ...duplicates];
		await Promise.allSettled(all.map(async (connection) => {
			// A connection without a ready socket cannot drain through QUIT.
			// Terminal shutdown cancels its next attempt and rejects its queue.
			if (connection.status !== 'ready') {
				closeConnection(connection);
				return;
			}
			// The socket can close after this check but before QUIT is written.
			// Never reconnect just to replay the work ahead of a queued QUIT.
			const stopRetry = () => closeConnection(connection);
			connection.once('reconnecting', stopRetry);
			try { await connection.quit(); }
			catch { closeConnection(connection); }
			finally { connection.removeListener('reconnecting', stopRetry); }
		}));
		duplicates.clear();
	}

	if (autoShutdown && typeof process !== 'undefined') {
		process.once('sveltekit:shutdown', quit);
	}

	return {
		redis,
		keyPrefix,

		key(k) {
			return keyPrefix + k;
		},

		duplicate(overrides) {
			if (shuttingDown) throw new ConnectionError('redis', 'client is shut down');
			// Subscriber connections must buffer their initial/reconnect admission.
			const dup = redis.duplicate({ enableOfflineQueue: true, ...overrides });
			boundOfflineQueue(dup, true);
			endOnDisconnectBetweenAttempts(dup);
			duplicates.add(dup);
			if (duplicates.size >= MAX_REDIS_DUPLICATES_PER_CLIENT && !duplicatesWarnFired) {
				duplicatesWarnFired = true;
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_DUPLICATES, {
					attributes: { duplicates: duplicates.size }
				});
			}
			// Untrack only on 'end' - the TERMINAL edge (manual quit/disconnect
			// or retry exhaustion). 'close' also fires on every transient
			// disconnect and is followed by 'reconnecting'; dropping the dup
			// there would let a reconnecting subscriber outlive quit() with a
			// live socket and retry timer.
			dup.on('end', () => duplicates.delete(dup));
			return dup;
		},

		quit
	};
}
