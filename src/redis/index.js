/**
 * Redis client factory for svelte-adapter-uws-extensions.
 *
 * Wraps ioredis with lifecycle management and graceful shutdown
 * via the SvelteKit `sveltekit:shutdown` event.
 *
 * @module svelte-adapter-uws-extensions/redis
 */

import Redis from 'ioredis';
import { ConnectionError, TimeoutError } from '../shared/errors.js';
import { MAX_REDIS_DUPLICATES_PER_CLIENT, MAX_REDIS_QUEUED_COMMANDS } from '../shared/caps.js';
import { monotonicNow, setTimer, clearTimer } from '../shared/runtime.js';
import { redactConnectionUrl } from '../shared/sensitive.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure } from '../shared/diagnostic.js';

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

// ioredis rejects timed-out commands without removing them from its offline
// queue. Remove only still-unsent entries; sent commands stay in protocol order.
function boundOfflineQueue(redis, keepOfflineQueue) {
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
	function expire() {
		timer = null;
		due = Infinity;
		const now = monotonicNow();
		let next = Infinity;
		for (const [command, deadline] of pending) {
			if (deadline <= now) command.reject(new TimeoutError('redis', 'command', redis.options.commandTimeout));
			else next = Math.min(next, deadline);
		}
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
		if (sharedDeadlines && (command._clearTimers === nativeClear || command._clearTimers === clearDeadline)) {
			command.setTimeout = setDeadline;
			command._clearTimers = clearDeadline;
		}
		return send.call(this, command, stream);
	}
	if (!keepOfflineQueue) redis.on('ready', () => {
		redis.options.enableOfflineQueue = false;
		// Offline admission no longer needs checking after first ready.
		redis.sendCommand = sendTimed;
	});
	redis.sendCommand = function (command, stream) {
		if (this.status === 'ready') return sendTimed.call(this, command, stream);
		const reply = sendTimed.call(this, command, stream);
		const queue = this.offlineQueue;
		if (queue?.peekAt(queue.length - 1)?.command !== command) return reply;
		if (queue.length > MAX_REDIS_QUEUED_COMMANDS) {
			queue.pop();
			command.reject(new ConnectionError('redis', 'offline command queue capacity exceeded'));
			return reply;
		}
		command.promise.catch(() => {
			const queued = this.offlineQueue;
			for (let i = 0; i < queued.length; i++) {
				if (queued.peekAt(i).command === command) { queued.remove(i, 1); break; }
			}
		});
		return reply;
	};
}

/**
 * @typedef {Object} RedisClientOptions
 * @property {string} [url='redis://localhost:6379'] - Redis connection URL
 * @property {string} [keyPrefix=''] - Prefix for all keys written by extensions
 * @property {boolean} [autoShutdown=true] - Listen for `sveltekit:shutdown` and disconnect
 * @property {import('ioredis').RedisOptions} [options] - Override ioredis defaults: one retry, 5s command/connect/socket deadlines, bounded startup buffering and no offline buffering after first ready
 */

/**
 * @typedef {Object} RedisClient
 * @property {import('ioredis').Redis} redis - The underlying ioredis instance
 * @property {string} keyPrefix - The key prefix
 * @property {(key: string) => string} key - Prefix a key: `keyPrefix + key`
 * @property {(overrides?: import('ioredis').RedisOptions) => import('ioredis').Redis} duplicate - Create a new connection with the same config (for subscribers)
 * @property {() => Promise<void>} quit - Gracefully disconnect
 */

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
	const url = options.url || 'redis://localhost:6379';
	const keyPrefix = options.keyPrefix || '';
	const autoShutdown = options.autoShutdown !== false;

	/** @type {import('ioredis').RedisOptions} */
	const redisOpts = {
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

	// Without a listener ioredis prints its own unindexed "Unhandled error
	// event" line, with the connection error unredacted.
	redis.on('error', (err) => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_CLIENT, { error: err }));

	// Track duplicate connections for cleanup
	/** @type {Set<import('ioredis').Redis>} */
	const duplicates = new Set();
	let duplicatesWarnFired = false;

	/** @type {boolean} */
	let shuttingDown = false;

	async function quit() {
		if (shuttingDown) return;
		shuttingDown = true;
		const all = [redis, ...duplicates];
		await Promise.allSettled(all.map((c) => c.quit().catch(() => c.disconnect())));
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
			// Subscriber connections must buffer their initial/reconnect admission.
			const dup = redis.duplicate({ enableOfflineQueue: true, ...overrides });
			boundOfflineQueue(dup, true);
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
