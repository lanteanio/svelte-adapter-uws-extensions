/**
 * Subscriber connections that keep their channels across a reconnect.
 *
 * On a reconnect ioredis re-issues SUBSCRIBE, PSUBSCRIBE and SSUBSCRIBE for
 * the channels the connection held, with nothing handling the replies. When
 * Redis refuses one - NOPERM after the user lost access to the channel, which
 * is itself what makes Redis close a subscribed connection - the rejection is
 * unhandled, which ends a Node process in the default unhandled-rejection mode,
 * and a refused SUBSCRIBE carrying several channels leaves all of them
 * unsubscribed.
 *
 * openSubscriber() switches that re-subscription off and does it here: the
 * connection records the channels its owner holds, restores them on every
 * reconnect with each refusal handled and reported, restores the channels
 * Redis still accepts when it refuses some, and retries the refused ones with
 * a backoff while the connection stays up, because nothing else would ask
 * again once the ACL is fixed.
 *
 * @module svelte-adapter-uws-extensions/shared/redis-subscriber
 */

import { setTimer, clearTimer } from './runtime.js';
import { isCluster } from './cluster.js';
import { emitRegisteredFailure } from './diagnostic.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';

const KINDS = Object.freeze([
	Object.freeze({ name: 'subscribe', undo: 'unsubscribe', batch: true }),
	Object.freeze({ name: 'psubscribe', undo: 'punsubscribe', batch: true }),
	// Restored one channel per command, as ioredis does.
	Object.freeze({ name: 'ssubscribe', undo: 'sunsubscribe', batch: false })
]);

const RETRY_FIRST_MS = 1000;
const RETRY_MAX_MS = 30000;

/**
 * Open a subscriber connection on `source`, a client wrapper from
 * createRedisClient, with guarded re-subscription.
 *
 * @param {{ duplicate(overrides?: object): any }} source
 * @param {string} owner - module name reported when a channel is refused
 * @returns {any}
 */
export function openSubscriber(source, owner) {
	return guardSubscriber(source.duplicate({ enableReadyCheck: false }), owner);
}

/**
 * The channel arguments of a (un)subscribe call: strings or arrays of them,
 * with an optional trailing callback.
 *
 * @param {unknown[]} args
 * @returns {string[]}
 */
function channelArgs(args) {
	const out = [];
	for (const arg of args) {
		if (typeof arg === 'function') continue;
		if (Array.isArray(arg)) {
			for (const channel of arg) out.push(String(channel));
		} else {
			out.push(String(arg));
		}
	}
	return out;
}

/**
 * Take over re-subscription for one ioredis connection. A connection whose
 * own re-subscription cannot be switched off is returned untouched, since
 * restoring channels next to ioredis would subscribe twice: a Cluster, whose
 * subscriber re-subscribes on every node change regardless of the option (it
 * catches a refusal without reporting or retrying it), and anything without
 * `options`, such as a test double that never reconnects.
 *
 * @param {any} conn
 * @param {string} owner
 * @returns {any}
 */
export function guardSubscriber(conn, owner) {
	if (!conn || !conn.options || typeof conn.on !== 'function' || isCluster(conn)) return conn;
	conn.options.autoResubscribe = false;

	/** @type {Map<string, Set<string>>} kind -> channels the owner holds */
	const held = new Map();
	for (const kind of KINDS) held.set(kind.name, new Set());
	// kind + channel -> id of the latest call on it still awaiting its reply.
	// A reply only updates `held` while its call is still the latest, so a
	// subscribe answered after a later unsubscribe does not re-add the channel.
	/** @type {Map<string, number>} */
	const inFlight = new Map();
	let callSeq = 0;

	/** @type {Map<string, Function>} */
	const original = new Map();
	for (const kind of KINDS) {
		original.set(kind.name, conn[kind.name]);
		original.set(kind.undo, conn[kind.undo]);
	}

	for (const kind of KINDS) {
		const set = /** @type {Set<string>} */ (held.get(kind.name));
		const subscribeFn = original.get(kind.name);
		const undoFn = original.get(kind.undo);
		// Nothing of a kind the connection cannot subscribe to is ever held.
		if (typeof subscribeFn !== 'function' || typeof undoFn !== 'function') continue;
		const prefix = kind.name + '\n';

		conn[kind.name] = function (...args) {
			const channels = channelArgs(args);
			const id = ++callSeq;
			for (const channel of channels) inFlight.set(prefix + channel, id);
			const reply = subscribeFn.apply(conn, args);
			const settle = (subscribed) => {
				for (const channel of channels) {
					if (inFlight.get(prefix + channel) !== id) continue;
					inFlight.delete(prefix + channel);
					if (subscribed) set.add(channel);
				}
			};
			// Records the outcome only; the caller gets the same promise and its
			// rejection.
			reply.then(() => settle(true), () => settle(false));
			return reply;
		};

		conn[kind.undo] = function (...args) {
			const channels = channelArgs(args);
			const id = ++callSeq;
			/** @type {string[]} */
			const keys = [];
			if (channels.length === 0) {
				// No channels: every channel of this kind, including those still
				// being subscribed.
				set.clear();
				for (const key of inFlight.keys()) {
					if (key.startsWith(prefix)) keys.push(key);
				}
			} else {
				for (const channel of channels) {
					set.delete(channel);
					keys.push(prefix + channel);
				}
			}
			for (const key of keys) inFlight.set(key, id);
			const reply = undoFn.apply(conn, args);
			const settle = () => {
				for (const key of keys) {
					if (inFlight.get(key) === id) inFlight.delete(key);
				}
			};
			reply.then(settle, settle);
			return reply;
		};
	}

	// Restoring ------------------------------------------------------------

	let ended = false;
	// Bumped by every restore; a result from an earlier one is dropped.
	let round = 0;
	/** @type {Array<{ kind: typeof KINDS[number], channel: string }>} */
	let refused = [];
	let refusedReported = false;
	let retryMs = 0;
	/** @type {any} */
	let retryTimer = null;

	function cancelRetry() {
		if (retryTimer !== null) {
			clearTimer(retryTimer);
			retryTimer = null;
		}
	}

	/** Resolves to the refusal, or null when Redis accepted the channel. */
	function restoreOne(kind, channel) {
		return /** @type {Function} */ (original.get(kind.name)).call(conn, channel).then(
			() => null,
			(err) => ({ kind, channel, err })
		);
	}

	function restoreKind(kind, channels) {
		if (!kind.batch || channels.length === 1) {
			return Promise.all(channels.map((channel) => restoreOne(kind, channel)));
		}
		// One refused channel fails the whole command, so on a refusal each
		// channel the owner still holds is asked for alone and the accepted ones
		// are restored. A channel released meanwhile already had its unsubscribe
		// sent, so subscribing it now would leave it subscribed.
		const set = /** @type {Set<string>} */ (held.get(kind.name));
		return /** @type {Function} */ (original.get(kind.name)).apply(conn, channels).then(
			() => [],
			() => Promise.all(channels.filter((channel) => set.has(channel)).map((channel) => restoreOne(kind, channel)))
		);
	}

	function settle(id, results) {
		if (id !== round || ended) return;
		refused = [];
		for (const result of results) {
			// A channel the owner dropped while it was being restored is not
			// retried.
			if (result && held.get(result.kind.name)?.has(result.channel)) refused.push(result);
		}
		if (refused.length === 0) {
			refusedReported = false;
			retryMs = 0;
			return;
		}
		if (!refusedReported) {
			refusedReported = true;
			const first = refused[0];
			// The channel is built from a topic; cut it should the error repeat it.
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE, {
				error: first.err,
				redact: [first.channel],
				attributes: { owner, kind: first.kind.name, refused: refused.length }
			});
		}
		retryMs = retryMs === 0 ? RETRY_FIRST_MS : Math.min(retryMs * 2, RETRY_MAX_MS);
		retryTimer = setTimer(retryRefused, retryMs);
		if (retryTimer && retryTimer.unref) retryTimer.unref();
	}

	function restoreAll() {
		cancelRetry();
		const id = ++round;
		const attempts = [];
		for (const kind of KINDS) {
			const channels = [.../** @type {Set<string>} */ (held.get(kind.name))];
			if (channels.length > 0) attempts.push(restoreKind(kind, channels));
		}
		if (attempts.length === 0) {
			settle(id, []);
			return;
		}
		Promise.all(attempts).then((results) => settle(id, results.flat()));
	}

	function retryRefused() {
		retryTimer = null;
		// While the connection is down the next 'ready' restores everything.
		if (ended || conn.status !== 'ready') return;
		const id = ++round;
		const attempts = [];
		for (const { kind, channel } of refused) {
			if (held.get(kind.name)?.has(channel)) attempts.push(restoreOne(kind, channel));
		}
		Promise.all(attempts).then((results) => settle(id, results));
	}

	// The first ready sends what the owner queued while connecting; every later
	// one follows a reconnect that dropped the subscriptions.
	let readyBefore = false;
	conn.on('ready', () => {
		if (!readyBefore) {
			readyBefore = true;
			return;
		}
		restoreAll();
	});
	conn.on('end', () => {
		ended = true;
		cancelRetry();
	});

	return conn;
}
