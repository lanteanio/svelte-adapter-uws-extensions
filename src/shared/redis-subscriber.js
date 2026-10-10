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
 * A Redis Cluster subscribes through one connection to one of its nodes, and
 * ioredis moves that connection to another node whenever it ends, re-issuing
 * each kind of subscription as one command and dropping a refusal without
 * reporting or retrying it. A Cluster opened here keeps its subscriptions on
 * a node connection of its own instead, moves them to a new node connection
 * the same way, and restores them there as above.
 *
 * A subscriber connection only receives, so a host that died without closing
 * the socket, or a NAT or load balancer that dropped the idle flow, leaves it
 * open and silent until TCP keepalive gives up, about two hours at the OS
 * default ioredis uses. Each connection is asked for a PING every 30 seconds,
 * and one that does not answer within 10 seconds is closed as a dropped
 * connection would be: a standalone one reconnects, a Cluster's moves to
 * another node. A Cluster subscriber also reads the cluster's slot map every
 * minute, since a node removed from the cluster keeps its connections open and
 * answering while PUBLISH no longer reaches it.
 *
 * @module svelte-adapter-uws-extensions/shared/redis-subscriber
 */

import Redis from 'ioredis';
import { setTimer, clearTimer, setIntervalTimer, clearIntervalTimer, setImmediateTimer, randomFloat } from './runtime.js';
import { isCluster } from './cluster.js';
import { emitRegisteredFailure } from './diagnostic.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';

const { ReplyError } = Redis;

const KINDS = Object.freeze([
	Object.freeze({ name: 'subscribe', undo: 'unsubscribe', batch: true }),
	Object.freeze({ name: 'psubscribe', undo: 'punsubscribe', batch: true }),
	// Restored one channel per command, as ioredis does.
	Object.freeze({ name: 'ssubscribe', undo: 'sunsubscribe', batch: false })
]);

// A shard channel lives on the node owning its slot, which one node
// connection cannot follow, so a Cluster's SSUBSCRIBE is left to ioredis.
const CLUSTER_KINDS = Object.freeze(KINDS.filter((kind) => kind.name !== 'ssubscribe'));
const UNDO_NAMES = new Set(CLUSTER_KINDS.map((kind) => kind.undo));

const RETRY_FIRST_MS = 1000;
const RETRY_MAX_MS = 30000;

// A cluster node connection that ends before any command on it succeeded is
// replaced after a delay growing by this step up to the cap, so a node that
// cannot be reached or refuses the login is not dialled in a tight loop; one
// that answered is replaced at once.
const REOPEN_STEP_MS = 100;
const REOPEN_MAX_MS = 2000;

// ioredis's default for how often a Cluster sends a command again after the
// connection under it closed.
const DEFAULT_MAX_REDIRECTIONS = 16;

// How often a subscriber connection is asked for a PING, and how long it has
// to answer before it is taken for dead (see the module comment).
const LIVENESS_INTERVAL_MS = 30000;
const LIVENESS_TIMEOUT_MS = 10000;

// A Cluster subscriber reads the slot map again every this many PING rounds,
// unless the Cluster already does so at least as often.
const MEMBERSHIP_ROUNDS = 2;

/** What a cluster node connection receives and the Cluster re-emits. */
const MESSAGE_EVENTS = Object.freeze(['message', 'messageBuffer']);
const PATTERN_EVENTS = Object.freeze(['pmessage', 'pmessageBuffer']);

/** ioredis's message for a command on a connection that has ended. */
const CLOSED = 'Connection is closed.';

/** ioredis's message for a slot map no node it knows would give. */
const CLUSTER_ALL_FAILED = 'Failed to refresh slots cache.';

// Internal lifecycle events, emitted after a restore starts and succeeds.
export const SUBSCRIBER_RESTORING = Symbol.for('uws.extensions.subscriber.restoring');
export const SUBSCRIBER_RESTORED = Symbol.for('uws.extensions.subscriber.restored');

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

/** What a PING says of its connection (see answersPing). */
const ANSWERED = 'answered';
const SILENT = 'silent';
const MOOT = 'moot';

/**
 * Ask each connection `list()` returns for a PING every LIVENESS_INTERVAL_MS
 * and hand one that stopped answering to `onSilent`, which closes it: one
 * from which nothing came within LIVENESS_TIMEOUT_MS of its PING, or one
 * still in its connect handshake on the same socket a round later, which
 * ioredis waits for without a time limit. The first silent connection of a
 * streak is reported under `owner`, with the master `identify` names when it
 * names one, and each master has a streak of its own; an answered PING ends
 * it. `onRound` runs at the start of every round.
 *
 * @param {string} owner - module name reported
 * @param {() => Iterable<any>} list
 * @param {(conn: any) => void} onSilent
 * @param {{ onRound?: () => void, identify?: (conn: any) => string | undefined }} [hooks]
 * @returns {{ stop(): void }}
 */
export function watchLiveness(owner, list, onSilent, { onRound, identify } = {}) {
	/** @type {WeakSet<object>} connections with a PING out */
	const probing = new WeakSet();
	/** @type {WeakMap<object, unknown>} the socket each was in its handshake on at the last round */
	const handshaking = new WeakMap();
	/** @type {Set<string>} the streaks reported, by master ('' without one) */
	const reported = new Set();
	let stopped = false;

	/**
	 * @param {any} conn
	 * @param {number} waitedMs - how long it went unanswered at least
	 */
	function silent(conn, waitedMs) {
		const master = identify ? identify(conn) : undefined;
		if (!reported.has(master ?? '')) {
			reported.add(master ?? '');
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT, {
				attributes: master === undefined
					? { owner, timeoutMs: waitedMs }
					: { owner, master, timeoutMs: waitedMs }
			});
		}
		onSilent(conn);
	}

	function round() {
		if (stopped) return;
		if (onRound) onRound();
		for (const conn of list()) {
			// TCP is up and the server does not answer the handshake; one still
			// connecting is bounded by ioredis's connect timeout.
			if (conn.status === 'connect') {
				if (handshaking.has(conn) && handshaking.get(conn) === conn.stream) {
					handshaking.delete(conn);
					silent(conn, LIVENESS_INTERVAL_MS);
				} else {
					handshaking.set(conn, conn.stream);
				}
				continue;
			}
			handshaking.delete(conn);
			if (probing.has(conn) || conn.status !== 'ready' || typeof conn.ping !== 'function') continue;
			probing.add(conn);
			answersPing(conn).then((verdict) => {
				probing.delete(conn);
				if (stopped || verdict === MOOT) return;
				if (verdict === ANSWERED) {
					reported.delete((identify ? identify(conn) : undefined) ?? '');
					return;
				}
				silent(conn, LIVENESS_TIMEOUT_MS);
			});
		}
	}

	const timer = setIntervalTimer(round, LIVENESS_INTERVAL_MS);
	if (timer && timer.unref) timer.unref();
	return {
		stop() {
			if (stopped) return;
			stopped = true;
			clearIntervalTimer(timer);
		}
	};
}

/**
 * Whether `err` is an error the server replied with, which proves the
 * connection works. The class is the test; its name stands in where a bundler
 * kept a second copy of it.
 *
 * @param {unknown} err
 * @returns {boolean}
 */
export function isReplyError(err) {
	return err instanceof ReplyError || /** @type {any} */ (err)?.name === 'ReplyError';
}

/**
 * Send `conn` a PING and resolve what it says of the connection: ANSWERED
 * when anything came in over its socket meanwhile (the reply, an error reply,
 * or a message ahead of the reply on a busy connection); SILENT when nothing
 * did within LIVENESS_TIMEOUT_MS; MOOT when the connection closed or
 * reconnected meanwhile, which is handled where it ends or reconnects. ioredis
 * giving up on the PING itself (its commandTimeout option) decides nothing:
 * the reply can still be on its way behind a burst of messages.
 *
 * @param {any} conn
 * @returns {Promise<string>}
 */
function answersPing(conn) {
	const stream = conn.stream;
	return new Promise((resolve) => {
		let settled = false;
		/** @param {string} verdict */
		const settle = (verdict) => {
			if (settled) return;
			settled = true;
			clearTimer(timer);
			if (listening) stream.removeListener('data', heard);
			resolve(verdict);
		};
		const heard = () => settle(ANSWERED);
		// Decided after the event loop has read what already arrived: a timer runs
		// before the socket is read in the same turn.
		const timer = setTimer(() => setImmediateTimer(() => settle(conn.status === 'ready' && conn.stream === stream ? SILENT : MOOT)), LIVENESS_TIMEOUT_MS);
		if (timer && timer.unref) timer.unref();
		const listening = !!stream && typeof stream.once === 'function' && typeof stream.removeListener === 'function';
		if (listening) stream.once('data', heard);
		try {
			Promise.resolve(conn.ping()).then(heard, (/** @type {any} */ err) => {
				if (isReplyError(err)) settle(ANSWERED);
				else if (err?.message !== 'Command timed out' && !(err?.name === 'TimeoutError' && err?.service === 'redis')) settle(MOOT);
			});
		} catch {
			settle(MOOT);
		}
	});
}

/**
 * Destroy `conn`'s socket, which closes the connection at once the way a
 * dropped one closes. Ending it instead waits for the peer to acknowledge,
 * which a dead host never does (newer ioredis destroys it after its
 * disconnectTimeout, 2 seconds by default). Returns whether there was a
 * socket to destroy.
 *
 * @param {any} conn
 * @returns {boolean}
 */
function dropSocket(conn) {
	const stream = conn.stream;
	if (!stream || typeof stream.destroy !== 'function' || stream.destroyed) return false;
	stream.destroy();
	return true;
}

/**
 * Close a connection that is not to reconnect, at once, whatever state its
 * peer is in.
 *
 * @param {any} conn
 */
export function closeConnection(conn) {
	conn.disconnect();
	dropSocket(conn);
}

/**
 * A node's address, as ioredis keys its node list.
 *
 * @param {any} node
 * @returns {string}
 */
function addressOf(node) {
	return node.options.host + ':' + node.options.port;
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
 * Take over re-subscription for one ioredis connection. A Cluster gets a node
 * connection of its own (see guardCluster). Anything without `options`, such
 * as a test double that never reconnects, is returned untouched.
 *
 * @param {any} conn
 * @param {string} owner
 * @returns {any}
 */
export function guardSubscriber(conn, owner) {
	if (!conn || !conn.options || typeof conn.on !== 'function') return conn;
	if (isCluster(conn)) return guardCluster(conn, owner);
	conn.options.autoResubscribe = false;

	/** @type {Map<string, Function>} */
	const original = new Map();
	for (const kind of KINDS) {
		original.set(kind.name, conn[kind.name]);
		original.set(kind.undo, conn[kind.undo]);
	}
	const send = (name, args) => /** @type {Function} */ (original.get(name)).apply(conn, args);
	const subs = trackSubscriptions(conn, owner, KINDS, {
		send,
		restore: send,
		isUp: () => conn.status === 'ready',
		// Hold handshake calls behind ioredis's older offline/unanswered queues.
		handshakeSocket: () => (conn.status === 'connect' && conn.stream ? conn.stream : null)
	});

	// The first ready sends what the owner queued while connecting; every later
	// one follows a reconnect that dropped the subscriptions. Either may follow
	// a socket that closed in its handshake.
	let readyBefore = false;
	conn.on('ready', () => {
		subs.flushHeld();
		if (!readyBefore) {
			readyBefore = true;
			return;
		}
		subs.restoreAll();
	});
	conn.on('error', (err) => subs.failHeld(err));
	conn.on('close', () => subs.failHeld(new Error(CLOSED)));
	// A connection that went silent, or stuck in its handshake, is closed so
	// ioredis reconnects it, and the 'ready' after restores its channels.
	const liveness = watchLiveness(owner, () => [conn], dropSocket);
	/** @type {any} */
	let quitTimer = null;
	conn.on('end', () => {
		subs.end();
		liveness.stop();
		if (quitTimer !== null) {
			clearTimer(quitTimer);
			quitTimer = null;
		}
	});
	// A QUIT goes out at once on a ready connection or in its handshake, and
	// closing it on purpose then stops the PINGs: a QUIT to a server that
	// stopped answering leaves the connection open until it times out. One
	// its QUIT has not closed within LIVENESS_TIMEOUT_MS is closed at once,
	// since a dead host never answers the QUIT and the open socket would keep
	// the process alive until TCP gives up. At any other time ioredis queues
	// the QUIT for the next connection, or closes the connection at once when
	// nothing else is queued, and the PINGs go on until it ends.
	const quit = conn.quit;
	if (typeof quit === 'function') {
		conn.quit = function (...args) {
			subs.failHeld(new Error(CLOSED));
			if (conn.status === 'ready' || conn.status === 'connect') {
				liveness.stop();
				if (quitTimer === null) {
					quitTimer = setTimer(() => {
						quitTimer = null;
						// Between two attempts the QUIT was queued, not sent.
						if (conn.status !== 'end' && conn.status !== 'reconnecting') closeConnection(conn);
					}, LIVENESS_TIMEOUT_MS);
					if (quitTimer && quitTimer.unref) quitTimer.unref();
				}
			}
			return quit.apply(this, args);
		};
	}
	const disconnect = conn.disconnect;
	if (typeof disconnect === 'function') {
		conn.disconnect = function (reconnect, ...rest) {
			// Closed for good. Between two attempts ioredis then emits no 'end'.
			if (!reconnect) {
				liveness.stop();
				subs.end();
			}
			return disconnect.call(this, reconnect, ...rest);
		};
	}

	return conn;
}

/**
 * Record the channels the owner holds through `target`'s (un)subscribe
 * methods, which are replaced to do the bookkeeping and send each call with
 * `io.send`, and restore them on request with `io.restore` on the connection
 * that carries the subscriptions.
 *
 * Calls made during a handshake are held until ready, after ioredis flushes
 * its older offline and unanswered commands. Failed attempts reject held
 * calls; later calls of the owner decide which channels still need sending.
 *
 * @param {any} target
 * @param {string} owner
 * @param {ReadonlyArray<typeof KINDS[number]>} kinds - the subscription kinds taken over
 * @param {{
 *   send: (name: string, args: unknown[]) => Promise<unknown>,
 *   restore: (name: string, args: unknown[]) => Promise<unknown>,
 *   isUp: () => boolean,
 *   handshakeSocket?: () => unknown
 * }} io - `isUp` says whether that connection is ready; `handshakeSocket`
 *   names the socket whose handshake holds owner calls, null otherwise
 */
function trackSubscriptions(target, owner, kinds, { send, restore, isUp, handshakeSocket }) {
	/** @type {Map<string, Set<string>>} kind -> channels the owner holds */
	const held = new Map();
	for (const kind of kinds) held.set(kind.name, new Set());
	// kind + channel -> id of the latest call on it still awaiting its reply.
	// A reply only updates `held` while its call is still the latest, so a
	// subscribe answered after a later unsubscribe does not re-add the channel.
	/** @type {Map<string, number>} */
	const inFlight = new Map();
	let callSeq = 0;
	/** @typedef {{ name: string, prefix: string, keys: string[], id: number, resolve: (v: unknown) => void, reject: (e: unknown) => void }} HeldCall */
	/** @type {HeldCall[]} owner calls held during the handshake, in order */
	let handshakeCalls = [];

	function dispatch(name, args, prefix, keys, id) {
		if (!handshakeSocket || handshakeSocket() === null) return send(name, args);
		const callback = typeof args[args.length - 1] === 'function' ? args[args.length - 1] : null;
		const reply = new Promise((resolve, reject) => {
			handshakeCalls.push({ name, prefix, keys, id, resolve, reject });
		});
		if (callback) reply.then((value) => callback(null, value), (error) => callback(error));
		return reply;
	}

	function failHeld(error) {
		const calls = handshakeCalls;
		handshakeCalls = [];
		for (const call of calls) call.reject(error);
	}

	for (const kind of kinds) {
		const set = /** @type {Set<string>} */ (held.get(kind.name));
		// Nothing of a kind the connection cannot subscribe to is ever held.
		if (typeof target[kind.name] !== 'function' || typeof target[kind.undo] !== 'function') continue;
		const prefix = kind.name + '\n';

		target[kind.name] = function (...args) {
			const channels = channelArgs(args);
			const id = ++callSeq;
			for (const channel of channels) inFlight.set(prefix + channel, id);
			const reply = dispatch(kind.name, args, prefix, channels.map((channel) => prefix + channel), id);
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

		target[kind.undo] = function (...args) {
			const channels = channelArgs(args);
			const id = ++callSeq;
			/** @type {string[]} */
			const keys = [];
			if (channels.length === 0) {
				// No channels: every channel of this kind, held or still being
				// subscribed.
				const all = new Set();
				for (const channel of set) all.add(prefix + channel);
				for (const key of inFlight.keys()) {
					if (key.startsWith(prefix)) all.add(key);
				}
				set.clear();
				keys.push(...all);
			} else {
				for (const channel of channels) {
					set.delete(channel);
					keys.push(prefix + channel);
				}
			}
			for (const key of keys) inFlight.set(key, id);
			const reply = dispatch(kind.undo, args, prefix, keys, id);
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
		return restore(kind.name, [channel]).then(
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
		return restore(kind.name, channels).then(
			() => [],
			() => Promise.all(channels.filter((channel) => set.has(channel)).map((channel) => restoreOne(kind, channel)))
		);
	}

	function settle(id, results) {
		// Results that arrive once the connection is gone describe that
		// connection; the next one restores everything again.
		if (id !== round || ended || !isUp()) return;
		refused = [];
		for (const result of results) {
			// A channel the owner dropped while it was being restored is not
			// retried.
			if (result && held.get(result.kind.name)?.has(result.channel)) refused.push(result);
		}
		if (refused.length === 0) {
			refusedReported = false;
			retryMs = 0;
			target.emit?.(SUBSCRIBER_RESTORED);
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
		if ([...held.values()].some((channels) => channels.size > 0)) target.emit?.(SUBSCRIBER_RESTORING);
		cancelRetry();
		const id = ++round;
		const attempts = [];
		for (const kind of kinds) {
			const channels = [.../** @type {Set<string>} */ (held.get(kind.name))];
			if (channels.length > 0) attempts.push(restoreKind(kind, channels));
		}
		if (attempts.length === 0) {
			// Nothing held is nothing refused, whatever state the connection is in.
			refused = [];
			refusedReported = false;
			retryMs = 0;
			return;
		}
		Promise.all(attempts).then((results) => settle(id, results.flat()));
	}

	function retryRefused() {
		retryTimer = null;
		// While the connection is down the next one restores everything.
		if (ended || !isUp()) return;
		const id = ++round;
		const attempts = [];
		for (const { kind, channel } of refused) {
			if (held.get(kind.name)?.has(channel)) attempts.push(restoreOne(kind, channel));
		}
		Promise.all(attempts).then((results) => settle(id, results));
	}

	return {
		restoreAll,
		/** Send held calls after ioredis has flushed older commands. */
		flushHeld() {
			const calls = handshakeCalls;
			handshakeCalls = [];
			for (const call of calls) {
				const open = [];
				for (const key of call.keys) {
					if (inFlight.get(key) === call.id) open.push(key.slice(call.prefix.length));
				}
				if (open.length === 0) {
					call.resolve(0);
					continue;
				}
				try { send(call.name, [open]).then(call.resolve, call.reject); }
				catch (error) { call.reject(error); }
			}
		},
		failHeld,
		/** Stop for good: held calls cannot reach another socket. */
		end() {
			ended = true;
			cancelRetry();
			failHeld(new Error(CLOSED));
		},
		/** Whether the owner holds any channel. */
		holdsAny() {
			for (const set of held.values()) {
				if (set.size > 0) return true;
			}
			return false;
		}
	};
}

/**
 * Keep a Cluster's subscriptions on a node connection of its own. The Cluster
 * object stays what the owner and the client wrapper hold: its subscribe and
 * psubscribe methods and their undo go to the node connection, what that
 * connection receives is emitted on the Cluster, and quitting the Cluster
 * closes it. ioredis's own cluster subscriber is never given such a channel.
 *
 * The node is sampled from every node the Cluster knows once it is ready, as
 * ioredis does: a classic PUBLISH reaches every node of a cluster. Node
 * connections never reconnect (ioredis gives them no retry strategy), so one
 * that ends, goes silent, or whose node a refreshed slot map of the Cluster
 * no longer lists, is replaced by a connection to a sampled node and the held
 * channels are restored there. The owner's calls
 * it had not answered go to the replacement in the order they were made, as
 * the Cluster would send them again; an undo with no subscribe ahead of it is
 * settled instead, since no server holds what it would undo.
 *
 * A node whose connection failed is sampled again only once every other node
 * the Cluster knows failed too. Then the slot map is read again, and the
 * failure reported when no node could be opened at all; when no known node
 * answers that read either, the Cluster reconnects, which reads it from the
 * startup nodes, as ioredis does for its own sharded subscribers. It would
 * not on its own: its connections to nodes it never sent a command to stay
 * unopened, so they never fail, and a rolling restart that gives every node a
 * new address leaves it dialling none of them.
 *
 * @param {any} cluster
 * @param {string} owner
 * @returns {any}
 */
function guardCluster(cluster, owner) {
	/** @type {any} the node connection carrying the subscriptions */
	let current = null;
	/** @typedef {{ name: string, args: unknown[], attempts: number, conn: any, resolve: (v: unknown) => void, reject: (e: unknown) => void }} Call */
	/** @type {Call[]} owner calls sent on `current` and not answered, in order */
	let sent = [];
	// Set once a command on `current` succeeded, which a login the server
	// refused or a connection it closed at once never gets to.
	let proven = false;
	// Set when `current` raised an error before that: ioredis can report such
	// a connection ready, and what it answers then says nothing of a channel.
	let faulted = false;
	let closed = false;
	/** @type {Call[]} owner calls waiting for a node connection, in order */
	let waiting = [];
	// Node connections in a row that ended unproven.
	let failedOpens = 0;
	/** @type {any} */
	let reopenTimer = null;
	// Addresses of the nodes whose connection ended unproven or went silent
	// since one last proved itself, of those that ended unproven, and the last
	// error such a connection raised.
	/** @type {Set<string>} */
	const failed = new Set();
	/** @type {Set<string>} */
	const unopened = new Set();
	/** @type {unknown} */
	let lastError = null;
	let unreachableReported = false;
	// Set while the slot map is read again after every known node failed.
	let rediscovering = false;
	let rounds = 0;
	const maxAttempts = cluster.options.maxRedirections > 0 ? cluster.options.maxRedirections : DEFAULT_MAX_REDIRECTIONS;
	const ownRefresh = !(cluster.options.slotsRefreshInterval > 0 &&
		cluster.options.slotsRefreshInterval <= LIVENESS_INTERVAL_MS * MEMBERSHIP_ROUNDS);

	// Set while a reconnect of the Cluster is this guard's doing, one it forced
	// when no node it knows answered (see rediscover) or one a slot map read of
	// its own set off (see readSlotMap), until the Cluster is ready again.
	let forcing = false;
	// ioredis reports each attempt of such a reconnect that finds no node as an
	// 'error', which the owner reports, and its default strategy tries again
	// about every 100 ms, so those attempts are spaced as the node connections
	// here are redialled, growing to 2 seconds, or longer when the strategy
	// asks. They do not stop, which the strategy may ask for, since without the
	// reconnect the subscriber would have gone on redialling its nodes. Any
	// other reconnect is the strategy's alone.
	const retryStrategy = cluster.options.clusterRetryStrategy;
	cluster.options.clusterRetryStrategy = function (/** @type {number} */ times, /** @type {unknown} */ reason) {
		const delay = typeof retryStrategy === 'function' ? retryStrategy.call(this, times, reason) : null;
		if (!forcing) return delay;
		const spaced = Math.min(Math.max(1, times) * REOPEN_STEP_MS, REOPEN_MAX_MS);
		return typeof delay === 'number' ? Math.max(delay, spaced) : spaced;
	};

	const subs = trackSubscriptions(cluster, owner, CLUSTER_KINDS, {
		send(name, args) {
			if (closed) return Promise.reject(new Error(CLOSED));
			return new Promise((resolve, reject) => {
				const call = { name, args, attempts: 0, conn: null, resolve, reject };
				if (current !== null) {
					transmit(call);
					return;
				}
				waiting.push(call);
				if (settleIdleUndos()) schedule();
			});
		},
		// Restores run right after a connection opens, or as retries while it is
		// up, so there always is one.
		restore: (name, args) => (current !== null ? run(current, name, args) : Promise.reject(new Error(CLOSED))),
		isUp: () => current !== null && !faulted && current.status === 'ready'
	});

	/** Send a command on `conn`; its success proves the connection. */
	function run(conn, name, args) {
		const reply = conn[name](...args);
		reply.then(() => {
			if (conn !== current) return;
			proven = true;
			failedOpens = 0;
			failed.clear();
			unopened.clear();
			lastError = null;
			unreachableReported = false;
		}, () => {});
		return reply;
	}

	/** @param {Call} call */
	function transmit(call) {
		const conn = current;
		call.conn = conn;
		call.attempts++;
		sent.push(call);
		run(conn, call.name, call.args).then((value) => {
			if (call.conn !== conn) return;
			drop(call);
			call.resolve(value);
		}, (err) => {
			// Moved to the next connection by the end of this one.
			if (call.conn !== conn) return;
			// The end of this connection, still to come, moves it.
			if (!closed && conn === current && /** @type {any} */ (err)?.message === CLOSED) return;
			drop(call);
			call.reject(err);
		});
	}

	/** @param {Call} call */
	function drop(call) {
		call.conn = null;
		const i = sent.indexOf(call);
		if (i !== -1) sent.splice(i, 1);
	}

	/**
	 * With no node connection, settle each waiting undo that has no subscribe
	 * ahead of it: no server holds a subscription of this Cluster then, and it
	 * must not wait for an outage to end. True when something still waits.
	 */
	function settleIdleUndos() {
		let subscribeAhead = false;
		waiting = waiting.filter((call) => {
			if (!UNDO_NAMES.has(call.name)) {
				subscribeAhead = true;
				return true;
			}
			if (subscribeAhead) return true;
			call.resolve(0);
			return false;
		});
		return waiting.length > 0;
	}

	function open() {
		if (current !== null || closed || reopenTimer !== null || rediscovering) return;
		// Nothing is sent before the Cluster is ready, as the Cluster itself
		// does; one created with lazyConnect is connected here, since nothing
		// else sends through it. 'ready' and '+node' call this again.
		if (cluster.status !== 'ready') {
			if (cluster.status === 'wait') cluster.connect().catch(() => {});
			return;
		}
		let nodes = [];
		try { nodes = cluster.nodes('all'); } catch { /* reset while reconnecting */ }
		if (!Array.isArray(nodes) || nodes.length === 0) return;
		const untried = nodes.filter((node) => !failed.has(addressOf(node)));
		if (untried.length === 0) {
			// Reported when none of them could be opened; one that only went
			// silent may well open again.
			if (nodes.every((node) => unopened.has(addressOf(node)))) reportUnreachable(nodes.length);
			rediscover();
			return;
		}
		const node = untried[Math.min(untried.length - 1, Math.floor(randomFloat() * untried.length))];
		const address = addressOf(node);
		let conn;
		try {
			conn = node.duplicate({ enableReadyCheck: false, autoResubscribe: false });
		} catch (err) {
			// Options the node's own connection was built from; this fails only
			// for good, so the waiting calls learn why.
			const calls = waiting;
			waiting = [];
			for (const call of calls) call.reject(err);
			return;
		}
		current = conn;
		sent = [];
		proven = false;
		faulted = false;
		// Not reported, as ioredis's own subscriber does not report its
		// connection's errors: one that fails the connection ends it, and it is
		// replaced then. The last one is reported once every node failed.
		conn.on('error', (err) => {
			if (conn !== current || proven) return;
			faulted = true;
			lastError = err;
		});
		// Forwarded exactly as ioredis does; a connection that stops carrying the
		// subscriptions drops these listeners instead of checking each message.
		// Block bodies: EventEmitter inspects whatever a listener returns.
		for (const event of MESSAGE_EVENTS) {
			conn.on(event, (channel, message) => { cluster.emit(event, channel, message); });
		}
		for (const event of PATTERN_EVENTS) {
			conn.on(event, (pattern, channel, message) => { cluster.emit(event, pattern, channel, message); });
		}
		conn.on('end', () => {
			retire(conn);
			if (conn !== current) return;
			current = null;
			// The calls it did not answer go first, in the order they were made.
			const cut = [];
			for (const call of sent) {
				call.conn = null;
				if (call.attempts >= maxAttempts) call.reject(new Error(CLOSED));
				else cut.push(call);
			}
			sent = [];
			waiting = cut.concat(waiting);
			settleIdleUndos();
			if (!proven) {
				failedOpens++;
				failed.add(address);
				unopened.add(address);
			}
			reopen();
		});
		// Sent through the connection's offline queue, which also connects it.
		subs.restoreAll();
		const calls = waiting;
		waiting = [];
		for (const call of calls) transmit(call);
	}

	/** Stop re-emitting what `conn` receives. */
	function retire(conn) {
		for (const event of MESSAGE_EVENTS) conn.removeAllListeners(event);
		for (const event of PATTERN_EVENTS) conn.removeAllListeners(event);
	}

	function reopen() {
		if (!subs.holdsAny() && waiting.length === 0) return;
		schedule();
	}

	/**
	 * No node the Cluster knows could be opened since a connection last proved
	 * itself: reported once per streak.
	 *
	 * @param {number} known - how many nodes the Cluster knows
	 */
	function reportUnreachable(known) {
		if (unreachableReported) return;
		unreachableReported = true;
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_UNREACHABLE, lastError === null
			? { attributes: { owner, nodes: known } }
			: { error: lastError, attributes: { owner, nodes: known } });
	}

	/**
	 * Every node the Cluster knows failed: read the slot map again, or
	 * reconnect the Cluster when no known node answers that either (see
	 * guardCluster). Its 'ready' then opens the next connection.
	 */
	function rediscover() {
		failed.clear();
		unopened.clear();
		if (typeof cluster.refreshSlotsCache !== 'function') {
			schedule();
			return;
		}
		rediscovering = true;
		readSlotMap((/** @type {any} */ err) => {
			rediscovering = false;
			if (closed) return;
			// ioredis tells this error by its message too. A Cluster already
			// reconnecting reads its startup nodes anyway.
			if (err && err.message === CLUSTER_ALL_FAILED && cluster.status === 'ready') {
				forcing = true;
				cluster.disconnect(true);
			} else {
				schedule();
			}
		});
	}

	/**
	 * Read the slot map. A node with no slots, one reset after it was removed
	 * for example, answers with an empty map, on which ioredis drops every node
	 * before the callback runs and reconnects the Cluster once the first of them
	 * has closed: that reconnect is this guard's doing, so the retry strategy
	 * treats it as forced (see above) until the Cluster is ready.
	 *
	 * @param {(err: unknown) => void} [onRead]
	 */
	function readSlotMap(onRead) {
		cluster.refreshSlotsCache((/** @type {any} */ err) => {
			if (!err && !closed && cluster.status === 'ready') {
				let known = 1;
				try { known = cluster.nodes('all').length; } catch { /* reset while reconnecting */ }
				if (known === 0) forcing = true;
			}
			if (onRead) onRead(err);
		});
	}

	/** Open a node connection now, or after the delay that unproven ones earned. */
	function schedule() {
		if (current !== null || closed || reopenTimer !== null) return;
		if (failedOpens === 0) {
			open();
			return;
		}
		reopenTimer = setTimer(() => {
			reopenTimer = null;
			open();
		}, Math.min(failedOpens * REOPEN_STEP_MS, REOPEN_MAX_MS));
		if (reopenTimer && reopenTimer.unref) reopenTimer.unref();
	}

	const onTopology = () => {
		if (current === null && (subs.holdsAny() || waiting.length > 0)) open();
	};
	// Whether the node still belongs to the cluster is read from the slot map
	// once it is refreshed, which this does every MEMBERSHIP_ROUNDS PING rounds
	// besides whatever else refreshes it. '-node' is no sign: ioredis also
	// emits it when its own connection to the node ends, which leaves this one
	// working.
	// A map that lists no node came from a node with no slots, one reset after
	// it was removed for example, and says nothing of this one's.
	const onRefresh = () => {
		if (current === null) return;
		let nodes = [];
		try { nodes = cluster.nodes('all'); } catch { return; }
		if (nodes.length === 0) return;
		const address = addressOf(current);
		if (!nodes.some((node) => addressOf(node) === address)) closeConnection(current);
	};
	const onReady = () => { forcing = false; };
	cluster.on('ready', onReady);
	cluster.on('ready', onTopology);
	cluster.on('+node', onTopology);
	cluster.on('refresh', onRefresh);

	const liveness = watchLiveness(owner, () => (current !== null ? [current] : []), (conn) => {
		if (conn !== current) return;
		failed.add(addressOf(conn));
		closeConnection(conn);
	}, {
		onRound() {
			if (++rounds % MEMBERSHIP_ROUNDS !== 0 || !ownRefresh || typeof cluster.refreshSlotsCache !== 'function') return;
			if (current !== null && cluster.status === 'ready') readSlotMap();
		}
	});

	function close() {
		if (closed) return;
		closed = true;
		forcing = false;
		subs.end();
		liveness.stop();
		if (reopenTimer !== null) {
			clearTimer(reopenTimer);
			reopenTimer = null;
		}
		cluster.removeListener('ready', onReady);
		cluster.removeListener('ready', onTopology);
		cluster.removeListener('+node', onTopology);
		cluster.removeListener('refresh', onRefresh);
		// Including calls sent on a connection whose closing rejection was set
		// aside for an end that is no longer handled.
		const calls = sent.concat(waiting);
		sent = [];
		waiting = [];
		for (const call of calls) {
			call.conn = null;
			call.reject(new Error(CLOSED));
		}
		if (current !== null) {
			const conn = current;
			current = null;
			retire(conn);
			// Not a QUIT, as ioredis stops its own subscriber: one would wait
			// behind a connect still in progress, or forever on a dead host,
			// and keep the process alive meanwhile.
			closeConnection(conn);
		}
	}

	const quit = cluster.quit;
	cluster.quit = function (...args) {
		close();
		return quit.apply(this, args);
	};
	const disconnect = cluster.disconnect;
	cluster.disconnect = function (reconnect, ...rest) {
		// With reconnect set ioredis is reconnecting the Cluster. The node
		// connection is its own and stays; the refresh that follows tells
		// whether its node still belongs to the cluster.
		if (!reconnect) close();
		return disconnect.call(this, reconnect, ...rest);
	};
	cluster.on('end', close);

	return cluster;
}
