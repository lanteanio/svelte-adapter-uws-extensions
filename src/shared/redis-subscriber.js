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
 * @module svelte-adapter-uws-extensions/shared/redis-subscriber
 */

import { setTimer, clearTimer, randomFloat } from './runtime.js';
import { isCluster } from './cluster.js';
import { emitRegisteredFailure } from './diagnostic.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';

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

/** What a cluster node connection receives and the Cluster re-emits. */
const MESSAGE_EVENTS = Object.freeze(['message', 'messageBuffer']);
const PATTERN_EVENTS = Object.freeze(['pmessage', 'pmessageBuffer']);

/** ioredis's message for a command on a connection that has ended. */
const CLOSED = 'Connection is closed.';

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
		isUp: () => conn.status === 'ready'
	});

	// The first ready sends what the owner queued while connecting; every later
	// one follows a reconnect that dropped the subscriptions.
	let readyBefore = false;
	conn.on('ready', () => {
		if (!readyBefore) {
			readyBefore = true;
			return;
		}
		subs.restoreAll();
	});
	conn.on('end', () => subs.end());

	return conn;
}

/**
 * Record the channels the owner holds through `target`'s (un)subscribe
 * methods, which are replaced to do the bookkeeping and send each call with
 * `io.send`, and restore them on request with `io.restore` on the connection
 * that carries the subscriptions.
 *
 * @param {any} target
 * @param {string} owner
 * @param {ReadonlyArray<typeof KINDS[number]>} kinds - the subscription kinds taken over
 * @param {{
 *   send: (name: string, args: unknown[]) => Promise<unknown>,
 *   restore: (name: string, args: unknown[]) => Promise<unknown>,
 *   isUp: () => boolean
 * }} io - `isUp` says whether that connection is ready
 */
function trackSubscriptions(target, owner, kinds, { send, restore, isUp }) {
	/** @type {Map<string, Set<string>>} kind -> channels the owner holds */
	const held = new Map();
	for (const kind of kinds) held.set(kind.name, new Set());
	// kind + channel -> id of the latest call on it still awaiting its reply.
	// A reply only updates `held` while its call is still the latest, so a
	// subscribe answered after a later unsubscribe does not re-add the channel.
	/** @type {Map<string, number>} */
	const inFlight = new Map();
	let callSeq = 0;

	for (const kind of kinds) {
		const set = /** @type {Set<string>} */ (held.get(kind.name));
		// Nothing of a kind the connection cannot subscribe to is ever held.
		if (typeof target[kind.name] !== 'function' || typeof target[kind.undo] !== 'function') continue;
		const prefix = kind.name + '\n';

		target[kind.name] = function (...args) {
			const channels = channelArgs(args);
			const id = ++callSeq;
			for (const channel of channels) inFlight.set(prefix + channel, id);
			const reply = send(kind.name, args);
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
			const reply = send(kind.undo, args);
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
		/** Stop for good. */
		end() {
			ended = true;
			cancelRetry();
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
 * that ends, or whose node a refreshed slot map of the Cluster no longer
 * lists, is replaced by a connection to a sampled node and the held channels
 * are restored there. The owner's calls
 * it had not answered go to the replacement in the order they were made, as
 * the Cluster would send them again; an undo with no subscribe ahead of it is
 * settled instead, since no server holds what it would undo.
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
	const maxAttempts = cluster.options.maxRedirections > 0 ? cluster.options.maxRedirections : DEFAULT_MAX_REDIRECTIONS;

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
		if (current !== null || closed || reopenTimer !== null) return;
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
		const node = nodes[Math.min(nodes.length - 1, Math.floor(randomFloat() * nodes.length))];
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
		// replaced then.
		conn.on('error', () => {
			if (conn === current && !proven) faulted = true;
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
			if (!proven) failedOpens++;
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
	// once it is refreshed. '-node' is no sign: ioredis also emits it when its
	// own connection to the node ends, which leaves this one working.
	const onRefresh = () => {
		if (current === null) return;
		let nodes = [];
		try { nodes = cluster.nodes('all'); } catch { return; }
		const { host, port } = current.options;
		if (!nodes.some((node) => node.options.host === host && node.options.port === port)) current.disconnect();
	};
	cluster.on('ready', onTopology);
	cluster.on('+node', onTopology);
	cluster.on('refresh', onRefresh);

	function close() {
		if (closed) return;
		closed = true;
		subs.end();
		if (reopenTimer !== null) {
			clearTimer(reopenTimer);
			reopenTimer = null;
		}
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
			// As ioredis stops its own subscriber: a QUIT would wait behind a
			// connect still in progress and keep the process alive meanwhile.
			conn.disconnect();
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
