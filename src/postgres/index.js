/**
 * Postgres client factory for svelte-adapter-uws-extensions.
 *
 * Wraps pg Pool with lifecycle management and graceful shutdown
 * via the SvelteKit `sveltekit:shutdown` event.
 *
 * @module svelte-adapter-uws-extensions/postgres
 */

import pg from 'pg';
import { ConnectionError } from '../shared/errors.js';
import { redactConnectionUrl } from '../shared/sensitive.js';

const { Pool, Client } = pg;

/**
 * @typedef {Object} PgClientOptions
 * @property {string} [connectionString] - Postgres connection string. Required UNLESS `pool` is provided.
 * @property {import('pg').Pool} [pool] - An existing `pg.Pool` to wrap instead of constructing a new one. Use when your app already maintains a pool (raw `pg` use elsewhere, another framework integration) and you want a single connection footprint against the database. When provided, `autoShutdown` defaults to `false` (the caller owns the pool's lifecycle) and `end()` becomes a no-op.
 * @property {boolean} [autoShutdown=true] - Listen for `sveltekit:shutdown` and disconnect. Defaults to `false` when `pool` is provided.
 * @property {import('pg').PoolConfig} [options] - Extra pg Pool options. Ignored when `pool` is provided.
 */

/**
 * @typedef {Object} PgClient
 * @property {import('pg').Pool} pool - The underlying pg Pool
 * @property {(text: string, values?: any[]) => Promise<import('pg').QueryResult>} query - Run a query
 * @property {() => Promise<void>} end - Gracefully close the pool. When the pool was provided externally, this is a no-op (the caller owns the lifecycle).
 */

/**
 * Create a Postgres client.
 *
 * Two construction modes:
 *
 *   1. Pass `connectionString`: a fresh `pg.Pool` is created and owned by
 *      this client. `end()` closes the pool; `autoShutdown` (default `true`)
 *      attaches a `sveltekit:shutdown` listener.
 *
 *   2. Pass `pool` (an existing `pg.Pool`): the client wraps the provided
 *      pool without constructing its own. `autoShutdown` defaults to `false`
 *      and `end()` is a no-op - the caller is responsible for closing the
 *      pool. Use this when your app already maintains a pool (e.g. shared
 *      with raw `pg` use elsewhere) and wants a single connection footprint
 *      against the database.
 *
 * `connectionString` and `pool` are mutually exclusive in spirit, but you may
 * pass `connectionString` alongside `pool` to enable `createClient()` (a
 * dedicated `pg.Client` for LISTEN/NOTIFY) without losing the
 * shared-pool ownership story.
 *
 * @param {PgClientOptions} opts
 * @returns {PgClient}
 */
export function createPgClient(opts) {
	if (!opts) {
		throw new ConnectionError('postgres', 'connectionString or pool is required');
	}
	const externalPool = opts.pool;
	if (externalPool === undefined && !opts.connectionString) {
		throw new ConnectionError('postgres', 'connectionString or pool is required');
	}
	if (externalPool !== undefined && (typeof externalPool !== 'object' || typeof externalPool.query !== 'function')) {
		throw new ConnectionError('postgres', 'pool must be a pg.Pool instance');
	}

	const ownedPool = externalPool === undefined;
	// Default: auto-shutdown the pool we own. Caller-owned pools default to
	// no auto-shutdown (the caller drives the lifecycle), but the option is
	// still honored if explicitly set.
	const autoShutdown = opts.autoShutdown !== undefined ? opts.autoShutdown !== false : ownedPool;

	let pool;
	if (ownedPool) {
		try {
			pool = new Pool({
				connectionString: opts.connectionString,
				...(opts.options || {})
			});
		} catch (err) {
			throw new ConnectionError('postgres', 'failed to create pool', err);
		}
		pool.on('error', (err) => {
			// Idle client errors should not crash the process.
			// pg Pool handles reconnection automatically.
			// pg embeds the connection DSN in some failure-mode message
			// strings (auth failed, host unreachable, SSL mismatch); pipe
			// through redactConnectionUrl so the password segment never
			// reaches stderr / log aggregators.
			const message = typeof err?.message === 'string' ? redactConnectionUrl(err.message) : String(err);
			console.error('postgres: idle client error', message);
		});
	} else {
		pool = externalPool;
	}

	let ended = false;

	async function end() {
		if (ended) return;
		ended = true;
		// Only close pools we own. The caller-provided pool is never ours
		// to close; this is a deliberate no-op so graceful-shutdown paths
		// can call end() unconditionally without worrying about ownership.
		if (ownedPool) {
			await pool.end();
		}
	}

	if (autoShutdown && typeof process !== 'undefined') {
		process.once('sveltekit:shutdown', end);
	}

	// createClient() needs a connectionString. If the user only passed a
	// pool, we don't have one - defer the error until createClient() is
	// actually invoked, since most consumers (the task runner, idempotency
	// stores, etc.) only need pool.query() and never createClient().
	const connectionConfig = opts.connectionString
		? { connectionString: opts.connectionString, ...(opts.options || {}) }
		: null;

	return {
		pool,

		query(text, values) {
			return pool.query(text, values).catch(redactAndRethrow);
		},

		/**
		 * Acquire a dedicated connection from the pool. Use this rather than
		 * `pool.connect()` directly: connection ACQUISITION is the failure
		 * mode whose pg error text carries the DSN, so it is the one that
		 * most needs redacting, and a raw handle would bypass it entirely.
		 * The returned client's `query` redacts too, and `release` is
		 * unchanged.
		 */
		async connect() {
			const client = await pool.connect().catch(redactAndRethrow);
			try {
				return wrapClientQuery(client);
			} catch (err) {
				// The connection is already checked out. A caller-supplied
				// pool can hand back a frozen, sealed or proxied client that
				// refuses the wrap, and leaking the checkout would exhaust the
				// pool (pg's default max is 10) with no way back.
				try { client.release?.(err); } catch { /* release itself failed; nothing left to do */ }
				throw err;
			}
		},

		createClient() {
			if (!connectionConfig) {
				throw new ConnectionError(
					'postgres',
					'createClient() requires connectionString - pass it alongside pool when wrapping an external pool'
				);
			}
			const client = new Client(connectionConfig);
			const rawConnect = client.connect.bind(client);
			// LISTEN/NOTIFY runs on a dedicated client, and its connect() is
			// the DSN-bearing path.
			client.connect = (...args) => {
				const out = rawConnect(...args);
				return out && typeof out.catch === 'function' ? out.catch(redactAndRethrow) : out;
			};
			return wrapClientQuery(client);
		},

		end
	};
}

/** Marks a client whose `query` this module has already wrapped. */
const QUERY_WRAPPED = Symbol.for('svti.pg.queryWrapped');

/**
 * Fallback marker for a client that refuses the symbol (sealed or
 * non-extensible). The symbol stays primary because it is the only marker
 * that survives a duplicated module instance; this set only has to stop the
 * same instance from stacking wrappers on the same client.
 */
const localWrapped = new WeakSet();

/**
 * Wrap `client.query` so its rejections are redacted, exactly once per
 * client object.
 *
 * The once-only guard is load-bearing, not tidiness: `pg-pool` hands back
 * the SAME client object on every acquire and `release()` does not restore
 * the method, so wrapping unconditionally would stack one closure per
 * checkout. That degrades every query on the connection and eventually
 * throws `RangeError: Maximum call stack size exceeded` - measured at
 * roughly 8,350 transactions on one pooled connection.
 *
 * All three pg call shapes are preserved: the callback form returns
 * `undefined` and must still invoke its callback, a submittable (pg-cursor,
 * pg-query-stream) is returned as-is, and only the promise form is
 * `.catch`-wrapped.
 *
 * @template T
 * @param {T} client
 * @returns {T}
 */
function wrapClientQuery(client) {
	if (!client || client[QUERY_WRAPPED] || localWrapped.has(client)) return client;
	if (typeof client.query !== 'function') return client;
	const rawQuery = client.query.bind(client);
	// Swapped FIRST, and deliberately unguarded. A FROZEN client throws here;
	// connect() then releases the checkout and rethrows, because redaction is
	// genuinely impossible for that client and failing loudly beats handing
	// back a handle that leaks the DSN on every error.
	client.query = (...args) => {
		const last = args[args.length - 1];
		if (typeof last === 'function') {
			args[args.length - 1] = (err, res) => last(err ? redactError(err) : err, res);
			return rawQuery(...args);
		}
		const out = rawQuery(...args);
		return out && typeof out.catch === 'function' ? out.catch(redactAndRethrow) : out;
	};
	// Marked only after the swap succeeded, and never at the cost of a throw.
	// A SEALED (or otherwise non-extensible) client accepts the swap above
	// but refuses a NEW property here - so marking first, or letting this
	// escape, failed every connect() on such a pool over a bookkeeping flag,
	// on a client that had just been wrapped successfully.
	try {
		Object.defineProperty(client, QUERY_WRAPPED, { value: true, enumerable: false, configurable: true });
	} catch {
		localWrapped.add(client);
	}
	return client;
}

/**
 * pg embeds the connection DSN in several failure-mode messages (auth
 * failed, host unreachable, SSL mismatch), and those errors reach logs,
 * error trackers and consumer-forwarded responses through every store.
 *
 * Returns a redacted COPY rather than mutating the original. `.stack` is
 * lazily materialized from `.message`, so assigning `err.message` fixes
 * `.stack` only when nothing has read it yet - any logger or pg internal
 * that touched it first pins the DSN in place. A caller-supplied pool can
 * also hand back a frozen error, where the assignment would throw and
 * destroy the original error entirely.
 *
 * The copy keeps the original prototype (so `err instanceof
 * pg.DatabaseError` still holds) and every own property (so `position`,
 * `hint`, `where`, `detail` and the rest - the fields that make a SQL error
 * diagnosable - survive), with string values passed through the redactor.
 *
 * @param {any} err
 * @returns {any}
 */
function redactError(err, seen) {
	if (typeof err === 'string') return redactConnectionUrl(err);
	if (!err || (typeof err !== 'object' && typeof err !== 'function')) return err;
	// Cycles: pg wraps errors in errors (pg-pool's connection-timeout error
	// carries the original as `cause`), and a self-referential chain must not
	// spin here.
	//
	// Memoizes the COPY, not merely "visited". A visited-set hands back the
	// RAW error on the second reference to the same object, so a cycle - or
	// a plain diamond, where the original is reachable as both `cause` and
	// some other key - put the unredacted DSN straight back into the output
	// the redactor was hiding it from. Registered BEFORE the walk so a cycle
	// resolves to this same copy once it is filled in.
	if (!seen) seen = new Map();
	const memo = seen.get(err);
	// An error's own properties are always walked from depth 0, so a memoized
	// error copy is always the most complete one there is.
	if (memo !== undefined) return memo.copy;

	// Guarded: a proxy can throw from its `getPrototypeOf` trap, and a revoked
	// one throws from every trap. An unguarded read here makes the REDACTOR
	// throw, and the caller receives that instead of its own error.
	let proto;
	try {
		proto = Object.getPrototypeOf(err);
	} catch {
		return unreadableErrorCopy(err);
	}
	const safe = Object.create(proto);
	seen.set(err, { copy: safe, depth: 0 });
	// `message` and `stack` are re-defined below with their original hidden
	// shape. Copying them here first would carry over a FROZEN error's
	// non-configurable descriptor, and the redefine would then throw
	// TypeError out of the redactor - destroying the DatabaseError and every
	// diagnostic on it (code, constraint, detail), which is what any store
	// retrying on `err.code === '40001'` reads.
	// Read ONCE, and guarded. `stack` is an accessor on V8 errors, and a
	// source-map or APM hook can install one that throws; an unguarded read
	// makes the REDACTOR throw, and the caller then receives that error
	// instead of the DatabaseError - losing `code`, `constraint` and `detail`,
	// which is exactly what a store retrying on `err.code === '40001'` reads.
	let ownMessage;
	try { ownMessage = err.message; } catch { ownMessage = undefined; }
	let ownStack;
	try { ownStack = err.stack; } catch { ownStack = undefined; }
	// Always claimed, even when the first read gave a non-string or threw.
	// Otherwise the loop below reaches the accessor and reads it a SECOND
	// time, so a non-idempotent getter is invoked twice and the copy takes
	// whichever value the second call happened to produce.
	const redefined = new Set(['message', 'stack']);
	// Guarded: `ownKeys` is a proxy trap, and it can throw or return
	// duplicates (which makes the for-of itself throw).
	let keys;
	try {
		keys = Reflect.ownKeys(err);
	} catch {
		return unreadableErrorCopy(err);
	}
	for (const k of keys) {
		if (redefined.has(k)) continue;
		let desc;
		try {
			desc = Object.getOwnPropertyDescriptor(err, k);
		} catch {
			continue;
		}
		if (!desc) continue;
		// Descriptors, not assignment. Assigning would make `message` and
		// `stack` own ENUMERABLE properties, and an app that does
		// `JSON.stringify(err)` into an error response would start shipping
		// the whole server stack trace to the client - an information
		// disclosure introduced by a redactor.
		if ('value' in desc) {
			try {
				desc = { ...desc, value: redactValue(desc.value, seen) };
			} catch {
				// The walk failed part-way (a proxy trap that throws, or a
				// RangeError from a pathological chain). Keeping the original
				// descriptor would put the RAW value - and any DSN inside it -
				// onto the copy the walk exists to sanitize, so the value is
				// dropped to a placeholder instead. Losing one diagnostic
				// field beats publishing the credential.
				desc = { ...desc, value: '[redacted: unreadable]' };
			}
		} else if (typeof desc.get === 'function') {
			// An accessor copied verbatim installs the ORIGINAL getter on the
			// copy, so reading the redacted error re-invokes it and returns
			// the DSN this walk exists to hide. Snapshot it through the getter
			// and store the redacted result: a redacted copy is a record for
			// logs, and a live accessor back onto the raw error has no meaning
			// on it.
			let snapshot;
			try {
				snapshot = redactValue(err[k], seen);
			} catch {
				// Getter threw. Dropping the key loses one diagnostic field;
				// keeping the accessor would leak, so drop it.
				continue;
			}
			desc = {
				value: snapshot,
				writable: true,
				enumerable: desc.enumerable,
				configurable: true
			};
		}
		try {
			Object.defineProperty(safe, k, desc);
		} catch { /* non-configurable on the fresh object is not possible, but never lose the error over it */ }
	}
	// message and stack keep their ORIGINAL enumerability. Forcing them
	// non-enumerable is right for a real Error (where they already are, and
	// where making them enumerable would ship the stack into any
	// JSON.stringify'd error response), but a rejection that is a plain
	// object carries an enumerable `message`, and hiding it there deletes the
	// message from every structured log line.
	// Both are claimed above so the descriptor loop never re-reads them, which
	// means both shapes have to be handled here: the ordinary string, and a
	// value that is present but not a string (which the loop would otherwise
	// have copied).
	for (const [key, own] of [['message', ownMessage], ['stack', ownStack]]) {
		if (own === undefined) continue;
		const value = typeof own === 'string' ? redactConnectionUrl(own) : redactValue(own, seen);
		defineCopied(safe, key, value, ownEnumerable(err, key));
	}
	return safe;
}

/**
 * How deep to walk plain containers hanging off an error. Deep enough for
 * the shapes pg and its wrappers actually produce, shallow enough that a
 * pathological structure cannot turn error handling into a graph traversal.
 */
const REDACT_MAX_DEPTH = 6;

/**
 * Redact one property value. Nested errors matter: `util.inspect` prints
 * `cause` and `errors`, which is exactly what `console.error(err)` and every
 * structured logger's error serializer do, so a redacted outer message with
 * a raw inner one leaks anyway.
 *
 * Arrays and plain objects are walked for the same reason: a DSN one level
 * inside `err.details` or `err.errors[0].hint` prints exactly as loudly as
 * one on `message`. Only plain containers are rebuilt - a class instance
 * (a Client, a Socket) is passed through untouched rather than shallow-copied
 * into something that is no longer that class.
 *
 * @param {any} v
 * @param {Map<object, any>} seen
 * @param {number} [depth]
 */
function redactValue(v, seen, depth = 0) {
	if (typeof v === 'string') return redactConnectionUrl(v);
	if (isErrorLike(v)) return redactError(v, seen);
	if (!v || typeof v !== 'object') return v;
	// Past the cap the subtree is NOT emitted. Returning it raw hands back
	// every string inside it unredacted, so a DSN one level deeper than the
	// walk goes is published by the redactor itself - and because a copy
	// built under this cap is memoized, that raw subtree then propagates up
	// to any shallower reference to the same object. A placeholder keeps the
	// bound the cap exists for without either leak. Strings and Errors are
	// handled above, so they stay redacted at any depth.
	if (depth >= REDACT_MAX_DEPTH) return '[redacted: max depth]';
	// The memo records the DEPTH its copy was built at. A copy built deep
	// carries placeholders for everything past the cap, and handing that same
	// copy back to a SHALLOWER reference to the object would truncate a
	// subtree the walk had budget to finish - making the output depend on
	// which reference the walk happened to reach first. A strictly shallower
	// encounter rebuilds. Depth only increases within a cycle, so a cycle
	// always takes the reuse branch, and the rebuild count per object is
	// bounded by REDACT_MAX_DEPTH.
	if (Array.isArray(v)) {
		const memo = seen.get(v);
		if (memo !== undefined && memo.depth <= depth) return memo.copy;
		const out = [];
		seen.set(v, { copy: out, depth });
		for (const e of v) out.push(redactValue(e, seen, depth + 1));
		return out;
	}
	let proto;
	try {
		proto = Object.getPrototypeOf(v);
	} catch {
		return '[redacted: unreadable]';
	}
	// A plain object is one whose prototype is a ROOT. An identity check
	// against this realm's `Object.prototype` misses a plain object from
	// another realm (a vm context, a worker), which was then returned raw
	// with whatever DSN it carried. Class instances still fail this, because
	// their prototype's prototype is `Object.prototype`, not null.
	if (proto !== null) {
		let protoProto;
		try {
			protoProto = Object.getPrototypeOf(proto);
		} catch {
			return '[redacted: unreadable]';
		}
		if (protoProto !== null) return v;
	}
	const memo = seen.get(v);
	if (memo !== undefined && memo.depth <= depth) return memo.copy;
	const out = Object.create(proto);
	seen.set(v, { copy: out, depth });
	for (const k of Object.keys(v)) {
		try { out[k] = redactValue(v[k], seen, depth + 1); } catch { /* getter-only */ }
	}
	return out;
}

/**
 * Fallback when the error itself refuses to be inspected - a proxy whose
 * `getPrototypeOf` or `ownKeys` trap throws, or a revoked one. Returns a
 * plain redacted record rather than letting the redactor throw and replace
 * the caller's error with its own.
 *
 * @param {any} err
 * @returns {any}
 */
function unreadableErrorCopy(err) {
	const out = new Error('[redacted: error could not be inspected]');
	try {
		const m = err.message;
		if (typeof m === 'string') out.message = redactConnectionUrl(m);
	} catch { /* the trap that got us here */ }
	defineCopied(out, 'stack', out.message);
	return out;
}

/**
 * Is this an Error from ANY realm? `instanceof` is realm-bound, so an error
 * crossing a `vm` context or a worker boundary fails it, falls through to
 * the plain-object test, and is returned RAW with its message - the one
 * field most likely to carry a DSN - unredacted.
 *
 * @param {any} v
 * @returns {boolean}
 */
function isErrorLike(v) {
	if (v instanceof Error) return true;
	try {
		return Object.prototype.toString.call(v) === '[object Error]';
	} catch {
		return false;
	}
}

/**
 * Was `key` an own ENUMERABLE property of `obj`? Non-enumerable on a real
 * Error, enumerable on a plain-object rejection.
 *
 * @param {any} obj
 * @param {string} key
 * @returns {boolean}
 */
function ownEnumerable(obj, key) {
	try {
		const d = Object.getOwnPropertyDescriptor(obj, key);
		return d !== undefined && d.enumerable === true;
	} catch {
		return false;
	}
}

/**
 * @param {object} target
 * @param {string} key
 * @param {any} value
 * @param {boolean} [enumerable=false]
 */
function defineCopied(target, key, value, enumerable = false) {
	Object.defineProperty(target, key, { value, writable: true, enumerable, configurable: true });
}

/**
 * @param {any} err
 * @returns {never}
 */
function redactAndRethrow(err) {
	throw redactError(err);
}
