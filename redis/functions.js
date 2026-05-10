/**
 * Redis Functions (FUNCTION LOAD / FCALL) wrapper for
 * svelte-adapter-uws-extensions.
 *
 * `createFunctionLibrary` takes a Lua function library declaration
 * (with the `#!lua name=<libname>` shebang Redis 7+ requires) and
 * exposes `load()` / `call()` / `delete()` against the server.
 *
 * Functions are versioned and hot-reloadable: `load()` issues
 * `FUNCTION LOAD REPLACE`, so an updated library swaps in atomically
 * without an app redeploy. `call(funcName, { keys, args })` issues
 * `FCALL` against the function (note: FCALL keys on function name,
 * not library name - function names are global across the server).
 *
 * Requires Redis 7+. `load()` runs `INFO server` on first call and
 * throws on older servers; users on Redis 6 should use `redis.eval`
 * directly with their own SHA caching.
 *
 * @module svelte-adapter-uws-extensions/redis/functions
 */

import { parseRedisVersion } from '../shared/redis-version.js';
import { withBreaker } from '../shared/breaker.js';

const SHEBANG_RE = /^#!lua\s+name=(\S+)/;

/**
 * @typedef {Object} FunctionLibraryOptions
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 */

/**
 * @typedef {Object} FunctionLibrary
 * @property {string} name - Library name parsed from the `#!lua` shebang.
 * @property {() => Promise<void>} load - Load (or replace) the library on the server.
 * @property {(funcName: string, opts?: { keys?: ReadonlyArray<string>, args?: ReadonlyArray<string | number> }) => Promise<unknown>} call - Invoke a registered function via FCALL.
 * @property {() => Promise<void>} delete - Drop the library via FUNCTION DELETE.
 */

/**
 * Create a function-library handle.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {string} code - Library source. Must start with `#!lua name=<name>`.
 * @param {FunctionLibraryOptions} [options]
 * @returns {FunctionLibrary}
 *
 * @example
 * ```js
 * const lib = createFunctionLibrary(redis, `#!lua name=ws-presence
 *   redis.register_function('cleanup', function(keys, args)
 *     - ...
 *   end)
 * `);
 * await lib.load();
 * await lib.call('cleanup', { keys: ['presence:room1'], args: [Date.now(), 90000] });
 * ```
 */
export function createFunctionLibrary(client, code, options = {}) {
	if (typeof code !== 'string' || code.length === 0) {
		throw new Error('redis functions: code must be a non-empty string');
	}
	const shebang = code.match(SHEBANG_RE);
	if (!shebang) {
		throw new Error('redis functions: code must start with `#!lua name=<libname>` shebang');
	}
	const name = shebang[1];

	const b = options.breaker;
	const m = options.metrics;
	const mLoads = m?.counter('redis_function_loads_total', 'FUNCTION LOAD calls', ['library']);
	const mCalls = m?.counter('redis_function_calls_total', 'FCALL calls', ['library', 'function']);
	const mErrors = m?.counter('redis_function_errors_total', 'FCALL errors', ['library', 'function']);

	let versionChecked = false;

	async function ensureVersion() {
		if (versionChecked) return;
		b?.guard();
		let info;
		try {
			info = await client.redis.info('server');
		} catch (err) {
			b?.failure(err);
			throw err;
		}
		const major = parseRedisVersion(info);
		if (major === null) {
			b?.failure(new Error('could not parse Redis version'));
			throw new Error('redis functions: could not parse Redis version from INFO server');
		}
		if (major < 7) {
			const err = new Error(`redis functions: requires Redis 7 or newer (server reports ${major}). Use redis.eval directly for older servers.`);
			b?.failure(err);
			throw err;
		}
		b?.success();
		versionChecked = true;
	}

	return {
		get name() { return name; },

		async load() {
			await ensureVersion();
			await withBreaker(b, () => client.redis.function('LOAD', 'REPLACE', code));
			mLoads?.inc({ library: name });
		},

		async call(funcName, opts = {}) {
			if (typeof funcName !== 'string' || funcName.length === 0) {
				throw new Error('redis functions: funcName must be a non-empty string');
			}
			const keys = opts.keys || [];
			const args = opts.args || [];
			if (!Array.isArray(keys) || !Array.isArray(args)) {
				throw new Error('redis functions: keys and args must be arrays');
			}

			let result;
			try {
				result = await withBreaker(b, () => client.redis.fcall(funcName, keys.length, ...keys, ...args));
			} catch (err) {
				mErrors?.inc({ library: name, function: funcName });
				throw err;
			}
			mCalls?.inc({ library: name, function: funcName });
			return result;
		},

		async delete() {
			await withBreaker(b, () => client.redis.function('DELETE', name));
		}
	};
}
