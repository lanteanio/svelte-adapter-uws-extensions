import { setTimer, clearTimer } from './runtime.js';

/**
 * Parse the major-version number out of an `INFO server` payload.
 * Returns null if the payload doesn't include `redis_version:` or the
 * major-version is unparseable.
 *
 * @param {unknown} info
 * @returns {number | null}
 */
export function parseRedisVersion(info) {
	if (typeof info !== 'string') return null;
	const m = info.match(/^redis_version:([0-9.]+)/m);
	if (!m) return null;
	const major = parseInt(m[1].split('.')[0], 10);
	return Number.isFinite(major) ? major : null;
}

/**
 * Whether the connected server provides per-field hash TTL (HEXPIRE / HPEXPIRE)
 * from an `INFO server` payload. Redis added these in 7.4. Valkey added them in
 * 9.0, but pins `redis_version` at 7.2.4 forever while reporting its real version
 * in `valkey_version` (with `server_name:valkey`) - so a Valkey 9.0 server, which
 * DOES have the commands, looks like Redis 7.2.4 to a naive `redis_version` check.
 *
 * Returns `{ supported, server, version }`. `supported` is null when the payload
 * cannot be parsed, so the caller can assume compatibility rather than locking
 * out an unrecognized server.
 *
 * @param {unknown} info
 * @returns {{ supported: boolean | null, server: 'redis' | 'valkey', version: string }}
 */
export function hashFieldTTLSupport(info) {
	if (typeof info !== 'string') return { supported: null, server: 'redis', version: '' };
	// Valkey identifies itself with server_name:valkey and reports its real
	// version in valkey_version; either marker is enough to route to the Valkey
	// floor (9.0) rather than the pinned redis_version:7.2.4.
	if (/^server_name:valkey/m.test(info) || /^valkey_version:/m.test(info)) {
		const m = /^valkey_version:(\d+)\.(\d+)/m.exec(info);
		if (!m) return { supported: null, server: 'valkey', version: '' };
		return { supported: Number(m[1]) >= 9, server: 'valkey', version: m[1] + '.' + m[2] };
	}
	const m = /^redis_version:(\d+)\.(\d+)/m.exec(info);
	if (!m) return { supported: null, server: 'redis', version: '' };
	const major = Number(m[1]);
	const minor = Number(m[2]);
	return { supported: major > 7 || (major === 7 && minor >= 4), server: 'redis', version: m[1] + '.' + m[2] };
}

// Upper bound on how long `ready()` waits for the probe's INFO before falling
// back to the safe (unsupported) answer. During an outage the INFO can sit on
// ioredis's offline queue without ever resolving OR rejecting, so a raw await
// would stall the hot publish path; this caps that stall. A healthy INFO
// answers in well under this, so the bound never false-negatives a live server.
const DEFAULT_HEXPIRE_PROBE_TIMEOUT_MS = 1000;

/**
 * A cached, SOFT per-field-hash-TTL (HEXPIRE / HPEXPIRE) capability probe. Unlike
 * the presence store's hard gate - which throws when the server is too old
 * because presence has no non-HEXPIRE implementation - this is for stores whose
 * per-field TTL is an OPTIMIZATION layered over a whole-key-EXPIRE fallback: the
 * dedup cache, the right-to-erasure indexes. It never throws and never locks a
 * caller out; it resolves a boolean the caller uses to pick the per-field or the
 * fallback path.
 *
 * Semantics tuned for a soft gate (the inverse of `hashFieldTTLSupport`'s
 * assume-compatible default):
 *   - Definitively supported (Redis 7.4+/Valkey 9.0+) => `true` (use HEXPIRE).
 *   - Definitively too old, OR an unparseable INFO (cannot confirm support) =>
 *     `false` (use the whole-key EXPIRE fallback; never HPEXPIRE a server that
 *     might reject it). An unrecognized-but-modern server thus forgoes the
 *     optimization rather than risking an `ERR unknown command` on the hot path.
 *   - A transient INFO failure leaves the result UNKNOWN and re-probes next call
 *     (this call reads `false`, the safe fallback).
 *
 * `supported()` is a synchronous best-effort read (kicks the probe, returns the
 * last known answer, `false` until the first probe resolves) for a hot path that
 * cannot await; `ready()` awaits the in-flight probe so a call that CAN await
 * gets the definitive answer from its first use. `ready()` bounds that wait: the
 * probe's INFO can sit on ioredis's offline queue indefinitely during an outage
 * (it never rejects, so it is not caught as a transient failure), so `ready()`
 * races it against a `timeoutMs` timer and returns the safe fallback rather than
 * stalling the caller. The timer is armed only while a probe is genuinely in
 * flight - once the answer is cached, `ready()` returns it without a timer.
 *
 * A reconnect can land on a DIFFERENT server (a failover to an older replica, a
 * rolling downgrade), so the cached capability must not outlive the connection
 * that produced it - a stale `true` would make the hot publish path HPEXPIRE a
 * server that rejects it (`ERR unknown command`). The probe drops its cached
 * answer on every `ready` event so the next call re-probes the current server,
 * and exposes `invalidate()` so a caller that catches an unknown-command error
 * can force the same re-detection. `invalidate()` also drops any still-stalled
 * probe so a forced re-detection is not blocked behind an INFO stuck on the
 * offline queue.
 *
 * @param {any} redis an ioredis Redis / Cluster instance (or the test double)
 * @param {{ timeoutMs?: number }} [options] `timeoutMs` caps how long `ready()`
 *   waits for the probe INFO before returning the safe fallback (default 1000).
 * @returns {{ supported: () => boolean, ready: () => Promise<boolean>, invalidate: () => void }}
 */
export function createHashFieldTTLProbe(redis, options = {}) {
	const timeoutMs = options.timeoutMs ?? DEFAULT_HEXPIRE_PROBE_TIMEOUT_MS;
	/** @type {boolean | null} null = not yet probed */
	let known = null;
	/** @type {Promise<void> | null} the in-flight INFO (may stall on the offline queue during an outage) */
	let inflight = null;
	// Bumped by invalidate() (and thus by the reconnect handler) so a reply that
	// races in after an invalidation cannot write a now-stale answer into `known`.
	let generation = 0;

	function trigger() {
		if (inflight || known !== null) return;
		const myGen = generation;
		const p = Promise.resolve()
			.then(() => redis.info('server'))
			.then((info) => {
				// Discard a reply that landed after an invalidate(): the server it
				// describes is no longer the connection we probed.
				if (myGen === generation) known = hashFieldTTLSupport(info).supported === true;
			})
			.catch(() => { /* transient: leave unknown so the next call re-probes */ })
			// Only the probe that still owns the slot clears it; a stale probe that
			// settles after invalidate() re-pointed inflight must not null a newer one.
			.finally(() => { if (inflight === p) inflight = null; });
		inflight = p;
	}

	function invalidate() {
		known = null;
		generation++;
		// Drop the reference to any pending probe so the next call re-probes
		// immediately instead of waiting behind an INFO stalled on the offline
		// queue; that probe is now stale (generation moved) and cannot write back.
		inflight = null;
	}

	// Re-detect after any (re)connect; guarded so a test double / non-emitter
	// client (which never reconnects) is a no-op.
	if (typeof redis.on === 'function') redis.on('ready', invalidate);
	return {
		supported() { trigger(); return known === true; },
		async ready() {
			trigger();
			const p = inflight;
			if (p) {
				let timer = null;
				const timed = new Promise((resolve) => { timer = setTimer(resolve, timeoutMs); });
				try { await Promise.race([p, timed]); }
				finally { clearTimer(timer); }
			}
			return known === true;
		},
		invalidate
	};
}
