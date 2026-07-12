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
 * gets the definitive answer from its first use.
 *
 * @param {any} redis an ioredis Redis / Cluster instance (or the test double)
 * @returns {{ supported: () => boolean, ready: () => Promise<boolean> }}
 */
export function createHashFieldTTLProbe(redis) {
	/** @type {boolean | null} null = not yet probed */
	let known = null;
	/** @type {Promise<void> | null} */
	let inflight = null;
	function trigger() {
		if (inflight || known !== null) return;
		inflight = Promise.resolve()
			.then(() => redis.info('server'))
			.then((info) => { known = hashFieldTTLSupport(info).supported === true; })
			.catch(() => { /* transient: leave unknown so the next call re-probes */ })
			.finally(() => { inflight = null; });
	}
	return {
		supported() { trigger(); return known === true; },
		async ready() { trigger(); if (inflight) await inflight; return known === true; }
	};
}
