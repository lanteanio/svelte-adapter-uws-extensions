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
