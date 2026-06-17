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
