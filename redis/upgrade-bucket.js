/**
 * Per-IP upgrade-admission token bucket for svelte-adapter-uws.
 *
 * The adapter already ships an in-process per-IP sliding-window limiter on the
 * WebSocket upgrade path (the correct zero-config default, which stays). This
 * module adds what an in-process limiter cannot: CLUSTER-WIDE per-IP accounting
 * (an attacker spread across N workers is one IP, not N independent buckets) and
 * POSTURE-driven budgets (the same IP gets a tighter bucket when the server is
 * under pressure). It is wired inside the app's `upgrade` hook and returns a
 * boolean the adapter already maps to a clean rejection.
 *
 * The budget for a request is selected by a posture string the caller passes in
 * (`'normal'` / `'elevated'` / `'siege'`, or any custom posture name that maps to
 * a configured override). The posture is a plain argument, so this module is
 * independent of any adapter `platform.protection` surface - the caller forwards
 * whatever live posture it has.
 *
 * The Redis variant is a thin caller of the same atomic Lua token bucket the
 * application rate limiter ships (the shared `CONSUME_SCRIPT`), so there is no
 * new Lua and no new race surface. A Redis outage fails OPEN (admits)
 * through the shared circuit breaker, so a backend blip never locks out every
 * client. The local variant is a pure in-process bucket with a hard size cap and
 * LRU eviction, so the bucket map cannot itself become a memory-exhaustion vector.
 *
 * @module svelte-adapter-uws-extensions/redis/upgrade-bucket
 */

import { CONSUME_SCRIPT } from './token-bucket-script.js';
import { withBreaker } from '../shared/breaker.js';

/** Refill interval for a per-minute budget, in milliseconds. */
const MINUTE_MS = 60000;

/** Default ceiling on the local fallback's IP map before LRU eviction kicks in. */
const DEFAULT_MAX_LOCAL_ENTRIES = 100000;

/**
 * @typedef {Object} UpgradeBudget
 * @property {number} perMinute - Upgrades permitted per IP per minute in this posture.
 * @property {number} [blockDuration] - Auto-ban duration in ms once the budget is spent. 0 = no ban.
 */

/**
 * @typedef {Object} UpgradeBucketOptions
 * @property {number} perMinute - Default budget when posture is 'normal'. Must be a positive integer.
 * @property {number} [blockDuration=0] - Default auto-ban duration in ms once a budget is spent. 0 = no ban.
 * @property {UpgradeBudget} [elevated] - Budget override for the 'elevated' posture. Inherits 'normal' when omitted.
 * @property {UpgradeBudget} [siege] - Budget override for the 'siege' posture. Inherits 'elevated' when omitted.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Fail-open circuit breaker for the Redis call.
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics] - Prometheus metrics registry.
 */

/**
 * @typedef {Object} UpgradeBucket
 * @property {(ip: string, posture?: string) => Promise<boolean>} admit - True to admit, false to reject.
 * @property {(ip: string) => Promise<void>} reset - Clear the bucket for one IP.
 * @property {() => Promise<void>} clear - Reset all per-IP state.
 */

/**
 * Validate the base budget plus its per-posture overrides, returning a resolver
 * that maps a posture string to the effective `{ perMinute, blockDuration }`.
 *
 * Posture inheritance walks from looser to tighter: an omitted `elevated`
 * inherits `normal`; an omitted `siege` inherits the resolved `elevated`. An
 * unknown posture string resolves to the `normal` budget, so a caller forwarding
 * an unexpected posture degrades to the loosest gate rather than throwing on the
 * upgrade hot path.
 *
 * @param {UpgradeBucketOptions} options
 * @param {string} label - Module name used in thrown-error prefixes.
 */
function buildBudgets(options, label) {
	if (!options || typeof options !== 'object') {
		throw new Error(label + ': options object is required');
	}

	const baseBlock = options.blockDuration ?? 0;

	function validateBudget(b, where) {
		if (!Number.isInteger(b.perMinute) || b.perMinute <= 0) {
			throw new Error(label + ': ' + where + ' perMinute must be a positive integer');
		}
		const block = b.blockDuration ?? baseBlock;
		if (typeof block !== 'number' || !Number.isFinite(block) || block < 0) {
			throw new Error(label + ': ' + where + ' blockDuration must be a non-negative number');
		}
		return { perMinute: b.perMinute, blockDuration: block };
	}

	if (typeof baseBlock !== 'number' || !Number.isFinite(baseBlock) || baseBlock < 0) {
		throw new Error(label + ': blockDuration must be a non-negative number');
	}

	const normal = validateBudget({ perMinute: options.perMinute, blockDuration: baseBlock }, 'normal');

	let elevated = normal;
	if (options.elevated !== undefined) {
		if (!options.elevated || typeof options.elevated !== 'object') {
			throw new Error(label + ': elevated must be a budget object');
		}
		elevated = validateBudget(options.elevated, 'elevated');
	}

	let siege = elevated;
	if (options.siege !== undefined) {
		if (!options.siege || typeof options.siege !== 'object') {
			throw new Error(label + ': siege must be a budget object');
		}
		siege = validateBudget(options.siege, 'siege');
	}

	return function resolve(posture) {
		if (posture === 'siege') return siege;
		if (posture === 'elevated') return elevated;
		return normal;
	};
}

/**
 * Create a Redis-backed per-IP upgrade-admission bucket.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {UpgradeBucketOptions} options
 * @returns {UpgradeBucket}
 *
 * @example
 * ```js
 * import { createUpgradeBucket } from 'svelte-adapter-uws-extensions/redis/upgrade-bucket';
 *
 * const bucket = createUpgradeBucket(redis, {
 *   perMinute: 60,
 *   elevated: { perMinute: 20 },
 *   siege: { perMinute: 5 }
 * });
 *
 * export async function upgrade({ remoteAddress, platform }) {
 *   if (!(await bucket.admit(remoteAddress, platform.protection))) return false;
 *   // ... normal auth / session work ...
 * }
 * ```
 */
export function createUpgradeBucket(client, options) {
	const resolve = buildBudgets(options, 'redis upgrade-bucket');

	const redis = client.redis;

	// Version prefix isolates Lua-script key spaces across deploys, matching the
	// application rate limiter so a rolling change never reads a stale layout.
	const SCRIPT_VERSION = 'v1';

	const b = options.breaker;
	const m = options.metrics;
	const mAdmitted = m?.counter('upgrade_bucket_admitted_total', 'Upgrades admitted by the per-IP bucket');
	const mRejected = m?.counter('upgrade_bucket_rejected_total', 'Upgrades rejected by the per-IP bucket');
	const mFailOpen = m?.counter('upgrade_bucket_fail_open_total', 'Upgrades admitted because the Redis bucket was unavailable');

	function bucketKey(ip) {
		return client.key(SCRIPT_VERSION + ':upgrade-bucket:' + ip);
	}

	return {
		async admit(ip, posture) {
			const key = String(ip == null ? 'unknown' : ip);
			const budget = resolve(posture);

			let result;
			try {
				result = await withBreaker(b, () =>
					redis.eval(
						CONSUME_SCRIPT,
						1,
						bucketKey(key),
						budget.perMinute,
						MINUTE_MS,
						1,
						budget.blockDuration
					)
				);
			} catch {
				// Fail open: a Redis outage (or an open breaker) must never lock out
				// every client. Admit and let the adapter's in-process per-IP limiter
				// remain the floor.
				mFailOpen?.inc();
				return true;
			}

			const allowed = result[0] === 1;
			if (allowed) {
				mAdmitted?.inc();
			} else {
				mRejected?.inc();
			}
			return allowed;
		},

		async reset(ip) {
			const key = String(ip == null ? 'unknown' : ip);
			try {
				await withBreaker(b, () => redis.del(bucketKey(key)));
			} catch {
				// Best-effort administrative reset; a backend blip must not throw at
				// the caller. The bucket self-expires via the script's TTL regardless.
			}
		},

		async clear() {
			try {
				await withBreaker(b, () => scanAndClear(redis, client.key(SCRIPT_VERSION + ':upgrade-bucket:*')));
			} catch {
				/* best-effort clear */
			}
		}
	};
}

/**
 * SCAN + UNLINK the bucket's versioned key space. Inlined (rather than importing
 * the shared scanner) so this module pulls in only the Lua script and the breaker.
 */
async function scanAndClear(redis, pattern) {
	let cursor = '0';
	do {
		const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
		cursor = next;
		if (keys.length) {
			if (typeof redis.unlink === 'function') await redis.unlink(...keys);
			else await redis.del(...keys);
		}
	} while (cursor !== '0');
}

/**
 * Create a local (no-Redis) per-IP upgrade-admission bucket.
 *
 * A pure in-process token bucket with the same posture-keyed budgets, backed by a
 * `Map<ip, entry>` with a hard size cap (default 100000) and LRU eviction so the
 * map itself cannot become a memory-exhaustion vector under a spoofed-IP flood.
 * For deployments without Redis, or as the in-process floor beneath the Redis
 * bucket.
 *
 * @param {UpgradeBucketOptions & { maxEntries?: number }} options
 * @returns {UpgradeBucket}
 */
export function createLocalUpgradeBucket(options) {
	const resolve = buildBudgets(options, 'local upgrade-bucket');

	const maxEntries = options.maxEntries ?? DEFAULT_MAX_LOCAL_ENTRIES;
	if (!Number.isInteger(maxEntries) || maxEntries <= 0) {
		throw new Error('local upgrade-bucket: maxEntries must be a positive integer');
	}

	const m = options.metrics;
	const mAdmitted = m?.counter('upgrade_bucket_admitted_total', 'Upgrades admitted by the per-IP bucket');
	const mRejected = m?.counter('upgrade_bucket_rejected_total', 'Upgrades rejected by the per-IP bucket');
	const mEvicted = m?.counter('upgrade_bucket_evicted_total', 'IP entries evicted from the local bucket by the size cap');

	// Insertion-ordered Map doubles as the LRU recency list: re-touching an IP
	// deletes and re-inserts it, so the oldest live key is always the Map's first
	// entry. The cap evicts from the front when a NEW IP would exceed it.
	/** @type {Map<string, { tokens: number, resetAt: number, bannedUntil: number }>} */
	const entries = new Map();

	function evictIfNeeded() {
		while (entries.size >= maxEntries) {
			const oldest = entries.keys().next().value;
			if (oldest === undefined) break;
			entries.delete(oldest);
			mEvicted?.inc();
		}
	}

	function touch(key) {
		const existing = entries.get(key);
		if (existing) {
			// Move to the most-recently-used end.
			entries.delete(key);
			entries.set(key, existing);
			return existing;
		}
		evictIfNeeded();
		const fresh = { tokens: 0, resetAt: 0, bannedUntil: 0 };
		entries.set(key, fresh);
		return fresh;
	}

	return {
		async admit(ip, posture) {
			const key = String(ip == null ? 'unknown' : ip);
			const budget = resolve(posture);
			const now = Date.now();
			const e = touch(key);

			// Lazily initialize / refill on a fresh interval.
			if (e.resetAt === 0 || e.resetAt <= now) {
				e.tokens = budget.perMinute;
				e.resetAt = now + MINUTE_MS;
				e.bannedUntil = 0;
			}

			if (e.bannedUntil > now) {
				mRejected?.inc();
				return false;
			}

			if (e.tokens >= 1) {
				e.tokens -= 1;
				mAdmitted?.inc();
				return true;
			}

			if (budget.blockDuration > 0) {
				e.bannedUntil = now + budget.blockDuration;
			}
			mRejected?.inc();
			return false;
		},

		async reset(ip) {
			entries.delete(String(ip == null ? 'unknown' : ip));
		},

		async clear() {
			entries.clear();
		}
	};
}
