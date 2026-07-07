/**
 * Composite multi-dimension rate limiter: several budgets, one atomic verdict.
 *
 * A single request is often bounded along more than one axis at once - the
 * account it acts for, the endpoint it hits, the action it performs. Checking
 * those as separate limiters races: a burst can pass the account check on
 * every in-flight request before any endpoint counter increments, so each
 * dimension alone under-counts. This limiter consults EVERY dimension in one
 * atomic script - most-strict-wins - and consumes from all of them only when
 * all of them have budget: either the whole request is admitted and every
 * bucket pays, or nothing is consumed anywhere and the verdict names the
 * dimension that tripped.
 *
 * Redis Cluster: a multi-key script requires all keys on one hash slot, so
 * every dimension key for a check embeds one shared hash tag - `{t:<tenant>}`
 * (or `{t:}` unscoped). The deliberate consequence is that one tenant's
 * composite buckets concentrate on one shard; that is the standard trade for
 * atomic multi-scope limiting (the alternative is the race above). Keep
 * per-dimension budgets sized so a single hot tenant's check rate fits one
 * shard's script throughput.
 *
 * The fleet-wide emergency factor (see ./emergency-scale.js) applies here
 * exactly as it does to the single-dimension limiter: every dimension's
 * budget scales by the same cached factor, passed as a script argument.
 *
 * Failure philosophy matches the single-dimension limiter: verdicts reject to
 * the caller when the store is down unless the opt-in local floor is enabled,
 * in which case an in-process mirror with the same all-or-nothing semantics
 * decides (per instance, so N instances allow up to N times the budget while
 * degraded).
 *
 * @module svelte-adapter-uws-extensions/redis/composite-ratelimit
 */

import { scanAndUnlink } from '../shared/redis-scan.js';
import { evalCached } from '../shared/eval-cached.js';
import { withBreaker } from '../shared/breaker.js';
import { wallEpoch } from '../shared/runtime.js';
import { createEmergencyScaleReader, createEmergencyScaleOps } from './emergency-scale.js';

// Same version discipline as the single-dimension limiter: a layout change
// moves to a fresh key space and old keys age out via TTL.
const SCRIPT_VERSION = 'v1';

// Bound on in-process floor entries per dimension (mirrors the single-dim
// limiter's cap; least-recently-touched evicted first).
const FLOOR_MAX_ENTRIES = 10000;

// Dimension names appear in Redis keys, metric labels, and the tripped
// verdict; keep them to a slug so the key stays unambiguous and the metric
// label bounded.
const DIM_NAME_RE = /^[A-Za-z0-9_-]{1,64}$/;

/**
 * All dimensions in one atomic evaluation. Two phases: first every bucket is
 * loaded, refilled, clamped to the (scaled) budget, and checked - without
 * writing; only when every dimension has budget does the second phase
 * decrement and persist all of them. On a deny nothing is consumed anywhere;
 * the tripped dimension's auto-ban (when configured) is the one write.
 *
 * KEYS[1..N]  = dimension bucket keys (one hash slot via the shared hash tag)
 * ARGV[1]     = N (dimension count)
 * ARGV[2]     = cost
 * ARGV[3]     = emergency scale factor (1 = neutral)
 * ARGV[4+3i]  = per-dimension: maxPoints, interval (ms), blockDuration (ms)
 *
 * Returns: [allowed (0/1), trippedIndex (1-based, 0 = none), resetMs,
 *           remaining_1 .. remaining_N]
 * - on allow: resetMs = the soonest refill across dimensions
 * - on deny: resetMs = the tripped dimension's retry-after
 */
const COMPOSITE_CONSUME_SCRIPT = `
-- COMPOSITE_CONSUME
local n = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local scale = tonumber(ARGV[3])
if n == nil or cost == nil then
  return redis.error_reply('COMPOSITE_CONSUME: n/cost must be numeric')
end
if scale == nil or scale <= 0 then scale = 1 end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)

local pts = {}
local resetAt = {}
local banned = {}
local maxPts = {}
local intervalOf = {}
local blockOf = {}
local tripped = 0
local retryMs = 0

for i = 1, n do
  local base = 4 + (i - 1) * 3
  local maxPoints = tonumber(ARGV[base])
  local interval = tonumber(ARGV[base + 1])
  local blockDuration = tonumber(ARGV[base + 2])
  if maxPoints == nil or interval == nil or blockDuration == nil then
    return redis.error_reply('COMPOSITE_CONSUME: dimension args must be numeric')
  end
  if scale ~= 1 then
    maxPoints = math.floor(maxPoints * scale)
    if maxPoints < 1 then maxPoints = 1 end
  end
  maxPts[i] = maxPoints
  intervalOf[i] = interval
  blockOf[i] = blockDuration

  local vals = redis.call('hmget', KEYS[i], 'points', 'resetAt', 'bannedUntil')
  local p = tonumber(vals[1])
  local r = tonumber(vals[2])
  local b = tonumber(vals[3])
  if p == nil then
    p = maxPoints
    r = now + interval
    b = 0
  end
  if r <= now then
    p = maxPoints
    r = now + interval
  end
  if p > maxPoints then
    p = maxPoints
  end
  pts[i] = p
  resetAt[i] = r
  banned[i] = b

  if tripped == 0 then
    if b > now then
      tripped = i
      retryMs = b - now
    elseif p < cost then
      tripped = i
      if blockDuration > 0 then
        retryMs = blockDuration
      else
        retryMs = r - now
      end
    end
  end
end

if tripped ~= 0 then
  -- Nothing is consumed on a deny; the tripped dimension's auto-ban (when
  -- configured and not already banned) is the one write.
  if blockOf[tripped] > 0 and banned[tripped] <= now then
    redis.call('hset', KEYS[tripped], 'points', pts[tripped], 'resetAt', resetAt[tripped], 'bannedUntil', now + blockOf[tripped])
    redis.call('pexpire', KEYS[tripped], blockOf[tripped] + 60000)
  end
  local out = {0, tripped, retryMs}
  for i = 1, n do
    if banned[i] > now then out[3 + i] = 0 else out[3 + i] = math.max(0, pts[i]) end
  end
  return out
end

local minReset = nil
for i = 1, n do
  pts[i] = pts[i] - cost
  redis.call('hset', KEYS[i], 'points', pts[i], 'resetAt', resetAt[i], 'bannedUntil', banned[i])
  redis.call('pexpire', KEYS[i], intervalOf[i] + blockOf[i] + 60000)
  local untilReset = resetAt[i] - now
  if minReset == nil or untilReset < minReset then minReset = untilReset end
end
local out = {1, 0, minReset}
for i = 1, n do out[3 + i] = pts[i] end
return out
`;

/**
 * Read-only companion to COMPOSITE_CONSUME_SCRIPT: the verdict a consume WOULD
 * return across every dimension without writing anything (not even the tripped
 * dimension's auto-ban) and without moving any counter. Same result shape;
 * on allow the reported remaining is pts[i] - cost (what a consume would leave).
 *
 * KEYS / ARGV are identical to COMPOSITE_CONSUME_SCRIPT.
 */
const COMPOSITE_PEEK_SCRIPT = `
-- COMPOSITE_PEEK
local n = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local scale = tonumber(ARGV[3])
if n == nil or cost == nil then
  return redis.error_reply('COMPOSITE_PEEK: n/cost must be numeric')
end
if scale == nil or scale <= 0 then scale = 1 end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)

local pts = {}
local resetAt = {}
local banned = {}
local tripped = 0
local retryMs = 0

for i = 1, n do
  local base = 4 + (i - 1) * 3
  local maxPoints = tonumber(ARGV[base])
  local interval = tonumber(ARGV[base + 1])
  local blockDuration = tonumber(ARGV[base + 2])
  if maxPoints == nil or interval == nil or blockDuration == nil then
    return redis.error_reply('COMPOSITE_PEEK: dimension args must be numeric')
  end
  if scale ~= 1 then
    maxPoints = math.floor(maxPoints * scale)
    if maxPoints < 1 then maxPoints = 1 end
  end

  local vals = redis.call('hmget', KEYS[i], 'points', 'resetAt', 'bannedUntil')
  local p = tonumber(vals[1])
  local r = tonumber(vals[2])
  local b = tonumber(vals[3])
  if p == nil then
    p = maxPoints
    r = now + interval
    b = 0
  end
  if r <= now then
    p = maxPoints
    r = now + interval
  end
  if p > maxPoints then
    p = maxPoints
  end
  pts[i] = p
  resetAt[i] = r
  banned[i] = b

  if tripped == 0 then
    if b > now then
      tripped = i
      retryMs = b - now
    elseif p < cost then
      tripped = i
      if blockDuration > 0 then
        retryMs = blockDuration
      else
        retryMs = r - now
      end
    end
  end
end

if tripped ~= 0 then
  local out = {0, tripped, retryMs}
  for i = 1, n do
    if banned[i] > now then out[3 + i] = 0 else out[3 + i] = math.max(0, pts[i]) end
  end
  return out
end

local minReset = nil
for i = 1, n do
  local untilReset = resetAt[i] - now
  if minReset == nil or untilReset < minReset then minReset = untilReset end
end
local out = {1, 0, minReset}
for i = 1, n do out[3 + i] = pts[i] - cost end
return out
`;

/**
 * @typedef {Object} CompositeDimension
 * @property {number} points - Budget per interval for this dimension. Positive integer.
 * @property {number} interval - Refill interval in milliseconds. Positive.
 * @property {number} [blockDuration=0] - Auto-ban duration (ms) when this dimension trips. 0 = no ban.
 * @property {'ip' | 'connection' | ((ws: any) => string)} keyBy - Key extraction for this
 *   dimension. REQUIRED per dimension: the whole point of a composite check is that each
 *   dimension counts along a different axis, so an accidental shared default would silently
 *   collapse the feature into one budget checked N times.
 */

/**
 * @typedef {Object} CompositeConsumeResult
 * @property {boolean} allowed
 * @property {string | null} tripped - The dimension that denied (declaration order decides
 *   when several would), or null on allow.
 * @property {Record<string, number>} remaining - Post-verdict remaining points per dimension.
 * @property {number} resetMs - Retry-after of the tripped dimension on deny; the soonest
 *   refill across dimensions on allow.
 */

/**
 * Create a composite multi-dimension rate limiter.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {{
 *   dimensions: Record<string, CompositeDimension>,
 *   tenant?: (ws: any) => (string | null | undefined),
 *   breaker?: import('../shared/breaker.js').CircuitBreaker,
 *   metrics?: import('../prometheus/index.js').MetricsRegistry,
 *   localFloorOnStorageFailure?: boolean,
 *   emergency?: { refreshMs?: number }
 * }} options
 */
export function createCompositeRateLimit(client, options) {
	if (!options || typeof options !== 'object') {
		throw new Error('redis composite ratelimit: options object is required');
	}
	const dims = options.dimensions;
	if (!dims || typeof dims !== 'object' || Array.isArray(dims)) {
		throw new Error('redis composite ratelimit: dimensions must be an object of name -> { points, interval, keyBy }');
	}
	const names = Object.keys(dims);
	if (names.length < 2) {
		throw new Error('redis composite ratelimit: at least two dimensions are required (use createRateLimit for one)');
	}
	if (names.length > 8) {
		throw new Error('redis composite ratelimit: at most 8 dimensions are supported per check');
	}
	/** @type {Array<{ name: string, points: number, interval: number, blockDuration: number, keyBy: any }>} */
	const dimList = [];
	for (const name of names) {
		if (!DIM_NAME_RE.test(name)) {
			throw new Error(`redis composite ratelimit: dimension name '${name}' must match ${DIM_NAME_RE}`);
		}
		const d = dims[name];
		if (!d || typeof d !== 'object') {
			throw new Error(`redis composite ratelimit: dimension '${name}' must be { points, interval, keyBy }`);
		}
		if (!Number.isInteger(d.points) || d.points <= 0) {
			throw new Error(`redis composite ratelimit: dimension '${name}' points must be a positive integer`);
		}
		if (typeof d.interval !== 'number' || !Number.isFinite(d.interval) || d.interval <= 0) {
			throw new Error(`redis composite ratelimit: dimension '${name}' interval must be a positive number`);
		}
		const blockDuration = d.blockDuration ?? 0;
		if (typeof blockDuration !== 'number' || !Number.isFinite(blockDuration) || blockDuration < 0) {
			throw new Error(`redis composite ratelimit: dimension '${name}' blockDuration must be a non-negative number`);
		}
		if (d.keyBy !== 'ip' && d.keyBy !== 'connection' && typeof d.keyBy !== 'function') {
			throw new Error(`redis composite ratelimit: dimension '${name}' keyBy is required ('ip', 'connection', or a function) - each dimension must count along its own axis`);
		}
		dimList.push({ name, points: d.points, interval: d.interval, blockDuration, keyBy: d.keyBy });
	}
	const tenant = options.tenant;
	if (tenant !== undefined && typeof tenant !== 'function') {
		throw new Error('redis composite ratelimit: tenant must be a function (ws) => id | null');
	}
	const floorOpt = options.localFloorOnStorageFailure;
	if (floorOpt !== undefined && typeof floorOpt !== 'boolean') {
		throw new Error('redis composite ratelimit: localFloorOnStorageFailure must be a boolean (floors mirror each dimension\'s own budget)');
	}

	const redis = client.redis;
	const b = options.breaker;
	const m = options.metrics;
	const mAllowed = m?.counter('ratelimit_composite_allowed_total', 'Composite checks allowed');
	const mDenied = m?.counter('ratelimit_composite_denied_total', 'Composite checks denied', ['dimension']);
	const mFloor = floorOpt === true
		? m?.counter('ratelimit_composite_storage_fallbacks_total', 'Composite verdicts decided by the in-process floor while the store was unreachable')
		: undefined;

	const emergencyScale = createEmergencyScaleReader(client, options.emergency);

	// Per-connection keying (dimension keyBy 'connection') shares one counter
	// across dimensions so the same ws maps to one stable synthetic key.
	const wsKeys = new WeakMap();
	let connCounter = 0;
	function resolveDimKey(keyBy, ws) {
		if (typeof keyBy === 'function') return String(keyBy(ws));
		if (keyBy === 'connection') {
			let k = wsKeys.get(ws);
			if (!k) {
				k = '__conn:' + (++connCounter);
				wsKeys.set(ws, k);
			}
			return k;
		}
		const ud = typeof ws.getUserData === 'function' ? ws.getUserData() : null;
		if (ud) return String(ud.remoteAddress || ud.ip || ud.address || 'unknown');
		return 'unknown';
	}

	// One hash tag per (tenant) check groups every dimension key onto one
	// cluster slot so the multi-key script is legal on Redis Cluster. The
	// tenant id is validated against the two characters that would break the
	// layout: NUL (the dimension delimiter) and the brace that would truncate
	// the hash tag.
	function tagFor(tenantId) {
		if (tenantId) {
			if (tenantId.indexOf('\0') !== -1 || tenantId.indexOf('{') !== -1 || tenantId.indexOf('}') !== -1) {
				throw new Error('redis composite ratelimit: tenant id must not contain NUL or brace characters (key-layout delimiters)');
			}
			return '{t:' + tenantId + '}';
		}
		return '{t:}';
	}
	function dimBucketKey(tenantId, dimName, key) {
		return client.key(SCRIPT_VERSION + ':ratelimitc:' + tagFor(tenantId) + ':' + dimName + '\0' + key);
	}

	// In-process floor: one bucket map per dimension with the same
	// init/refill/ban/clamp math as the script, evaluated in the same two
	// phases (peek everything, then consume everything or nothing).
	/** @type {Map<string, Map<string, { points: number, resetAt: number, bannedUntil: number }>> | null} */
	const floorBuckets = floorOpt === true ? new Map(dimList.map((d) => [d.name, new Map()])) : null;
	let warnedStorageFloor = false;
	function consumeFloorComposite(keys, cost) {
		const nowMs = wallEpoch();
		const scale = emergencyScale.current();
		const states = [];
		let tripped = 0;
		let retryMs = 0;
		for (let i = 0; i < dimList.length; i++) {
			const d = dimList[i];
			const effMax = scale > 0 && scale !== 1 ? Math.max(1, Math.floor(d.points * scale)) : d.points;
			const buckets = /** @type {Map<string, any>} */ (floorBuckets.get(d.name));
			let entry = buckets.get(keys[i]);
			if (entry !== undefined) {
				buckets.delete(keys[i]);
			} else {
				entry = { points: effMax, resetAt: nowMs + d.interval, bannedUntil: 0 };
			}
			buckets.set(keys[i], entry);
			if (buckets.size > FLOOR_MAX_ENTRIES) {
				buckets.delete(buckets.keys().next().value);
			}
			if (entry.bannedUntil <= nowMs && entry.resetAt <= nowMs) {
				entry.points = effMax;
				entry.resetAt = nowMs + d.interval;
			}
			if (entry.points > effMax) {
				entry.points = effMax;
			}
			states.push(entry);
			if (tripped === 0) {
				if (entry.bannedUntil > nowMs) {
					tripped = i + 1;
					retryMs = entry.bannedUntil - nowMs;
				} else if (entry.points < cost) {
					tripped = i + 1;
					retryMs = d.blockDuration > 0 ? d.blockDuration : entry.resetAt - nowMs;
				}
			}
		}
		const remaining = {};
		if (tripped !== 0) {
			const t = states[tripped - 1];
			const td = dimList[tripped - 1];
			if (td.blockDuration > 0 && t.bannedUntil <= nowMs) {
				t.bannedUntil = nowMs + td.blockDuration;
			}
			for (let i = 0; i < dimList.length; i++) {
				remaining[dimList[i].name] = states[i].bannedUntil > nowMs ? 0 : Math.max(0, states[i].points);
			}
			return { allowed: false, tripped: dimList[tripped - 1].name, remaining, resetMs: retryMs };
		}
		let minReset = Infinity;
		for (let i = 0; i < dimList.length; i++) {
			states[i].points -= cost;
			remaining[dimList[i].name] = states[i].points;
			const untilReset = states[i].resetAt - nowMs;
			if (untilReset < minReset) minReset = untilReset;
		}
		return { allowed: true, tripped: null, remaining, resetMs: minReset };
	}

	// Read-only floor: the verdict consumeFloorComposite would return during a
	// store outage, computed on a rolled snapshot WITHOUT mutating, creating, or
	// banning any in-process bucket.
	function peekFloorComposite(keys, cost) {
		const nowMs = wallEpoch();
		const scale = emergencyScale.current();
		const states = [];
		let tripped = 0;
		let retryMs = 0;
		for (let i = 0; i < dimList.length; i++) {
			const d = dimList[i];
			const effMax = scale > 0 && scale !== 1 ? Math.max(1, Math.floor(d.points * scale)) : d.points;
			const buckets = /** @type {Map<string, any>} */ (floorBuckets.get(d.name));
			const stored = buckets.get(keys[i]);
			let points, resetAt, bannedUntil;
			if (stored === undefined) {
				points = effMax; resetAt = nowMs + d.interval; bannedUntil = 0;
			} else {
				points = stored.points; resetAt = stored.resetAt; bannedUntil = stored.bannedUntil;
			}
			if (bannedUntil <= nowMs && resetAt <= nowMs) {
				points = effMax; resetAt = nowMs + d.interval;
			}
			if (points > effMax) points = effMax;
			states.push({ points, resetAt, bannedUntil });
			if (tripped === 0) {
				if (bannedUntil > nowMs) {
					tripped = i + 1;
					retryMs = bannedUntil - nowMs;
				} else if (points < cost) {
					tripped = i + 1;
					retryMs = d.blockDuration > 0 ? d.blockDuration : resetAt - nowMs;
				}
			}
		}
		const remaining = {};
		if (tripped !== 0) {
			for (let i = 0; i < dimList.length; i++) {
				remaining[dimList[i].name] = states[i].bannedUntil > nowMs ? 0 : Math.max(0, states[i].points);
			}
			return { allowed: false, tripped: dimList[tripped - 1].name, remaining, resetMs: retryMs };
		}
		let minReset = Infinity;
		for (let i = 0; i < dimList.length; i++) {
			remaining[dimList[i].name] = states[i].points - cost;
			const untilReset = states[i].resetAt - nowMs;
			if (untilReset < minReset) minReset = untilReset;
		}
		return { allowed: true, tripped: null, remaining, resetMs: minReset };
	}

	return {
		/**
		 * Consult every dimension atomically. Consumes from all of them only
		 * when all have budget; a deny consumes nothing and names the tripped
		 * dimension.
		 * @param {any} ws
		 * @param {number} [cost=1]
		 * @returns {Promise<CompositeConsumeResult>}
		 */
		async consume(ws, cost = 1) {
			if (typeof cost !== 'number' || !Number.isInteger(cost) || cost < 1) {
				throw new Error('redis composite ratelimit: cost must be a positive integer');
			}
			const tenantId = tenant ? tenant(ws) : null;
			const keys = dimList.map((d) => dimBucketKey(tenantId, d.name, resolveDimKey(d.keyBy, ws)));

			let verdict;
			try {
				const argv = [dimList.length, cost, emergencyScale.current()];
				for (const d of dimList) argv.push(d.points, d.interval, d.blockDuration);
				const result = await withBreaker(b, () =>
					evalCached(redis, COMPOSITE_CONSUME_SCRIPT, keys.length, ...keys, ...argv)
				);
				const trippedIdx = Number(result[1]);
				const remaining = {};
				for (let i = 0; i < dimList.length; i++) remaining[dimList[i].name] = Number(result[3 + i]);
				verdict = {
					allowed: Number(result[0]) === 1,
					tripped: trippedIdx > 0 ? dimList[trippedIdx - 1].name : null,
					remaining,
					resetMs: Number(result[2])
				};
			} catch (err) {
				if (floorBuckets === null) throw err;
				mFloor?.inc();
				if (!warnedStorageFloor) {
					warnedStorageFloor = true;
					console.warn(
						'redis composite ratelimit: store unreachable; deciding on the in-process floor. ' +
						'Limits hold per instance while degraded; cross-instance state resumes when the ' +
						'store recovers. This warning fires once.'
					);
				}
				verdict = consumeFloorComposite(keys, cost);
			}

			if (verdict.allowed) {
				mAllowed?.inc();
			} else {
				mDenied?.inc({ dimension: verdict.tripped || '' });
			}
			return verdict;
		},

		/**
		 * Read-only multi-dimension check: the verdict a consume(ws, cost) WOULD
		 * return without writing to any dimension bucket and without moving any
		 * counter. Same result shape as consume; the tripped dimension is reported
		 * without consuming. During a store outage with the local floor enabled it
		 * decides on the in-process floor read-only; without a floor it rejects.
		 * @param {any} ws
		 * @param {number} [cost=1]
		 * @returns {Promise<CompositeConsumeResult>}
		 */
		async peek(ws, cost = 1) {
			if (typeof cost !== 'number' || !Number.isInteger(cost) || cost < 1) {
				throw new Error('redis composite ratelimit: cost must be a positive integer');
			}
			const tenantId = tenant ? tenant(ws) : null;
			const keys = dimList.map((d) => dimBucketKey(tenantId, d.name, resolveDimKey(d.keyBy, ws)));
			try {
				const argv = [dimList.length, cost, emergencyScale.current()];
				for (const d of dimList) argv.push(d.points, d.interval, d.blockDuration);
				const result = await withBreaker(b, () =>
					evalCached(redis, COMPOSITE_PEEK_SCRIPT, keys.length, ...keys, ...argv)
				);
				const trippedIdx = Number(result[1]);
				const remaining = {};
				for (let i = 0; i < dimList.length; i++) remaining[dimList[i].name] = Number(result[3 + i]);
				return {
					allowed: Number(result[0]) === 1,
					tripped: trippedIdx > 0 ? dimList[trippedIdx - 1].name : null,
					remaining,
					resetMs: Number(result[2])
				};
			} catch (err) {
				if (floorBuckets === null) throw err;
				return peekFloorComposite(keys, cost);
			}
		},

		/**
		 * Clear one dimension bucket (admin op; rejects while the store is down).
		 * @param {string} dimName
		 * @param {string} key
		 * @param {string | null} [tenantId]
		 */
		async reset(dimName, key, tenantId) {
			if (!names.includes(dimName)) {
				throw new Error(`redis composite ratelimit: unknown dimension '${dimName}'`);
			}
			await withBreaker(b, () => redis.del(dimBucketKey(tenantId ?? null, dimName, key)));
		},

		/**
		 * Ban one key on one dimension for `duration` ms (subsequent checks trip
		 * that dimension without consuming anywhere).
		 * @param {string} dimName
		 * @param {string} key
		 * @param {number} duration
		 * @param {string | null} [tenantId]
		 */
		async ban(dimName, key, duration, tenantId) {
			const d = dimList.find((x) => x.name === dimName);
			if (!d) {
				throw new Error(`redis composite ratelimit: unknown dimension '${dimName}'`);
			}
			const dur = duration ?? (d.blockDuration || 60000);
			if (typeof dur !== 'number' || !Number.isFinite(dur) || dur <= 0) {
				throw new Error('redis composite ratelimit: ban duration must be positive');
			}
			const bk = dimBucketKey(tenantId ?? null, dimName, key);
			await withBreaker(b, async () => {
				// Seed points/resetAt alongside the ban (like the single-dim
				// BAN_SCRIPT): a ban-only hash would hit the consume script's
				// init branch, which re-seeds bannedUntil to 0 and erases the ban.
				const vals = await redis.hmget(bk, 'points', 'resetAt');
				const nowMs = wallEpoch();
				const pts = vals && vals[0] != null ? vals[0] : String(d.points);
				const rst = vals && vals[1] != null ? vals[1] : String(nowMs + d.interval);
				await redis.hset(bk, 'points', pts, 'resetAt', rst, 'bannedUntil', String(nowMs + dur));
				await redis.pexpire(bk, dur + 60000);
			});
		},

		/**
		 * Lift a ban on one dimension bucket.
		 * @param {string} dimName
		 * @param {string} key
		 * @param {string | null} [tenantId]
		 */
		async unban(dimName, key, tenantId) {
			if (!names.includes(dimName)) {
				throw new Error(`redis composite ratelimit: unknown dimension '${dimName}'`);
			}
			await withBreaker(b, () => redis.hset(dimBucketKey(tenantId ?? null, dimName, key), 'bannedUntil', '0'));
		},

		/**
		 * Clear buckets. With a tenant id, only that tenant's hash-tag space;
		 * without, the whole composite key space.
		 * @param {string | null} [tenantId]
		 */
		async clear(tenantId) {
			const pattern = tenantId
				? client.key(SCRIPT_VERSION + ':ratelimitc:' + tagFor(tenantId) + ':*')
				: client.key(SCRIPT_VERSION + ':ratelimitc:*');
			await withBreaker(b, () => scanAndUnlink(redis, pattern));
		},

		// The same fleet-wide emergency factor surface the single-dimension
		// limiter exposes (one shared key; either surface flips both).
		emergency: createEmergencyScaleOps(client)
	};
}
