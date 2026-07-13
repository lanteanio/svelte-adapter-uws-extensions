import { wallEpoch, randomU32, setTimer } from '../shared/runtime.js';
import { keySlot } from '../shared/cluster.js';

/**
 * In-memory mock that implements the subset of ioredis used by the extensions.
 * No real Redis connection needed.
 *
 * Clock determinism: every wall-clock read in this double goes through the
 * injectable runtime seam (`wallEpoch` from ../shared/runtime.js), never
 * `Date.now()`. Under the native default that returns the real wall clock; under
 * a seeded simulation harness that overrides `clock.wallEpoch` it returns the
 * virtual clock, so a replay advances field TTLs, stream ids, rate-limit
 * windows, and the TIME command in lockstep with the rest of the system. The
 * RNG used for TIME's sub-millisecond jitter routes through the same seam
 * (`randomU32`), so a seed reproduces it.
 *
 * @param {string} [keyPrefix]
 * @param {{ cluster?: boolean, nodeCount?: number }} [options] - When
 *   `cluster` is true the double models a multi-master cluster topology: a
 *   pipeline or transaction that spans more than one node's slots silently
 *   no-ops the off-node commands (the ioredis multi-slot hazard), so a
 *   cross-slot batch that would break in production is catchable in a unit test.
 *   `nodeCount` (default 3) sets how many masters the slot space is split across.
 */
export function mockRedisClient(keyPrefix = '', options = {}) {
	const clusterMode = options.cluster === true;
	const nodeCount = Number.isInteger(options.nodeCount) && options.nodeCount > 0
		? options.nodeCount
		: 3;
	// Optional seeded fault engine for cross-instance pub/sub delivery (a
	// simulation harness passes one). When set, each PUBLISH delivery to a
	// subscriber is drawn independently and deferred on the seam timer
	// (drop / delay / reorder / duplicate / corrupt). Absent (the default, and
	// every native / integration caller) => delivery stays synchronous and inline,
	// so existing behaviour is unchanged. The engine exposes `plan(payload) ->
	// [{ delayMs, payload }]` (the adapter's createFaultEngine shape).
	const relayFaultEngine = (options.faultEngine && typeof options.faultEngine.plan === 'function')
		? options.faultEngine
		: null;
	const store = new Map();       // key -> value (string)
	const sortedSets = new Map();  // key -> [{score, member}]
	const hashes = new Map();      // key -> Map<field, value>
	const hashFieldExpiry = new Map(); // key -> Map<field, expireAtMs> (Redis 7.4+ HEXPIRE)
	const streams = new Map();     // key -> [{id, fields: [[k, v], ...]}]
	const pubsubHandlers = [];     // {channel, handler}
	const functionLibraries = new Map(); // libname -> code
	const registeredFunctions = new Map(); // funcName -> (keys, args) => unknown
	const evalShapedCommands = new Set(); // defineCommand names: (numKeys, ...keys, ...argv)

	// The server clock for this double. Reads the EXACT wall-clock seam
	// (`wallEpoch`), not the ~1Hz-cached `now()`: a Redis server's clock is
	// precise, and the integration-tier callers (the clock-skew sampler) compare
	// against the same exact seam, so a coarse cached read would inject phantom
	// drift. Under the native default `wallEpoch()` is the real wall clock; under
	// a seeded simulation harness that overrides `clock.wallEpoch` it is the
	// virtual clock. Every wall-clock-dependent path in the double (field TTL
	// pruning, HEXPIRE/HTTL, XADD stream ids, the TIME command, and the rate-limit
	// / ban evaluators) reads through here, so they all advance in lockstep with
	// the rest of the system under replay.
	function serverNowMs() {
		return wallEpoch();
	}

	// Redis TIME reply for the current server clock: [unixSeconds, microseconds]
	// as strings, exactly the shape real Redis (and ioredis) return. The eval
	// path that needs a clock-skew-safe timestamp recombines it the same way the
	// real Lua does (`tonumber(t[1]) * 1000 + floor(tonumber(t[2]) / 1000)`),
	// which round-trips back to serverNowMs() at millisecond resolution.
	//
	// Real Redis TIME has microsecond resolution; the seam clock here is only
	// millisecond-resolution, so the sub-millisecond micros are filled from the
	// injectable RNG seam (`randomU32`). Two reads inside the same virtual
	// millisecond therefore differ in their sub-ms micros yet reproduce exactly
	// under a fixed seed. The jitter is bounded to [0, 999] micros so it can
	// never round up into the millisecond component the recombination keeps.
	function timeReply() {
		const ms = serverNowMs();
		const seconds = Math.floor(ms / 1000);
		const subMs = randomU32() % 1000;
		const micros = (ms % 1000) * 1000 + subMs;
		return [String(seconds), String(micros)];
	}

	// Millisecond timestamp derived through the TIME command, recombined exactly
	// as the real token-bucket / ban Lua does:
	//   `tonumber(t[1]) * 1000 + floor(tonumber(t[2]) / 1000)`.
	// Scripts that the real Lua clocks with `redis.call('TIME')` MUST source their
	// `now` here, not from a raw wall read, so the mirrored-JS path matches the
	// Lua bit-for-bit (the sub-ms jitter in TIME is discarded by floor(/1000), so
	// this round-trips back to serverNowMs() at millisecond resolution).
	function evalTimeNowMs() {
		const t = timeReply();
		return Number(t[0]) * 1000 + Math.floor(Number(t[1]) / 1000);
	}

	// Cluster topology model. Real Redis Cluster assigns the 16384 hash slots as
	// contiguous ranges across the masters, so a key's owning node is a pure
	// function of its slot. We split the slot space into `nodeCount` equal
	// contiguous ranges - any slot->node function works for modeling the hazard
	// as long as same-slot keys always land on the same node, which contiguous
	// ranges guarantee. The exact range boundaries are topology-dependent in a
	// real cluster, but the property the hazard test depends on - "two keys on
	// different slots CAN live on different nodes, two keys on the same slot
	// never do" - holds for this mapping.
	const SLOTS_PER_NODE = Math.ceil(16384 / nodeCount);
	function nodeForSlot(slot) {
		return Math.floor(slot / SLOTS_PER_NODE);
	}

	// The keys a command touches, used only to resolve its owning cluster node.
	// Most commands key on their first argument; eval/evalsha take their keys
	// from the slice after numKeys; a handful are keyless (routed to any node).
	function commandKeys(method, args) {
		const m = String(method).toLowerCase();
		if (m === 'eval' || m === 'evalsha') {
			const numKeys = Number(args[1]) || 0;
			return args.slice(2, 2 + numKeys).map(String);
		}
		// A defineCommand-registered script command has the eval SHAPE minus the
		// script argument: numKeys leads, then the keys. Without this the generic
		// fallback below would read numKeys as the key and mis-slot the command
		// (a cluster-mode pipeline would then no-op it with a phantom MOVED).
		if (evalShapedCommands.has(m)) {
			const numKeys = Number(args[0]) || 0;
			return args.slice(1, 1 + numKeys).map(String);
		}
		// Keyless commands: no slot, so they run on whichever node the batch
		// landed on (never no-op'd by the multi-slot hazard).
		const keyless = new Set([
			'publish', 'spublish', 'subscribe', 'psubscribe', 'ssubscribe',
			'unsubscribe', 'punsubscribe', 'sunsubscribe', 'time', 'wait',
			'info', 'scan', 'ping', 'function', 'multi', 'exec'
		]);
		if (keyless.has(m)) return [];
		return args.length > 0 ? [String(args[0])] : [];
	}

	// The cluster node a single command is routed to, or null when the command
	// has no key (keyless commands ride along with the batch's target node).
	function nodeForCommand(method, args) {
		const keys = commandKeys(method, args);
		if (keys.length === 0) return null;
		return nodeForSlot(keySlot(keys[0]));
	}

	// Lazy expiry of TTL'd hash fields. Real Redis 7.4+ expires fields at
	// background-task time; the mock checks at read time. Idempotent.
	function pruneExpiredFields(key) {
		const ex = hashFieldExpiry.get(key);
		if (!ex) return;
		const h = hashes.get(key);
		const now = serverNowMs();
		let droppedAny = false;
		for (const [field, expireAt] of ex) {
			if (expireAt <= now) {
				ex.delete(field);
				if (h) h.delete(field);
				droppedAny = true;
			}
		}
		if (ex.size === 0) hashFieldExpiry.delete(key);
		if (droppedAny && h && h.size === 0) hashes.delete(key);
	}

	function clearFieldExpiry(key, fields) {
		const ex = hashFieldExpiry.get(key);
		if (!ex) return;
		for (const f of fields) ex.delete(String(f));
		if (ex.size === 0) hashFieldExpiry.delete(key);
	}

	function parseHashFieldExpireArgs(args) {
		let condition = null;
		let i = 0;
		while (i < args.length) {
			const a = String(args[i]).toUpperCase();
			if (a === 'NX' || a === 'XX' || a === 'GT' || a === 'LT') {
				condition = a;
				i++;
			} else if (a === 'FIELDS') {
				i++;
				break;
			} else {
				throw new Error('mock-redis: HEXPIRE/HPEXPIRE: unexpected arg ' + a);
			}
		}
		const numFields = Number(args[i++]);
		const fields = [];
		for (let j = 0; j < numFields; j++) fields.push(String(args[i + j]));
		return { condition, fields };
	}

	function _hexpireCore(key, ttlMs, rest) {
		pruneExpiredFields(key);
		const { condition, fields } = parseHashFieldExpireArgs(rest);
		const h = hashes.get(key);
		const out = [];
		const newExpireAt = serverNowMs() + ttlMs;
		for (const field of fields) {
			if (!h || !h.has(field)) {
				out.push(-2);
				continue;
			}
			let ex = hashFieldExpiry.get(key);
			const currentExpireAt = ex?.get(field);
			if (condition === 'NX' && currentExpireAt !== undefined) { out.push(0); continue; }
			if (condition === 'XX' && currentExpireAt === undefined) { out.push(0); continue; }
			if (condition === 'GT' && currentExpireAt !== undefined && newExpireAt <= currentExpireAt) { out.push(0); continue; }
			if (condition === 'LT' && currentExpireAt !== undefined && newExpireAt >= currentExpireAt) { out.push(0); continue; }
			if (!ex) {
				ex = new Map();
				hashFieldExpiry.set(key, ex);
			}
			ex.set(field, newExpireAt);
			out.push(1);
		}
		return out;
	}

	function _httlCore(key, rest, divisor) {
		pruneExpiredFields(key);
		if (String(rest[0]).toUpperCase() !== 'FIELDS') {
			throw new Error('mock-redis: HTTL/HPTTL requires FIELDS keyword');
		}
		const numFields = Number(rest[1]);
		const fields = [];
		for (let j = 0; j < numFields; j++) fields.push(String(rest[2 + j]));
		const h = hashes.get(key);
		const ex = hashFieldExpiry.get(key);
		const out = [];
		const now = serverNowMs();
		for (const field of fields) {
			if (!h || !h.has(field)) { out.push(-2); continue; }
			const expireAt = ex?.get(field);
			if (expireAt === undefined) { out.push(-1); continue; }
			const remainingMs = expireAt - now;
			if (remainingMs <= 0) { out.push(-2); continue; }
			out.push(divisor === 1 ? remainingMs : Math.ceil(remainingMs / divisor));
		}
		return out;
	}

	function compareStreamIds(a, b) {
		const [aMs, aSeq] = a.split('-').map(Number);
		const [bMs, bSeq] = b.split('-').map(Number);
		if (aMs !== bMs) return aMs - bMs;
		return aSeq - bSeq;
	}

	function parseStreamRange(s) {
		if (s === '-') return { id: '0-0', exclusive: false };
		if (s === '+') return { id: '99999999999999-99999999999999', exclusive: false };
		const exclusive = String(s).startsWith('(');
		const raw = exclusive ? String(s).slice(1) : String(s);
		const id = raw.includes('-') ? raw : raw + '-0';
		return { id, exclusive };
	}

	function mockRedis() {
		const listeners = new Map();
		const subscribedChannels = new Set();
		const subscribedPatterns = new Set();
		const shardedChannels = new Set();

		const r = {
			// String ops
			async get(key) { return store.get(key) || null; },
			async set(key, val, ...flags) {
				// Real Redis SET accepts NX / XX / EX / PX / EXAT / PXAT / KEEPTTL
				// flags. Mock honors the conditional gates (NX / XX) since they
				// affect return value; TTL flags are accepted and ignored
				// (tests that exercise expiry should mutate the store directly
				// or simulate via test helpers).
				let nx = false;
				let xx = false;
				for (let i = 0; i < flags.length; i++) {
					const f = String(flags[i]).toUpperCase();
					if (f === 'NX') nx = true;
					else if (f === 'XX') xx = true;
					else if (f === 'EX' || f === 'PX' || f === 'EXAT' || f === 'PXAT') i++;
				}
				if (nx && store.has(key)) return null;
				if (xx && !store.has(key)) return null;
				store.set(key, String(val));
				return 'OK';
			},
			async incr(key) {
				const v = parseInt(store.get(key) || '0', 10) + 1;
				store.set(key, String(v));
				return v;
			},
			async del(...keys) {
				let count = 0;
				for (const k of keys) {
					if (store.delete(k)) count++;
					if (sortedSets.delete(k)) count++;
					if (hashes.delete(k)) count++;
					hashFieldExpiry.delete(k);
					if (streams.delete(k)) count++;
				}
				return count;
			},
			async unlink(...keys) {
				return r.del(...keys);
			},
			async exists(...keys) {
				// Real EXISTS counts every existing key argument (duplicates count).
				let count = 0;
				for (const key of keys) {
					if (store.has(key) || sortedSets.has(key) || hashes.has(key) || streams.has(key)) count++;
				}
				return count;
			},
			async expire(key) {
				// Real Redis EXPIRE returns 1 if the timeout was set, 0 if
				// the key does not exist. TTL itself is not simulated; tests
				// that exercise expiry should mutate the store directly.
				const exists = store.has(key) || sortedSets.has(key)
					|| hashes.has(key) || streams.has(key);
				return exists ? 1 : 0;
			},
			async pexpire(key) {
				const exists = store.has(key) || sortedSets.has(key)
					|| hashes.has(key) || streams.has(key);
				return exists ? 1 : 0;
			},

			// Replication ack stub. Tests configure behavior via:
			//   redis._waitAcks: number to override the ack count
			//   redis._waitError: an Error to throw from wait()
			// Default: return numReplicas so tests that don't care about
			// replication see no behavior change.
			async wait(numReplicas) {
				if (r._waitError) throw r._waitError;
				const v = r._waitAcks;
				if (typeof v === 'number') return v;
				return Number(numReplicas);
			},

			// Sorted set ops
			async zadd(key, score, member) {
				if (!sortedSets.has(key)) sortedSets.set(key, []);
				const set = sortedSets.get(key);
				// Real Redis ZADD upserts: a sorted set holds unique members, so an
				// existing member's score is updated in place rather than duplicated.
				// Returns the count of NEW members added (0 on a pure update).
				const existing = set.find((e) => String(e.member) === String(member));
				let added = 0;
				if (existing) {
					existing.score = Number(score);
				} else {
					set.push({ score: Number(score), member });
					added = 1;
				}
				set.sort((a, b) => a.score - b.score);
				return added;
			},
			async zcard(key) {
				const set = sortedSets.get(key);
				return set ? set.length : 0;
			},
			async zrange(key, start, stop) {
				const set = sortedSets.get(key);
				if (!set) return [];
				const len = set.length;
				const s = start < 0 ? Math.max(0, len + start) : Math.min(start, len);
				const e = stop < 0 ? len + stop : Math.min(stop, len - 1);
				if (s > e) return [];
				return set.slice(s, e + 1).map((entry) => entry.member);
			},
			async zrevrange(key, start, stop) {
				const set = sortedSets.get(key);
				if (!set) return [];
				// Highest score first (descending); ties keep ascending insertion order reversed.
				const rev = set.slice().reverse();
				const len = rev.length;
				const s = start < 0 ? Math.max(0, len + start) : Math.min(start, len);
				const e = stop < 0 ? len + stop : Math.min(stop, len - 1);
				if (s > e) return [];
				return rev.slice(s, e + 1).map((entry) => entry.member);
			},
			async zrangebyscore(key, min, max, ...extra) {
				const set = sortedSets.get(key);
				if (!set) return [];
				const lo = min === '-inf' ? -Infinity : Number(min);
				const hi = max === '+inf' ? Infinity : Number(max);
				let result = set.filter((e) => e.score >= lo && e.score <= hi).map((e) => e.member);
				const limitIdx = extra.indexOf('LIMIT');
				if (limitIdx !== -1) {
					const offset = Number(extra[limitIdx + 1]);
					const count = Number(extra[limitIdx + 2]);
					result = result.slice(offset, offset + count);
				}
				return result;
			},
			async zremrangebyrank(key, start, stop) {
				const set = sortedSets.get(key);
				if (!set) return 0;
				const removed = set.splice(start, stop - start + 1);
				return removed.length;
			},
			async zremrangebyscore(key, min, max) {
				const set = sortedSets.get(key);
				if (!set) return 0;
				// Inclusive bounds only (the exclusive `(` prefix is treated as
				// inclusive; 1ms of slack is immaterial against a multi-second TTL).
				const lo = min === '-inf' ? -Infinity : Number(typeof min === 'string' && min[0] === '(' ? min.slice(1) : min);
				const hi = max === '+inf' ? Infinity : Number(typeof max === 'string' && max[0] === '(' ? max.slice(1) : max);
				let removed = 0;
				for (let i = set.length - 1; i >= 0; i--) {
					if (set[i].score >= lo && set[i].score <= hi) { set.splice(i, 1); removed++; }
				}
				return removed;
			},
			async zrem(key, ...members) {
				const set = sortedSets.get(key);
				if (!set) return 0;
				const want = new Set(members.map((m) => String(m)));
				let removed = 0;
				for (let i = set.length - 1; i >= 0; i--) {
					if (want.has(String(set[i].member))) { set.splice(i, 1); removed++; }
				}
				return removed;
			},

			// Stream ops
			async xadd(key, ...args) {
				let i = 0;
				let maxLen = -1;
				if (args[i] === 'MAXLEN' || args[i] === 'maxlen') {
					i++;
					if (args[i] === '~' || args[i] === '=') i++;
					maxLen = Number(args[i]);
					i++;
				}
				const idArg = String(args[i++]);
				const fields = [];
				while (i < args.length) {
					fields.push([String(args[i]), String(args[i + 1])]);
					i += 2;
				}
				if (!streams.has(key)) streams.set(key, []);
				const stream = streams.get(key);

				let resolvedId;
				if (idArg === '*') {
					// Auto-id timestamp comes from the server clock seam, so a
					// seeded harness produces the same stream ids on replay. The
					// sequence disambiguation (same-ms collisions) stays
					// deterministic, matching real Redis XADD * semantics.
					const ms = serverNowMs();
					const last = stream[stream.length - 1];
					if (last) {
						const [lastMs, lastSeq] = last.id.split('-').map(Number);
						resolvedId = ms <= lastMs
							? `${lastMs}-${lastSeq + 1}`
							: `${ms}-0`;
					} else {
						resolvedId = `${ms}-0`;
					}
				} else {
					resolvedId = idArg.includes('-') ? idArg : idArg + '-0';
					if (stream.length > 0) {
						const last = stream[stream.length - 1];
						if (compareStreamIds(last.id, resolvedId) >= 0) {
							throw new Error('ERR The ID specified in XADD is equal or smaller than the target stream top item');
						}
					}
				}

				stream.push({ id: resolvedId, fields });
				if (maxLen >= 0 && stream.length > maxLen) {
					stream.splice(0, stream.length - maxLen);
				}
				return resolvedId;
			},
			async xrange(key, start, end, ...rest) {
				let count = -1;
				for (let i = 0; i < rest.length; i++) {
					if (rest[i] === 'COUNT' || rest[i] === 'count') {
						count = Number(rest[i + 1]);
						break;
					}
				}
				const stream = streams.get(key);
				if (!stream) return [];
				const startCmp = parseStreamRange(start);
				const endCmp = parseStreamRange(end);
				const out = [];
				for (const entry of stream) {
					if (compareStreamIds(entry.id, startCmp.id) < 0) continue;
					if (startCmp.exclusive && entry.id === startCmp.id) continue;
					if (compareStreamIds(entry.id, endCmp.id) > 0) break;
					if (endCmp.exclusive && entry.id === endCmp.id) continue;
					const flat = [];
					for (const [f, v] of entry.fields) flat.push(f, v);
					out.push([entry.id, flat]);
					if (count > 0 && out.length >= count) break;
				}
				return out;
			},
			async xlen(key) {
				const stream = streams.get(key);
				return stream ? stream.length : 0;
			},

			// Hash ops
			async hset(key, ...args) {
				pruneExpiredFields(key);
				if (!hashes.has(key)) hashes.set(key, new Map());
				const h = hashes.get(key);
				let added = 0;
				const touchedFields = [];
				if (args.length === 2) {
					const f = String(args[0]);
					if (!h.has(f)) added++;
					h.set(f, String(args[1]));
					touchedFields.push(f);
				} else if (typeof args[0] === 'object' && args[0] !== null) {
					for (const [f, v] of Object.entries(args[0])) {
						const fs = String(f);
						if (!h.has(fs)) added++;
						h.set(fs, String(v));
						touchedFields.push(fs);
					}
				} else {
					for (let i = 0; i < args.length; i += 2) {
						const f = String(args[i]);
						if (!h.has(f)) added++;
						h.set(f, String(args[i + 1]));
						touchedFields.push(f);
					}
				}
				// Real Redis 7.4+: HSET on an existing field with a TTL clears that TTL.
				// Mirror that semantics so callers cannot accidentally rely on the TTL
				// persisting across an HSET-overwrite. New fields get no TTL by default.
				clearFieldExpiry(key, touchedFields);
				return added;
			},
			async hmset(key, ...args) {
				pruneExpiredFields(key);
				if (!hashes.has(key)) hashes.set(key, new Map());
				const h = hashes.get(key);
				const touchedFields = [];
				if (typeof args[0] === 'object' && args[0] !== null) {
					for (const [f, v] of Object.entries(args[0])) {
						const fs = String(f);
						h.set(fs, String(v));
						touchedFields.push(fs);
					}
				} else {
					for (let i = 0; i < args.length; i += 2) {
						const fs = String(args[i]);
						h.set(fs, String(args[i + 1]));
						touchedFields.push(fs);
					}
				}
				clearFieldExpiry(key, touchedFields);
				return 'OK';
			},
			async hget(key, field) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				return h ? (h.get(field) || null) : null;
			},
			async hmget(key, ...fields) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				return fields.map((f) => (h ? (h.get(String(f)) ?? null) : null));
			},
			async hgetall(key) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				if (!h) return {};
				const result = {};
				for (const [k, v] of h) result[k] = v;
				return result;
			},
			async hvals(key) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				return h ? [...h.values()] : [];
			},
			async hexists(key, field) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				return h && h.has(String(field)) ? 1 : 0;
			},
			async hdel(key, ...fields) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				if (!h) return 0;
				let count = 0;
				for (const f of fields) {
					if (h.delete(String(f))) count++;
				}
				clearFieldExpiry(key, fields);
				if (h.size === 0) hashes.delete(key);
				return count;
			},
			async hlen(key) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				return h ? h.size : 0;
			},
			async hkeys(key) {
				pruneExpiredFields(key);
				const h = hashes.get(key);
				return h ? [...h.keys()] : [];
			},
			async hincrby(key, field, delta) {
				pruneExpiredFields(key);
				if (!hashes.has(key)) hashes.set(key, new Map());
				const h = hashes.get(key);
				const fs = String(field);
				const next = (Number(h.get(fs)) || 0) + Number(delta);
				h.set(fs, String(next));
				return next;
			},

			// Redis 7.4+: per-field TTL primitives.
			// Returns an array of integers, one per requested field:
			//   1  = TTL set / refreshed
			//   0  = condition failed (NX with existing TTL, XX with no TTL, etc.)
			//  -2  = field does not exist on the hash
			async hexpire(key, ttlSec, ...rest) {
				return _hexpireCore(key, Number(ttlSec) * 1000, rest);
			},
			async hpexpire(key, ttlMs, ...rest) {
				return _hexpireCore(key, Number(ttlMs), rest);
			},
			async httl(key, ...rest) {
				return _httlCore(key, rest, 1000);
			},
			async hpttl(key, ...rest) {
				return _httlCore(key, rest, 1);
			},

			// Pub/sub
			async publish(channel, message) {
				for (const handler of pubsubHandlers) {
					if (!handler.channels.has(channel)) continue;
					const msgListener = handler.listeners.get('message');
					if (!msgListener) continue;
					if (relayFaultEngine) {
						// Each subscriber's delivery is drawn independently and deferred on
						// the seam timer, modeling cross-instance pub/sub unreliability. The
						// timers are refed so a delayed / reordered relay still lands before
						// the run quiesces. A byte-flipped payload reaches the subscriber
						// as-is and then either fails JSON.parse, is dropped by the bus
						// envelope-shape / validator gate, or parses to a valid-but-mutated
						// envelope that is delivered - all reproducible under a seed.
						const plan = relayFaultEngine.plan(message);
						for (const d of plan) {
							const payload = d.payload;
							setTimer(() => { try { msgListener(channel, payload); } catch { /* receiver errors are not the publisher's */ } }, d.delayMs);
						}
					} else {
						msgListener(channel, message);
					}
				}
				return 1;
			},
			async subscribe(...channels) {
				// Real ioredis SUBSCRIBE accepts variadic channels and reports
				// the new total count. Single-arg callers continue to work.
				for (const ch of channels) subscribedChannels.add(ch);
				return subscribedChannels.size;
			},
			async unsubscribe(...channels) {
				if (channels.length === 0) {
					// Zero-arg unsubscribe = "drop everything" per ioredis semantics.
					subscribedChannels.clear();
					return 0;
				}
				for (const ch of channels) subscribedChannels.delete(ch);
				return subscribedChannels.size;
			},
			// Pattern-subscribe stubs. Pattern matching is not simulated
			// (publish() never fires pmessage); tests that exercise pmessage
			// fish the listener out of `_listeners` and dispatch directly.
			async psubscribe(pattern) {
				subscribedPatterns.add(pattern);
				return 1;
			},
			async punsubscribe(pattern) {
				subscribedPatterns.delete(pattern);
				return 1;
			},
			// Sharded pub/sub. In a single-process mock there is no actual
			// shard topology, so SPUBLISH / SSUBSCRIBE behave the same as
			// regular pub/sub but dispatched on a separate `smessage`
			// channel set so consumers using one model don't see traffic
			// from the other.
			async spublish(channel, message) {
				for (const handler of pubsubHandlers) {
					if (handler.shardedChannels && handler.shardedChannels.has(channel)) {
						const listener = handler.listeners.get('smessage');
						if (listener) listener(channel, message);
					}
				}
				return 1;
			},
			async ssubscribe(...channels) {
				for (const ch of channels) shardedChannels.add(ch);
				return channels.length;
			},
			async sunsubscribe(...channels) {
				if (channels.length === 0) {
					shardedChannels.clear();
					return 0;
				}
				for (const ch of channels) shardedChannels.delete(ch);
				return channels.length;
			},
			// Server INFO stub. Tests can override `_info` (a string) to
			// drive version detection in callers. Default reports 7.4.0 since
			// the mock now supports per-field HEXPIRE; callers that probe the
			// server version to gate on Redis 7.4+ (e.g. presence) see a
			// compatible server out of the box.
			async info(/* section */) {
				return r._info ?? '# Server\nredis_version:7.4.0\n';
			},

			// Redis TIME command. Returns [unixSeconds, microseconds] as strings,
			// the exact shape real Redis (and ioredis's `.time()`) return, sourced
			// from the server clock seam. The clock-skew sampler (redis/clock-skew.js)
			// and any caller that wants a clock-skew-safe timestamp read this; under
			// a seeded simulation harness it returns the virtual clock, so skew is
			// reproducible or deliberately injectable with no real Redis.
			async time() {
				return timeReply();
			},

			// Redis Functions. The mock does NOT execute Lua; it stores
			// loaded library code by name, and tests register handlers
			// via the wrapped client's `_registerFunction(name, handler)`
			// helper. fcall looks up the registered handler.
			async function(subcommand, ...args) {
				const sub = String(subcommand).toUpperCase();
				if (sub === 'LOAD') {
					const replace = String(args[0]).toUpperCase() === 'REPLACE';
					const code = replace ? args[1] : args[0];
					const m = String(code).match(/^#!lua\s+name=(\S+)/);
					if (!m) throw new Error('ERR Missing library name in shebang');
					const libname = m[1];
					if (!replace && functionLibraries.has(libname)) {
						throw new Error('ERR Library already exists');
					}
					functionLibraries.set(libname, code);
					return libname;
				}
				if (sub === 'DELETE') {
					const libname = args[0];
					if (!functionLibraries.has(libname)) {
						throw new Error('ERR Library not found');
					}
					functionLibraries.delete(libname);
					return 'OK';
				}
				if (sub === 'LIST') {
					return [...functionLibraries.keys()];
				}
				if (sub === 'FLUSH') {
					functionLibraries.clear();
					return 'OK';
				}
				throw new Error('ERR mock-redis: unsupported FUNCTION subcommand ' + sub);
			},
			async fcall(funcName, numKeys, ...rest) {
				const handler = registeredFunctions.get(funcName);
				if (!handler) {
					throw new Error(`ERR Function not found: ${funcName}`);
				}
				const n = Number(numKeys);
				const keys = rest.slice(0, n);
				const args = rest.slice(n);
				return handler(keys, args);
			},

			// Eval - dispatches based on script content.
			//
			// Lua atomicity contract: a real Redis EVAL runs the whole script
			// as one indivisible unit - no other command can interleave between
			// its internal redis.call()s. This double upholds the same contract
			// by dispatching to a fully SYNCHRONOUS evaluator: each evalXxx()
			// helper completes all its reads and writes against the in-memory
			// maps with no `await` in between, so even though eval() is declared
			// `async` (to match ioredis's promise-returning surface) the script
			// body never yields the microtask queue mid-execution. No concurrent
			// command can observe a half-applied script. Keep every evaluator
			// synchronous; introducing an `await` inside one would break this.
			async eval(script, numKeys, ...args) {
				// Ban script (atomic ban with Redis TIME)
				if (script.includes('defaultPoints') && script.includes('defaultInterval')) {
					return evalBanScript(args);
				}
				// Read-only / alternate-refill rate-limit scripts. EVERY rate-limit
				// script mentions bannedUntil and would otherwise route to the
				// SPENDING evaluator below; each carries a unique marker comment so a
				// read-only peek or a sliding/gcra consume is dispatched to the
				// matching evaluator FIRST (before COMPOSITE_CONSUME and bannedUntil).
				if (script.includes('-- PEEK_WINDOW')) {
					return evalPeekRateLimit(args);
				}
				if (script.includes('-- SLIDING_PEEK')) {
					return evalSlidingRateLimit(args, true);
				}
				if (script.includes('-- SLIDING_CONSUME')) {
					return evalSlidingRateLimit(args, false);
				}
				if (script.includes('-- GCRA_PEEK')) {
					return evalGcraRateLimit(args, true);
				}
				if (script.includes('-- GCRA_CONSUME')) {
					return evalGcraRateLimit(args, false);
				}
				if (script.includes('-- COMPOSITE_PEEK')) {
					return evalCompositeRateLimit(numKeys, args, true);
				}
				// Composite multi-dimension rate limit (all-or-nothing consume).
				// Dispatched before the single-bucket token bucket: both scripts
				// mention bannedUntil, the marker disambiguates.
				if (script.includes('COMPOSITE_CONSUME')) {
					return evalCompositeRateLimit(numKeys, args, false);
				}
				// Rate limit script (token bucket)
				if (script.includes('bannedUntil')) {
					return evalRateLimit(args);
				}
				// Replay publish script (atomic incr + zadd + trim)
				if (script.includes('zremrangebyrank') && script.includes('cjson.encode')) {
					return evalReplayPublish(numKeys, args);
				}
				// Streams replay idempotent publish script (hget cache, then incr + xadd + hset)
				if (script.includes('xadd') && script.includes('hget') && script.includes('hset')) {
					return evalIdmpStreamReplayPublish(numKeys, args);
				}
				// Streams replay publish script (atomic incr + xadd MAXLEN)
				if (script.includes('xadd') && script.includes('MAXLEN')) {
					return evalStreamReplayPublish(numKeys, args);
				}
				// Presence JOIN (Design G: per-user hash + per-topic hash, HPEXPIRE field TTL)
				if (script.includes('PRESENCE_JOIN') && script.includes('HPEXPIRE')) {
					return evalPresenceJoinG(args);
				}
				// Presence UPDATE (field-level durable merge into the per-topic hash value)
				if (script.includes('PRESENCE_UPDATE')) {
					return evalPresenceUpdate(args);
				}
				// Presence LEAVE (Design G: HDEL + HLEN check, no scan)
				if (script.includes('HDEL') && script.includes('HLEN')
					&& !script.includes('HPEXPIRE') && !script.includes('hdel')) {
					return evalPresenceLeaveG(args);
				}
				// Presence join script (hset + expire, no dedup scan) - legacy pre-Design-G
				if (script.includes('hset') && script.includes('expire') && !script.includes('hdel') && !script.includes('suffix')) {
					return evalPresenceJoin(args);
				}
				// Presence leave script (hdel + check remaining by suffix) - legacy pre-Design-G
				if (script.includes('hdel') && script.includes('suffix')) {
					return evalPresenceLeave(args);
				}
				// Stale field cleanup script (server-side HGETALL + HDEL)
				if (script.includes('CLEANUP_STALE')) {
					return evalCleanupStale(args);
				}
				// Count dedup script (presence: deduplicated by userKey via | separator)
				if (script.includes('seen[userKey] = true') && script.includes('pairs(seen)')) {
					return evalCountDedupScript(args);
				}
				// Count script (server-side live entry count)
				if (script.includes('count = count + 1') && !script.includes('hdel') && !script.includes('hset')) {
					return evalCountScript(args);
				}
				// List script (server-side presence list with dedup)
				if (script.includes('seen[userKey]') && script.includes('best[userKey]')) {
					return evalListScript(args);
				}
				// Group join script (atomic capacity check + insert)
				if (script.includes('cjson.decode') && script.includes('liveCount')) {
					return evalGroupJoin(numKeys, args);
				}
				// Idempotency acquire (SET NX EX, then GET, distinguish pending vs result)
				if (script.includes("'NX', 'EX', ARGV[2]") && script.includes("return {0, '', 1}")) {
					return evalIdempotencyAcquire(args);
				}
				// Idempotency commit (compare-and-set: SET result iff owner token matches).
				// Matched before the fence scripts: both compare a stored value, the
				// IDEM_COMMIT marker disambiguates.
				if (script.includes('IDEM_COMMIT')) {
					return evalIdempotencyCommit(args);
				}
				// Idempotency abort (compare-and-delete: DEL iff owner token matches).
				// Its DEL would otherwise look like the fence-release script; the
				// IDEM_ABORT marker keeps them distinct and is matched first.
				if (script.includes('IDEM_ABORT')) {
					return evalIdempotencyAbort(args);
				}
				// Fence heartbeat (refresh PEXPIRE iff value matches)
				if (script.includes("redis.call('PEXPIRE'") && script.includes('v == ARGV[1]')) {
					return evalFenceHeartbeat(args);
				}
				// Fence release (DEL iff value matches)
				if (script.includes("redis.call('DEL', KEYS[1])") && script.includes('v == ARGV[1]')) {
					return evalFenceRelease(args);
				}
				// Registry compare-and-delete (HGET instanceId, UNLINK iff matches)
				if (script.includes("'hget'") && script.includes("'unlink'") && script.includes('current == ours')) {
					return evalRegistryCompareDelete(args);
				}
				throw new Error('mock-redis: unrecognized eval script');
			},

			// Scan
			async scan(cursor, ...args) {
				// Simple mock: return all matching keys in one go
				const matchIdx = args.indexOf('MATCH');
				const pattern = matchIdx !== -1 ? args[matchIdx + 1] : '*';
				const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');

				const allKeys = [...store.keys(), ...sortedSets.keys(), ...hashes.keys(), ...streams.keys()];
				const matched = allKeys.filter((k) => regex.test(k));
				return ['0', matched];
			},

			// Pipeline support for batched commands. The collected commands run
			// through the shared batch executor (see execBatch below), which in
			// cluster mode models the ioredis multi-slot hazard.
			pipeline() {
				return makeBatch();
			},

			// MULTI / EXEC transaction support. A real Redis MULTI on a cluster is
			// still confined to a single node, so it carries the SAME multi-slot
			// hazard as a pipeline: a transaction referencing keys on more than one
			// node no-ops the off-node commands. The mock therefore shares the batch
			// executor with pipeline(); in cluster mode a cross-node MULTI surfaces
			// the off-node drops, and in standalone mode all commands run (the mock
			// does not model Redis-side rollback - tests that need true
			// transactional all-or-nothing semantics belong on the integration tier).
			multi() {
				return makeBatch();
			},

			// Lifecycle
			duplicate(/* overrides */) {
				const dup = mockRedis();
				// Register this duplicate as a pub/sub receiver
				pubsubHandlers.push({
					channels: dup._subscribedChannels,
					shardedChannels: dup._shardedChannels,
					listeners: dup._listeners
				});
				return dup;
			},
			async quit() {},
			disconnect() {},

			// Event handling
			on(event, fn) {
				listeners.set(event, fn);
				return r;
			},

			defineCommand(name, { lua }) {
				r[name] = async (numKeys, ...args) => r.eval(lua, numKeys, ...args);
				// Record the eval shape so commandKeys slots a pipelined call of
				// this command correctly (numKeys leads, then the keys).
				evalShapedCommands.add(String(name).toLowerCase());
			},

			_subscribedChannels: subscribedChannels,
			_subscribedPatterns: subscribedPatterns,
			_shardedChannels: shardedChannels,
			_listeners: listeners
		};

		// Execute a collected batch in ioredis pipeline shape: an array of
		// [err, value] tuples index-aligned with the commands.
		//
		// In standalone (default) mode every command runs - the mock has a single
		// keyspace, so there is no node to be off. In cluster mode the batch models
		// the ioredis multi-slot hazard: a single pipeline / transaction is
		// delivered to ONE node (the node owning the first keyed command's slot).
		// Every command whose key belongs to a different node is silently NO-OP'd -
		// it never touches the store, and its result slot carries a MOVED
		// placeholder error (never a thrown rejection), exactly as a real cluster
		// pipeline surfaces it. A same-slot (or same-node) batch runs in full; a
		// cross-slot batch that spans nodes leaves the off-node writes unapplied, so
		// the regression is catchable here instead of only against a live cluster.
		//
		// Every command runs synchronously relative to the others (the `await`s here
		// only resolve the already-synchronous in-memory ops), so a batch applies as
		// an ordered unit with no foreign command interleaving mid-batch.
		async function execBatch(commands) {
			let targetNode = null;
			if (clusterMode) {
				for (const { method, args } of commands) {
					const node = nodeForCommand(method, args);
					if (node !== null) { targetNode = node; break; }
				}
			}
			const results = [];
			for (const { method: m, args } of commands) {
				if (clusterMode && targetNode !== null) {
					const node = nodeForCommand(m, args);
					if (node !== null && node !== targetNode) {
						// Off-node command: the cluster delivered the whole pipeline to
						// targetNode, which does not own this slot, so it is dropped
						// with a MOVED reply rather than run.
						const slot = keySlot(commandKeys(m, args)[0]);
						results.push([
							new Error('MOVED ' + slot + ' mock-node-' + node),
							undefined
						]);
						continue;
					}
				}
				try {
					const result = await r[m](...args);
					results.push([null, result]);
				} catch (err) {
					results.push([err, null]);
				}
			}
			return results;
		}

		// Build a pipeline/transaction collector. Chained command calls accumulate;
		// exec() runs them through execBatch.
		function makeBatch() {
			const commands = [];
			const p = new Proxy({}, {
				get(_, method) {
					if (method === 'exec') {
						return () => execBatch(commands);
					}
					return (...args) => {
						commands.push({ method, args });
						return p;
					};
				}
			});
			return p;
		}

		// Rate limit Lua script simulation
		function evalRateLimit(args) {
			const key = args[0];
			let maxPoints = Number(args[1]);
			const interval = Number(args[2]);
			const cost = Number(args[3]);
			const blockDuration = Number(args[4]);
			// ARGV[5]: emergency scale factor (optional). Mirror the Lua:
			// effective budget max(1, floor(maxPoints * scale)) + a mid-window
			// clamp below.
			const scale = args.length > 5 ? Number(args[5]) : 1;
			if (Number.isFinite(scale) && scale > 0 && scale !== 1) {
				maxPoints = Math.max(1, Math.floor(maxPoints * scale));
			}
			// Mirror the real Lua: obtain the timestamp via the TIME command
			// (clock-skew-safe) rather than a raw wall read, so this matches
			// CONSUME_SCRIPT and follows the seam clock under a harness.
			const now = evalTimeNowMs();

			if (!hashes.has(key)) hashes.set(key, new Map());
			const h = hashes.get(key);

			let pts = h.has('points') ? Number(h.get('points')) : null;
			let resetAt = h.has('resetAt') ? Number(h.get('resetAt')) : null;
			let bannedUntil = h.has('bannedUntil') ? Number(h.get('bannedUntil')) : null;

			if (pts === null) {
				pts = maxPoints;
				resetAt = now + interval;
				bannedUntil = 0;
			}

			if (bannedUntil > now) {
				return [0, 0, bannedUntil - now];
			}

			if (resetAt <= now) {
				pts = maxPoints;
				resetAt = now + interval;
			}

			if (pts > maxPoints) {
				pts = maxPoints;
			}

			if (pts >= cost) {
				pts -= cost;
				h.set('points', String(pts));
				h.set('resetAt', String(resetAt));
				h.set('bannedUntil', String(bannedUntil));
				return [1, pts, resetAt - now];
			}

			if (blockDuration > 0) {
				bannedUntil = now + blockDuration;
				h.set('points', String(pts));
				h.set('resetAt', String(resetAt));
				h.set('bannedUntil', String(bannedUntil));
				return [0, 0, blockDuration];
			}

			h.set('points', String(pts));
			h.set('resetAt', String(resetAt));
			h.set('bannedUntil', String(bannedUntil));
			return [0, Math.max(0, pts), resetAt - now];
		}

		// Read-only fixed-window peek simulation (PEEK_SCRIPT). Reports the verdict
		// a consume would return WITHOUT writing or creating the hash and WITHOUT
		// moving any counter.
		function evalPeekRateLimit(args) {
			const key = args[0];
			let maxPoints = Number(args[1]);
			const interval = Number(args[2]);
			const cost = Number(args[3]);
			const blockDuration = Number(args[4]);
			const scale = args.length > 5 ? Number(args[5]) : 1;
			if (Number.isFinite(scale) && scale > 0 && scale !== 1) {
				maxPoints = Math.max(1, Math.floor(maxPoints * scale));
			}
			const now = evalTimeNowMs();
			const h = hashes.get(key); // read-only: never create the hash
			let points = h && h.has('points') ? Number(h.get('points')) : null;
			let resetAt = h && h.has('resetAt') ? Number(h.get('resetAt')) : null;
			let bannedUntil = h && h.has('bannedUntil') ? Number(h.get('bannedUntil')) : null;
			if (points === null) {
				points = maxPoints;
				resetAt = now + interval;
				bannedUntil = 0;
			}
			if (bannedUntil > now) return [0, 0, bannedUntil - now];
			if (resetAt <= now) { points = maxPoints; resetAt = now + interval; }
			if (points > maxPoints) points = maxPoints;
			if (points >= cost) return [1, points - cost, resetAt - now];
			if (blockDuration > 0) return [0, 0, blockDuration];
			return [0, Math.max(0, points), resetAt - now];
		}

		// Sliding-window-counter simulation (SLIDING_SCRIPT / SLIDING_PEEK_SCRIPT).
		// `peekOnly` suppresses every write (and never creates the hash), so a peek
		// is a pure read.
		function evalSlidingRateLimit(args, peekOnly) {
			const key = args[0];
			let maxPoints = Number(args[1]);
			const interval = Number(args[2]);
			const cost = Number(args[3]);
			const blockDuration = Number(args[4]);
			const scale = args.length > 5 ? Number(args[5]) : 1;
			if (Number.isFinite(scale) && scale > 0 && scale !== 1) {
				maxPoints = Math.max(1, Math.floor(maxPoints * scale));
			}
			const now = evalTimeNowMs();
			const winStartNow = now - (now % interval);
			const existing = hashes.get(key);
			let curr = existing && existing.has('curr') ? Number(existing.get('curr')) : null;
			let prev = existing && existing.has('prev') ? Number(existing.get('prev')) : null;
			let windowStart = existing && existing.has('windowStart') ? Number(existing.get('windowStart')) : null;
			let bannedUntil = existing && existing.has('bannedUntil') ? Number(existing.get('bannedUntil')) : null;
			if (bannedUntil === null) bannedUntil = 0;
			if (curr === null) { curr = 0; prev = 0; windowStart = winStartNow; }
			if (bannedUntil > now) return [0, 0, bannedUntil - now];
			const elapsed = winStartNow - windowStart;
			if (elapsed >= interval * 2) { prev = 0; curr = 0; windowStart = winStartNow; }
			else if (elapsed >= interval) { prev = curr; curr = 0; windowStart = winStartNow; }
			if (curr > maxPoints) curr = maxPoints;
			const into = now - windowStart;
			let prevWeight = (interval - into) / interval;
			if (prevWeight < 0) prevWeight = 0;
			if (prevWeight > 1) prevWeight = 1;
			const weighted = curr + prev * prevWeight;
			const resetMs = windowStart + interval - now;
			function persist() {
				if (peekOnly) return;
				if (!hashes.has(key)) hashes.set(key, new Map());
				const h = hashes.get(key);
				h.set('curr', String(curr));
				h.set('prev', String(prev));
				h.set('windowStart', String(windowStart));
				h.set('bannedUntil', String(bannedUntil));
			}
			if (weighted + cost <= maxPoints) {
				curr = curr + cost;
				persist();
				return [1, Math.floor(Math.max(0, maxPoints - weighted - cost)), resetMs];
			}
			if (blockDuration > 0) {
				bannedUntil = now + blockDuration;
				persist();
				return [0, 0, blockDuration];
			}
			persist();
			return [0, Math.floor(Math.max(0, maxPoints - weighted)), resetMs];
		}

		// GCRA / leaky-bucket simulation (GCRA_SCRIPT / GCRA_PEEK_SCRIPT). `peekOnly`
		// suppresses every write (and never creates the hash).
		function evalGcraRateLimit(args, peekOnly) {
			const key = args[0];
			let maxPoints = Number(args[1]);
			const interval = Number(args[2]);
			const cost = Number(args[3]);
			const blockDuration = Number(args[4]);
			const scale = args.length > 5 ? Number(args[5]) : 1;
			if (Number.isFinite(scale) && scale > 0 && scale !== 1) {
				maxPoints = Math.max(1, Math.floor(maxPoints * scale));
			}
			const now = evalTimeNowMs();
			const emission = interval / maxPoints;
			const burst = interval;
			const existing = hashes.get(key);
			const tat = existing && existing.has('tat') ? Number(existing.get('tat')) : null;
			let bannedUntil = existing && existing.has('bannedUntil') ? Number(existing.get('bannedUntil')) : null;
			if (bannedUntil === null) bannedUntil = 0;
			if (bannedUntil > now) return [0, 0, bannedUntil - now];
			const tatEff = (tat !== null && tat > now) ? tat : now;
			const newTat = tatEff + emission * cost;
			const allowAt = newTat - burst;
			function persist(tatVal) {
				if (peekOnly) return;
				if (!hashes.has(key)) hashes.set(key, new Map());
				const h = hashes.get(key);
				h.set('tat', String(tatVal));
				h.set('bannedUntil', String(bannedUntil));
			}
			if (now >= allowAt) {
				persist(newTat);
				return [1, Math.max(0, Math.floor((now - allowAt) / emission)), Math.floor(newTat - now)];
			}
			if (blockDuration > 0) {
				bannedUntil = now + blockDuration;
				persist(tatEff);
				return [0, 0, blockDuration];
			}
			return [0, Math.max(0, Math.floor((now - (tatEff - burst)) / emission)), Math.ceil(allowAt - now)];
		}

		// Composite multi-dimension rate limit simulation. Mirrors
		// COMPOSITE_CONSUME_SCRIPT: load/refill/clamp every dimension without
		// writing, deny on the first tripped dimension (writing only its
		// auto-ban), or consume from all of them. With `peekOnly` it mirrors
		// COMPOSITE_PEEK_SCRIPT instead: identical verdict math, zero writes (not
		// even the tripped ban), and on allow it reports pts[i]-cost as remaining.
		// args: [key_1..key_N, n, cost, scale, (maxPoints, interval, blockDuration) x N]
		function evalCompositeRateLimit(numKeys, args, peekOnly) {
			const keys = args.slice(0, numKeys);
			const argv = args.slice(numKeys);
			const n = Number(argv[0]);
			const cost = Number(argv[1]);
			let scale = Number(argv[2]);
			if (!Number.isFinite(scale) || scale <= 0) scale = 1;
			const now = evalTimeNowMs();

			const pts = [];
			const resetAt = [];
			const banned = [];
			const maxPts = [];
			const intervalOf = [];
			const blockOf = [];
			let tripped = 0;
			let retryMs = 0;

			for (let i = 0; i < n; i++) {
				let maxPoints = Number(argv[3 + i * 3]);
				const interval = Number(argv[4 + i * 3]);
				const blockDuration = Number(argv[5 + i * 3]);
				if (scale !== 1) {
					maxPoints = Math.max(1, Math.floor(maxPoints * scale));
				}
				maxPts[i] = maxPoints;
				intervalOf[i] = interval;
				blockOf[i] = blockDuration;

				const h = hashes.get(keys[i]);
				let p = h && h.has('points') ? Number(h.get('points')) : null;
				let r = h && h.has('resetAt') ? Number(h.get('resetAt')) : null;
				let b = h && h.has('bannedUntil') ? Number(h.get('bannedUntil')) : null;
				if (p === null) {
					p = maxPoints;
					r = now + interval;
					b = 0;
				}
				if (r <= now) {
					p = maxPoints;
					r = now + interval;
				}
				if (p > maxPoints) {
					p = maxPoints;
				}
				pts[i] = p;
				resetAt[i] = r;
				banned[i] = b;

				if (tripped === 0) {
					if (b > now) {
						tripped = i + 1;
						retryMs = b - now;
					} else if (p < cost) {
						tripped = i + 1;
						retryMs = blockDuration > 0 ? blockDuration : r - now;
					}
				}
			}

			if (tripped !== 0) {
				const t = tripped - 1;
				if (!peekOnly && blockOf[t] > 0 && banned[t] <= now) {
					if (!hashes.has(keys[t])) hashes.set(keys[t], new Map());
					const h = hashes.get(keys[t]);
					h.set('points', String(pts[t]));
					h.set('resetAt', String(resetAt[t]));
					h.set('bannedUntil', String(now + blockOf[t]));
				}
				const out = [0, tripped, retryMs];
				for (let i = 0; i < n; i++) {
					out.push(banned[i] > now ? 0 : Math.max(0, pts[i]));
				}
				return out;
			}

			let minReset = null;
			for (let i = 0; i < n; i++) {
				pts[i] -= cost;
				if (!peekOnly) {
					if (!hashes.has(keys[i])) hashes.set(keys[i], new Map());
					const h = hashes.get(keys[i]);
					h.set('points', String(pts[i]));
					h.set('resetAt', String(resetAt[i]));
					h.set('bannedUntil', String(banned[i]));
				}
				const untilReset = resetAt[i] - now;
				if (minReset === null || untilReset < minReset) minReset = untilReset;
			}
			const out = [1, 0, minReset];
			for (let i = 0; i < n; i++) out.push(pts[i]);
			return out;
		}

		// Ban Lua script simulation
		function evalBanScript(args) {
			const key = args[0];
			const duration = Number(args[1]);
			const defaultPoints = Number(args[2]);
			const defaultInterval = Number(args[3]);
			// BAN_SCRIPT clocks itself with redis.call('TIME'); mirror that.
			const now = evalTimeNowMs();

			if (!hashes.has(key)) hashes.set(key, new Map());
			const h = hashes.get(key);

			const pts = h.get('points') ?? String(defaultPoints);
			const rst = h.get('resetAt') ?? String(now + defaultInterval);

			h.set('points', String(pts));
			h.set('resetAt', String(rst));
			h.set('bannedUntil', String(now + duration));
			return 1;
		}

		// Presence join Lua script simulation
		// HSET + EXPIRE, always returns 1.  Cross-instance dedup was removed
		// from the real Lua script (O(N) scan per join was the bottleneck).
		function evalPresenceJoin(args) {
			const key = args[0];
			const field = args[1];
			const value = args[2];
			// args[3] = ttlSec (for EXPIRE, no-op in mock)

			if (!hashes.has(key)) hashes.set(key, new Map());
			hashes.get(key).set(field, value);

			return 1;
		}

		// Presence JOIN (Design G) - per-user hash + per-topic hash + HPEXPIRE.
		// Mirrors the Lua semantics: HSET + HPEXPIRE on userHash; newer-ts
		// conditional HSET on topicHash; HPEXPIRE on topicHash; returns 1 iff
		// userHash was empty before (caller broadcasts a join).
		function evalPresenceJoinG(args) {
			const userHashKey = args[0];
			const topicHashKey = args[1];
			const instanceId = args[2];
			const userKeyStr = args[3];
			const topicHashValue = args[4];
			const newTs = Number(args[5]);
			const ttlMs = Number(args[6]);

			// HLEN check (after pruning expired fields)
			pruneExpiredFields(userHashKey);
			const userHash = hashes.get(userHashKey);
			const wasEmpty = !userHash || userHash.size === 0;

			// HSET userHash + HPEXPIRE
			if (!hashes.has(userHashKey)) hashes.set(userHashKey, new Map());
			hashes.get(userHashKey).set(instanceId, String(newTs));
			clearFieldExpiry(userHashKey, [instanceId]);
			let ex = hashFieldExpiry.get(userHashKey);
			if (!ex) { ex = new Map(); hashFieldExpiry.set(userHashKey, ex); }
			ex.set(instanceId, serverNowMs() + ttlMs);

			// Newer-ts conditional set on topicHash, preserving any durable
			// fields already stored for the user (mirrors JOIN_SCRIPT's
			// fields-preservation across a newer-data overwrite).
			pruneExpiredFields(topicHashKey);
			const topicHash = hashes.get(topicHashKey);
			let shouldWrite = true;
			let existingParsed = null;
			if (topicHash && topicHash.has(userKeyStr)) {
				try {
					existingParsed = JSON.parse(topicHash.get(userKeyStr));
					const existingTs = Number(existingParsed.ts) || 0;
					if (newTs < existingTs) shouldWrite = false;
				} catch { existingParsed = null; /* corrupted - allow overwrite */ }
			}
			if (shouldWrite) {
				let valueToWrite = topicHashValue;
				if (existingParsed && existingParsed.fields && typeof existingParsed.fields === 'object') {
					try {
						const incoming = JSON.parse(topicHashValue);
						incoming.fields = existingParsed.fields;
						valueToWrite = JSON.stringify(incoming);
					} catch { /* keep topicHashValue */ }
				}
				if (!hashes.has(topicHashKey)) hashes.set(topicHashKey, new Map());
				hashes.get(topicHashKey).set(userKeyStr, valueToWrite);
				clearFieldExpiry(topicHashKey, [userKeyStr]);
			}
			// Always refresh TTL on topicHash field (even when write was skipped
			// for newer-ts, to keep the field alive on heartbeat).
			let tex = hashFieldExpiry.get(topicHashKey);
			if (!tex) { tex = new Map(); hashFieldExpiry.set(topicHashKey, tex); }
			tex.set(userKeyStr, serverNowMs() + ttlMs);

			return wasEmpty ? 1 : 0;
		}

		// Presence LEAVE (Design G) - HDEL my instanceId from per-user hash,
		// HLEN check, conditional HDEL from per-topic hash if zero remaining.
		function evalPresenceLeaveG(args) {
			const userHashKey = args[0];
			const topicHashKey = args[1];
			const instanceId = args[2];
			const userKeyStr = args[3];

			pruneExpiredFields(userHashKey);
			const userHash = hashes.get(userHashKey);
			if (userHash) {
				userHash.delete(instanceId);
				clearFieldExpiry(userHashKey, [instanceId]);
				if (userHash.size === 0) hashes.delete(userHashKey);
			}

			const remaining = hashes.get(userHashKey);
			const isFullyGone = !remaining || remaining.size === 0;
			if (isFullyGone) {
				const topicHash = hashes.get(topicHashKey);
				if (topicHash) {
					topicHash.delete(userKeyStr);
					clearFieldExpiry(topicHashKey, [userKeyStr]);
					if (topicHash.size === 0) hashes.delete(topicHashKey);
				}
				return 1;
			}
			return 0;
		}

		// Presence UPDATE (field-level) Lua script simulation. Mirrors
		// UPDATE_SCRIPT: merge changed DURABLE fields into the per-topic hash
		// value's `fields`, bump ts, refresh the field TTL. Returns 1 if applied,
		// 0 if the user is not present on the topic.
		function evalPresenceUpdate(args) {
			const topicHashKey = args[0];
			const userKey = args[1];
			const durableJson = args[2];
			const newTs = Number(args[3]);
			const ttlMs = Number(args[4]);

			pruneExpiredFields(topicHashKey);
			const topicHash = hashes.get(topicHashKey);
			if (!topicHash || !topicHash.has(userKey)) return 0;
			let parsed;
			try { parsed = JSON.parse(topicHash.get(userKey)); } catch { return 0; }
			if (!parsed || typeof parsed !== 'object') return 0;
			let durable;
			try { durable = JSON.parse(durableJson); } catch { return 0; }
			if (!durable || typeof durable !== 'object') return 0;
			if (!parsed.fields || typeof parsed.fields !== 'object') parsed.fields = {};
			for (const k of Object.keys(durable)) parsed.fields[k] = durable[k];
			parsed.ts = newTs;
			topicHash.set(userKey, JSON.stringify(parsed));
			clearFieldExpiry(topicHashKey, [userKey]);
			let tex = hashFieldExpiry.get(topicHashKey);
			if (!tex) { tex = new Map(); hashFieldExpiry.set(topicHashKey, tex); }
			tex.set(userKey, serverNowMs() + ttlMs);
			return 1;
		}

		// Presence leave Lua script simulation
		function evalPresenceLeave(args) {
			const key = args[0];
			const field = args[1];
			const suffix = args[2];
			const now = Number(args[3]);
			const ttlMs = Number(args[4]);

			// hdel
			const h = hashes.get(key);
			if (h) {
				h.delete(field);
				if (h.size === 0) hashes.delete(key);
			}

			// Check remaining fields for suffix match, ignoring stale entries
			const remaining = hashes.get(key);
			if (remaining) {
				for (const [f, v] of remaining) {
					if (f.length >= suffix.length && f.slice(-suffix.length) === suffix) {
						try {
							const parsed = JSON.parse(v);
							if (parsed.ts && (now - parsed.ts) <= ttlMs) {
								return 0; // User still present on another live instance
							}
						} catch { /* skip */ }
					}
				}
			}
			return 1; // User is gone
		}

		// Group join Lua script simulation (2 keys: members, closed)
		function evalGroupJoin(numKeys, args) {
			const key = args[0];
			const closedFlag = numKeys >= 2 ? args[1] : null;
			const argOffset = numKeys;
			const maxMembers = Number(args[argOffset]);
			const memberId = args[argOffset + 1];
			const memberData = args[argOffset + 2];
			const now = Number(args[argOffset + 3]);
			const memberTtlMs = Number(args[argOffset + 4]);

			if (closedFlag && store.get(closedFlag) === '1') {
				return [-1];
			}

			if (!hashes.has(key)) hashes.set(key, new Map());
			const h = hashes.get(key);

			let liveCount = 0;
			const toRemove = [];
			const live = [];
			for (const [f, v] of h) {
				try {
					const val = JSON.parse(v);
					if (val.ts && (now - val.ts) <= memberTtlMs) {
						liveCount++;
						live.push(v);
					} else {
						toRemove.push(f);
					}
				} catch {
					toRemove.push(f);
				}
			}
			for (const f of toRemove) h.delete(f);

			if (liveCount >= maxMembers) {
				return [0];
			}
			h.set(memberId, memberData);
			live.push(memberData);
			return [1, ...live];
		}

		// Streams idempotent replay publish Lua script simulation
		// args layout: [idmpKey, seqKey, bufKey, epochKey, requestId, maxSize, ttl, idmpTtl, event, dataJson, hexpireSupported]
		function evalIdmpStreamReplayPublish(numKeys, args) {
			const idmpKey = args[0];
			const seqKey = args[1];
			const bufKey = args[2];
			const epochKey = args[3];
			const requestId = args[4];
			const maxSize = Number(args[5]);
			const idmpTtl = Number(args[7]);
			const event = args[8];
			const dataJson = args[9];
			const hexpire = String(args[10]) === '1';

			// Match the real server: a per-field-expired dedup entry is gone before
			// the HGET, so it reads as a fresh publish.
			pruneExpiredFields(idmpKey);
			if (!hashes.has(idmpKey)) hashes.set(idmpKey, new Map());
			const idmp = hashes.get(idmpKey);
			const curEpoch = parseInt(store.get(epochKey) || '0', 10);

			if (idmp.has(requestId)) {
				const cached = idmp.get(requestId);
				const sep = cached.indexOf(':');
				if (sep === -1) {
					// Legacy bare-seq (pre-versioning): honorable only while no reset
					// has bumped the epoch (curEpoch === 0); see the Lua for the
					// rationale. Otherwise fall through and re-publish.
					if (curEpoch === 0) {
						return [1, parseInt(cached, 10)];
					}
				} else {
					const cachedEpoch = parseInt(cached.slice(0, sep), 10);
					const cachedSeq = parseInt(cached.slice(sep + 1), 10);
					if (cachedEpoch === curEpoch) {
						return [1, cachedSeq];
					}
					// Stale generation: fall through and re-publish into the current one.
				}
			}

			const seq = parseInt(store.get(seqKey) || '0', 10) + 1;
			store.set(seqKey, String(seq));
			let epoch = curEpoch;
			// Reset edge: a fresh seq space (seq == 1) bumps the epoch.
			if (seq === 1) {
				epoch = curEpoch + 1;
				store.set(epochKey, String(epoch));
			}

			const id = `${seq}-0`;
			if (!streams.has(bufKey)) streams.set(bufKey, []);
			const stream = streams.get(bufKey);
			stream.push({
				id,
				fields: [['v', '1'], ['event', event], ['data', dataJson]]
			});
			if (stream.length > maxSize) {
				stream.splice(0, stream.length - maxSize);
			}

			idmp.set(requestId, epoch + ':' + seq);
			if (idmpTtl > 0 && hexpire) {
				// Per-field HPEXPIRE (readable via pruneExpiredFields). The fallback
				// path (whole-hash EXPIRE) is a no-op stub in this mock, like `expire`.
				let ex = hashFieldExpiry.get(idmpKey);
				if (!ex) { ex = new Map(); hashFieldExpiry.set(idmpKey, ex); }
				ex.set(requestId, serverNowMs() + idmpTtl * 1000);
			}

			return [0, seq];
		}

		// Streams replay publish Lua script simulation
		// args layout: [seqKey, bufKey, epochKey, maxSize, ttl, event, dataJson]
		function evalStreamReplayPublish(numKeys, args) {
			const seqKey = args[0];
			const bufKey = args[1];
			const epochKey = args[2];
			const maxSize = Number(args[3]);
			const event = args[5];
			const dataJson = args[6];

			const v = parseInt(store.get(seqKey) || '0', 10) + 1;
			store.set(seqKey, String(v));
			const seq = v;

			// Reset edge: a fresh seq space (seq == 1) bumps the epoch.
			if (seq === 1) {
				store.set(epochKey, String(parseInt(store.get(epochKey) || '0', 10) + 1));
			}

			const id = `${seq}-0`;
			if (!streams.has(bufKey)) streams.set(bufKey, []);
			const stream = streams.get(bufKey);
			stream.push({
				id,
				fields: [['v', '1'], ['event', event], ['data', dataJson]]
			});
			if (stream.length > maxSize) {
				stream.splice(0, stream.length - maxSize);
			}
			return seq;
		}

		// Replay publish Lua script simulation
		// args layout: [seqKey, bufKey, epochKey, event, dataJson, maxSize, ttl]
		function evalReplayPublish(numKeys, args) {
			const seqKey = args[0];
			const bufKey = args[1];
			const epochKey = args[2];
			const event = args[3];
			const dataJson = args[4];
			const maxSize = Number(args[5]);

			// Increment seq
			const v = parseInt(store.get(seqKey) || '0', 10) + 1;
			store.set(seqKey, String(v));
			const seq = v;

			// Reset edge: a fresh seq space (seq == 1) bumps the epoch.
			if (seq === 1) {
				store.set(epochKey, String(parseInt(store.get(epochKey) || '0', 10) + 1));
			}

			// zadd a versioned, topic-less envelope (topic lives in the key).
			const data = JSON.parse(dataJson);
			const payload = JSON.stringify({ v: 1, seq, event, data });
			if (!sortedSets.has(bufKey)) sortedSets.set(bufKey, []);
			const set = sortedSets.get(bufKey);
			set.push({ score: seq, member: payload });
			set.sort((a, b) => a.score - b.score);

			// Trim
			if (set.length > maxSize) {
				set.splice(0, set.length - maxSize);
			}

			return seq;
		}

		// Count dedup Lua script simulation (presence: deduplicated by userKey)
		function evalCountDedupScript(args) {
			const key = args[0];
			const now = Number(args[1]);
			const ttlMs = Number(args[2]);
			const h = hashes.get(key);
			if (!h) return 0;
			const seen = new Set();
			for (const [field, v] of h) {
				try {
					const parsed = JSON.parse(v);
					if (parsed.ts && (now - parsed.ts) <= ttlMs) {
						const sep = field.indexOf('|');
						const userKey = sep !== -1 ? field.slice(sep + 1) : field;
						seen.add(userKey);
					}
				} catch { /* skip */ }
			}
			return seen.size;
		}

		// Count Lua script simulation
		function evalCountScript(args) {
			const key = args[0];
			const now = Number(args[1]);
			const ttlMs = Number(args[2]);
			const h = hashes.get(key);
			if (!h) return 0;
			let count = 0;
			for (const [, v] of h) {
				try {
					const parsed = JSON.parse(v);
					if (parsed.ts && (now - parsed.ts) <= ttlMs) count++;
				} catch { /* skip */ }
			}
			return count;
		}

		// List Lua script simulation (presence list with per-user dedup)
		function evalListScript(args) {
			const key = args[0];
			const now = Number(args[1]);
			const ttlMs = Number(args[2]);
			const h = hashes.get(key);
			if (!h) return [];
			const seen = new Map();
			for (const [field, v] of h) {
				try {
					const parsed = JSON.parse(v);
					if (parsed.ts && (now - parsed.ts) <= ttlMs) {
						const sep = field.indexOf('|');
						const userKey = sep !== -1 ? field.slice(sep + 1) : field;
						const existing = seen.get(userKey);
						if (!existing || parsed.ts > existing.ts) {
							seen.set(userKey, { ts: parsed.ts, json: v });
						}
					}
				} catch { /* skip */ }
			}
			const out = [];
			for (const [k, v] of seen) {
				out.push(k, v.json);
			}
			return out;
		}

		// Idempotency acquire Lua script simulation
		// args layout: [key, ownerToken, acquireTtlSec, pendingPrefix]
		// Returns: [1, '', 0] acquired, [0, '', 1] pending, [0, value, 0] cached result.
		// Pending is detected by the prefix (each owner writes a distinct token),
		// matching the real ACQUIRE_SCRIPT's string.sub check.
		// TTL is not simulated; tests that exercise expiry should mutate the
		// store directly or call the store's purge/clear surface.
		function evalIdempotencyAcquire(args) {
			const key = args[0];
			const ownerToken = args[1];
			const prefix = args[3];
			const existing = store.get(key);
			if (existing === undefined) {
				store.set(key, ownerToken);
				return [1, '', 0];
			}
			if (typeof existing === 'string' && existing.startsWith(prefix)) {
				return [0, '', 1];
			}
			return [0, existing, 0];
		}

		// Idempotency commit compare-and-set simulation.
		// args layout: [key, ownerToken, value, ttlSec]
		// Writes the value only if the stored owner token still matches (the owner
		// still holds the slot); returns 1 on success, 0 if the lease was lost.
		function evalIdempotencyCommit(args) {
			const key = args[0];
			const ownerToken = args[1];
			const value = args[2];
			if (store.get(key) === ownerToken) {
				store.set(key, value);
				return 1;
			}
			return 0;
		}

		// Idempotency abort compare-and-delete simulation.
		// args layout: [key, ownerToken]
		// Deletes the slot only if the stored owner token still matches; returns 1
		// if released, 0 if the lease was lost (no-op).
		function evalIdempotencyAbort(args) {
			const key = args[0];
			const ownerToken = args[1];
			if (store.get(key) === ownerToken) {
				store.delete(key);
				return 1;
			}
			return 0;
		}

		// Fence heartbeat Lua script simulation
		// args layout: [key, expectedFence, ttlMs]
		// Returns 1 if value matches and the TTL is refreshed (no-op in mock), 0 otherwise.
		function evalFenceHeartbeat(args) {
			const key = args[0];
			const expected = args[1];
			const existing = store.get(key);
			if (existing === expected) {
				return 1;
			}
			return 0;
		}

		// Fence release Lua script simulation
		// args layout: [key, expectedFence]
		// Returns 1 if value matches and the key is deleted, 0 otherwise.
		function evalFenceRelease(args) {
			const key = args[0];
			const expected = args[1];
			const existing = store.get(key);
			if (existing === expected) {
				store.delete(key);
				return 1;
			}
			return 0;
		}

		// Registry compare-and-delete: only UNLINK if the stored
		// `instanceId` field matches `ours`.
		function evalRegistryCompareDelete(args) {
			const key = args[0];
			const ours = args[1];
			const h = hashes.get(key);
			if (!h) return 0;
			if (h.get('instanceId') === ours) {
				hashes.delete(key);
				return 1;
			}
			return 0;
		}

		// Stale field cleanup Lua script simulation
		function evalCleanupStale(args) {
			const key = args[0];
			const now = Number(args[1]);
			const ttlMs = Number(args[2]);

			const h = hashes.get(key);
			if (!h) return 0;

			const toRemove = [];
			for (const [f, v] of h) {
				try {
					const parsed = JSON.parse(v);
					if (!parsed.ts || (now - parsed.ts) > ttlMs) {
						toRemove.push(f);
					}
				} catch {
					toRemove.push(f);
				}
			}
			for (const f of toRemove) h.delete(f);
			if (h.size === 0) hashes.delete(key);
			return toRemove.length;
		}

		return r;
	}

	const redis = mockRedis();

	return {
		redis,
		keyPrefix,
		key(k) { return keyPrefix + k; },
		duplicate(overrides) { return redis.duplicate(overrides); },
		async quit() {},
		// Test helpers
		_store: store,
		_sortedSets: sortedSets,
		_hashes: hashes,
		_streams: streams,
		_pubsubHandlers: pubsubHandlers,
		_functionLibraries: functionLibraries,
		_registerFunction(funcName, handler) {
			registeredFunctions.set(funcName, handler);
		},
		_unregisterFunction(funcName) {
			registeredFunctions.delete(funcName);
		},
		// Cluster-mode introspection. `_cluster` reports whether the multi-slot
		// hazard is modeled; `_slotOf` and `_nodeOf` expose the same slot->node
		// mapping the batch executor uses, so a test can build a key pair that is
		// guaranteed same-node (a batch that must succeed) or cross-node (a batch
		// whose off-node commands must no-op).
		_cluster: clusterMode,
		_slotOf(key) { return keySlot(String(key)); },
		_nodeOf(key) { return Math.floor(keySlot(String(key)) / Math.ceil(16384 / nodeCount)); }
	};
}
