/**
 * In-memory mock that implements the subset of ioredis used by the extensions.
 * No real Redis connection needed.
 */
export function mockRedisClient(keyPrefix = '') {
	const store = new Map();       // key -> value (string)
	const sortedSets = new Map();  // key -> [{score, member}]
	const hashes = new Map();      // key -> Map<field, value>
	const pubsubHandlers = [];     // {channel, handler}

	function mockRedis() {
		const listeners = new Map();
		const subscribedChannels = new Set();

		const r = {
			// String ops
			async get(key) { return store.get(key) || null; },
			async set(key, val) { store.set(key, String(val)); return 'OK'; },
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
				}
				return count;
			},
			async unlink(...keys) {
				return r.del(...keys);
			},
			async expire() { return 1; },
			async pexpire() { return 1; },

			// Sorted set ops
			async zadd(key, score, member) {
				if (!sortedSets.has(key)) sortedSets.set(key, []);
				const set = sortedSets.get(key);
				set.push({ score: Number(score), member });
				set.sort((a, b) => a.score - b.score);
				return 1;
			},
			async zcard(key) {
				const set = sortedSets.get(key);
				return set ? set.length : 0;
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

			// Hash ops
			async hset(key, field, value) {
				if (!hashes.has(key)) hashes.set(key, new Map());
				hashes.get(key).set(String(field), String(value));
				return 1;
			},
			async hmset(key, ...args) {
				if (!hashes.has(key)) hashes.set(key, new Map());
				const h = hashes.get(key);
				// hmset(key, field, value, field, value, ...)
				// or hmset(key, { field: value, ... })
				if (typeof args[0] === 'object' && args[0] !== null) {
					for (const [f, v] of Object.entries(args[0])) {
						h.set(String(f), String(v));
					}
				} else {
					for (let i = 0; i < args.length; i += 2) {
						h.set(String(args[i]), String(args[i + 1]));
					}
				}
				return 'OK';
			},
			async hget(key, field) {
				const h = hashes.get(key);
				return h ? (h.get(field) || null) : null;
			},
			async hmget(key, ...fields) {
				const h = hashes.get(key);
				return fields.map((f) => (h ? (h.get(String(f)) ?? null) : null));
			},
			async hgetall(key) {
				const h = hashes.get(key);
				if (!h) return {};
				const result = {};
				for (const [k, v] of h) result[k] = v;
				return result;
			},
			async hdel(key, ...fields) {
				const h = hashes.get(key);
				if (!h) return 0;
				let count = 0;
				for (const f of fields) {
					if (h.delete(f)) count++;
				}
				if (h.size === 0) hashes.delete(key);
				return count;
			},
			async hlen(key) {
				const h = hashes.get(key);
				return h ? h.size : 0;
			},
			async hkeys(key) {
				const h = hashes.get(key);
				return h ? [...h.keys()] : [];
			},

			// Pub/sub
			async publish(channel, message) {
				for (const handler of pubsubHandlers) {
					if (handler.channels.has(channel)) {
						const msgListener = handler.listeners.get('message');
						if (msgListener) msgListener(channel, message);
					}
				}
				return 1;
			},
			async subscribe(channel) {
				subscribedChannels.add(channel);
				return 1;
			},
			async unsubscribe(channel) {
				subscribedChannels.delete(channel);
				return 1;
			},

			// Eval - dispatches based on script content
			async eval(script, numKeys, ...args) {
				// Ban script (atomic ban with Redis TIME)
				if (script.includes('defaultPoints') && script.includes('defaultInterval')) {
					return evalBanScript(args);
				}
				// Rate limit script (token bucket)
				if (script.includes('bannedUntil')) {
					return evalRateLimit(args);
				}
				// Replay publish script (atomic incr + zadd + trim)
				if (script.includes('zremrangebyrank') && script.includes('cjson.encode')) {
					return evalReplayPublish(numKeys, args);
				}
				// Presence join script (hset + expire, no dedup scan)
				if (script.includes('hset') && script.includes('expire') && !script.includes('hdel') && !script.includes('suffix')) {
					return evalPresenceJoin(args);
				}
				// Presence leave script (hdel + check remaining by suffix)
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
				throw new Error('mock-redis: unrecognized eval script');
			},

			// Scan
			async scan(cursor, ...args) {
				// Simple mock: return all matching keys in one go
				const matchIdx = args.indexOf('MATCH');
				const pattern = matchIdx !== -1 ? args[matchIdx + 1] : '*';
				const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');

				const allKeys = [...store.keys(), ...sortedSets.keys(), ...hashes.keys()];
				const matched = allKeys.filter((k) => regex.test(k));
				return ['0', matched];
			},

			// Pipeline support for batched commands
			pipeline() {
				const commands = [];
				const p = new Proxy({}, {
					get(_, method) {
						if (method === 'exec') {
							return async () => {
								const results = [];
								for (const { method: m, args } of commands) {
									try {
										const result = await r[m](...args);
										results.push([null, result]);
									} catch (err) {
										results.push([err, null]);
									}
								}
								return results;
							};
						}
						return (...args) => {
							commands.push({ method, args });
							return p;
						};
					}
				});
				return p;
			},

			// Lifecycle
			duplicate(/* overrides */) {
				const dup = mockRedis();
				// Register this duplicate as a pub/sub receiver
				pubsubHandlers.push({
					channels: dup._subscribedChannels,
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
			},

			_subscribedChannels: subscribedChannels,
			_listeners: listeners
		};

		// Rate limit Lua script simulation
		function evalRateLimit(args) {
			const key = args[0];
			const maxPoints = Number(args[1]);
			const interval = Number(args[2]);
			const cost = Number(args[3]);
			const blockDuration = Number(args[4]);
			const now = Date.now(); // Simulates Redis TIME command

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

		// Ban Lua script simulation
		function evalBanScript(args) {
			const key = args[0];
			const duration = Number(args[1]);
			const defaultPoints = Number(args[2]);
			const defaultInterval = Number(args[3]);
			const now = Date.now();

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

		// Replay publish Lua script simulation
		// args layout: [seqKey, bufKey, topic, event, dataJson, maxSize, ttl]
		function evalReplayPublish(numKeys, args) {
			const seqKey = args[0];
			const bufKey = args[1];
			const topic = args[2];
			const event = args[3];
			const dataJson = args[4];
			const maxSize = Number(args[5]);

			// Increment seq
			const v = parseInt(store.get(seqKey) || '0', 10) + 1;
			store.set(seqKey, String(v));
			const seq = v;

			// zadd
			const data = JSON.parse(dataJson);
			const payload = JSON.stringify({ seq, topic, event, data });
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
		_hashes: hashes
	};
}
