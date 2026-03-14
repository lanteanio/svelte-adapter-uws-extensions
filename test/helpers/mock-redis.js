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
			async zrangebyscore(key, min, max) {
				const set = sortedSets.get(key);
				if (!set) return [];
				const lo = min === '-inf' ? -Infinity : Number(min);
				const hi = max === '+inf' ? Infinity : Number(max);
				return set.filter((e) => e.score >= lo && e.score <= hi).map((e) => e.member);
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
				// Rate limit script (token bucket)
				if (script.includes('bannedUntil')) {
					return evalRateLimit(args);
				}
				// Replay publish script (atomic incr + zadd + trim)
				if (script.includes('zremrangebyrank') && script.includes('cjson.encode')) {
					return evalReplayPublish(numKeys, args);
				}
				// Presence join script (hset + check if first live instance for user)
				if (script.includes('hset') && script.includes('suffix') && !script.includes('hdel')) {
					return evalPresenceJoin(args);
				}
				// Presence leave script (hdel + check remaining by suffix)
				if (script.includes('hdel') && script.includes('suffix')) {
					return evalPresenceLeave(args);
				}
				// Group join script (atomic capacity check + insert)
				if (script.includes('cjson.decode') && script.includes('liveCount')) {
					return evalGroupJoin(args);
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

			_subscribedChannels: subscribedChannels,
			_listeners: listeners
		};

		// Rate limit Lua script simulation
		function evalRateLimit(args) {
			const key = args[0];
			const maxPoints = Number(args[1]);
			const interval = Number(args[2]);
			const cost = Number(args[3]);
			const now = Number(args[4]);
			const blockDuration = Number(args[5]);

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

		// Presence join Lua script simulation
		function evalPresenceJoin(args) {
			const key = args[0];
			const field = args[1];
			const value = args[2];
			const suffix = args[3];
			const now = Number(args[4]);
			const ttlMs = Number(args[5]);

			// hset
			if (!hashes.has(key)) hashes.set(key, new Map());
			hashes.get(key).set(field, value);

			// Check if another live instance already has this user
			const h = hashes.get(key);
			for (const [f, v] of h) {
				if (f === field) continue;
				if (f.length >= suffix.length && f.slice(-suffix.length) === suffix) {
					try {
						const parsed = JSON.parse(v);
						if (parsed.ts && (now - parsed.ts) <= ttlMs) {
							return 0; // User already present on another live instance
						}
					} catch { /* skip */ }
				}
			}
			return 1; // First live instance for this user
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

		// Group join Lua script simulation
		function evalGroupJoin(args) {
			const key = args[0];
			const maxMembers = Number(args[1]);
			const memberId = args[2];
			const memberData = args[3];
			const now = Number(args[4]);
			const memberTtlMs = Number(args[5]);

			if (!hashes.has(key)) hashes.set(key, new Map());
			const h = hashes.get(key);

			// Count live members, remove stale
			let liveCount = 0;
			const toRemove = [];
			for (const [f, v] of h) {
				try {
					const val = JSON.parse(v);
					if (val.ts && (now - val.ts) <= memberTtlMs) {
						liveCount++;
					} else {
						toRemove.push(f);
					}
				} catch {
					toRemove.push(f);
				}
			}
			for (const f of toRemove) h.delete(f);

			if (liveCount >= maxMembers) {
				return 0;
			}
			h.set(memberId, memberData);
			return 1;
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
