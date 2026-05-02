/**
 * In-memory mock that implements the subset of ioredis used by the extensions.
 * No real Redis connection needed.
 */
export function mockRedisClient(keyPrefix = '') {
	const store = new Map();       // key -> value (string)
	const sortedSets = new Map();  // key -> [{score, member}]
	const hashes = new Map();      // key -> Map<field, value>
	const streams = new Map();     // key -> [{id, fields: [[k, v], ...]}]
	const pubsubHandlers = [];     // {channel, handler}
	const functionLibraries = new Map(); // libname -> code
	const registeredFunctions = new Map(); // funcName -> (keys, args) => unknown

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
					if (streams.delete(k)) count++;
				}
				return count;
			},
			async unlink(...keys) {
				return r.del(...keys);
			},
			async expire() { return 1; },
			async pexpire() { return 1; },

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
				set.push({ score: Number(score), member });
				set.sort((a, b) => a.score - b.score);
				return 1;
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
					const ms = Date.now();
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
				if (!hashes.has(key)) hashes.set(key, new Map());
				const h = hashes.get(key);
				let added = 0;
				if (args.length === 2) {
					if (!h.has(String(args[0]))) added++;
					h.set(String(args[0]), String(args[1]));
				} else if (typeof args[0] === 'object' && args[0] !== null) {
					for (const [f, v] of Object.entries(args[0])) {
						if (!h.has(String(f))) added++;
						h.set(String(f), String(v));
					}
				} else {
					for (let i = 0; i < args.length; i += 2) {
						if (!h.has(String(args[i]))) added++;
						h.set(String(args[i]), String(args[i + 1]));
					}
				}
				return added;
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
			// drive version detection in callers.
			async info(/* section */) {
				return r._info ?? '# Server\nredis_version:7.2.0\n';
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
				// Streams replay idempotent publish script (hget cache, then incr + xadd + hset)
				if (script.includes('xadd') && script.includes('hget') && script.includes('hset')) {
					return evalIdmpStreamReplayPublish(numKeys, args);
				}
				// Streams replay publish script (atomic incr + xadd MAXLEN)
				if (script.includes('xadd') && script.includes('MAXLEN')) {
					return evalStreamReplayPublish(numKeys, args);
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
				// Idempotency acquire (SET NX EX, then GET, distinguish pending vs result)
				if (script.includes("'NX', 'EX', ARGV[2]") && script.includes("return {0, '', 1}")) {
					return evalIdempotencyAcquire(args);
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
			},

			_subscribedChannels: subscribedChannels,
			_subscribedPatterns: subscribedPatterns,
			_shardedChannels: shardedChannels,
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

		// Streams idempotent replay publish Lua script simulation
		// args layout: [idmpKey, seqKey, bufKey, requestId, maxSize, ttl, idmpTtl, topic, event, dataJson]
		function evalIdmpStreamReplayPublish(numKeys, args) {
			const idmpKey = args[0];
			const seqKey = args[1];
			const bufKey = args[2];
			const requestId = args[3];
			const maxSize = Number(args[4]);
			const topic = args[7];
			const event = args[8];
			const dataJson = args[9];

			if (!hashes.has(idmpKey)) hashes.set(idmpKey, new Map());
			const idmp = hashes.get(idmpKey);
			if (idmp.has(requestId)) {
				return [1, parseInt(idmp.get(requestId), 10)];
			}

			const v = parseInt(store.get(seqKey) || '0', 10) + 1;
			store.set(seqKey, String(v));
			const seq = v;

			const id = `${seq}-0`;
			if (!streams.has(bufKey)) streams.set(bufKey, []);
			const stream = streams.get(bufKey);
			stream.push({
				id,
				fields: [['topic', topic], ['event', event], ['data', dataJson]]
			});
			if (stream.length > maxSize) {
				stream.splice(0, stream.length - maxSize);
			}

			idmp.set(requestId, String(seq));

			return [0, seq];
		}

		// Streams replay publish Lua script simulation
		// args layout: [seqKey, bufKey, maxSize, ttl, topic, event, dataJson]
		function evalStreamReplayPublish(numKeys, args) {
			const seqKey = args[0];
			const bufKey = args[1];
			const maxSize = Number(args[2]);
			const topic = args[4];
			const event = args[5];
			const dataJson = args[6];

			const v = parseInt(store.get(seqKey) || '0', 10) + 1;
			store.set(seqKey, String(v));
			const seq = v;

			const id = `${seq}-0`;
			if (!streams.has(bufKey)) streams.set(bufKey, []);
			const stream = streams.get(bufKey);
			stream.push({
				id,
				fields: [['topic', topic], ['event', event], ['data', dataJson]]
			});
			if (stream.length > maxSize) {
				stream.splice(0, stream.length - maxSize);
			}
			return seq;
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

		// Idempotency acquire Lua script simulation
		// args layout: [key, sentinel, acquireTtlSec]
		// Returns: [1, '', 0] acquired, [0, '', 1] pending, [0, value, 0] cached result.
		// TTL is not simulated; tests that exercise expiry should mutate the
		// store directly or call the store's purge/clear surface.
		function evalIdempotencyAcquire(args) {
			const key = args[0];
			const sentinel = args[1];
			const existing = store.get(key);
			if (existing === undefined) {
				store.set(key, sentinel);
				return [1, '', 0];
			}
			if (existing === sentinel) {
				return [0, '', 1];
			}
			return [0, existing, 0];
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
		}
	};
}
