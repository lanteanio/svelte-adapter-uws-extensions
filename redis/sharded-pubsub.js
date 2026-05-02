/**
 * Sharded Redis pub/sub bus for svelte-adapter-uws.
 *
 * SPUBLISH / SSUBSCRIBE variant of `createPubSubBus`. In Redis Cluster
 * deployments, sharded pub/sub keeps messages on the shard that owns
 * the channel rather than fanning out across every node, which reduces
 * cluster bus bandwidth proportionally to the number of subscribers
 * actually interested in each topic.
 *
 * Differs from `createPubSubBus` in three ways:
 *
 * - Per-topic channels (`channelPrefix + shardKey(topic)`), not one
 *   shared channel for everything. SSUBSCRIBE has no wildcard, so
 *   every subscriber must SSUBSCRIBE to each channel it cares about.
 * - Dynamic subscription: `follow(topic)` and `unfollow(topic)`
 *   maintain a refcount; `SSUBSCRIBE` on first follower per channel,
 *   `SUNSUBSCRIBE` on the last one out. Wire via `bus.hooks` for
 *   automatic management against WebSocket subscribe/unsubscribe.
 * - Requires Redis 7+. `activate()` runs `INFO server` and throws on
 *   older servers; users on Redis 6 should keep using `createPubSubBus`.
 *
 * @module svelte-adapter-uws-extensions/redis/sharded-pubsub
 */

import { randomBytes } from 'node:crypto';
import { parseRedisVersion } from '../shared/redis-version.js';

/**
 * @typedef {Object} ShardedBusOptions
 * @property {string} [channelPrefix='uws:sharded:'] - Prefix for sharded pub/sub channels.
 * @property {(topic: string) => string} [shardKey] - Map a topic to a shard label. The channel name is `channelPrefix + shardKey(topic)`. Default: identity (one channel per topic).
 */

/**
 * @typedef {Object} ShardedBus
 * @property {(platform: import('svelte-adapter-uws').Platform) => import('svelte-adapter-uws').Platform} wrap
 * @property {(platform: import('svelte-adapter-uws').Platform) => Promise<void>} activate
 * @property {() => Promise<void>} deactivate
 * @property {(topic: string) => Promise<void>} follow
 * @property {(topic: string) => Promise<void>} unfollow
 * @property {{ subscribe: Function, unsubscribe: Function, close: Function }} hooks
 */

/**
 * Create a sharded Redis pub/sub bus.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {ShardedBusOptions} [options]
 * @returns {ShardedBus}
 */
export function createShardedBus(client, options = {}) {
	const channelPrefix = options.channelPrefix ?? 'uws:sharded:';
	if (typeof channelPrefix !== 'string') {
		throw new Error('sharded bus: channelPrefix must be a string');
	}
	if (options.shardKey !== undefined && typeof options.shardKey !== 'function') {
		throw new Error('sharded bus: shardKey must be a function');
	}
	const shardKey = options.shardKey || ((topic) => topic);
	const instanceId = randomBytes(8).toString('hex');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mRelayed = m?.counter('sharded_pubsub_messages_relayed_total', 'Messages SPUBLISHed', ['topic']);
	const mReceived = m?.counter('sharded_pubsub_messages_received_total', 'Messages received via SSUBSCRIBE', ['topic']);
	const mEchoSuppressed = m?.counter('sharded_pubsub_echo_suppressed_total', 'Messages dropped by echo suppression');
	const mParseErrors = m?.counter('sharded_pubsub_parse_errors_total', 'Malformed envelopes dropped on receive');
	const mFollows = m?.counter('sharded_pubsub_ssubscribes_total', 'SSUBSCRIBE calls');
	const mUnfollows = m?.counter('sharded_pubsub_sunsubscribes_total', 'SUNSUBSCRIBE calls');

	function channelFor(topic) {
		return channelPrefix + shardKey(topic);
	}

	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	let active = false;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;

	/** @type {Map<string, number>} topic -> follow refcount */
	const followCounts = new Map();
	/** @type {Map<string, number>} channel -> # of topics resolving to it */
	const channelRefcounts = new Map();
	/** @type {Set<string>} */
	const subscribedChannels = new Set();
	/** @type {WeakMap<any, Set<string>>} ws -> set of followed topics */
	const wsFollows = new WeakMap();

	// Per-channel microtask batch: coalesce SPUBLISHes for the same
	// channel within one tick into a single pipelined call. Each entry
	// tracks the underlying topic list so the relayed-messages counter
	// stays accurate when batch envelopes carry many messages.
	/** @type {Map<string, Array<{msg: string, topics: string[]}>>} */
	let channelBatches = new Map();
	let relayScheduled = false;

	function scheduleRelay(channel, msg, topics) {
		let arr = channelBatches.get(channel);
		if (!arr) {
			arr = [];
			channelBatches.set(channel, arr);
		}
		arr.push({ msg, topics });
		if (!relayScheduled) {
			relayScheduled = true;
			queueMicrotask(flushRelay);
		}
	}

	function flushRelay() {
		const batches = channelBatches;
		channelBatches = new Map();
		relayScheduled = false;
		if (b) {
			try { b.guard(); } catch { return; }
		}
		const pipe = client.redis.pipeline();
		const counts = []; // for metric inc per channel
		for (const [ch, entries] of batches) {
			for (const { msg } of entries) {
				pipe.spublish(ch, msg);
			}
			counts.push({ entries });
		}
		pipe.exec().then(() => {
			for (const { entries } of counts) {
				for (const { topics } of entries) {
					for (let i = 0; i < topics.length; i++) {
						mRelayed?.inc({ topic: mt(topics[i]) });
					}
				}
			}
			b?.success();
		}).catch((err) => { b?.failure(err); });
	}

	async function ensureSubscriber(platform) {
		activePlatform = platform;
		if (subscriber) return;
		subscriber = client.duplicate({ enableReadyCheck: false });
		subscriber.on('error', (err) => {
			console.error('sharded bus subscriber error:', err.message);
		});
		subscriber.on('smessage', (ch, message) => {
			try {
				const parsed = JSON.parse(message);
				// One echo check per envelope; batched envelopes carry
				// one instanceId for the whole batch.
				if (parsed.instanceId === instanceId) {
					mEchoSuppressed?.inc();
					return;
				}
				if (!activePlatform) return;
				if (Array.isArray(parsed.batch)) {
					const local = new Array(parsed.batch.length);
					for (let i = 0; i < parsed.batch.length; i++) {
						const m = parsed.batch[i];
						mReceived?.inc({ topic: mt(m.topic) });
						local[i] = {
							topic: m.topic,
							event: m.event,
							data: m.data,
							options: { relay: false }
						};
					}
					activePlatform.publishBatched(local);
				} else {
					mReceived?.inc({ topic: mt(parsed.topic) });
					activePlatform.publish(parsed.topic, parsed.event, parsed.data, { relay: false });
				}
			} catch {
				// Malformed envelope; counted so a stream of bad messages
				// is observable instead of silently swallowed.
				mParseErrors?.inc();
			}
		});
	}

	async function activate(platform) {
		activePlatform = platform;
		if (active) return;

		b?.guard();
		let info;
		try {
			info = await client.redis.info('server');
		} catch (err) {
			b?.failure(err);
			throw err;
		}
		const major = parseRedisVersion(info);
		if (major === null) {
			b?.failure(new Error('could not parse Redis version'));
			throw new Error('sharded bus: could not parse Redis version from INFO server');
		}
		if (major < 7) {
			b?.failure(new Error(`Redis ${major} too old`));
			throw new Error(`sharded bus: requires Redis 7 or newer (server reports ${major}). Use createPubSubBus for older servers.`);
		}
		b?.success();

		await ensureSubscriber(platform);
		active = true;
	}

	async function deactivate() {
		if (!active) return;
		active = false;
		activePlatform = null;
		if (subscriber) {
			for (const ch of subscribedChannels) {
				await subscriber.sunsubscribe(ch).catch(() => {});
			}
			subscribedChannels.clear();
			await subscriber.quit().catch(() => subscriber.disconnect());
			subscriber = null;
		}
		channelBatches = new Map();
		relayScheduled = false;
		followCounts.clear();
		channelRefcounts.clear();
	}

	async function follow(topic) {
		if (!subscriber) {
			throw new Error('sharded bus: activate() must be called before follow()');
		}
		const count = followCounts.get(topic) || 0;
		followCounts.set(topic, count + 1);
		// Channel refcount tracks distinct active topics resolving to
		// the channel, not raw follow-call count. Only bump when this
		// topic transitions from 0 to active.
		if (count > 0) return;

		const channel = channelFor(topic);
		const chCount = channelRefcounts.get(channel) || 0;
		channelRefcounts.set(channel, chCount + 1);
		if (chCount > 0) return;

		subscribedChannels.add(channel);
		b?.guard();
		try {
			await subscriber.ssubscribe(channel);
			b?.success();
			mFollows?.inc();
		} catch (err) {
			b?.failure(err);
			subscribedChannels.delete(channel);
			channelRefcounts.delete(channel);
			followCounts.set(topic, count);
			throw err;
		}
	}

	async function unfollow(topic) {
		const count = followCounts.get(topic) || 0;
		if (count === 0) return;
		const newCount = count - 1;
		if (newCount > 0) {
			followCounts.set(topic, newCount);
			return;
		}
		followCounts.delete(topic);

		const channel = channelFor(topic);
		const chCount = channelRefcounts.get(channel) || 0;
		const newChCount = chCount - 1;
		if (newChCount > 0) {
			channelRefcounts.set(channel, newChCount);
			return;
		}
		channelRefcounts.delete(channel);

		if (subscriber && subscribedChannels.has(channel)) {
			subscribedChannels.delete(channel);
			try {
				await subscriber.sunsubscribe(channel);
				mUnfollows?.inc();
			} catch {
				// SUNSUBSCRIBE failure is non-fatal -- the connection
				// will be torn down on deactivate anyway.
			}
		}
	}

	const bus = {
		wrap(platform) {
			const wrapped = {
				publish(topic, event, data, opts) {
					const result = platform.publish(topic, event, data, opts);
					if (!opts || opts.relay !== false) {
						const channel = channelFor(topic);
						scheduleRelay(channel, JSON.stringify({ instanceId, topic, event, data }), [topic]);
					}
					return result;
				},
				// Per-event loop. Mirrors `platform.batch` semantics: N submitted
				// messages produce N wire frames per subscriber. Use
				// `publishBatched` instead when you want one wire frame per
				// subscriber per call.
				batch(messages) {
					const results = platform.batch(messages);
					for (let i = 0; i < messages.length; i++) {
						const mes = messages[i];
						if (!mes.options || mes.options.relay !== false) {
							const channel = channelFor(mes.topic);
							scheduleRelay(channel, JSON.stringify({
								instanceId, topic: mes.topic, event: mes.event, data: mes.data
							}), [mes.topic]);
						}
					}
					return results;
				},
				// Wire-batched publish. One SPUBLISH envelope per shard channel
				// per call. Receivers fan out via local `platform.publishBatched`
				// for one wire frame per subscriber. Empty arrays no-op.
				publishBatched(messages) {
					if (!Array.isArray(messages)) {
						throw new TypeError('sharded bus: publishBatched requires an array of messages');
					}
					if (messages.length === 0) return;

					platform.publishBatched(messages);

					/** @type {Map<string, Array<{topic: string, event: string, data: unknown}>>} */
					const byChannel = new Map();
					for (let i = 0; i < messages.length; i++) {
						const mes = messages[i];
						if (mes.options && mes.options.relay === false) continue;
						const ch = channelFor(mes.topic);
						let arr = byChannel.get(ch);
						if (!arr) {
							arr = [];
							byChannel.set(ch, arr);
						}
						arr.push({ topic: mes.topic, event: mes.event, data: mes.data });
					}

					for (const [ch, batch] of byChannel) {
						const topics = new Array(batch.length);
						for (let i = 0; i < batch.length; i++) topics[i] = batch[i].topic;
						scheduleRelay(ch, JSON.stringify({ instanceId, batch }), topics);
					}
				},
				send: platform.send.bind(platform),
				sendTo: platform.sendTo.bind(platform),
				sendCoalesced: platform.sendCoalesced.bind(platform),
				request: platform.request.bind(platform),
				get connections() { return platform.connections; },
				get requestId() { return platform.requestId; },
				get pressure() { return platform.pressure; },
				onPressure: platform.onPressure.bind(platform),
				subscribers: platform.subscribers.bind(platform),
				topic(t) {
					return {
						publish(event, data) { wrapped.publish(t, event, data); },
						created(data) { wrapped.publish(t, 'created', data); },
						updated(data) { wrapped.publish(t, 'updated', data); },
						deleted(data) { wrapped.publish(t, 'deleted', data); },
						set(value) { wrapped.publish(t, 'set', value); },
						increment(amount) { wrapped.publish(t, 'increment', amount); },
						decrement(amount) { wrapped.publish(t, 'decrement', amount); }
					};
				}
			};
			return wrapped;
		},
		activate,
		deactivate,
		follow,
		unfollow,
		hooks: {
			async subscribe(ws, topic) {
				if (topic.startsWith('__')) return;
				let topics = wsFollows.get(ws);
				if (!topics) {
					topics = new Set();
					wsFollows.set(ws, topics);
				}
				if (topics.has(topic)) return;
				topics.add(topic);
				await follow(topic);
			},
			async unsubscribe(ws, topic) {
				const topics = wsFollows.get(ws);
				if (!topics || !topics.has(topic)) return;
				topics.delete(topic);
				if (topics.size === 0) wsFollows.delete(ws);
				await unfollow(topic);
			},
			async close(ws) {
				const topics = wsFollows.get(ws);
				if (!topics) return;
				wsFollows.delete(ws);
				const pending = [];
				for (const t of topics) pending.push(unfollow(t));
				await Promise.all(pending);
			}
		}
	};
	return bus;
}
