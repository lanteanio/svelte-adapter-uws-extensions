/**
 * Redis I/O subsystem for the Redis-backed cursor tracker: the cross-instance
 * pub/sub relay + the coalesced HSET snapshot store + the stale-entry cleanup
 * + the cursor-hash key builder. Owns the swap-and-drain pending-snapshot map,
 * the subscriber connection, and the cleanup/snapshot interval timers.
 *
 * SLOT-SENSITIVE. The cluster correctness invariants live here and MUST NOT be
 * altered: hashKey `cursor:{topic}` braces colocate one topic onto one slot;
 * the multi-slot batches go through execMultiSlot (NEVER a raw redis.pipeline(),
 * which silently no-ops other-node commands); the one single-slot pipeline in
 * queueSnapshot is the legacy per-flush path (same hashKey hset+expire); a
 * single global pub/sub channel, no ssubscribe.
 *
 * @module svelte-adapter-uws-extensions/redis/cursor/redis-io
 */

import { now, setIntervalTimer, clearIntervalTimer } from '../../shared/runtime.js';
import { evalCached } from '../../shared/eval-cached.js';
import { CLEANUP_SCRIPT } from '../../shared/scripts.js';
import { execMultiSlot } from '../../shared/cluster.js';
import { EVENTS } from './events.js';

/**
 * Create the cursor Redis I/O subsystem. State (pending snapshots, subscriber,
 * timers) is private to the returned closure; the tracker drives it through the
 * exposed surface. The peer-message router (`onMessage`) and the local wire
 * `emit` are injected so this module never imports the scheduler.
 *
 * @param {{
 *   client: import('../index.js').RedisClient, redis: import('ioredis').Redis,
 *   channel: string, instanceId: string, b: any, validator: any,
 *   cursorTtl: number, snapshotIntervalMs: number, activeTopics: Set<string>,
 *   emit: Function, onMessage: (parsed: any, platform: any) => void, queueRemove: Function
 * }} deps
 */
export function createRedisIo({
	client,
	redis,
	channel,
	instanceId,
	b,
	validator,
	cursorTtl,
	snapshotIntervalMs,
	activeTopics,
	emit,
	onMessage,
	queueRemove
}) {
	const cursorTtlMs = cursorTtl * 1000;
	const cleanupInterval = Math.max(cursorTtlMs, 10000);

	/**
	 * Coalesced HSET writes. Latest-wins per (topic, key). Drained on the
	 * `snapshotIntervalMs` timer into a single `pipe.hset(topic, f1, v1, ...)`
	 * per topic per tick. The broadcast/relay path queues entries here but
	 * does not await the write.
	 * @type {Map<string, Map<string, { user: any, data: any, ts: number }>>}
	 */
	let redisPending = new Map();

	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	let subscriberReady = null;
	let cleanupTimer = null;
	let snapshotTimer = null;

	function ensureSubscriber(platform) {
		activePlatform = platform;
		if (subscriber) return;
		if (b && b.state === 'broken') return;
		const sub = client.duplicate({ enableReadyCheck: false });
		subscriber = sub;
		sub.on('error', (err) => {
			console.error('cursor subscriber error:', err.message);
		});
		sub.on('message', (ch, message) => {
			if (ch !== channel) return;
			if (!validator.acceptRaw(message)) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
				if (!validator.acceptEnvelope(parsed.topic, parsed.event)) return;
				if (!activePlatform) return;
				onMessage(parsed, activePlatform);
			} catch {
				// Malformed, skip
			}
		});
		subscriberReady = sub.subscribe(channel).then(async () => {
			if (!activePlatform || activeTopics.size === 0) return;
			const topicList = [...activeTopics];
			const commands = topicList.map((topic) => ['hgetall', hashKey(topic)]);
			let results;
			try {
				results = await execMultiSlot(redis, commands);
			} catch { return; }
			if (!activePlatform) return;
			const nowTs = now();
			for (let i = 0; i < topicList.length; i++) {
				const [, all] = results[i];
				if (!all) continue;
				const topic = topicList[i];
				const catalogEntries = [];
				const positionEntries = [];
				for (const key of Object.keys(all)) {
					if (key.startsWith(instanceId + ':')) continue;
					try {
						const parsed = JSON.parse(all[key]);
						if (parsed.ts && (nowTs - parsed.ts) <= cursorTtlMs) {
							catalogEntries.push({ key, user: parsed.user });
							positionEntries.push({ key, data: parsed.data });
						}
					} catch { /* skip */ }
				}
				if (catalogEntries.length > 0 && activePlatform) {
					emit('__cursor:' + topic, EVENTS.CATALOG, catalogEntries, activePlatform, { relay: false });
					emit('__cursor:' + topic, EVENTS.BULK, positionEntries, activePlatform, { relay: false });
				}
			}
		}).catch(() => {
			sub.quit().catch(() => sub.disconnect());
			if (subscriber === sub) {
				subscriber = null;
			}
		}).finally(() => {
			subscriberReady = null;
		});
	}

	function startCleanupTimer() {
		if (cleanupTimer) return;
		cleanupTimer = setIntervalTimer(() => {
			const nowTs = now();
			for (const topic of activeTopics) {
				evalCached(redis, CLEANUP_SCRIPT, 1, hashKey(topic), nowTs, cursorTtlMs).catch((err) => {
					console.warn('cursor cleanup: stale removal failed for topic "' + topic + '":', err.message);
				});
			}
		}, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	function stopCleanupTimer() {
		if (cleanupTimer && activeTopics.size === 0) {
			clearIntervalTimer(cleanupTimer);
			cleanupTimer = null;
		}
		if (snapshotTimer && activeTopics.size === 0) {
			clearIntervalTimer(snapshotTimer);
			snapshotTimer = null;
		}
	}

	function startSnapshotTimer() {
		if (snapshotTimer || snapshotIntervalMs === 0) return;
		snapshotTimer = setIntervalTimer(flushSnapshot, snapshotIntervalMs);
		if (snapshotTimer.unref) snapshotTimer.unref();
	}

	function flushSnapshot() {
		if (redisPending.size === 0) return;
		if (b) { try { b.guard(); } catch { redisPending = new Map(); return; } }
		const pending = redisPending;
		redisPending = new Map();
		const commands = [];
		let queued = 0;
		for (const [topic, entries] of pending) {
			if (entries.size === 0) continue;
			const args = [];
			for (const [key, entry] of entries) {
				args.push(key, JSON.stringify({ user: entry.user, data: entry.data, ts: entry.ts }));
			}
			commands.push(['hset', hashKey(topic), ...args]);
			commands.push(['expire', hashKey(topic), cursorTtl]);
			queued += entries.size;
		}
		if (queued === 0) return;
		execMultiSlot(redis, commands).then(() => b?.success()).catch((err) => b?.failure(err));
	}

	function queueSnapshot(topic, key, user, data) {
		if (snapshotIntervalMs === 0) {
			if (b) { try { b.guard(); } catch { return; } }
			const pipe = redis.pipeline();
			pipe.hset(hashKey(topic), key, JSON.stringify({ user, data, ts: now() }));
			pipe.expire(hashKey(topic), cursorTtl);
			pipe.exec().then(() => b?.success()).catch((err) => b?.failure(err));
			return;
		}
		let topicPending = redisPending.get(topic);
		if (!topicPending) {
			topicPending = new Map();
			redisPending.set(topic, topicPending);
		}
		topicPending.set(key, { user, data, ts: now() });
	}

	function hashKey(topic) {
		return client.key('cursor:{' + topic + '}');
	}

	function relay(topic, event, payload) {
		if (b) { try { b.guard(); } catch { return; } }
		const msg = JSON.stringify({ instanceId, topic, event, payload });
		if (subscriberReady) {
			subscriberReady.then(() => redis.publish(channel, msg).catch(() => {}));
		} else {
			redis.publish(channel, msg).catch(() => {});
		}
	}

	async function broadcastRemove(topic, key, platform) {
		if (b) { try { b.guard(); } catch { return false; } }

		try {
			await redis.hdel(hashKey(topic), key);
			b?.success();
		} catch (err) {
			b?.failure(err);
			return false;
		}

		// Drop any pending snapshot write for this key so we do not
		// resurrect a removed cursor on the next snapshot tick.
		const topicPending = redisPending.get(topic);
		if (topicPending) {
			topicPending.delete(key);
			if (topicPending.size === 0) redisPending.delete(topic);
		}

		queueRemove(topic, key, platform);
		relay(topic, EVENTS.REMOVE, { key });
		return true;
	}

	/** The pending-snapshot map for a topic, or undefined. Read by list(). */
	function getTopicPending(topic) {
		return redisPending.get(topic);
	}

	/** Drop a topic's entire pending-snapshot bucket (topic teardown). */
	function dropTopicPending(topic) {
		redisPending.delete(topic);
	}

	/** Drop one key's pending snapshot, removing the bucket when it empties. */
	function dropKeyPending(topic, key) {
		const topicPending = redisPending.get(topic);
		if (topicPending) {
			topicPending.delete(key);
			if (topicPending.size === 0) redisPending.delete(topic);
		}
	}

	/**
	 * Remove one connection key across several topics in one multi-slot batch:
	 * an HDEL of the key from each topic hash plus the REMOVE publish, atomic per
	 * slot. execMultiSlot (NOT pipeline) so the hdel+publish for an other-node
	 * topic is not silently dropped. Returns false on breaker-open/Redis error.
	 */
	async function removeKeysBatch(topics, key) {
		const commands = [];
		for (const t of topics) {
			commands.push(['hdel', hashKey(t), key]);
			commands.push(['publish', channel, JSON.stringify({
				instanceId, topic: t, event: EVENTS.REMOVE, payload: { key }
			})]);
		}
		try {
			await execMultiSlot(redis, commands);
			b?.success();
			return true;
		} catch (err) {
			b?.failure(err);
			return false;
		}
	}

	/** Swap in a fresh pending map (tracker clear()). The old map is dropped. */
	function resetPending() {
		redisPending = new Map();
	}

	/** Terminal teardown (tracker destroy()): stop timers, quit the subscriber. */
	function dispose() {
		if (cleanupTimer) clearIntervalTimer(cleanupTimer);
		cleanupTimer = null;
		if (snapshotTimer) clearIntervalTimer(snapshotTimer);
		snapshotTimer = null;
		if (subscriber) {
			subscriber.quit().catch(() => subscriber.disconnect());
			subscriber = null;
		}
		activePlatform = null;
	}

	return {
		ensureSubscriber,
		startCleanupTimer,
		startSnapshotTimer,
		stopCleanupTimer,
		flushSnapshot,
		queueSnapshot,
		hashKey,
		relay,
		broadcastRemove,
		getTopicPending,
		dropTopicPending,
		dropKeyPending,
		removeKeysBatch,
		resetPending,
		dispose
	};
}
