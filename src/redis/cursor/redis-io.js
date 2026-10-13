/**
 * Redis I/O subsystem for the Redis-backed cursor tracker: the cross-instance
 * pub/sub relay + the coalesced HSET snapshot store + the stale-entry cleanup
 * + the cursor-hash key builder. Owns the swap-and-drain pending-snapshot map,
 * the subscriber connection, and the cleanup/snapshot interval timers.
 *
 * SLOT-SENSITIVE. The cluster correctness invariants live here and MUST NOT be
 * altered: hashKey `cursor:{topic}` braces colocate one topic onto one slot;
 * the multi-slot batches go through execMultiSlot or coalescedByOwner (NEVER a
 * raw redis.pipeline(), which silently no-ops other-node commands); the one
 * single-slot pipeline in queueSnapshot is the legacy per-flush path (same
 * hashKey hset+expire); a single global pub/sub channel, no ssubscribe.
 *
 * @module svelte-adapter-uws-extensions/redis/cursor/redis-io
 */

import { now, setIntervalTimer, clearIntervalTimer, setTimer, clearTimer } from '../../shared/runtime.js';
import { evalCachedName } from '../../shared/eval-cached.js';
import { CLEANUP_SCRIPT } from '../../shared/scripts.js';
import { execMultiSlot, firstCommandError, coalescedByOwner } from '../../shared/cluster.js';
import { EVENTS } from './events.js';
import { EXTENSIONS_ERROR_IDS } from '../../error-registry.js';
import { emitRegisteredFailure } from '../../shared/diagnostic.js';
import { createInputDropReporter } from '../../shared/input-drops.js';
import { openSubscriber, SUBSCRIBER_RESTORING, SUBSCRIBER_RESTORED } from '../../shared/redis-subscriber.js';

// Backoff for reopening a relay subscription that could not open.
const SUBSCRIBE_RETRY_MIN_MS = 1000;
const SUBSCRIBE_RETRY_MAX_MS = 30000;

/**
 * Create the cursor Redis I/O subsystem. State (pending snapshots, subscriber,
 * timers) is private to the returned closure; the tracker drives it through the
 * exposed surface. The peer-message router (`onMessage`) and the local wire
 * recovery delivery are injected so this module never imports the scheduler.
 *
 * @param {{
 *   client: import('../index.js').RedisClient, redis: import('ioredis').Redis,
 *   channel: string, instanceId: string, b: any, validator: any,
 *   cursorTtl: number, snapshotIntervalMs: number, activeTopics: Set<string>, mergeRoster: Function, watchedTopics: Function, recoverTopic: Function,
 *   onMessage: (parsed: any, platform: any) => void, queueRemove: Function,
 *   inUse: () => boolean, inputDropped?: (reason: string, stage: string) => void
 * }} deps
 */
export function createRedisIo({
	inputDropped = createInputDropReporter(null, 'cursor', EXTENSIONS_ERROR_IDS.CURSOR_INPUT_DROPPED),
	client,
	redis,
	channel,
	instanceId,
	b,
	validator,
	cursorTtl,
	snapshotIntervalMs,
	activeTopics,
	mergeRoster,
	watchedTopics,
	recoverTopic,
	onMessage,
	queueRemove,
	inUse
}) {
	const cursorTtlMs = cursorTtl * 1000;
	const cleanupInterval = Math.max(cursorTtlMs, 10000);
	const hashKeyPrefix = client.key('cursor:{');

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
	// One report per failure streak: every cursor update retries a failed subscribe.
	let subscribeFailing = false;
	// An instance whose connections only watch never calls update(), so a
	// subscription that could not open (the breaker open, SUBSCRIBE refused) is
	// retried on a timer, backing off to SUBSCRIBE_RETRY_MAX_MS, while a
	// connection on this instance still uses the cursor (inUse); after that the
	// next attach or update opens it.
	let subscribeRetryTimer = null;
	let subscribeRetryMs = SUBSCRIBE_RETRY_MIN_MS;
	let disposed = false;
	let cleanupTimer = null;
	let snapshotTimer = null;

	let recoveryNeeded = true;
	let recoveryRunning = null;
	let recoveryTimer = null;
	let recoveryRetryMs = SUBSCRIBE_RETRY_MIN_MS;
	let recoveryGeneration = 0;
	let relayOnline = false;

	function retryRecoveryLater() {
		if (disposed || recoveryTimer !== null || !recoveryNeeded || !inUse()) return;
		recoveryTimer = setTimer(() => {
			recoveryTimer = null;
			catchUp();
		}, recoveryRetryMs);
		recoveryTimer?.unref?.();
		recoveryRetryMs = Math.min(recoveryRetryMs * 2, SUBSCRIBE_RETRY_MAX_MS);
	}

	function catchUp() {
		if (recoveryRunning) return recoveryRunning;
		if (!recoveryNeeded || !relayOnline || disposed || !activePlatform) return Promise.resolve();
		const generation = recoveryGeneration;
		const topicList = [...new Set([...activeTopics, ...watchedTopics()])];
		if (topicList.length === 0) { recoveryNeeded = false; return Promise.resolve(); }
		const run = (async () => {
			try { b?.guard(); } catch { inputDropped('breaker_open', 'catch_up'); return; }
			let results;
			try {
				results = await execMultiSlot(redis, topicList.map((topic) => ['hgetall', hashKey(topic)]));
				const error = firstCommandError(results);
				if (error) throw error;
				b?.success();
			} catch (error) { inputDropped('redis_failure', 'catch_up'); b?.failure(error); return; }
			if (disposed || !activePlatform || generation !== recoveryGeneration || !relayOnline) return;
			for (let i = 0; i < topicList.length; i++) {
				const topic = topicList[i];
				const cursors = mergeRoster(topic, results[i][1]);
				recoverTopic(topic, cursors, activePlatform);
			}
			recoveryNeeded = false;
			recoveryRetryMs = SUBSCRIBE_RETRY_MIN_MS;
		})().catch(() => { inputDropped('delivery_failed', 'catch_up'); }).finally(() => {
			if (recoveryRunning === run) recoveryRunning = null;
			if (recoveryNeeded) retryRecoveryLater();
		});
		recoveryRunning = run;
		return run;
	}

	function retrySubscribeLater() {
		if (subscribeRetryTimer !== null || disposed) return;
		subscribeRetryTimer = setTimer(() => {
			subscribeRetryTimer = null;
			if (subscriber || !activePlatform) return;
			if (!inUse()) {
				// Nothing here uses the cursor any more: the next attach or update
				// opens the relay, starting a new backoff and failure streak.
				subscribeRetryMs = SUBSCRIBE_RETRY_MIN_MS;
				subscribeFailing = false;
				return;
			}
			ensureSubscriber(activePlatform);
		}, subscribeRetryMs);
		if (subscribeRetryTimer.unref) subscribeRetryTimer.unref();
		subscribeRetryMs = Math.min(subscribeRetryMs * 2, SUBSCRIBE_RETRY_MAX_MS);
	}

	/**
	 * Open the relay subscription if it is not open. Returns the pending
	 * subscribe (settled once SUBSCRIBE is answered and the catch-up has run,
	 * never rejected) so a watcher can wait for it before reading the snapshot,
	 * or undefined when there is nothing to wait for.
	 * @returns {Promise<void> | undefined}
	 */
	function ensureSubscriber(platform) {
		if (disposed) return undefined;
		activePlatform = platform;
		if (subscriber) return subscriberReady ?? undefined;
		if (b && b.state === 'broken') {
			inputDropped('breaker_open', 'subscribe');
			retrySubscribeLater();
			return undefined;
		}
		const sub = openSubscriber(client, 'cursor');
		subscriber = sub;
		sub.on(SUBSCRIBER_RESTORING, () => {
			relayOnline = false;
			recoveryNeeded = true;
			recoveryGeneration++;
			// Keep the TTL-bounded live roster: the hash can lag the relay or
			// disappear on restart, so a gap is not evidence that users left.
		});
		sub.on(SUBSCRIBER_RESTORED, () => {
			relayOnline = true;
			catchUp();
		});
		sub.on('error', (err) => {
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.CURSOR_SUBSCRIBER, { error: err });
		});
		sub.on('message', (ch, message) => {
			if (ch !== channel) return;
			if (!validator.acceptRaw(message)) { inputDropped('invalid_raw', 'relay_receive'); return; }
			let parsed;
			try { parsed = JSON.parse(message); } catch { inputDropped('invalid_json', 'relay_receive'); return; }
			try {
				if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) { inputDropped('invalid_envelope', 'relay_receive'); return; }
				if (parsed.instanceId === instanceId) return;
				if (!validator.acceptEnvelope(parsed.topic, parsed.event)) { inputDropped('invalid_envelope', 'relay_receive'); return; }
				if (!activePlatform) return;
				onMessage(parsed, activePlatform);
			} catch {
				inputDropped('delivery_failed', 'relay_receive');
			}
		});
		const ready = sub.subscribe(channel).then(() => {
			subscribeFailing = false;
			subscribeRetryMs = SUBSCRIBE_RETRY_MIN_MS;
			relayOnline = true;
			return catchUp();
		}, (err) => {
			sub.quit().catch(() => sub.disconnect());
			if (subscriber === sub) {
				subscriber = null;
			}
			if (!subscribeFailing) {
				subscribeFailing = true;
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.CURSOR_SUBSCRIBE, { error: err });
			}
			retrySubscribeLater();
		}).finally(() => {
			// A newer subscribe may have started after this one failed.
			if (subscriberReady === ready) subscriberReady = null;
		});
		subscriberReady = ready;
		return ready;
	}

	// One report per failure streak: a Redis outage fails every topic on every
	// sweep, and a line per topic per sweep would bury everything else. A streak
	// that narrows from every topic failing (an outage) to only some (a bad
	// entry) is reported once more, so the narrower failure is not hidden behind
	// the outage that preceded it.
	/** @type {'all' | 'some' | null} */
	let cleanupReported = null;

	// Sweeps and snapshot writes run one at a time per Redis owner, so an owner
	// that stops answering never holds back the topics on the others.
	const sweepTopics = coalescedByOwner(redis, {
		key: hashKey,
		commands(topic, out) {
			if (activeTopics.has(topic)) out.push([evalCachedName(redis, CLEANUP_SCRIPT), 1, hashKey(topic), now(), cursorTtlMs]);
		},
		onComplete(sweep) {
			// A sweep that skipped a busy owner does not end a failure streak.
			if (sweep.error === null) { if (sweep.total > 0 && sweep.deferred === 0) cleanupReported = null; return; }
			const scope = sweep.failed === sweep.total ? 'all' : 'some';
			if (cleanupReported !== null && !(cleanupReported === 'all' && scope === 'some')) return;
			cleanupReported = scope;
			const key = sweep.failedCommand?.[2];
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.CURSOR_CLEANUP, {
				error: sweep.error,
				redact: typeof key === 'string' ? [key, key.slice(hashKeyPrefix.length, -1)] : [],
				attributes: { failed: sweep.failed, topics: sweep.total }
			});
		}
	});

	function cleanup() {
		if (disposed || activeTopics.size === 0) return;
		sweepTopics(Array.from(activeTopics));
	}

	function startCleanupTimer() {
		if (cleanupTimer) return;
		cleanupTimer = setIntervalTimer(cleanup, cleanupInterval);
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

	// A topic's pending values stay in redisPending, merging the newest ones,
	// until a write for its owner starts and takes them.
	const writeSnapshots = coalescedByOwner(redis, {
		key: hashKey,
		commands(topic, out) {
			const entries = redisPending.get(topic);
			if (!entries) return;
			redisPending.delete(topic);
			const args = [];
			for (const [key, entry] of entries) {
				try { args.push(key, JSON.stringify({ user: entry.user, data: entry.data, ts: entry.ts })); }
				catch { inputDropped('serialization_failed', 'snapshot_write'); }
			}
			if (args.length === 0) return;
			const key = hashKey(topic);
			out.push(['hset', key, ...args], ['expire', key, cursorTtl]);
		},
		// A write that waited for its owner skips while the breaker is not
		// healthy; the next flush decides on the values it left pending.
		current: b ? () => b.isHealthy : null,
		onError(err) { inputDropped('redis_failure', 'snapshot_write'); b?.failure(err); },
		onComplete(write) {
			if (write.total > 0 && write.error === null) b?.success();
		}
	});

	function flushSnapshot() {
		if (disposed) return;
		if (redisPending.size === 0) return;
		if (b) { try { b.guard(); } catch { inputDropped('breaker_open', 'snapshot_write'); redisPending = new Map(); return; } }
		writeSnapshots(Array.from(redisPending.keys()));
	}

	function queueSnapshot(topic, key, user, data) {
		if (snapshotIntervalMs === 0) {
			if (b) { try { b.guard(); } catch { inputDropped('breaker_open', 'snapshot_write'); return; } }
			let value;
			try { value = JSON.stringify({ user, data, ts: now() }); } catch { inputDropped('serialization_failed', 'snapshot_write'); return; }
			const pipe = redis.pipeline();
			pipe.hset(hashKey(topic), key, value);
			pipe.expire(hashKey(topic), cursorTtl);
			pipe.exec().then(settleSnapshot).catch((err) => { inputDropped('redis_failure', 'snapshot_write'); b?.failure(err); });
			return;
		}
		let topicPending = redisPending.get(topic);
		if (!topicPending) {
			topicPending = new Map();
			redisPending.set(topic, topicPending);
		}
		topicPending.set(key, { user, data, ts: now() });
	}

	// A snapshot write fails when Redis refuses any of its commands, not only
	// when the batch is rejected.
	function settleSnapshot(results) {
		const err = firstCommandError(results);
		if (err) { inputDropped('redis_failure', 'snapshot_write'); b?.failure(err); }
		else b?.success();
	}

	function hashKey(topic) {
		return client.key('cursor:{' + topic + '}');
	}

	function relay(topic, event, payload) {
		let msg;
		try { msg = JSON.stringify({ instanceId, topic, event, payload }); } catch { inputDropped('serialization_failed', 'relay_send'); return; }
		const publish = () => {
			if (disposed) return;
			try { b?.guard(); } catch { inputDropped('breaker_open', 'relay_send'); return; }
			redis.publish(channel, msg).then(() => b?.success(), (error) => { inputDropped('redis_failure', 'relay_send'); b?.failure(error); });
		};
		if (subscriberReady) subscriberReady.then(publish);
		else publish();
	}

	async function broadcastRemove(topic, key, platform) {
		if (b) { try { b.guard(); } catch { inputDropped('breaker_open', 'remove'); return false; } }

		try {
			await redis.hdel(hashKey(topic), key);
			b?.success();
		} catch (err) {
			inputDropped('redis_failure', 'remove');
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
	 * topic is not silently dropped. Returns false when Redis did not take an
	 * HDEL; a refused REMOVE publish still reaches the breaker but does not
	 * undo the erasure (other instances age the entry out).
	 */
	async function removeKeysBatch(topics, key) {
		const commands = [];
		for (const t of topics) {
			commands.push(['hdel', hashKey(t), key]);
			commands.push(['publish', channel, JSON.stringify({
				instanceId, topic: t, event: EVENTS.REMOVE, payload: { key }
			})]);
		}
		let results;
		try {
			results = await execMultiSlot(redis, commands);
		} catch (err) {
			inputDropped('redis_failure', 'remove');
			b?.failure(err);
			return false;
		}
		const err = firstCommandError(results);
		if (err) { inputDropped('redis_failure', 'remove'); b?.failure(err); }
		else b?.success();
		for (let i = 0; i < results.length; i += 2) {
			if (results[i] && results[i][0]) return false;
		}
		return true;
	}

	/** Swap in a fresh pending map (tracker clear()). The old map is dropped. */
	function resetPending() {
		redisPending = new Map();
	}

	/** Terminal teardown (tracker destroy()): stop timers, quit the subscriber. */
	function dispose() {
		disposed = true;
		sweepTopics.stop();
		writeSnapshots.stop();
		if (recoveryTimer !== null) clearTimer(recoveryTimer);
		recoveryTimer = null;
		if (subscribeRetryTimer !== null) clearTimer(subscribeRetryTimer);
		subscribeRetryTimer = null;
		if (cleanupTimer) clearIntervalTimer(cleanupTimer);
		cleanupTimer = null;
		if (snapshotTimer) clearIntervalTimer(snapshotTimer);
		snapshotTimer = null;
		if (subscriber) {
			const sub = subscriber;
			subscriber = null;
			sub.quit().catch(() => sub.disconnect());
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
