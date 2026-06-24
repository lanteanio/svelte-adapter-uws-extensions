/**
 * One-time migration of a topic's replay buffer from the default sorted-set
 * storage to the stream storage.
 *
 * Both backends share the per-topic `replay:seq:{topic}` and
 * `replay:epoch:{topic}` counter keys (identical key names and semantics), so a
 * migration only has to copy the message BUFFER - it never touches the seq high
 * water or the epoch generation, and resume-by-seq / resume-by-epoch keep
 * working across the switch automatically. The source buffer
 * (`replay:buf:{topic}`, a sorted set) and the target buffer
 * (`replay:streambuf:{topic}`, a stream) use different key prefixes, so the
 * migration is non-destructive by construction: it writes the stream and leaves
 * the sorted set in place for the operator to delete after verifying.
 *
 * Opt-in (the on-the-wire entry IDs change shape from a sorted-set score to a
 * `<seq>-0` stream ID), discrete-command (no Lua, so it is cluster-portable),
 * and idempotent at TOPIC granularity (a re-run skips an already-migrated topic
 * unless `force` is set). The stream is written in the current entry format
 * (`event` + `data`, no per-entry topic field), so a migrated buffer is already
 * in the compact layout.
 *
 * @module svelte-adapter-uws-extensions/redis/replay-migrate
 */

import { scanKeys } from '../shared/redis-scan.js';

// The sorted-set per-topic buffer key shape: `replay:buf:{<topic>}`. The braces
// are LITERAL Redis hash-tag characters wrapping the topic (so all of a topic's
// keys co-locate on one slot), not a glob group. The discovery SCAN globs the
// topic with `*` inside the literal braces; the stream backend's buffer is
// `replay:streambuf:{<topic>}`, a distinct prefix, so the glob never matches it.
const SRC_BUF_PREFIX = 'replay:buf:{';

function srcBufKey(client, topic) {
	return client.key('replay:buf:{' + topic + '}');
}

function dstBufKey(client, topic) {
	return client.key('replay:streambuf:{' + topic + '}');
}

/**
 * Migrate one or more topics' replay buffers from the sorted-set backend to the
 * stream backend.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {object} [options]
 * @param {string[]} [options.topics] - Migrate exactly these topics. Omitted ->
 *   discover every sorted-set buffer via a cluster-aware SCAN and migrate all.
 * @param {number} [options.size=1000] - The `MAXLEN ~` cap applied to the target
 *   stream. Match the `size` the deployment passes to `createReplay`.
 * @param {boolean} [options.force=false] - When the target stream already has
 *   entries: `false` skips the topic (`reason: 'target-exists'`); `true` UNLINKs
 *   the target first and re-migrates (recovery from a crashed run).
 * @param {boolean} [options.dryRun=false] - Compute and report the plan without
 *   writing anything.
 * @returns {Promise<{ migrated: Array<{ topic: string, entries: number, highWaterSeq: number }>, skipped: Array<{ topic: string, reason: string, error?: string }> }>}
 *   `migrated[].entries` is the number of source entries written to the stream
 *   (before any `MAXLEN ~` trim); `highWaterSeq` is the largest seq migrated (0
 *   for an empty source).
 */
export async function migrateReplayToStream(client, options = {}) {
	if (!client || typeof client !== 'object' || !client.redis || typeof client.key !== 'function') {
		throw new Error('migrateReplayToStream: a redis client (from createRedisClient) is required');
	}
	const { topics, size = 1000, force = false, dryRun = false } = options;
	if (topics !== undefined &&
		(!Array.isArray(topics) || topics.some((t) => typeof t !== 'string' || t.length === 0))) {
		throw new Error('migrateReplayToStream: topics must be an array of non-empty strings');
	}
	if (typeof size !== 'number' || !Number.isInteger(size) || size < 1) {
		throw new Error(`migrateReplayToStream: size must be a positive integer, got ${size}`);
	}
	if (typeof force !== 'boolean') {
		throw new Error(`migrateReplayToStream: force must be a boolean, got ${force}`);
	}
	if (typeof dryRun !== 'boolean') {
		throw new Error(`migrateReplayToStream: dryRun must be a boolean, got ${dryRun}`);
	}

	const redis = client.redis;
	const migrated = [];
	const skipped = [];

	// 1. Enumerate the topics to migrate. An explicit list is used verbatim;
	//    otherwise discover every sorted-set buffer key and strip the literal
	//    `replay:buf:{` prefix and trailing `}` to recover each topic. (A SCAN
	//    failure here is fatal - we cannot know what to migrate; a single
	//    topic's later failure is isolated below.)
	let topicList;
	if (topics !== undefined) {
		topicList = topics;
	} else {
		const matchPrefix = client.key(SRC_BUF_PREFIX);
		const keys = await scanKeys(redis, matchPrefix + '*}');
		topicList = [];
		for (const k of keys) {
			if (k.startsWith(matchPrefix) && k.endsWith('}')) {
				topicList.push(k.slice(matchPrefix.length, -1));
			}
		}
	}

	for (const topic of topicList) {
		try {
			const dstKey = dstBufKey(client, topic);

			// 2. Guard the target. A non-empty target means the topic was already
			//    migrated; skip unless forced (then UNLINK and re-migrate).
			const existing = await redis.xlen(dstKey);
			if (existing > 0) {
				if (!force) {
					skipped.push({ topic, reason: 'target-exists' });
					continue;
				}
				if (!dryRun) await redis.unlink(dstKey);
			}

			// 3. Read the source in ascending score (== ascending seq == oldest
			//    first), parse each member, and skip any that are corrupt or carry
			//    a non-increasing seq (an explicit `<seq>-0` XADD requires strictly
			//    increasing IDs - the ascending read guarantees it for well-formed
			//    data, and the guard makes a degenerate duplicate a skip, not a
			//    throw that would abort the topic).
			const members = await redis.zrange(srcBufKey(client, topic), 0, -1);
			const entries = [];
			let highWaterSeq = 0;
			for (const member of members) {
				let parsed;
				try {
					parsed = JSON.parse(member);
				} catch {
					continue; // skip corrupt member (mirrors the backends' skip-corrupt)
				}
				if (!parsed || typeof parsed !== 'object') continue;
				const seq = parsed.seq;
				if (!Number.isInteger(seq) || seq < 1 || seq <= highWaterSeq) continue;
				if (typeof parsed.event !== 'string') continue;
				highWaterSeq = seq;
				// Re-stringify data so the stream's `data` field matches the
				// JSON.stringify(data ?? null) the backend's own publish writes.
				entries.push({ seq, event: parsed.event, data: JSON.stringify(parsed.data ?? null) });
			}

			// 4. Write to the stream, preserving the seq as the `<seq>-0` ID, in the
			//    current compact format (no per-entry topic field). MAXLEN ~ keeps
			//    the newest `size`. Skipped entirely on a dry run.
			if (!dryRun) {
				for (const e of entries) {
					await redis.xadd(
						dstKey, 'MAXLEN', '~', size, e.seq + '-0',
						'event', e.event, 'data', e.data
					);
				}
			}

			migrated.push({ topic, entries: entries.length, highWaterSeq });
		} catch (err) {
			// Isolate a single topic's failure so the rest of the batch still runs;
			// the operator re-runs the errored topic with `force` to repair any
			// partial target left behind.
			skipped.push({ topic, reason: 'error', error: String(err?.message ?? err) });
		}
	}

	return { migrated, skipped };
}
