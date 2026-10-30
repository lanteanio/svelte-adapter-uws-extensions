/**
 * Postgres-backed replay buffer for svelte-adapter-uws.
 *
 * Same API as the core createReplay plugin, but stores messages in a
 * Postgres table for durable history that survives restarts.
 *
 * Table schema (migrated automatically if autoMigrate is true):
 *   svti_replay (
 *     svti_replay_id BIGSERIAL PRIMARY KEY,
 *     topic          TEXT        NOT NULL,
 *     seq            BIGINT      NOT NULL,
 *     event          TEXT        NOT NULL,
 *     data           JSONB,
 *     created_at     TIMESTAMPTZ DEFAULT now()
 *   )
 *   + index on (topic, seq)
 *
 *   svti_replay_seq (
 *     topic        TEXT        PRIMARY KEY,
 *     seq          BIGINT      NOT NULL DEFAULT 0,
 *     epoch        BIGINT      NOT NULL DEFAULT nextval('svti_replay_seq_epoch_seq'),
 *     last_used_at TIMESTAMPTZ NOT NULL DEFAULT now()
 *   )
 *
 * Sequences are generated atomically via the _seq table (an UPDATE of the
 * topic row, or an INSERT ... ON CONFLICT DO UPDATE for a new topic), so they
 * are safe across multiple server instances without races. The same row
 * carries a per-topic epoch: the generation of the current seq space, taken
 * from one sequence whenever the seq space restarts (a new or recreated topic,
 * clearTopic), so a reconnecting client whose topic reset is cold-rehydrated
 * instead of being served the restarted numbering as if it continued the old
 * one. Because no epoch is ever handed out twice, a topic row may be deleted:
 * `maxTopics` evicts the least recently published topics, and with a `ttl` a
 * topic idle for longer than it loses its row along with its expired events.
 *
 * @module svelte-adapter-uws-extensions/postgres/replay
 */

import { assertSafeTableName } from '../shared/pg-migrate.js';
import { createStoreSchema } from './migrations.js';
import { MAX_REPLAY_EPOCH_CACHE_TOPICS, MAX_STORE_PAYLOAD_BYTES } from '../shared/caps.js';
import { createLruMap } from '../shared/lru-map.js';
import { withBreaker, pgFailure, guardBreaker } from './_breaker.js';
import { scheduleCleanup } from './_cleanup.js';
import { withTransaction } from '../shared/pg-tx.js';
import { ReplayStorageError, ReplaySerializationError, createResumeHook } from '../shared/replay-helpers.js';
import { checkReplayAccess, RESUME_PREAUTHORIZED } from '../shared/replay-gate.js';
import { tenantTopicSql } from '../shared/tenant-topic.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure, firstReportInProcess } from '../shared/diagnostic.js';
import { UNSEQUENCED_PUBLISH } from '../shared/sequence-authority.js';
import { replayHasHole } from '../shared/replay-envelope.js';
import { defineOptions, option } from '../shared/options.js';
export { ReplayStorageError, ReplaySerializationError };

/**
 * Encode caller-supplied replay data once, before entering a storage breaker.
 * JSON.stringify can succeed by returning undefined for top-level functions,
 * symbols, or a toJSON that returns undefined; that is still not a JSON
 * representation and must use the same caller-input error contract as a
 * thrown serialization failure.
 */
function encodeReplayPayload(data, op, maxDataBytes) {
	let payload;
	try {
		payload = JSON.stringify(data ?? null);
	} catch (err) {
		throw new ReplaySerializationError(op, err);
	}
	if (typeof payload !== 'string') {
		throw new ReplaySerializationError(
			op,
			new TypeError(`postgres replay: data has no JSON representation (${typeof data})`)
		);
	}
	if (Buffer.byteLength(payload) > maxDataBytes) {
		throw new ReplaySerializationError(
			op,
			new Error(`postgres replay: data exceeds maxDataBytes (${maxDataBytes} bytes)`)
		);
	}
	return payload;
}

/**
 * Maximum topic names placed in one Postgres `ANY($1)` epoch lookup.
 * MAX_RESUME_TOPICS bounds a whole frame; this smaller transport chunk keeps
 * one accepted frame from becoming a single oversized query payload.
 */
const EPOCH_QUERY_CHUNK_TOPICS = 1_000;

/** Topics one creating publish evicts at most; cleanup drains any rest. */
const CREATE_EVICTION_TOPICS = 4;

/** Topics one cleanup statement removes. */
const TOPIC_RETENTION_BATCH = 8;

/**
 * @typedef {Object} PgReplayOptions
 * @property {string} [table='svti_replay'] - Table name
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds (0 = no expiry). Rows older than this are cleaned up periodically.
 * @property {boolean} [autoMigrate=true] - Apply the versioned schema migrations automatically through a session-capable connection; false only validates the schema read-only on ready().
 * @property {number} [cleanupInterval=60000] - Maximum jitter delay between bounded maintenance passes (ms). 0 disables.
 * @property {number} [maxDataBytes=262144] - Positive-integer byte cap on one JSON-encoded payload, including each publishBatch entry.
 * @property {number} [maxTopics=100] - Max topics holding replay state in this table, across every instance sharing it. Creating a topic past the cap evicts the least recently published topic's events and sequence row.
 */

/**
 * @typedef {Object} PgReplayBuffer
 * @property {(platform: import('svelte-adapter-uws').Platform, topic: string, event: string, data?: unknown) => Promise<boolean>} publish
 * @property {(topic: string) => Promise<number>} seq
 * @property {(topic: string, lastSeenSeq: number) => Promise<{truncated: boolean, missingFrom: number | null}>} gap
 * @property {(topic: string, since: number) => Promise<Array<{seq: number, topic: string, event: string, data: unknown}>>} since
 * @property {(ws: any, topic: string, sinceSeq: number, platform: import('svelte-adapter-uws').Platform) => Promise<number | undefined>} replay
 * @property {() => Promise<void>} clear
 * @property {(topic: string) => Promise<void>} clearTopic
 * @property {(topic: string) => Promise<number>} currentEpoch - Stored seq-space generation (baseline 0); bumped on every reset
 * @property {(topic: string) => number} cachedEpoch - Synchronous best-effort read of the generation from the in-process cache
 * @property {() => void} destroy - Stop cleanup timer
 */

const validateOptions = defineOptions({
	factory: 'createReplay',
	label: 'postgres replay',
	types: 'PgReplayOptions in postgres/replay.d.ts'
}, {
	table: option.known,
	size: option.known,
	ttl: option.known,
	autoMigrate: option.boolean,
	cleanupInterval: option.known,
	localFanoutOnStorageFailure: option.known,
	maxDataBytes: option.known,
	forgetUserId: option.known,
	breaker: option.collaborator,
	metrics: option.collaborator
});

/**
 * Create a Postgres-backed replay buffer.
 *
 * @param {import('./index.js').PgClient} client
 * @param {PgReplayOptions} [options]
 * @returns {PgReplayBuffer}
 */
export function createReplay(client, options = {}) {
	options = validateOptions(options);
	if (options.size !== undefined) {
		if (typeof options.size !== 'number' || options.size < 1 || !Number.isInteger(options.size)) {
			throw new Error(`postgres replay: size must be a positive integer, got ${options.size}`);
		}
	}
	if (options.ttl !== undefined) {
		if (typeof options.ttl !== 'number' || options.ttl < 0 || !Number.isInteger(options.ttl)) {
			throw new Error(`postgres replay: ttl must be a non-negative integer, got ${options.ttl}`);
		}
	}
	if (options.localFanoutOnStorageFailure !== undefined &&
		typeof options.localFanoutOnStorageFailure !== 'boolean') {
		throw new Error(`postgres replay: localFanoutOnStorageFailure must be a boolean, got ${options.localFanoutOnStorageFailure}`);
	}
	const localFanoutOnStorageFailure = options.localFanoutOnStorageFailure === true;
	// Number.isInteger, not `typeof === 'number'`: NaN is a number and
	// `NaN < 1` is false, so NaN would pass this guard and then make every
	// `bytes > maxDataBytes` comparison false, silently disabling the very
	// cap it configures.
	if (options.maxDataBytes !== undefined && (!Number.isInteger(options.maxDataBytes) || options.maxDataBytes < 1)) {
		throw new Error('postgres replay: maxDataBytes must be a positive integer (bytes)');
	}
	const maxDataBytes = options.maxDataBytes ?? MAX_STORE_PAYLOAD_BYTES;
	if (options.maxTopics !== undefined && (typeof options.maxTopics !== 'number' || !Number.isInteger(options.maxTopics) || options.maxTopics < 1)) {
		throw new Error(`postgres replay: maxTopics must be a positive integer, got ${options.maxTopics}`);
	}
	const maxTopics = options.maxTopics || 100;
	if (options.forgetUserId !== undefined && typeof options.forgetUserId !== 'function') {
		throw new Error('postgres replay: forgetUserId must be a function ({ topic, event, data }) => userId');
	}
	// Right-to-erasure: a buffered event payload is app-defined, so the store
	// cannot tell whose event it is. When set, this extracts the authoring userId
	// at publish time into a user_id column so `live.forget` can drop the
	// forgotten user's buffered events (else a resume/gap-fill replays them).
	// Unset => buffered events are not user-purgeable.
	const forgetUserId = options.forgetUserId;

	const table = options.table || 'svti_replay';
	const seqTable = table + '_seq';
	const epochSequence = seqTable + '_epoch_seq';
	const pkCol = table + '_id';
	const maxSize = options.size || 1000;
	const ttl = options.ttl || 0;
	if (ttl === 0 && !forgetUserId && firstReportInProcess(EXTENSIONS_ERROR_IDS.POSTGRES_REPLAY_RETENTION_UNBOUNDED)) {
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.POSTGRES_REPLAY_RETENTION_UNBOUNDED);
	}
	const autoMigrate = options.autoMigrate !== false;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 60000;

	assertSafeTableName(table, 'postgres replay');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic || (() => 'all');
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);
	const mResumeTopicOverflows = m?.counter(
		'replay_resume_topic_overflows_total',
		'Resume frames whose topic set exceeded MAX_RESUME_TOPICS'
	);
	const mStorageFallbacks = localFanoutOnStorageFailure
		? m?.counter('replay_storage_fallbacks_total', 'Publishes that fell back to local fanout when storage failed', ['topic'])
		: null;

	const schema = createStoreSchema(client, 'replay', table, autoMigrate);
	const ensureTable = schema.ensure;

	let cleanupTopic = '';
	async function trimExpired(budget) {
		if (ttl <= 0) return;
		while (budget.available) {
			const limit = budget.limit;
			const count = await budget.remove(`WITH expired AS (
				SELECT ${pkCol} FROM ${table}
				WHERE created_at < now() - interval '1 second' * $1
				ORDER BY created_at LIMIT $2 FOR UPDATE SKIP LOCKED
			) DELETE FROM ${table} r USING expired WHERE r.${pkCol} = expired.${pkCol}`, [ttl, limit]);
			if (count < limit) break;
		}
	}
	async function trimSizes(budget) {
		if (!budget.available) return;
		// The sequence table has one indexed row per topic. Walk at most 32
		// topics per pass, retaining the cursor instead of DISTINCT-scanning data.
		let result = await budget.query(`SELECT topic FROM ${seqTable}
			WHERE topic > $1 ORDER BY topic LIMIT 32`, [cleanupTopic]);
		if (result.rows.length === 0 && cleanupTopic && budget.available) {
			cleanupTopic = '';
			result = await budget.query(`SELECT topic FROM ${seqTable} WHERE topic > $1 ORDER BY topic LIMIT 32`, [cleanupTopic]);
		}
		for (const { topic } of result.rows) {
			if (!budget.available) break;
			const cutoff = await budget.query(`SELECT seq FROM ${table}
				WHERE topic = $1 ORDER BY seq DESC OFFSET $2 LIMIT 1`, [topic, maxSize]);
			if (!cutoff.rows.length) { cleanupTopic = topic; continue; }
			let complete = false;
			while (budget.available) {
				const limit = budget.limit;
				const count = await budget.remove(`WITH expired AS (
					SELECT ${pkCol} FROM ${table} WHERE topic = $1 AND seq <= $2
					ORDER BY seq LIMIT $3 FOR UPDATE SKIP LOCKED
				) DELETE FROM ${table} r USING expired WHERE r.${pkCol} = expired.${pkCol}`, [topic, cutoff.rows[0].seq, limit]);
				if (count < limit) { complete = true; break; }
			}
			cleanupTopic = topic;
			// Revisit a busy topic after the cursor wraps, allowing later topics
			// to progress even while its backlog keeps growing.
			if (!complete) break;
		}
	}
	// Topic rows past maxTopics, least recently published first. A topic and
	// its events go in one statement; rows a publish holds locked are skipped,
	// so a topic being written is never the one removed. The sequence row is
	// deleted with the events: its epoch is never handed out again, so the
	// topic's next publish starts a seq space no client can mistake for this one.
	const evictSql = `WITH victims AS (
			SELECT topic FROM ${seqTable}
			 ORDER BY last_used_at DESC, topic DESC
			OFFSET $1 LIMIT $2
			   FOR UPDATE SKIP LOCKED
		), purged AS (
			DELETE FROM ${table} r USING victims v WHERE r.topic = v.topic
		)
		DELETE FROM ${seqTable} s USING victims v WHERE s.topic = v.topic
		RETURNING s.topic`;
	// With a ttl, a topic not published for longer than it has no events left
	// to resume from, so its row goes too.
	const idleSql = `WITH idle AS (
			SELECT topic FROM ${seqTable}
			 WHERE last_used_at < now() - interval '1 second' * $1
			 ORDER BY last_used_at LIMIT $2
			   FOR UPDATE SKIP LOCKED
		), purged AS (
			DELETE FROM ${table} r USING idle i WHERE r.topic = i.topic
		)
		DELETE FROM ${seqTable} s USING idle i WHERE s.topic = i.topic`;
	async function trimTopics(budget) {
		while (ttl > 0 && budget.available) {
			if (await budget.remove(idleSql, [ttl, TOPIC_RETENTION_BATCH]) < TOPIC_RETENTION_BATCH) break;
		}
		while (budget.available) {
			if (await budget.remove(evictSql, [maxTopics, TOPIC_RETENTION_BATCH]) < TOPIC_RETENTION_BATCH) break;
		}
	}
	const policies = [trimExpired, trimSizes, trimTopics];
	let turn = 0;
	const cleanup = scheduleCleanup({
		client, table, kind: 'replay', interval: cleanupInterval, metrics: m,
		ready: ensureTable, healthy: () => !b || b.isHealthy,
		success: () => b?.success(), failure: (error) => pgFailure(b, error),
		async work(budget) {
			// Each pass starts with a different policy, so no backlog of one
			// kind can starve the others.
			const first = turn;
			turn = (turn + 1) % policies.length;
			for (let i = 0; i < policies.length; i++) await policies[(first + i) % policies.length](budget);
		}
	});

	// Last epoch this process observed for a topic, so a synchronous caller (the
	// subscribe-ack carrier, which cannot await Postgres) can read a recent
	// value. Read-through populated by currentEpoch and refreshed by the publish
	// / clearTopic bumps; a topic not yet seen reads as the baseline 0.
	/** @type {Map<string, number>} */
	const epochCache = createLruMap(MAX_REPLAY_EPOCH_CACHE_TOPICS);
	function epochCacheSet(topic, epoch) {
		epochCache.set(topic, epoch);
	}


	// Read the stored epoch for a topic. A topic whose seq space has never reset
	// has no row yet (or epoch defaulted 0): that is the baseline and reads as 0.
	// Read-through populates the cache. An old client presenting no epoch SKIPS
	// this compare entirely (handled in resumeHook) - a literal compare against 0
	// would be wrong because the first publish bumps a fresh topic 0 -> 1.
	async function currentEpoch(topic) {
		const res = await withBreaker(b, async () => {
			await ensureTable();
			return client.query({
				name: 'replay_epoch_' + table,
				text: `SELECT COALESCE(epoch, 0)::bigint AS epoch
				         FROM ${seqTable}
				        WHERE topic = $1`,
				values: [topic]
			});
		});
		const epoch = res.rows.length > 0 ? parseInt(res.rows[0].epoch, 10) : 0;
		epochCacheSet(topic, epoch);
		return epoch;
	}

	// Batched epoch read for the resume hook: one query for every topic the
	// client presented an epoch for, chunked so one accepted resume frame does
	// not become one oversized ANY($1) payload. A topic with no row reads as the
	// baseline 0, matching currentEpoch.
	async function currentEpochs(topics) {
		const out = new Map();
		for (let offset = 0; offset < topics.length; offset += EPOCH_QUERY_CHUNK_TOPICS) {
			const chunk = topics.slice(offset, offset + EPOCH_QUERY_CHUNK_TOPICS);
			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'replay_epochs_' + table,
					text: `SELECT topic, COALESCE(epoch, 0)::bigint AS epoch
					         FROM ${seqTable}
					        WHERE topic = ANY($1)`,
					values: [chunk]
				});
			});
			for (const row of res.rows) {
				const epoch = parseInt(row.epoch, 10);
				epochCacheSet(row.topic, epoch);
				out.set(row.topic, epoch);
			}
			for (const topic of chunk) {
				if (!out.has(topic)) {
					epochCacheSet(topic, 0);
					out.set(topic, 0);
				}
			}
		}
		return out;
	}

	// Latch so the storage-fallback degradation warns ONCE per tracker, not per
	// event (the per-publish volume under a sustained outage is the
	// replay_storage_fallbacks_total metric). The one warn carries the first
	// degraded publish's requestId as a correlation anchor; the raw topic is
	// omitted (it can embed user ids - the metric carries the sanitized label).
	let warnedStorageFallback = false;

	function warnStorageFallbackOnce(platform, err) {
		if (warnedStorageFallback) return;
		warnedStorageFallback = true;
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.POSTGRES_REPLAY_STORAGE_FALLBACK, {
			error: err,
			attributes: platform?.requestId ? { requestId: platform.requestId } : null
		});
	}

	// Trim by sequence number: seqs are contiguous per topic (1, 2, 3, ...) so
	// the cutoff is trivially computable. Trim failure must not block the live
	// publish path -- the periodic cleanup will catch any excess rows later.
	// Shared by publish and publishBatch so both reuse one prepared statement
	// (same name requires byte-identical text).
	async function trimTopicTo(topic, cutoff) {
		try {
			await client.query({
				name: 'replay_trim_' + table,
				text: `DELETE FROM ${table}
				 WHERE topic = $1
				   AND seq <= $2`,
				values: [topic, cutoff]
			});
		} catch {
			// Non-fatal: the next successful publish re-trims with a
			// higher cutoff. With cleanupInterval: 0, persistent trim
			// failures cause unbounded growth.
		}
	}

	// Enforce maxTopics after a publish created a topic row. Like the trim, a
	// failure must not fail the publish that already committed: the cleanup
	// pass and the next creation evict again.
	async function enforceMaxTopics() {
		try {
			const res = await client.query({
				name: 'replay_evict_' + table,
				text: evictSql,
				values: [maxTopics, CREATE_EVICTION_TOPICS]
			});
			for (const row of res.rows) epochCache.delete(row.topic);
		} catch {
			// Non-fatal; see above.
		}
	}

	const tracker = {
		ready: schema.ready,
		status: schema.status,
		async publish(platform, topic, event, data) {
			// Serialize and size-check before the storage try-block. Caller-input
			// failures must not enter the breaker or trigger local fan-out.
			const payload = encodeReplayPayload(data, 'publish', maxDataBytes);
			let userId = null;
			if (forgetUserId) {
				try { const u = forgetUserId({ topic, event, data }); if (typeof u === 'string' && u.length > 0) userId = u; } catch { /* extractor best-effort */ }
			}
			let res;
			try {
				res = await withBreaker(b, async () => {
					await ensureTable();
					// An existing topic takes the UPDATE branch, which also
					// stamps its last use; only a new topic reaches the INSERT,
					// so the epoch sequence is read only when a seq space starts.
					// ON CONFLICT covers a concurrent first publish of the topic.
					return client.query({
						name: 'replay_publish_' + table,
						text: `WITH bumped AS (
							UPDATE ${seqTable} SET seq = seq + 1, last_used_at = now()
							 WHERE topic = $1
							RETURNING seq, epoch, false AS created
						), fresh AS (
							INSERT INTO ${seqTable} (topic, seq)
							SELECT $1, 1 WHERE NOT EXISTS (SELECT 1 FROM bumped)
							ON CONFLICT (topic)
							  DO UPDATE SET seq = ${seqTable}.seq + 1, last_used_at = now()
							  RETURNING seq, epoch, (xmax = 0) AS created
						), new_seq AS (
							SELECT seq, epoch, created FROM bumped
							UNION ALL
							SELECT seq, epoch, created FROM fresh
						)
						INSERT INTO ${table} (topic, seq, event, data, user_id)
						SELECT $1, new_seq.seq, $2, $3, $4
						  FROM new_seq
						RETURNING seq, (SELECT epoch FROM new_seq) AS epoch, (SELECT created FROM new_seq) AS created`,
						values: [topic, event, payload, userId]
					});
				});
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					mStorageFallbacks?.inc({ topic: mt(topic) });
					warnStorageFallbackOnce(platform, err);
					return platform.publish(topic, event, data, UNSEQUENCED_PUBLISH);
				}
				throw new ReplayStorageError('publish', err);
			}
			const seq = parseInt(res.rows[0].seq, 10);
			// Cache the epoch the CTE returned so cachedEpoch is fresh for the
			// synchronous ack carrier without a follow-up read. A new topic takes
			// the next generation; an existing one carries its epoch forward.
			if (res.rows[0].epoch != null) epochCacheSet(topic, parseInt(res.rows[0].epoch, 10));
			mPublishes?.inc({ topic: mt(topic) });
			if (res.rows[0].created === true) await enforceMaxTopics();

			if (seq > maxSize) {
				await trimTopicTo(topic, seq - maxSize);
			}

			// Thread the authoritative CTE seq onto the live frame so a resuming
			// client dedups against the same per-topic contiguous seq space the table
			// stores (across a restart, or across instances via the shared table)
			// instead of the adapter's per-worker counter. seq is a contiguous
			// positive integer from the CTE; guarded for safety. A frame without one,
			// like the degraded local-fanout fallback above, goes out with seq: false:
			// a local counter value in this topic's seq space could move a client's
			// watermark past rows the table still holds.
			return Number.isInteger(seq) && seq >= 1
				? platform.publish(topic, event, data, { seq })
				: platform.publish(topic, event, data, UNSEQUENCED_PUBLISH);
		},

		async publishBatch(platform, messages) {
			if (!Array.isArray(messages)) {
				throw new Error('postgres replay: publishBatch expects an array of { topic, event, data } messages');
			}
			if (messages.length === 0) return true;

			// Fail-closed: validate and serialize EVERY message before the
			// storage try-block. One bad payload rejects the whole batch with
			// nothing persisted - mirroring publish(), a caller-input bug must
			// not trigger the localFanoutOnStorageFailure durability fallback.
			const topics = new Array(messages.length);
			const events = new Array(messages.length);
			const payloads = new Array(messages.length);
			const userIds = new Array(messages.length);
			for (let i = 0; i < messages.length; i++) {
				const msg = messages[i];
				if (!msg || typeof msg.topic !== 'string' || msg.topic.length === 0 || typeof msg.event !== 'string') {
					throw new Error('postgres replay: publishBatch messages need a non-empty string topic and a string event');
				}
				// Same one-pass serialization and bound as publish().
				payloads[i] = encodeReplayPayload(msg.data, 'publishBatch', maxDataBytes);
				topics[i] = msg.topic;
				events[i] = msg.event;
				let userId = null;
				if (forgetUserId) {
					try { const u = forgetUserId({ topic: msg.topic, event: msg.event, data: msg.data }); if (typeof u === 'string' && u.length > 0) userId = u; } catch { /* extractor best-effort */ }
				}
				userIds[i] = userId;
			}

			let res;
			try {
				res = await withBreaker(b, async () => {
					await ensureTable();
					// One statement, so it is atomic on its own (no transaction
					// wrapper): the per-topic seq bumps and the multi-row insert
					// commit or roll back together, and concurrent instances
					// serialize on the seq-table row locks, keeping each batch's
					// per-topic seq run contiguous. UNNEST arrays keep the
					// statement text N-invariant so the prepared-statement cache
					// holds exactly one entry regardless of batch size. Per-topic
					// seq order follows caller order via WITH ORDINALITY +
					// row_number(); the seq-table branches mirror the single
					// path exactly (existing topic: UPDATE seq+=n, epoch carried,
					// last use stamped; new topic: INSERT seq=n with the next
					// generation).
					return client.query({
						name: 'replay_publishbatch_' + table,
						text: `WITH input AS (
							SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[])
							  WITH ORDINALITY AS t(topic, event, data, user_id, ord)
						), counts AS (
							SELECT topic, COUNT(*)::bigint AS n FROM input GROUP BY topic
						), updated AS (
							UPDATE ${seqTable} s SET seq = s.seq + c.n, last_used_at = now()
							  FROM counts c
							 WHERE s.topic = c.topic
							RETURNING s.topic, s.seq AS new_high, s.epoch, false AS created
						), inserted AS (
							INSERT INTO ${seqTable} (topic, seq)
							SELECT c.topic, c.n FROM counts c
							 WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.topic = c.topic)
							ON CONFLICT (topic)
							  DO UPDATE SET seq = ${seqTable}.seq + EXCLUDED.seq, last_used_at = now()
							  RETURNING topic, seq AS new_high, epoch, (xmax = 0) AS created
						), bumped AS (
							SELECT topic, new_high, epoch, created FROM updated
							UNION ALL
							SELECT topic, new_high, epoch, created FROM inserted
						), numbered AS (
							SELECT i.*, row_number() OVER (PARTITION BY i.topic ORDER BY i.ord) AS rn
							  FROM input i
						), ins AS (
							INSERT INTO ${table} (topic, seq, event, data, user_id)
							SELECT n.topic, b.new_high - c.n + n.rn, n.event, n.data::jsonb, n.user_id
							  FROM numbered n
							  JOIN bumped b ON b.topic = n.topic
							  JOIN counts c ON c.topic = n.topic
							RETURNING topic, seq
						)
						SELECT topic, new_high, epoch, created FROM bumped`,
						values: [topics, events, payloads, userIds]
					});
				});
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					for (const topic of topics) mStorageFallbacks?.inc({ topic: mt(topic) });
					warnStorageFallbackOnce(platform, err);
					// Degraded local fan-out: no authoritative seq, so seq: false
					// (as on the single path's fallback).
					if (typeof platform.publishBatched === 'function') {
						return platform.publishBatched(messages.map((msg) => ({
							topic: msg.topic, event: msg.event, data: msg.data, options: UNSEQUENCED_PUBLISH
						}))) ?? true;
					}
					let ok = true;
					for (const msg of messages) {
						if (platform.publish(msg.topic, msg.event, msg.data, UNSEQUENCED_PUBLISH) === false) ok = false;
					}
					return ok;
				}
				throw new ReplayStorageError('publishBatch', err);
			}

			// Reconstruct each message's authoritative seq from the per-topic
			// highs: this batch's run per topic is (new_high - n + 1) .. new_high
			// in caller order - the same contiguous seq space the table stores.
			const highs = new Map();
			let created = false;
			for (const row of res.rows) {
				const high = parseInt(row.new_high, 10);
				highs.set(row.topic, high);
				if (row.epoch != null) epochCacheSet(row.topic, parseInt(row.epoch, 10));
				if (row.created === true) created = true;
			}
			if (created) await enforceMaxTopics();
			const counts = new Map();
			for (const topic of topics) counts.set(topic, (counts.get(topic) || 0) + 1);
			const nextSeq = new Map();
			for (const [topic, n] of counts) {
				const high = highs.get(topic);
				nextSeq.set(topic, Number.isInteger(high) ? high - n + 1 : NaN);
			}
			const batch = new Array(messages.length);
			for (let i = 0; i < messages.length; i++) {
				const topic = topics[i];
				const seq = nextSeq.get(topic);
				nextSeq.set(topic, seq + 1);
				mPublishes?.inc({ topic: mt(topic) });
				batch[i] = { topic, event: events[i], data: messages[i].data, options: Number.isInteger(seq) && seq >= 1 ? { seq } : UNSEQUENCED_PUBLISH };
			}

			for (const [topic, high] of highs) {
				if (high > maxSize) {
					await trimTopicTo(topic, high - maxSize);
				}
			}

			// Broadcast live in one wire-batched call when the adapter supports
			// it (duck-typed - no peer floor bump); otherwise per-message.
			if (typeof platform.publishBatched === 'function') {
				return platform.publishBatched(batch) ?? true;
			}
			let ok = true;
			for (const msg of batch) {
				if (platform.publish(msg.topic, msg.event, msg.data, msg.options) === false) ok = false;
			}
			return ok;
		},

		async seq(topic) {
			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'replay_seq_' + table,
					text: `SELECT COALESCE(seq, 0)::bigint AS current_seq
					         FROM ${seqTable}
					        WHERE topic = $1`,
					values: [topic]
				});
			});
			return res.rows.length > 0 ? parseInt(res.rows[0].current_seq, 10) : 0;
		},

		async gap(topic, lastSeenSeq) {
			if (!Number.isInteger(lastSeenSeq) || lastSeenSeq < 0) {
				throw new Error(`postgres replay: lastSeenSeq must be a non-negative integer, got ${lastSeenSeq}`);
			}
			if (lastSeenSeq === 0) return { truncated: false, missingFrom: null };

			const target = lastSeenSeq + 1;
			const cancelProbe = guardBreaker(b);

			try {
				await ensureTable();
				const nextRes = await client.query({
					name: 'replay_gap_' + table,
					text: `SELECT seq FROM ${table}
					        WHERE topic = $1 AND seq >= $2
					        ORDER BY seq ASC LIMIT 1`,
					values: [topic, target]
				});
				if (nextRes.rows.length > 0) {
					const nextSeq = parseInt(nextRes.rows[0].seq, 10);
					b?.success();
					if (nextSeq > target) {
						return { truncated: true, missingFrom: target };
					}
					return { truncated: false, missingFrom: null };
				}

				const seqRes = await client.query({
					name: 'replay_seq_' + table,
					text: `SELECT COALESCE(seq, 0)::bigint AS current_seq
					         FROM ${seqTable}
					        WHERE topic = $1`,
					values: [topic]
				});
				const currentSeq = seqRes.rows.length > 0 ? parseInt(seqRes.rows[0].current_seq, 10) : 0;
				b?.success();
				// A counter of 0 means the topic has no seq space (never published,
				// cleared, idle past the ttl or evicted): a nonzero position belongs
				// to a numbering the store no longer holds.
				if (currentSeq > lastSeenSeq || currentSeq === 0) {
					return { truncated: true, missingFrom: target };
				}
				return { truncated: false, missingFrom: null };
			} catch (err) {
				pgFailure(b, err, cancelProbe);
				throw err;
			}
		},

		async since(topic, since) {
			// Reject malformed since values defensively, matching the Redis
			// backends: a negative bound in `seq > $2` returns the entire
			// buffer - a data-leak vector for buggy host code that forwards
			// client input unchecked.
			if (!Number.isInteger(since) || since < 0) return [];
			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'replay_since_' + table,
					text: `SELECT seq, topic, event, data
					   FROM ${table}
					  WHERE topic = $1
					    AND seq > $2
					  ORDER BY seq ASC`,
					values: [topic, since]
				});
			});
			return res.rows.map((row) => ({
				seq: parseInt(row.seq, 10),
				topic: row.topic,
				event: row.event,
				data: row.data
			}));
		},

		async replay(ws, topic, sinceSeq, platform, reqId, preAuthorized) {
			// The resume hook has just run this exact gate for this exact
			// topic. Re-running it would invoke the APP's subscribe hook a
			// second time per resumed topic - doubling its authorization load
			// and any side effects it has (audit rows, rate-limit tokens, a
			// presence join). Identity-compared against the private token, not
			// truthiness: this is a public method, so a plain boolean here
			// would let any caller skip the gate by passing `true`.
			if (preAuthorized !== RESUME_PREAUTHORIZED && !await checkReplayAccess(ws, topic, platform, reqId)) return;
			const replayTopic = '__replay:' + topic;
			// Same input gate as since() and the Redis backends: a malformed
			// sinceSeq would return the entire buffer via `seq > $2` with a
			// negative bound. Emit a bare `end` marker so the wire protocol
			// shape is preserved.
			if (!Number.isInteger(sinceSeq) || sinceSeq < 0) {
				platform.send(ws, replayTopic, 'end', { reqId: reqId ?? undefined });
				return;
			}
			const cancelProbe = guardBreaker(b);
			try {
				await ensureTable();
			} catch (err) {
				pgFailure(b, err, cancelProbe);
				throw err;
			}

			// The rows and the seq counter come from one statement, so they share
			// one snapshot: a publish raises the counter and inserts its row in a
			// single statement, so every seq the counter reports has its row in
			// the same read. Reading them in two round trips let a publish land
			// in between and pass for a missing entry. The counter subquery is a
			// primary-key lookup and the rows keep the (topic, seq) index scan;
			// the LEFT JOIN returns one row with null columns when nothing is
			// newer than sinceSeq, which still carries the counter.
			let fillRes;
			try {
				fillRes = await client.query({
					name: 'replay_fill_' + table,
					text: `SELECT c.current_seq, r.seq, r.topic, r.event, r.data
					   FROM (SELECT COALESCE((SELECT seq FROM ${seqTable} WHERE topic = $1), 0)::bigint AS current_seq) c
					   LEFT JOIN LATERAL (
					         SELECT seq, topic, event, data
					           FROM ${table}
					          WHERE topic = $1
					            AND seq > $2
					          ORDER BY seq ASC
					   ) r ON true`,
					values: [topic, sinceSeq]
				});
			} catch (err) {
				pgFailure(b, err, cancelProbe);
				throw err;
			}
			const fillRows = fillRes.rows;
			const currentSeq = fillRows.length > 0 ? parseInt(fillRows[0].current_seq, 10) : 0;
			const missed = [];
			let ordered = true;
			for (let i = 0; i < fillRows.length; i++) {
				const row = fillRows[i];
				if (row.seq === null || row.seq === undefined) continue;
				const seq = parseInt(row.seq, 10);
				if (missed.length > 0 && seq < missed[missed.length - 1].seq) ordered = false;
				missed.push({ seq, topic: row.topic, event: row.event, data: row.data });
			}
			// The join keeps the inner index order in practice; SQL does not
			// promise it without an outer ORDER BY, which would add a sort over
			// every payload, so restore the order here if it ever differs.
			if (!ordered) missed.sort((x, y) => x.seq - y.seq);

			// Every seq from sinceSeq + 1 through the counter must be in the read:
			// a trimmed head, a row removed in the middle (purgeUser, a manual
			// delete) or a missing tail truncates. Position 0 is owed seq 1.
			const truncated = replayHasHole(missed, sinceSeq, currentSeq);
			if (truncated) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
			}
			b?.success();

			for (let i = 0; i < missed.length; i++) {
				const msg = missed[i];
				platform.send(ws, replayTopic, 'msg', {
					...(reqId == null ? {} : { reqId }),
					seq: msg.seq,
					event: msg.event,
					data: msg.data
				});
			}
			if (missed.length > 0) mReplayed?.inc({ topic: mt(topic) }, missed.length);
			platform.send(ws, replayTopic, 'end', { reqId: reqId ?? undefined, ...(truncated ? { truncated: true } : {}) });
			// Covered watermark for the adapter's recovery barrier: the highest seq
			// this gap-fill actually delivered (ORDER BY seq ASC, so the last row is
			// the highest). Everything at or below it is already on the wire, so
			// the barrier dedups its held live frames exactly instead of falling
			// back to the pre-window floor.
			if (missed.length > 0) return missed[missed.length - 1].seq;
			if (sinceSeq === 0) return 0;
			// Empty gap-fill: the client's claim of being current at sinceSeq is
			// wire input, trusted only when the seq counter confirms the store
			// ever issued that seq. An offset above the counter (corrupt client
			// state, or a failover to a lagging standby regressing the counter
			// with the epoch unchanged) resolves undefined instead, so the
			// adapter falls back to its conservative pre-window floor and an
			// in-window publish is delivered at-least-once rather than silently
			// skipped. Not clamped to the counter: an in-window publish can bump
			// the counter to the held frame's own seq before this read, so a
			// clamped watermark could still swallow that frame.
			return currentSeq >= sinceSeq ? sinceSeq : undefined;
		},

		/**
		 * Right-to-erasure (`live.forget`): delete every buffered event stamped
		 * with this user's id (requires a `forgetUserId` extractor; a no-op
		 * otherwise). Leaves the seq counters untouched - a hole in the buffer is
		 * already handled by the truncation-detection path on resume.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} buffered events removed
		 */
		async purgeUser(tenantId, userId) {
			if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
			// Scoped by the topic's tenant namespace, the same rule the room-owner
			// and presence-roster legs of the SAME purge already apply. Deleting
			// on `user_id` alone honoured the tenant argument nowhere: one
			// `purgeUser('acme', u)` scoped the Redis legs to acme and erased that
			// user's buffered events in every OTHER tenant too. An untenanted
			// deployment is unaffected - no topic carries the prefix, so the null
			// scope still matches all of them.
			const scope = tenantTopicSql(tenantId, 2);
			return withBreaker(b, async () => {
				await ensureTable();
				const res = await client.query(
					`DELETE FROM ${table} WHERE user_id = $1 AND ${scope.sql}`,
					[userId, scope.value]
				);
				return res.rowCount || 0;
			});
		},

		async clear() {
			await withBreaker(b, async () => {
				await ensureTable();
				// Delete every seq-table row and every data row. A republished
				// topic takes a new generation from the epoch sequence, above
				// every epoch handed out before the clear, so a long-lived client
				// holding a pre-clear epoch cannot match it. Run both in a
				// transaction so an interruption cannot leave data rows gone while
				// the seq counters remain. A pooled-connection BEGIN/COMMIT is
				// required because the default `client.query()` may check out a
				// different connection per call.
				await withTransaction(client, async (tx) => {
					// Lock/delete every seq-counter row FIRST - the same row a
					// publish's UPDATE contends on. Deleting the data first left the
					// counter unlocked, so a publish could interleave between the two
					// deletes, bump the old counter, and persist a row whose seq the
					// post-clear numbering then re-issues (a duplicate (topic, seq)).
					// Holding the counter locks for the rest of the transaction
					// serializes any concurrent publish behind COMMIT.
					await tx.query(`DELETE FROM ${seqTable}`);
					await tx.query(`DELETE FROM ${table}`);
				});
			});
			// Drop the in-process cache so cachedEpoch does not serve a stale
			// value; the next currentEpoch read-through repopulates it.
			epochCache.clear();
		},

		async clearTopic(topic) {
			await withBreaker(b, async () => {
				await ensureTable();
				const res = await withTransaction(client, async (tx) => {
					// Lock/reset the seq-table row FIRST - the same row a publish to
					// this topic contends on. Keep the row with seq reset to 0 and a
					// new generation from the epoch sequence, so the next publish
					// issues seq = 1 under it: a client that subscribed after the
					// clear resumes by gap-fill. Holding this row lock for the rest of
					// the transaction serializes any concurrent publish behind COMMIT;
					// deleting the data first (the old order) left a window where a
					// publish bumped the old counter and persisted a row whose seq the
					// reset numbering then re-issued - a duplicate (topic, seq).
					const r = await tx.query(
						`INSERT INTO ${seqTable} (topic, seq, epoch, last_used_at)
						      VALUES ($1, 0, nextval('${epochSequence}'::regclass), now())
						 ON CONFLICT (topic)
						   DO UPDATE SET seq = 0, epoch = EXCLUDED.epoch, last_used_at = now()
						   RETURNING epoch, (xmax = 0) AS created`, [topic]);
					await tx.query(
						`DELETE FROM ${table}
						  WHERE topic = $1`, [topic]);
					return r;
				});
				if (res.rows.length > 0) epochCacheSet(topic, parseInt(res.rows[0].epoch, 10));
				if (res.rows[0]?.created === true) await enforceMaxTopics();
			});
		},

		destroy() {
			cleanup.stop();
		},

		/**
		 * Current stored generation of a topic's seq space. A topic whose seq
		 * space has never reset reads as the baseline 0. Used by the resume
		 * hook to compare against the client's presented epoch.
		 * @param {string} topic
		 * @returns {Promise<number>}
		 */
		currentEpoch(topic) {
			return currentEpoch(topic);
		},

		/**
		 * Synchronous best-effort read of a topic's epoch from the in-process
		 * cache (populated by currentEpoch / the publish and clearTopic bumps).
		 * For the subscribe-ack carrier, which cannot await Postgres; wire it to
		 * `platform.topicEpoch` so the ack carries the per-topic generation a
		 * resuming client then presents back. Returns the baseline 0 for a topic
		 * not yet observed.
		 * @param {string} topic
		 * @returns {number}
		 */
		cachedEpoch(topic) {
			return epochCache.get(topic) ?? 0;
		},

		// Returns a hook function for `hooks.ws.resume`. Shared body (epoch
		// match/rehydrate semantics + batched epoch reads + concurrent
		// per-topic gap-fills): `createResumeHook` in shared/replay-helpers.js.
		resumeHook() {
			return createResumeHook({
				currentEpochs,
				// Forwards `reqId` and `preAuthorized` too. A 4-arity forwarder
				// drops the flag, and `replay()` then re-runs the app's subscribe
				// hook for every topic the hook just authorized - once more per
				// topic, and from inside Promise.all, so with no concurrency
				// bound at all.
				replay: (ws, topic, seq, platform, reqId, preAuthorized) =>
					tracker.replay(ws, topic, seq, platform, reqId, preAuthorized),
				onTruncate: () => mResumeTopicOverflows?.inc()
			});
		}
	};
	return tracker;
}
