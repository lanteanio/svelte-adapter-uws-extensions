/**
 * Postgres-backed replay buffer for svelte-adapter-uws.
 *
 * Same API as the core createReplay plugin, but stores messages in a
 * Postgres table for durable history that survives restarts.
 *
 * Table schema (auto-created if autoMigrate is true):
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
 *     topic TEXT   PRIMARY KEY,
 *     seq   BIGINT NOT NULL DEFAULT 0,
 *     epoch BIGINT NOT NULL DEFAULT 0
 *   )
 *
 * Sequences are generated atomically via the _seq table using
 * INSERT ... ON CONFLICT DO UPDATE, so they are safe across multiple
 * server instances without races. The same row carries a per-topic epoch:
 * a durable generation that increments whenever the seq space restarts
 * (clearTopic, or a publish landing on a fresh/reaped counter), so a
 * reconnecting client whose topic reset is cold-rehydrated instead of being
 * served the restarted numbering as if it continued the old one.
 *
 * @module svelte-adapter-uws-extensions/postgres/replay
 */

import { safeCreate, assertSafeTableName } from '../shared/pg-migrate.js';
import { withBreaker } from '../shared/breaker.js';
import { setIntervalTimer, clearIntervalTimer } from '../shared/runtime.js';
import { withTransaction } from '../shared/pg-tx.js';
import { ReplayStorageError, ReplaySerializationError, createResumeHook } from '../shared/replay-helpers.js';
import { checkReplayAccess } from '../shared/replay-gate.js';
export { ReplayStorageError, ReplaySerializationError };

/**
 * @typedef {Object} PgReplayOptions
 * @property {string} [table='svti_replay'] - Table name
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds (0 = no expiry). Rows older than this are cleaned up periodically.
 * @property {boolean} [autoMigrate=true] - Auto-create table on first use
 * @property {number} [cleanupInterval=60000] - How often to run cleanup (ms). 0 to disable.
 */

/**
 * @typedef {Object} PgReplayBuffer
 * @property {(platform: import('svelte-adapter-uws').Platform, topic: string, event: string, data?: unknown) => Promise<boolean>} publish
 * @property {(topic: string) => Promise<number>} seq
 * @property {(topic: string, lastSeenSeq: number) => Promise<{truncated: boolean, missingFrom: number | null}>} gap
 * @property {(topic: string, since: number) => Promise<Array<{seq: number, topic: string, event: string, data: unknown}>>} since
 * @property {(ws: any, topic: string, sinceSeq: number, platform: import('svelte-adapter-uws').Platform) => Promise<void>} replay
 * @property {() => Promise<void>} clear
 * @property {(topic: string) => Promise<void>} clearTopic
 * @property {(topic: string) => Promise<number>} currentEpoch - Stored seq-space generation (baseline 0); bumped on every reset
 * @property {(topic: string) => number} cachedEpoch - Synchronous best-effort read of the generation from the in-process cache
 * @property {() => void} destroy - Stop cleanup timer
 */

/**
 * Create a Postgres-backed replay buffer.
 *
 * @param {import('./index.js').PgClient} client
 * @param {PgReplayOptions} [options]
 * @returns {PgReplayBuffer}
 */
export function createReplay(client, options = {}) {
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
	const pkCol = table + '_id';
	const maxSize = options.size || 1000;
	const ttl = options.ttl || 0;
	const autoMigrate = options.autoMigrate !== false;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 60000;

	assertSafeTableName(table, 'postgres replay');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);
	const mStorageFallbacks = localFanoutOnStorageFailure
		? m?.counter('replay_storage_fallbacks_total', 'Publishes that fell back to local fanout when storage failed', ['topic'])
		: null;

	let migrated = false;

	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await safeCreate(client, `
			CREATE TABLE IF NOT EXISTS ${table} (
				${pkCol} BIGSERIAL   PRIMARY KEY,
				topic    TEXT        NOT NULL,
				seq      BIGINT      NOT NULL,
				event    TEXT        NOT NULL,
				data     JSONB,
				created_at TIMESTAMPTZ DEFAULT now()
			)
		`, { table, columns: [pkCol, 'topic', 'seq', 'event', 'data', 'created_at'] });
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_topic_seq ON ${table} (topic, seq)
		`);
		// Right-to-erasure column (ALTER so existing buffers forward-migrate).
		await safeCreate(client, `ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS user_id TEXT`);
		await safeCreate(client, `CREATE INDEX IF NOT EXISTS idx_${table}_user ON ${table} (user_id)`);
		// `topic` is the natural primary key: one counter row per topic, so a
		// surrogate `_id` would add no identity. A documented divergence from the
		// `<tablename>_id` convention the event table (svti_replay) follows - the
		// same single-row-per-key shape as svti_alarms and svti_idempotency.
		await safeCreate(client, `
			CREATE TABLE IF NOT EXISTS ${seqTable} (
				topic TEXT   PRIMARY KEY,
				seq   BIGINT NOT NULL DEFAULT 0,
				epoch BIGINT NOT NULL DEFAULT 0
			)
		`, { table: seqTable, columns: ['topic', 'seq', 'epoch'] });
		// Additive migration for an already-deployed seq table created before the
		// epoch column existed. Idempotent and runs under the same migrated-once
		// guard, so a fresh table (just created above) is a no-op and an old one
		// gains the column with a safe default.
		await safeCreate(client, `
			ALTER TABLE ${seqTable}
			  ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0
		`);
		migrated = true;
	}

	// Periodic cleanup
	let cleanupTimer = null;
	let cleanupRunning = false;
	if (cleanupInterval > 0) {
		cleanupTimer = setIntervalTimer(async () => {
			if (cleanupRunning) return;
			if (b && !b.isHealthy) return;
			cleanupRunning = true;
			try {
				await ensureTable();

				// Trim by size: for each topic, keep only the newest `maxSize` rows
				await client.query(`
					DELETE FROM ${table} r
					 USING (
					   SELECT sub.topic,
					          (SELECT seq FROM ${table}
					            WHERE topic = sub.topic
					            ORDER BY seq DESC
					            OFFSET $1
					            LIMIT 1) AS cutoff_seq
					     FROM (SELECT DISTINCT topic FROM ${table}) sub
					 ) cutoffs
					 WHERE r.topic = cutoffs.topic
					   AND cutoffs.cutoff_seq IS NOT NULL
					   AND r.seq <= cutoffs.cutoff_seq
				`, [maxSize]);

				// Trim by TTL
				if (ttl > 0) {
					await client.query(
						`DELETE FROM ${table}
						  WHERE created_at < now() - interval '1 second' * $1`,
						[ttl]
					);
				}

				b?.success();
				} catch (err) {
				b?.failure(err);
			} finally {
				cleanupRunning = false;
			}
		}, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	// Last epoch this process observed for a topic, so a synchronous caller (the
	// subscribe-ack carrier, which cannot await Postgres) can read a recent
	// value. Read-through populated by currentEpoch and refreshed by the publish
	// / clearTopic bumps; a topic not yet seen reads as the baseline 0.
	/** @type {Map<string, number>} */
	const epochCache = new Map();

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
		epochCache.set(topic, epoch);
		return epoch;
	}

	// Batched epoch read for the resume hook: one query for every topic the
	// client presented an epoch for, instead of one round trip per topic. A
	// topic with no row reads as the baseline 0, matching currentEpoch.
	async function currentEpochs(topics) {
		const res = await withBreaker(b, async () => {
			await ensureTable();
			return client.query({
				name: 'replay_epochs_' + table,
				text: `SELECT topic, COALESCE(epoch, 0)::bigint AS epoch
				         FROM ${seqTable}
				        WHERE topic = ANY($1)`,
				values: [topics]
			});
		});
		const out = new Map();
		for (const row of res.rows) {
			const epoch = parseInt(row.epoch, 10);
			epochCache.set(row.topic, epoch);
			out.set(row.topic, epoch);
		}
		for (const topic of topics) {
			if (!out.has(topic)) {
				epochCache.set(topic, 0);
				out.set(topic, 0);
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
		console.warn(
			'[postgres replay] storage failed; falling back to local publish, durability degraded' +
			(platform?.requestId ? ' (requestId=' + platform.requestId + ')' : '') +
			'. Further occurrences are suppressed; see the replay_storage_fallbacks_total metric. Cause: ' +
			(err?.message ?? err)
		);
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

	const tracker = {
		async publish(platform, topic, event, data) {
			// Serialize BEFORE entering the storage try-block. A JSON.stringify
			// throw (BigInt, circular reference, etc.) is a caller-input bug,
			// not a transient storage failure, and must not trigger the
			// localFanoutOnStorageFailure fallback - that would silently
			// degrade the durability contract on payloads the user thought
			// were being persisted.
			let payload;
			try {
				payload = JSON.stringify(data ?? null);
			} catch (err) {
				throw new ReplaySerializationError('publish', err);
			}
			let userId = null;
			if (forgetUserId) {
				try { const u = forgetUserId({ topic, event, data }); if (typeof u === 'string' && u.length > 0) userId = u; } catch { /* extractor best-effort */ }
			}
			let res;
			try {
				res = await withBreaker(b, async () => {
					await ensureTable();
					return client.query({
						name: 'replay_publish_' + table,
						text: `WITH new_seq AS (
							INSERT INTO ${seqTable} (topic, seq, epoch)
							     VALUES ($1, 1, 1)
							ON CONFLICT (topic)
							  DO UPDATE SET seq = ${seqTable}.seq + 1
							  RETURNING seq, epoch
						)
						INSERT INTO ${table} (topic, seq, event, data, user_id)
						SELECT $1, new_seq.seq, $2, $3, $4
						  FROM new_seq
						RETURNING seq, (SELECT epoch FROM new_seq) AS epoch`,
						values: [topic, event, payload, userId]
					});
				});
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					mStorageFallbacks?.inc({ topic: mt(topic) });
					warnStorageFallbackOnce(platform, err);
					return platform.publish(topic, event, data);
				}
				throw new ReplayStorageError('publish', err);
			}
			const seq = parseInt(res.rows[0].seq, 10);
			// Cache the epoch the CTE returned so cachedEpoch is fresh for the
			// synchronous ack carrier without a follow-up read. The INSERT branch
			// (fresh/cleared topic) seeds epoch 1; the UPDATE branch carries the
			// climbed value forward.
			if (res.rows[0].epoch != null) epochCache.set(topic, parseInt(res.rows[0].epoch, 10));
			mPublishes?.inc({ topic: mt(topic) });

			if (seq > maxSize) {
				await trimTopicTo(topic, seq - maxSize);
			}

			// Thread the authoritative CTE seq onto the live frame so a resuming
			// client dedups against the same per-topic contiguous seq space the table
			// stores (across a restart, or across instances via the shared table)
			// instead of the adapter's per-worker counter. seq is a contiguous
			// positive integer from the CTE; guarded for safety. The degraded
			// local-fanout fallback above stays counter-stamped (no authoritative seq).
			return Number.isInteger(seq) && seq >= 1
				? platform.publish(topic, event, data, { seq })
				: platform.publish(topic, event, data);
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
				try {
					payloads[i] = JSON.stringify(msg.data ?? null);
				} catch (err) {
					throw new ReplaySerializationError('publishBatch', err);
				}
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
					// path exactly (fresh topic: INSERT seq=n, epoch=1; existing:
					// UPDATE seq+=n, epoch carried).
					return client.query({
						name: 'replay_publishbatch_' + table,
						text: `WITH input AS (
							SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[])
							  WITH ORDINALITY AS t(topic, event, data, user_id, ord)
						), counts AS (
							SELECT topic, COUNT(*)::bigint AS n FROM input GROUP BY topic
						), bumped AS (
							INSERT INTO ${seqTable} (topic, seq, epoch)
							SELECT topic, n, 1 FROM counts
							ON CONFLICT (topic)
							  DO UPDATE SET seq = ${seqTable}.seq + EXCLUDED.seq
							  RETURNING topic, seq AS new_high, epoch
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
						SELECT topic, new_high, epoch FROM bumped`,
						values: [topics, events, payloads, userIds]
					});
				});
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					for (const topic of topics) mStorageFallbacks?.inc({ topic: mt(topic) });
					warnStorageFallbackOnce(platform, err);
					// Degraded local fan-out: no authoritative seq (mirrors the
					// single path's counter-stamped fallback).
					if (typeof platform.publishBatched === 'function') {
						return platform.publishBatched(messages.map((msg) => ({
							topic: msg.topic, event: msg.event, data: msg.data
						}))) ?? true;
					}
					let ok = true;
					for (const msg of messages) {
						if (platform.publish(msg.topic, msg.event, msg.data) === false) ok = false;
					}
					return ok;
				}
				throw new ReplayStorageError('publishBatch', err);
			}

			// Reconstruct each message's authoritative seq from the per-topic
			// highs: this batch's run per topic is (new_high - n + 1) .. new_high
			// in caller order - the same contiguous seq space the table stores.
			const highs = new Map();
			for (const row of res.rows) {
				const high = parseInt(row.new_high, 10);
				highs.set(row.topic, high);
				if (row.epoch != null) epochCache.set(row.topic, parseInt(row.epoch, 10));
			}
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
				batch[i] = Number.isInteger(seq) && seq >= 1
					? { topic, event: events[i], data: messages[i].data, options: { seq } }
					: { topic, event: events[i], data: messages[i].data };
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
				const r = msg.options
					? platform.publish(msg.topic, msg.event, msg.data, msg.options)
					: platform.publish(msg.topic, msg.event, msg.data);
				if (r === false) ok = false;
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
			if (b) b.guard();

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
				if (currentSeq > lastSeenSeq) {
					return { truncated: true, missingFrom: target };
				}
				return { truncated: false, missingFrom: null };
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async since(topic, since) {
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

		async replay(ws, topic, sinceSeq, platform, reqId) {
			if (!await checkReplayAccess(ws, topic, platform, reqId)) return;
			const replayTopic = '__replay:' + topic;
			b?.guard();
			try {
				await ensureTable();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			let missedRes;
			try {
				missedRes = await client.query({
					name: 'replay_since_' + table,
					text: `SELECT seq, topic, event, data
					   FROM ${table}
					  WHERE topic = $1
					    AND seq > $2
					  ORDER BY seq ASC`,
					values: [topic, sinceSeq]
				});
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const missed = missedRes.rows.map((row) => ({
				seq: parseInt(row.seq, 10),
				topic: row.topic,
				event: row.event,
				data: row.data
			}));

			if (sinceSeq > 0 && missed.length > 0 && missed[0].seq > sinceSeq + 1) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
			} else if (sinceSeq > 0 && missed.length === 0) {
				try {
					const seqRes = await client.query({
						name: 'replay_seq_' + table,
						text: `SELECT COALESCE(seq, 0)::bigint AS current_seq
						         FROM ${seqTable}
						        WHERE topic = $1`,
						values: [topic]
					});
					const currentSeq = seqRes.rows.length > 0 ? parseInt(seqRes.rows[0].current_seq, 10) : 0;
					if (currentSeq > sinceSeq) {
						mTruncations?.inc({ topic: mt(topic) });
						platform.send(ws, replayTopic, 'truncated', null);
					}
				} catch (err) {
					b?.failure(err);
					throw err;
				}
			}
			b?.success();

			for (let i = 0; i < missed.length; i++) {
				const msg = missed[i];
				platform.send(ws, replayTopic, 'msg', {
					seq: msg.seq,
					event: msg.event,
					data: msg.data
				});
			}
			if (missed.length > 0) mReplayed?.inc({ topic: mt(topic) }, missed.length);
			platform.send(ws, replayTopic, 'end', { reqId: reqId || undefined });
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
			return withBreaker(b, async () => {
				await ensureTable();
				const res = await client.query(`DELETE FROM ${table} WHERE user_id = $1`, [userId]);
				return res.rowCount || 0;
			});
		},

		async clear() {
			await withBreaker(b, async () => {
				await ensureTable();
				// Delete every data row, but bump-and-keep each seq-table row
				// rather than wiping it: a globally-cleared topic that is then
				// republished must keep climbing its epoch (an INSERT branch would
				// snap it back to 1, and a long-lived client holding the pre-clear
				// epoch 1 could then spuriously match). Run both in a transaction
				// so an interruption cannot leave data rows gone while the seq
				// counters are stale. A pooled-connection BEGIN/COMMIT is required
				// because the default `client.query()` may check out a different
				// connection per call.
				await withTransaction(client, async (tx) => {
					// Lock/reset every seq-counter row FIRST - the same row a publish's
					// INSERT ... ON CONFLICT DO UPDATE contends on. Deleting the data
					// first left the counter unlocked, so a publish could interleave
					// between the delete and the reset, bump the old counter, and
					// persist a row whose seq the post-reset numbering then re-issues
					// (a duplicate (topic, seq)). Holding the counter lock for the rest
					// of the transaction serializes any concurrent publish behind COMMIT.
					await tx.query(`UPDATE ${seqTable} SET seq = 0, epoch = epoch + 1`);
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
					// this topic contends on via ON CONFLICT DO UPDATE. Keep the row
					// so the epoch survives the reset (the durable analogue of a no-ttl
					// epoch counter): bump epoch, reset seq to 0 so the next publish
					// issues seq = 1 on the fresh space. Holding this row lock for the
					// rest of the transaction serializes any concurrent publish behind
					// COMMIT; deleting the data first (the old order) left a window
					// where a publish bumped the old counter and persisted a row whose
					// seq the reset numbering then re-issued - a duplicate (topic, seq).
					const r = await tx.query(
						`INSERT INTO ${seqTable} (topic, seq, epoch)
						      VALUES ($1, 0, 1)
						 ON CONFLICT (topic)
						   DO UPDATE SET seq = 0, epoch = ${seqTable}.epoch + 1
						   RETURNING epoch`, [topic]);
					await tx.query(
						`DELETE FROM ${table}
						  WHERE topic = $1`, [topic]);
					return r;
				});
				if (res.rows.length > 0) epochCache.set(topic, parseInt(res.rows[0].epoch, 10));
			});
		},

		destroy() {
			if (cleanupTimer) {
				clearIntervalTimer(cleanupTimer);
				cleanupTimer = null;
			}
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
				replay: (ws, topic, seq, platform) => tracker.replay(ws, topic, seq, platform)
			});
		}
	};
	return tracker;
}
