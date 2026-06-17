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
import { ReplayStorageError, ReplaySerializationError } from '../shared/replay-helpers.js';
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
						INSERT INTO ${table} (topic, seq, event, data)
						SELECT $1, new_seq.seq, $2, $3
						  FROM new_seq
						RETURNING seq, (SELECT epoch FROM new_seq) AS epoch`,
						values: [topic, event, payload]
					});
				});
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					mStorageFallbacks?.inc({ topic: mt(topic) });
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

			// Trim by sequence number: seqs are contiguous per topic
			// (1, 2, 3, ...) so the cutoff is trivially computable.
			// This avoids the previous COUNT(*) + subquery approach
			// that was O(N) on every publish.
			// Trim failure must not block the live publish path --
			// the periodic cleanup will catch any excess rows later.
			if (seq > maxSize) {
				try {
					await client.query({
						name: 'replay_trim_' + table,
						text: `DELETE FROM ${table}
						 WHERE topic = $1
						   AND seq <= $2`,
						values: [topic, seq - maxSize]
					});
				} catch {
					// Non-fatal: the next successful publish re-trims with a
					// higher cutoff. With cleanupInterval: 0, persistent trim
					// failures cause unbounded growth.
				}
			}

			return platform.publish(topic, event, data);
		},

		async seq(topic) {
			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'replay_seq_' + table,
					text: `SELECT COALESCE(seq, 0)::int AS current_seq
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
					text: `SELECT COALESCE(seq, 0)::int AS current_seq
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
						text: `SELECT COALESCE(seq, 0)::int AS current_seq
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
					await tx.query(`DELETE FROM ${table}`);
					await tx.query(`UPDATE ${seqTable} SET seq = 0, epoch = epoch + 1`);
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
					await tx.query(
						`DELETE FROM ${table}
						  WHERE topic = $1`, [topic]);
					// Keep the seq-table row so the epoch survives the reset (the
					// durable analogue of a no-ttl epoch counter). Bump epoch,
					// reset seq to 0 so the next publish's UPDATE branch issues
					// seq = 1 on the fresh space. Atomic with the data delete.
					return tx.query(
						`INSERT INTO ${seqTable} (topic, seq, epoch)
						      VALUES ($1, 0, 1)
						 ON CONFLICT (topic)
						   DO UPDATE SET seq = 0, epoch = ${seqTable}.epoch + 1
						   RETURNING epoch`, [topic]);
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

		// Returns a hook function for `hooks.ws.resume`. Loops over the
		// client's per-topic lastSeenSeqs. For each topic it compares the
		// client's presented epoch (ctx.lastSeenEpochs, threaded by the
		// adapter; absent -> the baseline 0) to the topic's stored epoch. On a
		// MATCH it gap-fills via the existing replay() pipeline, which already
		// detects + emits truncation per topic. On a MISMATCH the seq space
		// reset since the client last saw it, so it SKIPS gap-fill for that
		// topic and emits a `rehydrate` marker on the same `__replay:{topic}`
		// channel the client already handles for `truncated`/`denied`, telling
		// the client to drop its stale offset and re-read from scratch instead
		// of being served a reset seq space as if it were contiguous.
		resumeHook() {
			return async (ws, ctx) => {
				if (!ctx || !ctx.lastSeenSeqs || !ctx.platform) return;
				const presented = (ctx.lastSeenEpochs && typeof ctx.lastSeenEpochs === 'object')
					? ctx.lastSeenEpochs
					: null;
				for (const [topic, sinceSeq] of Object.entries(ctx.lastSeenSeqs)) {
					// Normalize wire-supplied sinceSeq. Reject non-integers
					// (fractional, NaN, Infinity, non-number) by falling through
					// to 0 (resume from start). Negative values also fall through;
					// tracker.replay() validates internally as defense-in-depth.
					const seq = Number.isInteger(sinceSeq) && sinceSeq >= 0 ? sinceSeq : 0;
					// Additive: a topic the client presented NO epoch for (old
					// client, or one that never received an epoch) is always a
					// match and gap-fills exactly as before - never compared
					// against the stored epoch, so the byte-identical old path is
					// preserved even though the first publish bumps a fresh
					// topic's epoch 0 -> 1. Only a presented integer epoch that
					// differs from the stored one is a reset.
					const want = presented && Number.isInteger(presented[topic]) ? presented[topic] : null;
					if (want !== null) {
						const have = await currentEpoch(topic);
						if (want !== have) {
							ctx.platform.send(ws, '__replay:' + topic, 'rehydrate', { epoch: have });
							continue;
						}
					}
					await tracker.replay(ws, topic, seq, ctx.platform);
				}
			};
		}
	};
	return tracker;
}
