/**
 * Postgres-backed replay buffer for svelte-adapter-uws.
 *
 * Same API as the core createReplay plugin, but stores messages in a
 * Postgres table for durable history that survives restarts.
 *
 * Table schema (auto-created if autoMigrate is true):
 *   ws_replay (
 *     ws_replay_id BIGSERIAL PRIMARY KEY,
 *     topic        TEXT        NOT NULL,
 *     seq          BIGINT      NOT NULL,
 *     event        TEXT        NOT NULL,
 *     data         JSONB,
 *     created_date TIMESTAMPTZ DEFAULT now()
 *   )
 *   + index on (topic, seq)
 *
 *   ws_replay_seq (
 *     topic TEXT   PRIMARY KEY,
 *     seq   BIGINT NOT NULL DEFAULT 0
 *   )
 *
 * Sequences are generated atomically via the _seq table using
 * INSERT ... ON CONFLICT DO UPDATE, so they are safe across multiple
 * server instances without races.
 *
 * @module svelte-adapter-uws-extensions/postgres/replay
 */

/**
 * @typedef {Object} PgReplayOptions
 * @property {string} [table='ws_replay'] - Table name
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds (0 = no expiry). Rows older than this are cleaned up periodically.
 * @property {boolean} [autoMigrate=true] - Auto-create table on first use
 * @property {number} [cleanupInterval=60000] - How often to run cleanup (ms). 0 to disable.
 */

/**
 * @typedef {Object} PgReplayBuffer
 * @property {(platform: import('svelte-adapter-uws').Platform, topic: string, event: string, data?: unknown) => Promise<boolean>} publish
 * @property {(topic: string) => Promise<number>} seq
 * @property {(topic: string, since: number) => Promise<Array<{seq: number, topic: string, event: string, data: unknown}>>} since
 * @property {(ws: any, topic: string, sinceSeq: number, platform: import('svelte-adapter-uws').Platform) => Promise<void>} replay
 * @property {() => Promise<void>} clear
 * @property {(topic: string) => Promise<void>} clearTopic
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

	const table = options.table || 'ws_replay';
	const seqTable = table + '_seq';
	const pkCol = table + '_id';
	const maxSize = options.size || 1000;
	const ttl = options.ttl || 0;
	const autoMigrate = options.autoMigrate !== false;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 60000;

	// Validate table name to prevent SQL injection (alphanumeric + underscore only)
	if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table)) {
		throw new Error(`postgres replay: invalid table name "${table}"`);
	}

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);

	let migrated = false;

	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await client.query(`
			CREATE TABLE IF NOT EXISTS ${table} (
				${pkCol} BIGSERIAL   PRIMARY KEY,
				topic    TEXT        NOT NULL,
				seq      BIGINT      NOT NULL,
				event    TEXT        NOT NULL,
				data     JSONB,
				created_date TIMESTAMPTZ DEFAULT now()
			)
		`);
		await client.query(`
			CREATE INDEX IF NOT EXISTS idx_${table}_topic_seq ON ${table} (topic, seq)
		`);
		await client.query(`
			CREATE TABLE IF NOT EXISTS ${seqTable} (
				topic TEXT   PRIMARY KEY,
				seq   BIGINT NOT NULL DEFAULT 0
			)
		`);
		migrated = true;
	}

	// Periodic cleanup
	let cleanupTimer = null;
	let cleanupRunning = false;
	if (cleanupInterval > 0) {
		cleanupTimer = setInterval(async () => {
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
						  WHERE created_date < now() - interval '1 second' * $1`,
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

	return {
		async publish(platform, topic, event, data) {
			b?.guard();
			let res;
			try {
				await ensureTable();
				res = await client.query({
					name: 'replay_publish_' + table,
					text: `WITH new_seq AS (
						INSERT INTO ${seqTable} (topic, seq)
						     VALUES ($1, 1)
						ON CONFLICT (topic)
						  DO UPDATE SET seq = ${seqTable}.seq + 1
						  RETURNING seq
					)
					INSERT INTO ${table} (topic, seq, event, data)
					SELECT $1, new_seq.seq, $2, $3
					  FROM new_seq
					RETURNING seq`,
					values: [topic, event, JSON.stringify(data ?? null)]
				});
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const seq = parseInt(res.rows[0].seq, 10);
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
			if (b) b.guard();
			let res;
			try {
				await ensureTable();
				res = await client.query({
					name: 'replay_seq_' + table,
					text: `SELECT COALESCE(seq, 0)::int AS current_seq
					         FROM ${seqTable}
					        WHERE topic = $1`,
					values: [topic]
				});
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			return res.rows.length > 0 ? parseInt(res.rows[0].current_seq, 10) : 0;
		},

		async since(topic, since) {
			if (b) b.guard();
			let res;
			try {
				await ensureTable();
				res = await client.query({
					name: 'replay_since_' + table,
					text: `SELECT seq, topic, event, data
					   FROM ${table}
					  WHERE topic = $1
					    AND seq > $2
					  ORDER BY seq ASC`,
					values: [topic, since]
				});
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			return res.rows.map((row) => ({
				seq: parseInt(row.seq, 10),
				topic: row.topic,
				event: row.event,
				data: row.data
			}));
		},

		async replay(ws, topic, sinceSeq, platform, reqId) {
			b?.guard();
			try {
				await ensureTable();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const replayTopic = '__replay:' + topic;

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
			if (b) b.guard();
			try {
				await ensureTable();
				await client.query(`DELETE FROM ${table}`);
				await client.query(`DELETE FROM ${seqTable}`);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async clearTopic(topic) {
			if (b) b.guard();
			try {
				await ensureTable();
				await client.query(
					`DELETE FROM ${table}
					  WHERE topic = $1`, [topic]);
				await client.query(
					`DELETE FROM ${seqTable}
					  WHERE topic = $1`, [topic]);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		destroy() {
			if (cleanupTimer) {
				clearInterval(cleanupTimer);
				cleanupTimer = null;
			}
		}
	};
}
