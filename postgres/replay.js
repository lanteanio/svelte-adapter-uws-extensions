/**
 * Postgres-backed replay buffer for svelte-adapter-uws.
 *
 * Same API as the core createReplay plugin, but stores messages in a
 * Postgres table for durable history that survives restarts.
 *
 * Table schema (auto-created if autoMigrate is true):
 *   ws_replay (
 *     id       BIGSERIAL PRIMARY KEY,
 *     topic    TEXT NOT NULL,
 *     seq      BIGINT NOT NULL,
 *     event    TEXT NOT NULL,
 *     data     JSONB,
 *     created_at TIMESTAMPTZ DEFAULT now()
 *   )
 *   + index on (topic, seq)
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
	const maxSize = options.size || 1000;
	const ttl = options.ttl || 0;
	const autoMigrate = options.autoMigrate !== false;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 60000;

	// Validate table name to prevent SQL injection (alphanumeric + underscore only)
	if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table)) {
		throw new Error(`postgres replay: invalid table name "${table}"`);
	}

	let migrated = false;

	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await client.query(`
			CREATE TABLE IF NOT EXISTS ${table} (
				id BIGSERIAL PRIMARY KEY,
				topic TEXT NOT NULL,
				seq BIGINT NOT NULL,
				event TEXT NOT NULL,
				data JSONB,
				created_at TIMESTAMPTZ DEFAULT now()
			)
		`);
		await client.query(`
			CREATE INDEX IF NOT EXISTS idx_${table}_topic_seq ON ${table} (topic, seq)
		`);
		migrated = true;
	}

	// Sequence counters cached in memory, seeded from DB on first access per topic.
	// This avoids a DB roundtrip on every publish while still being correct
	// across restarts (we read max(seq) on first publish to a topic).
	/** @type {Map<string, number>} */
	const seqCache = new Map();

	// Per-topic lock to prevent concurrent nextSeq() calls from racing on
	// the seed query. Without this, two concurrent publishes to the same
	// never-before-seen topic would both read MAX(seq)=0, both get seq=1.
	/** @type {Map<string, Promise<void>>} */
	const seqLocks = new Map();

	async function nextSeq(topic) {
		// Wait for any pending seed for this topic
		while (seqLocks.has(topic)) {
			await seqLocks.get(topic);
		}

		let current = seqCache.get(topic);
		if (current === undefined) {
			// Lock this topic while we seed from DB
			let unlock;
			const lock = new Promise((resolve) => { unlock = resolve; });
			seqLocks.set(topic, lock);
			try {
				const res = await client.query(
					`SELECT COALESCE(MAX(seq), 0) AS max_seq FROM ${table} WHERE topic = $1`,
					[topic]
				);
				current = parseInt(res.rows[0].max_seq, 10);
			} finally {
				seqLocks.delete(topic);
				unlock();
			}
		}
		current++;
		seqCache.set(topic, current);
		return current;
	}

	// Periodic cleanup
	let cleanupTimer = null;
	if (cleanupInterval > 0) {
		cleanupTimer = setInterval(async () => {
			try {
				await ensureTable();

				// Trim by size: for each topic, keep only the newest `maxSize` rows
				await client.query(`
					DELETE FROM ${table} WHERE id IN (
						SELECT id FROM (
							SELECT id, ROW_NUMBER() OVER (PARTITION BY topic ORDER BY seq DESC) AS rn
							FROM ${table}
						) ranked WHERE rn > $1
					)
				`, [maxSize]);

				// Trim by TTL
				if (ttl > 0) {
					await client.query(
						`DELETE FROM ${table} WHERE created_at < now() - interval '1 second' * $1`,
						[ttl]
					);
				}
			} catch {
				// Cleanup failures are non-fatal
			}
		}, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	return {
		async publish(platform, topic, event, data) {
			await ensureTable();
			const seq = await nextSeq(topic);

			await client.query(
				`INSERT INTO ${table} (topic, seq, event, data) VALUES ($1, $2, $3, $4)`,
				[topic, seq, event, JSON.stringify(data)]
			);

			// Inline trim: if we have way more than maxSize for this topic, prune now
			const countRes = await client.query(
				`SELECT COUNT(*)::int AS cnt FROM ${table} WHERE topic = $1`,
				[topic]
			);
			const cnt = countRes.rows[0].cnt;
			if (cnt > maxSize) {
				await client.query(`
					DELETE FROM ${table} WHERE topic = $1 AND id NOT IN (
						SELECT id FROM ${table} WHERE topic = $1 ORDER BY seq DESC LIMIT $2
					)
				`, [topic, maxSize]);
			}

			return platform.publish(topic, event, data);
		},

		async seq(topic) {
			await ensureTable();
			const res = await client.query(
				`SELECT COALESCE(MAX(seq), 0)::int AS max_seq FROM ${table} WHERE topic = $1`,
				[topic]
			);
			return parseInt(res.rows[0].max_seq, 10);
		},

		async since(topic, since) {
			await ensureTable();
			const res = await client.query(
				`SELECT seq, topic, event, data FROM ${table} WHERE topic = $1 AND seq > $2 ORDER BY seq ASC`,
				[topic, since]
			);
			return res.rows.map((row) => ({
				seq: parseInt(row.seq, 10),
				topic: row.topic,
				event: row.event,
				data: row.data
			}));
		},

		async replay(ws, topic, sinceSeq, platform) {
			const missed = await this.since(topic, sinceSeq);
			const replayTopic = '__replay:' + topic;

			for (let i = 0; i < missed.length; i++) {
				const msg = missed[i];
				platform.send(ws, replayTopic, 'msg', {
					seq: msg.seq,
					event: msg.event,
					data: msg.data
				});
			}
			platform.send(ws, replayTopic, 'end', null);
		},

		async clear() {
			await ensureTable();
			await client.query(`DELETE FROM ${table}`);
			seqCache.clear();
		},

		async clearTopic(topic) {
			await ensureTable();
			await client.query(`DELETE FROM ${table} WHERE topic = $1`, [topic]);
			seqCache.delete(topic);
		},

		destroy() {
			if (cleanupTimer) {
				clearInterval(cleanupTimer);
				cleanupTimer = null;
			}
		}
	};
}
