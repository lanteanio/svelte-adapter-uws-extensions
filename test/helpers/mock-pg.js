/**
 * In-memory mock that implements the PgClient interface.
 * Parses SQL enough to simulate the ws_replay table operations
 * and the ws_replay_seq table for atomic sequence generation.
 */
export function mockPgClient() {
	/** @type {Array<{ws_replay_id: number, topic: string, seq: number, event: string, data: any, created_date: Date}>} */
	let rows = [];
	let nextId = 1;
	let tableCreated = false;

	/** @type {Map<string, number>} topic -> seq */
	const seqCounters = new Map();

	return {
		pool: {},

		async query(text, values = []) {
			const sql = text.trim().replace(/\s+/g, ' ');

			// CREATE TABLE
			if (sql.startsWith('CREATE TABLE')) {
				tableCreated = true;
				return { rows: [], rowCount: 0 };
			}

			// CREATE INDEX
			if (sql.startsWith('CREATE INDEX')) {
				return { rows: [], rowCount: 0 };
			}

			// INSERT INTO *_seq (atomic sequence generation)
			if (sql.includes('ON CONFLICT') && sql.includes('RETURNING seq')) {
				const topic = values[0];
				const current = seqCounters.get(topic) || 0;
				const next = current + 1;
				seqCounters.set(topic, next);
				return { rows: [{ seq: String(next) }], rowCount: 1 };
			}

			// INSERT
			if (sql.startsWith('INSERT INTO')) {
				const row = {
					ws_replay_id: nextId++,
					topic: values[0],
					seq: parseInt(values[1], 10),
					event: values[2],
					data: typeof values[3] === 'string' ? JSON.parse(values[3]) : values[3],
					created_date: new Date()
				};
				rows.push(row);
				return { rows: [row], rowCount: 1 };
			}

			// SELECT COALESCE(MAX(seq)
			if (sql.includes('MAX(seq)')) {
				const topic = values[0];
				const topicRows = rows.filter((r) => r.topic === topic);
				const maxSeq = topicRows.reduce((max, r) => Math.max(max, r.seq), 0);
				return { rows: [{ max_seq: String(maxSeq) }], rowCount: 1 };
			}

			// SELECT COUNT
			if (sql.includes('COUNT(*)')) {
				const topic = values[0];
				const message_count = rows.filter((r) => r.topic === topic).length;
				return { rows: [{ message_count }], rowCount: 1 };
			}

			// SELECT seq, topic, event, data ... WHERE topic = $1 AND seq > $2
			if (sql.includes('SELECT seq, topic, event, data')) {
				const topic = values[0];
				const since = parseInt(values[1], 10);
				const result = rows
					.filter((r) => r.topic === topic && r.seq > since)
					.sort((a, b) => a.seq - b.seq)
					.map((r) => ({
						seq: String(r.seq),
						topic: r.topic,
						event: r.event,
						data: r.data
					}));
				return { rows: result, rowCount: result.length };
			}

			// DELETE FROM table WHERE topic = $1 AND ws_replay_id NOT IN (... LIMIT $2)
			if (sql.includes('DELETE FROM') && sql.includes('NOT IN') && sql.includes('LIMIT')) {
				const topic = values[0];
				const limit = parseInt(values[1], 10);
				const topicRows = rows
					.filter((r) => r.topic === topic)
					.sort((a, b) => b.seq - a.seq);
				const keepIds = new Set(topicRows.slice(0, limit).map((r) => r.ws_replay_id));
				const before = rows.length;
				rows = rows.filter((r) => r.topic !== topic || keepIds.has(r.ws_replay_id));
				return { rows: [], rowCount: before - rows.length };
			}

			// DELETE FROM *_seq WHERE topic = $1
			if (sql.includes('DELETE FROM') && sql.includes('_seq') && sql.includes('WHERE topic')) {
				const topic = values[0];
				seqCounters.delete(topic);
				return { rows: [], rowCount: 1 };
			}

			// DELETE FROM table WHERE topic = $1
			if (sql.includes('DELETE FROM') && sql.includes('WHERE topic')) {
				const topic = values[0];
				const before = rows.length;
				rows = rows.filter((r) => r.topic !== topic);
				return { rows: [], rowCount: before - rows.length };
			}

			// DELETE FROM *_seq (clear all sequences)
			if (sql.includes('DELETE FROM') && sql.includes('_seq')) {
				seqCounters.clear();
				return { rows: [], rowCount: 0 };
			}

			// DELETE FROM table (clear all)
			if (sql.startsWith('DELETE FROM')) {
				const before = rows.length;
				rows = [];
				return { rows: [], rowCount: before };
			}

			return { rows: [], rowCount: 0 };
		},

		async end() {},

		// Test helpers
		_getRows() { return rows; },
		_getSeqCounters() { return seqCounters; },
		_reset() { rows = []; nextId = 1; tableCreated = false; seqCounters.clear(); }
	};
}
