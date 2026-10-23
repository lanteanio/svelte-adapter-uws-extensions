/**
 * Versioned replay-envelope decode for both Redis replay backends (the sorted-set
 * store in `redis/replay.js` and the Streams store in `redis/replay-stream.js`).
 *
 * The stored envelope now carries an explicit version discriminator `v`, so a
 * future format change is a self-describing migration instead of a silent
 * reinterpretation. The read side validates the required fields strictly and
 * DERIVES the always-known coordinates from the storage itself rather than
 * trusting a duplicated copy:
 *   - topic  <- the per-topic key (sorted-set key / stream key), never a stored field.
 *   - seq    <- the sorted-set score / the `<seq>-0` stream id (the stream backend);
 *               the sorted-set member still carries seq (it is the read authority
 *               there and equals the score by construction).
 *
 * Backward compatibility: an envelope written before versioning has no `v` and
 * reads as v1 (the rule the README already documented). Such a legacy sorted-set
 * member may also carry a redundant `topic` field - it is ignored in favor of the
 * key. An envelope whose `v` is present but not the current version, or that is
 * missing a required field or is not parseable, is treated as CORRUPTION: the
 * decoder returns `null` and the caller drops the entry (which reads as a replay
 * gap -> a full rehydrate, the safe outcome) and increments a corruption metric.
 *
 * @module svelte-adapter-uws-extensions/shared/replay-envelope
 */

/** The current on-the-wire replay envelope version. */
export const REPLAY_ENVELOPE_VERSION = 1;

/**
 * Decode + validate one sorted-set member (a JSON string). `topic` is supplied by
 * the caller (derived from the key) and always wins over any stored topic.
 *
 * @param {string} raw - the ZSET member JSON.
 * @param {string} topic - topic derived from the buffer key.
 * @returns {{ seq: number, topic: string, event: string, data: unknown } | null}
 *   the normalized entry, or `null` when the member is corrupt / an unknown version.
 */
export function decodeSortedSetMember(raw, topic) {
	let p;
	try { p = JSON.parse(raw); } catch { return null; }
	if (p === null || typeof p !== 'object' || Array.isArray(p)) return null;
	// Unknown future version -> not this reader's format; treat as a gap.
	if (p.v !== undefined && p.v !== REPLAY_ENVELOPE_VERSION) return null;
	if (!Number.isInteger(p.seq) || p.seq < 1) return null;
	if (typeof p.event !== 'string') return null;
	return { seq: p.seq, topic, event: p.event, data: p.data };
}

/**
 * Decode + validate one stream entry (a fields object from `XRANGE`). `seq` comes
 * from the `<seq>-0` stream id and `topic` from the stream key; both are supplied
 * by the caller. The entry stores only `event` and (JSON-encoded) `data`, plus a
 * `v` discriminator on new writes. A legacy entry may carry a redundant `topic`
 * field - ignored.
 *
 * @param {Record<string, string>} fields - the entry's flat fields as an object.
 * @param {number} seq - seq parsed from the stream id.
 * @param {string} topic - topic derived from the stream key.
 * @returns {{ seq: number, topic: string, event: string, data: unknown } | null}
 *   the normalized entry, or `null` when the entry is corrupt / an unknown version.
 */
export function decodeStreamFields(fields, seq, topic) {
	if (fields.v !== undefined && fields.v !== String(REPLAY_ENVELOPE_VERSION)) return null;
	if (typeof fields.event !== 'string') return null;
	if (fields.data === undefined) return null;
	let data;
	try { data = JSON.parse(fields.data); } catch { return null; }
	return { seq, topic, event: fields.event, data };
}

/**
 * Whether a replay read missed any seq it should have delivered. `entries` are
 * the decoded entries above `sinceSeq` in ascending seq order (corrupt ones
 * already dropped) and `currentSeq` is the topic's seq counter, read before
 * the entries. A client at `sinceSeq` expects `sinceSeq + 1` next; position 0
 * has seen nothing, so it expects seq 1, the same rule the in-memory replay
 * plugin applies (truncated when the oldest retained seq is above
 * `sinceSeq + 1`). Every seq through the counter was issued with its entry,
 * so a seq absent from that run was trimmed away, expired or stored corrupt:
 * a hole before the first entry, between two entries, or after the last one.
 *
 * @param {ReadonlyArray<{ seq: number }>} entries
 * @param {number} sinceSeq
 * @param {number} currentSeq
 * @returns {boolean}
 */
export function replayHasHole(entries, sinceSeq, currentSeq) {
	let expected = sinceSeq + 1;
	for (let i = 0; i < entries.length; i++) {
		const seq = entries[i].seq;
		if (seq > expected) return true;
		if (seq === expected) expected = seq + 1;
	}
	return currentSeq >= expected;
}
