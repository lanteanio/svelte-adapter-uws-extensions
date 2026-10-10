/**
 * Match the adapter's latest-value batch rule before relay filtering or shard
 * splitting: the last occurrence of a key wins across the entire call, at its
 * original position. Unkeyed batches retain their original array.
 * @template {{ coalesceKey?: string }} T
 * @param {T[]} messages
 * @returns {T[]}
 */
export function collapseByCoalesceKey(messages) {
	let hasCoalesce = false;
	for (let i = 0; i < messages.length; i++) {
		// Keep malformed input on the platform's existing rejection path.
		if (!messages[i]) return messages;
		if (messages[i].coalesceKey !== undefined) { hasCoalesce = true; break; }
	}
	if (!hasCoalesce) return messages;
	const lastByKey = new Map();
	for (let i = 0; i < messages.length; i++) {
		if (!messages[i]) return messages;
		const key = messages[i].coalesceKey;
		if (key !== undefined) lastByKey.set(key, i);
	}
	const out = [];
	for (let i = 0; i < messages.length; i++) {
		const key = messages[i].coalesceKey;
		if (key === undefined || lastByKey.get(key) === i) out.push(messages[i]);
	}
	return out;
}
