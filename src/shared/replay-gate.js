/**
 * Wire-level subscribe gate for replay() calls. The `resume` frame
 * iterates client-supplied `lastSeenSeqs` and calls replay() per topic;
 * without this check, anyone who can guess a topic name reads its replay
 * buffer regardless of subscribe auth. Older adapters without
 * `checkSubscribe` degrade to the previous behavior so a peerDep
 * mismatch does not hard-fail.
 *
 * Returns `true` if the caller should continue (no denial). Returns
 * `false` after sending a `denied` event on `__replay:{topic}`; the
 * caller should return without reading the buffer.
 *
 * @param {any} ws
 * @param {string} topic
 * @param {any} platform
 * @param {string | undefined} reqId
 * @returns {Promise<boolean>}
 */
export async function checkReplayAccess(ws, topic, platform, reqId) {
	if (typeof platform.checkSubscribe !== 'function') return true;
	const denial = await platform.checkSubscribe(ws, topic);
	if (!denial) return true;
	platform.send(ws, '__replay:' + topic, 'denied', { code: denial, reqId: reqId || undefined });
	return false;
}
