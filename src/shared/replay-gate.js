/**
 * Wire-level subscribe gate for replay() calls. The `resume` frame
 * iterates client-supplied `lastSeenSeqs` and calls replay() per topic;
 * without this check, anyone who can guess a topic name reads its replay
 * buffer regardless of subscribe auth.
 *
 * Fails CLOSED when the platform cannot authorize: the adapter peer floor
 * has provided `checkSubscribe` for many releases, so an absent check
 * means a custom platform. Degrade availability, never authorization.
 *
 * Delivering the denial is best-effort and never fatal. The platform that
 * lacks `checkSubscribe` is by construction the non-standard one, so it is
 * also the one most likely to lack `send`; and a resume covers many topics
 * through one `Promise.all`, where a throw here would take every other
 * topic's gap-fill down with it rather than denying one.
 *
 * Returns `true` if the caller should continue (no denial). Returns
 * `false` after attempting a `denied` event on `__replay:{topic}`; the
 * caller should return without reading the buffer.
 *
 * @param {any} ws
 * @param {string} topic
 * @param {any} platform
 * @param {string | undefined} reqId
 * @returns {Promise<boolean>}
 */
/**
 * Trust token for the resume path.
 *
 * The resume hook authorizes every topic itself before gap-filling, so the
 * store must not run the app's subscribe hook a second time per topic. But
 * `replay()` is a PUBLIC method: a boolean "already authorized" argument on
 * its signature is an authorization bypass that any caller can set. This
 * symbol is not reachable through the package export map, so only the
 * in-tree resume hook can present it.
 */
export const RESUME_PREAUTHORIZED = Symbol('svti.replay.resume-preauthorized');

export async function checkReplayAccess(ws, topic, platform, reqId) {
	if (!platform || typeof platform.checkSubscribe !== 'function') {
		sendDenial(ws, topic, platform, 'FORBIDDEN', reqId);
		return false;
	}
	let denial;
	// Deliberately WITHOUT the observer lane (`{ requireGrant: true }`) that
	// `presence.sync` and `cursor.snapshot` pass, and this is the one place
	// that decision is easy to get backwards - the resume gate looks like the
	// most valuable observer lane there is, because it yields a topic's whole
	// replay buffer rather than a roster snapshot.
	//
	// It is not an observer lane. Resume-on-subscribe runs INSIDE the subscribe
	// handshake, after the adapter's own subscribe gate has already authorized
	// the topic and BEFORE `ws.subscribe` lands the grant. So at this moment the
	// socket legitimately holds no grant, and requiring one would deny every
	// resume - the same reason the flag must not go on `attach` or `join`.
	// Verified against the adapter's single-topic and batch recover paths, which
	// both gate, then resume, then subscribe, in that order.
	try {
		denial = await platform.checkSubscribe(ws, topic);
	} catch {
		// An authorizer that throws has not said yes.
		sendDenial(ws, topic, platform, 'FORBIDDEN', reqId);
		return false;
	}
	// `false` is a denial in the adapter's subscribe-hook vocabulary, and a
	// falsy check would read it as allow.
	if (denial === false) {
		sendDenial(ws, topic, platform, 'FORBIDDEN', reqId);
		return false;
	}
	if (!denial) return true;
	sendDenial(ws, topic, platform, denial, reqId);
	return false;
}

/**
 * @param {any} ws
 * @param {string} topic
 * @param {any} platform
 * @param {string} code
 * @param {string | undefined} reqId
 */
function sendDenial(ws, topic, platform, code, reqId) {
	try {
		platform?.send?.(ws, '__replay:' + topic, 'denied', { code, reqId: reqId || undefined });
	} catch {
		// Socket closed, or a platform without send: the denial itself is
		// what matters and it has already been decided.
	}
}
