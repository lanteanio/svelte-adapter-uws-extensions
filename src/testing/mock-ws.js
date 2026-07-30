import { WS_SUBSCRIPTIONS } from '../shared/ws-subscriptions.js';

/**
 * Create a mock WebSocket that mimics the uWS/vite wrapper API.
 * Call ws.close() to simulate a dead socket whose close handler never fired.
 * After close(), subscribe/unsubscribe/getBufferedAmount throw, matching uWS.
 *
 * The subscription-registry slot is stamped at construction, exactly as the
 * adapter's handler stamps it on connection-open. Without it every
 * `addWsSubscription` in the package is a silent no-op against this double,
 * so plugin code that mirrors native membership into the registry looks
 * correct under test while excluding the socket from the binary publish
 * walk in production.
 *
 * @param {Record<string, any>} [userData]
 */
export function mockWs(userData = {}) {
	const topics = new Set();
	if (!userData[WS_SUBSCRIPTIONS]) userData[WS_SUBSCRIPTIONS] = new Set();
	let closed = false;
	function assertOpen() {
		if (closed) throw new Error('WebSocket is closed');
	}
	return {
		getUserData: () => userData,
		subscribe: (topic) => { assertOpen(); topics.add(topic); return true; },
		unsubscribe: (topic) => { assertOpen(); topics.delete(topic); return true; },
		isSubscribed: (topic) => topics.has(topic),
		getBufferedAmount: () => { assertOpen(); return 0; },
		close: () => { closed = true; },
		get _closed() { return closed; },
		_topics: topics
	};
}
