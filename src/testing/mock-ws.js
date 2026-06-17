/**
 * Create a mock WebSocket that mimics the uWS/vite wrapper API.
 * Call ws.close() to simulate a dead socket whose close handler never fired.
 * After close(), subscribe/unsubscribe/getBufferedAmount throw, matching uWS.
 * @param {Record<string, any>} [userData]
 */
export function mockWs(userData = {}) {
	const topics = new Set();
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
