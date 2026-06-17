/**
 * Subscription-registry bookkeeping for plugins that manage server-side
 * topic membership with raw uWS socket calls.
 *
 * The adapter's `platform.publishWire` per-subscriber walk delivers by the
 * connection's subscription registry (native uWS membership is not
 * enumerable from JS), so a plugin that calls `ws.subscribe()` without
 * mirroring the topic into the registry silently excludes that socket from
 * every stateful-codec binary publish. These helpers do ONLY the registry
 * half: call them right after the native subscribe/unsubscribe attempt, and
 * keep whatever closed-socket throw/catch contract the call site already
 * has - the registry ops never throw (a dead socket's registry dies with
 * the connection record anyway).
 *
 * The slot symbol is `Symbol.for` so this module resolves the SAME registry
 * the adapter's handler stamps, regardless of bundling or module-instance
 * duplication.
 *
 * @module svelte-adapter-uws-extensions/shared/ws-subscriptions
 */

const WS_SUBSCRIPTIONS = Symbol.for('adapter-uws.ws.subscriptions');

/**
 * Mirror a native `ws.subscribe(topic)` into the connection's subscription
 * registry. Never throws.
 * @param {any} ws
 * @param {string} topic
 */
export function addWsSubscription(ws, topic) {
	try {
		const subs = ws.getUserData()[WS_SUBSCRIPTIONS];
		if (subs) subs.add(topic);
	} catch { /* socket already closed; the registry dies with the connection */ }
}

/**
 * Mirror a native `ws.unsubscribe(topic)` into the registry, so the binary
 * publish walk stops delivering the moment native membership ends. Never
 * throws.
 * @param {any} ws
 * @param {string} topic
 */
export function removeWsSubscription(ws, topic) {
	try {
		const subs = ws.getUserData()[WS_SUBSCRIPTIONS];
		if (subs) subs.delete(topic);
	} catch { /* socket already closed; nothing left to clean */ }
}
