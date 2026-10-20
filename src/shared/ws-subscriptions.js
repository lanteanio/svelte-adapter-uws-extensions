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

export const WS_SUBSCRIPTIONS = Symbol.for('adapter-uws.ws.subscriptions');

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
 * Is this connection recorded as subscribed to `topic`?
 *
 * For a `__`-prefixed internal topic this is a SERVER-GRANTED signal, not a
 * client-asserted one: the adapter's wire-level gate refuses client-initiated
 * subscribes to `__` topics, so the only way the entry exists is that an
 * authorized server path put it there. That makes it a sound membership test
 * for the frame hot path, and it is the same signal the bundled plugins gate
 * on. Never throws.
 *
 * @param {any} ws
 * @param {string} topic
 * @returns {boolean}
 */
export function hasWsSubscription(ws, topic) {
	try {
		const subs = ws.getUserData()[WS_SUBSCRIPTIONS];
		return subs ? subs.has(topic) : false;
	} catch {
		return false;
	}
}

/**
 * `platform.checkSubscribe` options for an OBSERVER lane - a call that asks
 * "may this connection SEE what it already holds?" rather than "may it be
 * granted this?". The roster/snapshot handshakes are observer lanes; attach
 * and join are grant-ESTABLISHING lanes and must not pass this, because
 * requiring the grant to exist already would deny every legitimate join.
 *
 * It matters only in a pure-grant deployment - wire-subscribe authorization
 * armed and no app `subscribe` hook - where the subscribe gate has no hook to
 * consult and therefore allows every topic name, so an ungated observer lane
 * hands any connected socket any room's roster. The peer floor implements the
 * option, so the gate is load-bearing across the supported adapter range.
 */
export const OBSERVER_LANE = Object.freeze({ requireGrant: true });

/**
 * The adapter's marker for a `subscribe` / `subscribeBatch` hook that is a
 * plugin SIDE EFFECT rather than the app's authorization decision.
 *
 * With `authorizeWireSubscribe` armed (non-strict), the adapter hands the
 * topic decision to the app whenever the handler module exports a subscribe
 * hook, and the server-grant gate steps aside. The documented wiring for
 * presence, cursor and groups re-exports their `hooks.subscribe`, so an
 * unmarked plugin hook made the adapter believe the app had taken over and
 * disarmed the gate for every topic. A marked hook still runs, and may still
 * refuse its own internal channel; it just does not count as the app's
 * gate. An app that wraps a plugin hook in its own function is not marked,
 * and the gate steps aside as documented. Registered symbol, so it is the
 * same key the adapter reads regardless of module duplication.
 */
const WS_HOOK_SIDE_EFFECT_ONLY = Symbol.for('adapter-uws.hook.side-effect-only');

/**
 * Mark each named hook on `hooks` as a side effect rather than a decision.
 * @template {Record<string, any>} T
 * @param {T} hooks
 * @param {string[]} names
 * @returns {T} the same object
 */
export function markSideEffectHooks(hooks, names) {
	for (const name of names) {
		const fn = hooks[name];
		if (typeof fn === 'function') {
			Object.defineProperty(fn, WS_HOOK_SIDE_EFFECT_ONLY, { value: true, writable: true, enumerable: true, configurable: true });
		}
	}
	return hooks;
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
