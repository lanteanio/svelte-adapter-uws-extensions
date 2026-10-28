/**
 * Accounted subscription membership for plugins that manage server-side topics.
 *
 * The adapter's tracked helpers keep native membership, its publish registry,
 * per-connection cap, shared-codec cohort and subscription counter together.
 * Callers use the same closed-socket and refusal handling as other adapter
 * subscription paths.
 *
 * The slot symbol is `Symbol.for` so this module resolves the SAME registry
 * the adapter's handler stamps, regardless of bundling or module-instance
 * duplication.
 *
 * @module svelte-adapter-uws-extensions/shared/ws-subscriptions
 */

import { trackedSubscribe, trackedUnsubscribe } from 'svelte-adapter-uws/subscriptions';

export const WS_SUBSCRIPTIONS = Symbol.for('adapter-uws.ws.subscriptions');

/**
 * Subscribe natively and charge the adapter's registry, cap, codec cohort and
 * subscription counter together.
 * @param {any} ws
 * @param {string} topic
 * @returns {boolean} false when the socket is closed or the cap is reached
 */
export function addWsSubscription(ws, topic) {
	return trackedSubscribe(ws, topic);
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
 * Unsubscribe natively and release the adapter's registry, codec cohort and
 * subscription accounting together.
 * @param {any} ws
 * @param {string} topic
 * @returns {boolean} false when the socket was already closed
 */
export function removeWsSubscription(ws, topic) {
	return trackedUnsubscribe(ws, topic);
}

/**
 * The adapter's registry of topic prefixes a plugin decides for itself.
 *
 * With `authorizeWireSubscribe` armed, the server-grant gate refuses a topic
 * the server did not pre-authorize before any hook runs. A group is joined by
 * the client subscribing to `__group:<name>`, and only the group's own
 * subscribe hook authorizes that, so a marked groups hook would leave every
 * group unjoinable under an armed gate. A registered prefix makes the gate
 * defer to the hook for that namespace only; the adapter still refuses the
 * subscribe when it lands unless the hook established tracked membership,
 * and its system-topic guard admits the namespace without the broad
 * `allowSystemTopicSubscribe` opt-out. Same globalThis slot and
 * `Symbol.for` key the adapter reads, so the registration reaches it
 * regardless of which module instance loaded first.
 */
const PLUGIN_OWNED_PREFIXES = Symbol.for('adapter-uws.plugin-owned-topic-prefixes');

/**
 * Declare that topics under `prefix` are decided by this package's own
 * subscribe hook. Idempotent. The prefix must be `__<name>:`, the only shape
 * the adapter accepts.
 * @param {string} prefix
 */
export function registerPluginOwnedPrefix(prefix) {
	if (typeof prefix !== 'string' || prefix.length > 64 || !/^__[A-Za-z][A-Za-z0-9_-]*:$/.test(prefix)) {
		throw new Error('registerPluginOwnedPrefix: "' + String(prefix) + '" must match __<name>: with name [A-Za-z][A-Za-z0-9_-]*');
	}
	let set = /** @type {any} */ (globalThis)[PLUGIN_OWNED_PREFIXES];
	if (!(set instanceof Set)) {
		set = new Set();
		Object.defineProperty(globalThis, PLUGIN_OWNED_PREFIXES, { value: set, writable: true, enumerable: true, configurable: true });
	}
	set.add(prefix);
}
