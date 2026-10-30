// @ts-check
/**
 * Health events and refetch hints for buses with a breaker.
 *
 * Every bus with a breaker that publishes on one platform and topic shares one
 * announcer, kept in a `Symbol.for` slot on the platform so bundler-duplicated
 * copies of this module find the same one. A platform a bus wrapped forwards
 * the slot of the platform it wraps, so a bus activated with another bus's
 * wrapped platform joins the same announcer. The announcer listens to every
 * breaker its buses were given and publishes for all of them together:
 * `degraded` once when the first leaves healthy, `recovered` once when the last
 * is healthy again. The degradation policy of the most recently attached bus
 * that has one shapes both, so a policy edited before a development reload
 * applies to the bus the reload created.
 *
 * A breaker leaves `probing` only on an outcome, so a bus that carries no
 * traffic would hold its breaker, and with it every other bus's `recovered`,
 * open for good. While one of its buses' breakers is probing, the announcer
 * sends that bus's own connection a PING through the breaker, so the breaker
 * recovers without application traffic. A PING that fails opens the breaker
 * again, and the breaker's reset timeout spaces the next one; a probe slot
 * another caller holds is asked again after one second, doubling up to 30 s.
 * A bus whose connection ended for good is detached.
 *
 * A bus delivers locally while Redis is unreachable and queues nothing for
 * later, so nothing lost is replayed. Instead the clients are asked to reload:
 * a `recovered` event carries `recovery: { refetch: true }`.
 *
 * Inbound loss (a subscriber missed messages, or a peer reported that its
 * relays were dropped) is announced once every breaker is healthy and no
 * subscriber is restoring. A refetch event waits HINT_HOLD_MS, and each
 * further report that arrives meanwhile extends the wait by as much again, so
 * the reports of several peers after an outage they all saw become one event.
 * The wait never exceeds HINT_MAX_HOLD_MS, and two refetch events are at least
 * HINT_MAX_HOLD_MS apart, so an instance that keeps dropping relays makes its
 * peers' clients reload at most once per HINT_MAX_HOLD_MS. A breaker
 * `recovered` event that asks for a refetch waits HINT_HOLD_MS for the same
 * reason, but not for the spacing: clients learn of a recovery promptly.
 *
 * Outbound loss (relays a bus dropped) is reported to the peers by that bus
 * with one `recovered` envelope, after its breaker recovers or after its next
 * relay that succeeds while its breaker is healthy. The envelope holds the
 * request and a timestamp, nothing about the outage.
 *
 * @module svelte-adapter-uws-extensions/shared/delivery-hints
 */

import { now, monotonicNow, setTimer, clearTimer } from './runtime.js';
import { guardBreaker } from './breaker-admission.js';

// Frames about this worker's own breakers go to this worker's subscribers
// only, and they make no monotonic promise.
export const SYSTEM_FRAME = Object.freeze({ relay: false, seq: false });

// How long a refetch event waits for further reports to merge into it.
export const HINT_HOLD_MS = 1000;
// The longest a refetch event waits, and the least time between two: bounds
// the reload load an instance under sustained loss puts on its peers' clients
// to one reload per client per 10 s, while a client never stays stale more
// than 10 s after a loss was reported.
export const HINT_MAX_HOLD_MS = 10000;

// A probe slot held by another caller is asked again after these.
const PROBE_RETRY_MIN_MS = 1000;
const PROBE_RETRY_MAX_MS = 30000;

const REFETCH = Object.freeze({ refetch: true });
const NO_TRANSITION = Object.freeze({ from: 'healthy', to: 'healthy' });

/**
 * The platform slot holding its announcers. A bus's wrapped platform
 * forwards it to the platform it wraps.
 */
export const HEALTH_ANNOUNCERS = Symbol.for('svelte-adapter-uws-extensions.health-announcers.v1');
// Platforms that cannot carry the slot keep their announcers here.
const FALLBACK = Symbol.for('svelte-adapter-uws-extensions.health-announcers-fallback.v1');

/**
 * Whether a relayed envelope is a peer's refetch request. An application's
 * own `recovered` without it passes through as any other envelope.
 *
 * @param {any} parsed
 */
export function isRefetchRequest(parsed) {
	return parsed.event === 'recovered' && parsed.data?.recovery?.refetch === true;
}

/**
 * The announcers of `platform`, created on first use. A wrapped platform's
 * slot getter answers with those of the platform it wraps.
 *
 * @param {any} platform
 * @returns {Map<string, any>}
 */
export function announcersOf(platform) {
	let map = platform[HEALTH_ANNOUNCERS];
	if (map instanceof Map) return map;
	map = new Map();
	try {
		Object.defineProperty(platform, HEALTH_ANNOUNCERS, { value: map, configurable: true });
		return map;
	} catch {
		/** @type {WeakMap<object, Map<string, any>>} */
		let fallback = /** @type {any} */ (globalThis)[FALLBACK];
		if (!(fallback instanceof WeakMap)) {
			fallback = new WeakMap();
			Object.defineProperty(globalThis, FALLBACK, { value: fallback, configurable: true });
		}
		let found = fallback.get(platform);
		if (!found) fallback.set(platform, found = new Map());
		return found;
	}
}

/**
 * @typedef {{
 *   breaker: { state?: string, subscribe(listener: (from: string, to: string) => void): () => void },
 *   policy?: { onDegraded(transition: { from: string, to: string }): any, onRecovered(transition: { from: string, to: string }): any },
 *   relay: (data: object) => void,
 *   ping?: () => unknown,
 *   gone?: () => boolean,
 *   counters?: { degraded?: { inc(): void }, recovered?: { inc(): void } }
 * }} HealthSource
 */

/**
 * Attach a bus to the announcer of `platform` and `topic`, creating it on
 * first use. `relay` sends the bus's peer request; `ping` checks the bus's
 * own connection; `gone` says whether that connection ended for good;
 * `counters` count each event the announcer publishes, a counter shared by
 * several buses once.
 *
 * @param {any} platform
 * @param {string} topic
 * @param {HealthSource} source
 */
export function attachHealth(platform, topic, source) {
	const map = announcersOf(platform);
	let announcer = map.get(topic);
	if (!announcer) {
		announcer = createAnnouncer(platform, topic, () => { if (map.get(topic) === announcer) map.delete(topic); });
		map.set(topic, announcer);
	}
	return announcer.attach(source);
}

/**
 * @param {any} platform
 * @param {string} topic
 * @param {() => void} forget
 */
function createAnnouncer(platform, topic, forget) {
	/** @type {Set<any>} */
	const handles = new Set();
	/** @type {Map<any, { unsubscribe: () => void, count: number, probe: any, retryMs: number }>} */
	const breakers = new Map();
	// Clients were told `degraded` and not yet `recovered`.
	let degraded = false;
	// Messages from peers may have been missed since the last refetch event.
	let inboundLost = false;
	/** @type {Set<any>} the handles whose subscriber is restoring */
	const restoring = new Set();
	/** @type {{ at: number, out: any } | null} a breaker `recovered` event, held */
	let held = null;
	/** @type {any} */
	let timer = null;
	let dueAt = 0;
	let windowStart = 0;
	let lastRefetchAt = -Infinity;

	const healthy = (b) => typeof b.state !== 'string' || b.state === 'healthy';
	function allHealthy() {
		for (const b of breakers.keys()) if (!healthy(b)) return false;
		return true;
	}
	function policy() {
		let latest;
		for (const h of handles) if (h.source.policy) latest = h.source.policy;
		return latest;
	}
	function count(event) {
		const seen = new Set();
		for (const h of handles) {
			const counter = h.source.counters?.[event];
			if (counter && !seen.has(counter)) {
				seen.add(counter);
				counter.inc();
			}
		}
	}
	function policyOut(kind, transition) {
		const p = policy();
		if (!p) return null;
		try { return kind === 'degraded' ? p.onDegraded(transition) : p.onRecovered(transition); } catch { return null; /* a policy must never break the emit */ }
	}
	const frameOptions = (out) => (out && out.jitterMs ? { ...SYSTEM_FRAME, jitterMs: out.jitterMs } : SYSTEM_FRAME);

	function publishRecovered(at, out, refetch) {
		let recovery = out && out.recovery ? out.recovery : null;
		// A policy that sets refetch itself has decided.
		if (refetch) {
			if (recovery === null) recovery = REFETCH;
			else if (recovery.refetch === undefined) recovery = { ...recovery, refetch: true };
		}
		if (recovery?.refetch === true) lastRefetchAt = monotonicNow();
		platform.publish(topic, 'recovered', recovery ? { at, recovery } : { at }, frameOptions(out));
		count('recovered');
	}

	function schedule(due) {
		if (timer !== null) {
			if (due === dueAt) return;
			clearTimer(timer);
		} else {
			windowStart = monotonicNow();
		}
		dueAt = due;
		timer = setTimer(release, Math.max(0, due - monotonicNow()));
		timer?.unref?.();
	}

	function release() {
		timer = null;
		const frame = held;
		held = null;
		// A breaker that opened again meanwhile published `degraded`; the next
		// recovery carries what is still owed.
		if (!allHealthy()) return;
		const refetch = inboundLost && restoring.size === 0;
		if (refetch) inboundLost = false;
		if (frame !== null) publishRecovered(frame.at, frame.out, refetch);
		else if (refetch) publishRecovered(now(), policyOut('recovered', NO_TRANSITION), true);
	}

	function announce() {
		if (!inboundLost || restoring.size > 0 || !allHealthy()) return;
		const t = monotonicNow();
		const start = timer === null ? t : windowStart;
		let due = Math.min(Math.max(t + HINT_HOLD_MS, lastRefetchAt + HINT_MAX_HOLD_MS), start + HINT_MAX_HOLD_MS);
		// A held `recovered` event is not delayed further.
		if (held !== null) due = Math.min(due, dueAt);
		schedule(due);
	}

	// Probe a breaker that is probing through the connection of one of its
	// buses. Runs off the breaker's listener, never inside it.
	function probe(b) {
		const entry = breakers.get(b);
		if (!entry) return;
		entry.probe = null;
		if (b.state !== 'probing') {
			entry.retryMs = 0;
			return;
		}
		let pinger = null;
		for (const h of handles) {
			if (h.source.breaker !== b || typeof h.source.ping !== 'function') continue;
			if (h.source.gone?.()) {
				h.detach();
				if (!breakers.has(b)) return;
				continue;
			}
			pinger = h;
			break;
		}
		if (pinger === null) return;
		try {
			guardBreaker(b);
		} catch {
			// Another caller holds the probe slot; its outcome may never come.
			entry.retryMs = entry.retryMs === 0 ? PROBE_RETRY_MIN_MS : Math.min(entry.retryMs * 2, PROBE_RETRY_MAX_MS);
			entry.probe = setTimer(() => probe(b), entry.retryMs);
			entry.probe?.unref?.();
			return;
		}
		entry.retryMs = 0;
		let reply;
		try { reply = Promise.resolve(pinger.source.ping()); } catch (err) { reply = Promise.reject(err); }
		reply.then(() => { /** @type {any} */ (b).success?.(); }, (err) => { /** @type {any} */ (b).failure?.(err); });
	}
	function probeSoon(b) {
		const entry = breakers.get(b);
		if (!entry || entry.probe !== null) return;
		entry.probe = setTimer(() => probe(b), 0);
		entry.probe?.unref?.();
	}

	function transition(b, from, to) {
		if (to === 'probing') probeSoon(b);
		if (from === 'healthy' && to !== 'healthy') {
			if (degraded) return;
			degraded = true;
			const out = policyOut('degraded', { from, to });
			platform.publish(topic, 'degraded', out && out.mitigation ? { at: now(), mitigation: out.mitigation } : { at: now() }, frameOptions(out));
			count('degraded');
		} else if (from !== 'healthy' && to === 'healthy') {
			for (const h of handles) if (h.source.breaker === b && h.outboundLost) h.relayRequest();
			if (!degraded || !allHealthy()) return;
			degraded = false;
			const out = policyOut('recovered', { from, to });
			if ((inboundLost && restoring.size === 0) || out?.recovery?.refetch === true) {
				held = { at: now(), out };
				schedule(timer === null ? monotonicNow() + HINT_HOLD_MS : Math.min(dueAt, monotonicNow() + HINT_HOLD_MS));
			} else {
				publishRecovered(now(), out, false);
			}
		}
	}

	return {
		/** @param {HealthSource} source */
		attach(source) {
			const b = source.breaker;
			let entry = breakers.get(b);
			if (!entry) {
				entry = { unsubscribe: b.subscribe((from, to) => transition(b, from, to)), count: 0, probe: null, retryMs: 0 };
				breakers.set(b, entry);
			}
			entry.count++;
			// A breaker already open when its bus attaches recovers later with a
			// `recovered` event, as it did when the bus announced alone.
			if (!healthy(b)) degraded = true;
			let live = true;
			const handle = {
				source,
				outboundLost: false,
				relayRequest() {
					handle.outboundLost = false;
					source.relay({ at: now(), recovery: REFETCH });
				},
				/** Messages from peers may have been missed; announce() says so. */
				lostInbound() { if (live) inboundLost = true; },
				announce() { if (live) announce(); },
				/** A peer reported dropped relays. */
				peerLost() {
					if (!live) return;
					inboundLost = true;
					announce();
				},
				/** This bus's subscriber lost its channels and is restoring them. */
				restoreStarted() {
					if (!live) return;
					restoring.add(handle);
					inboundLost = true;
				},
				restoreEnded() {
					if (!live) return;
					restoring.delete(handle);
					announce();
				},
				/** Relays were dropped. */
				lostOutbound() { if (live) handle.outboundLost = true; },
				/** A relay succeeded; peers hear of earlier drops now if healthy. */
				relayed() {
					if (live && handle.outboundLost && healthy(b)) handle.relayRequest();
				},
				detach() {
					if (!live) return;
					live = false;
					handles.delete(handle);
					restoring.delete(handle);
					const e = breakers.get(b);
					if (e && --e.count === 0) {
						e.unsubscribe();
						if (e.probe !== null) clearTimer(e.probe);
						breakers.delete(b);
					}
					if (handles.size > 0) {
						// The breaker that kept the clients degraded may have left.
						if (degraded && allHealthy()) {
							degraded = false;
							publishRecovered(now(), policyOut('recovered', NO_TRANSITION), false);
						}
						announce();
						return;
					}
					if (timer !== null) {
						clearTimer(timer);
						timer = null;
					}
					held = null;
					forget();
				}
			};
			handles.add(handle);
			if (b.state === 'probing') probeSoon(b);
			return handle;
		}
	};
}
