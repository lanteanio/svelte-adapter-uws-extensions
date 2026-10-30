// @ts-check
/**
 * Refetch hints for a bus with a breaker.
 *
 * A bus delivers locally while Redis is unreachable and queues nothing for
 * later, so what other instances published meanwhile never arrives, and what
 * this instance relayed never reaches them. Nothing is replayed after
 * recovery. Instead the clients are asked to reload: a `recovered` frame on
 * the system topic carries `recovery: { refetch: true }`.
 *
 * Inbound loss (the subscriber missed messages, or a peer reported that its
 * relays were dropped) is announced to this instance's own clients once the
 * breaker is healthy and no restore is running. A frame that asks for a
 * refetch waits HINT_HOLD_MS so the reports that arrive meanwhile, one per
 * peer after an outage they all saw, merge into it: a client gets one refetch
 * frame per hold, not one per peer. The breaker's own `recovered` frame is
 * held the same way when it asks for one.
 *
 * Outbound loss (relays this instance dropped) is reported to the peers with
 * one `recovered` envelope on the system topic, after the breaker recovers or
 * after the next relay that succeeds while it is healthy. The envelope holds
 * the request and a timestamp, nothing about this instance's outage.
 *
 * Every frame goes through the degradation policy, so its recovery fields and
 * de-herd window apply; a frame outside a breaker transition passes it
 * `{ from: 'healthy', to: 'healthy' }`.
 *
 * @module svelte-adapter-uws-extensions/shared/delivery-hints
 */

import { now, setTimer, clearTimer } from './runtime.js';

// Frames about this worker's own breaker go to this worker's subscribers only,
// and they make no monotonic promise.
export const SYSTEM_FRAME = Object.freeze({ relay: false, seq: false });

// How long a refetch frame waits for further reports to merge into it.
export const HINT_HOLD_MS = 1000;

const REFETCH = Object.freeze({ refetch: true });
const NO_TRANSITION = Object.freeze({ from: 'healthy', to: 'healthy' });

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
 * @param {{
 *   breaker: { state?: string },
 *   channel: string,
 *   policy?: { onRecovered(transition: { from: string, to: string }): any },
 *   platform: () => any,
 *   relay: (data: object) => void,
 *   emitted?: () => void
 * }} io - `platform` answers the platform frames go to, or null while the bus
 *   is inactive; `relay` sends the peer request on the bus; `emitted` counts a
 *   published `recovered` frame.
 */
export function createDeliveryHints({ breaker, channel, policy, platform, relay, emitted }) {
	// Two flags, so the bookkeeping is bounded however long an outage lasts.
	let inboundLost = false;
	let outboundLost = false;
	// The subscriber is restoring its channels and may still miss messages.
	let restoring = false;
	/** @type {{ at: number, out: any } | null} the breaker's own frame, held */
	let held = null;
	/** @type {any} */
	let holdTimer = null;

	const healthy = () => typeof breaker.state !== 'string' || breaker.state === 'healthy';

	function policyOut(transition) {
		if (!policy) return null;
		try { return policy.onRecovered(transition); } catch { return null; /* a policy must never break the emit */ }
	}

	function publish(at, out, refetch) {
		const p = platform();
		if (!p) return;
		let recovery = out && out.recovery ? out.recovery : null;
		// A policy that sets refetch itself has decided.
		if (refetch) {
			if (recovery === null) recovery = REFETCH;
			else if (recovery.refetch === undefined) recovery = { ...recovery, refetch: true };
		}
		p.publish(channel, 'recovered', recovery ? { at, recovery } : { at }, out && out.jitterMs ? { ...SYSTEM_FRAME, jitterMs: out.jitterMs } : SYSTEM_FRAME);
		emitted?.();
	}

	function hold() {
		if (holdTimer !== null) return;
		holdTimer = setTimer(release, HINT_HOLD_MS);
		holdTimer?.unref?.();
	}

	function release() {
		holdTimer = null;
		const frame = held;
		held = null;
		// A breaker that opened again meanwhile published `degraded`; its next
		// recovery carries what is still owed.
		if (!healthy()) return;
		const refetch = inboundLost && !restoring;
		if (refetch) inboundLost = false;
		if (frame !== null) publish(frame.at, frame.out, refetch);
		else if (refetch) publish(now(), policyOut(NO_TRANSITION), true);
	}

	function announce() {
		if (!inboundLost || restoring || !healthy() || !platform()) return;
		hold();
	}

	function relayRequest() {
		outboundLost = false;
		relay({ at: now(), recovery: REFETCH });
	}

	return {
		/** The breaker returned to healthy. */
		recovered(from, to) {
			const out = policyOut({ from, to });
			if (platform() && ((inboundLost && !restoring) || out?.recovery?.refetch === true)) {
				held = { at: now(), out };
				hold();
			} else {
				publish(now(), out, false);
			}
			if (outboundLost) relayRequest();
		},
		/** Messages from peers may have been missed; announce() says so. */
		lostInbound() { inboundLost = true; },
		announce,
		/** A peer reported dropped relays. */
		peerLost() {
			inboundLost = true;
			announce();
		},
		/** The subscriber lost its channels and is restoring them. */
		restoreStarted() {
			restoring = true;
			inboundLost = true;
		},
		restoreEnded() {
			restoring = false;
			announce();
		},
		/** Relays were dropped. */
		lostOutbound() { outboundLost = true; },
		/** A relay succeeded; peers hear of earlier drops now if healthy. */
		relayed() {
			if (outboundLost && healthy()) relayRequest();
		},
		reset() {
			inboundLost = false;
			outboundLost = false;
			restoring = false;
			held = null;
			if (holdTimer !== null) {
				clearTimer(holdTimer);
				holdTimer = null;
			}
		}
	};
}
