// @ts-check
//
// `createDegradationPolicy`: turn a bare degraded-state signal into a PROACTIVE
// client mitigation. When a circuit breaker degrades, the bus already emits a
// `degraded` event on the `__realtime` system channel; on its own each client just
// learns "something is degraded" and typically reacts by retrying immediately - the
// worst response under load. A policy precomputes the recommended client action for
// the failure (go read-only, retry after N ms, show this banner) and hands it to the
// bus, which ships it ALONG WITH the event and pushes it with the de-herd window, so
// the resulting client cache-reads / retries / fallbacks spread across the cooldown
// instead of collapsing to t+0.
//
// One policy backs one bus's breaker (the breaker carries no name and there is one
// breaker per bus, so the bus IS the failure class). A function form covers dynamic
// policies that vary the mitigation by the transition.

/** Default de-herd cap: spread retries across a quarter of the cooldown, capped here. */
const DEFAULT_JITTER_CAP_MS = 5000;
/** Hard ceiling on any de-herd window (matches the publish-side cap). */
const MAX_JITTER_MS = 60000;

/**
 * Validate a mitigation / recovery envelope. All fields optional; unknown fields are
 * passed through untouched (the client interprets them).
 * @param {any} env
 * @param {string} label
 * @returns {any}
 */
function validateEnvelope(env, label) {
	if (env == null) return null;
	if (typeof env !== 'object') throw new Error('degradation policy: ' + label + ' must be an object or null');
	if (env.streams !== undefined && (!Array.isArray(env.streams) || !env.streams.every((s) => typeof s === 'string'))) throw new Error('degradation policy: ' + label + '.streams must be an array of topic strings');
	if (env.rpcs !== undefined && (!Array.isArray(env.rpcs) || !env.rpcs.every((s) => typeof s === 'string'))) throw new Error('degradation policy: ' + label + '.rpcs must be an array of path strings');
	if (env.retryAfterMs !== undefined && (typeof env.retryAfterMs !== 'number' || !Number.isFinite(env.retryAfterMs) || env.retryAfterMs < 0)) {
		throw new Error('degradation policy: ' + label + '.retryAfterMs must be a non-negative number');
	}
	if (env.bannerCopy !== undefined && typeof env.bannerCopy !== 'string') throw new Error('degradation policy: ' + label + '.bannerCopy must be a string');
	if (env.refetch !== undefined && typeof env.refetch !== 'boolean') throw new Error('degradation policy: ' + label + '.refetch must be a boolean');
	if (env.clearCache !== undefined && typeof env.clearCache !== 'boolean') throw new Error('degradation policy: ' + label + '.clearCache must be a boolean');
	return env;
}

function assertWindow(value, label) {
	if (value === undefined) return;
	if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || value > MAX_JITTER_MS) {
		throw new Error('degradation policy: ' + label + ' must be a number in [0, ' + MAX_JITTER_MS + '] ms');
	}
}

/**
 * Resolve the de-herd window for a push. An explicit `spec` value wins; otherwise
 * derive from the envelope's `retryAfterMs` (a quarter of the cooldown, capped) so the
 * retries land spread across the window rather than at its end. No cooldown -> no jitter.
 * @param {any} envelope
 * @param {number | undefined} explicit
 * @returns {number}
 */
function resolveJitter(envelope, explicit) {
	if (explicit !== undefined) return explicit;
	const r = envelope && typeof envelope.retryAfterMs === 'number' ? envelope.retryAfterMs : 0;
	return r > 0 ? Math.min(Math.floor(r / 4), DEFAULT_JITTER_CAP_MS) : 0;
}

/**
 * @typedef {Object} Mitigation
 * @property {string[]} [streams] - Stream topics to treat as unavailable while degraded.
 * @property {string[]} [rpcs] - RPC paths to treat as unavailable while degraded.
 * @property {number} [retryAfterMs] - How long the client should hold off before retrying.
 * @property {string} [bannerCopy] - Ready-to-render notice text.
 */

/**
 * @typedef {Object} DegradationSpec
 * @property {Mitigation | ((transition: { from: string, to: string }) => Mitigation | null)} [mitigation]
 *   The envelope broadcast on degrade (or a function of the transition).
 * @property {number} [jitterMs] - De-herd window for the degraded push. Default
 *   `min(retryAfterMs / 4, 5000)` when `retryAfterMs` is set, else 0.
 * @property {Object | ((transition: { from: string, to: string }) => Object | null)} [recovery]
 *   Optional envelope broadcast on recover (e.g. `{ refetch, clearCache, bannerCopy }`).
 * @property {number} [recoveryJitterMs] - De-herd window for the recovered push.
 */

/**
 * Create a degradation policy. Pass it to `createPubSubBus(client, { breaker,
 * degradationPolicy })`; on a breaker transition the bus calls the policy and ships the
 * returned envelope on the `degraded` / `recovered` system event, de-herded with the
 * resolved jitter window.
 *
 * @param {DegradationSpec} [spec]
 */
export function createDegradationPolicy(spec = {}) {
	if (typeof spec !== 'object' || spec === null) throw new Error('degradation policy: spec must be an object');
	assertWindow(spec.jitterMs, 'jitterMs');
	assertWindow(spec.recoveryJitterMs, 'recoveryJitterMs');

	const mitigationFn = typeof spec.mitigation === 'function' ? spec.mitigation : null;
	const staticMitigation = mitigationFn ? null : validateEnvelope(spec.mitigation ?? null, 'mitigation');
	const recoveryFn = typeof spec.recovery === 'function' ? spec.recovery : null;
	const staticRecovery = recoveryFn ? null : validateEnvelope(spec.recovery ?? null, 'recovery');

	return {
		/**
		 * @param {{ from: string, to: string }} transition
		 * @returns {{ mitigation: Mitigation, jitterMs: number } | null}
		 */
		onDegraded(transition) {
			const env = validateEnvelope(mitigationFn ? mitigationFn(transition) : staticMitigation, 'mitigation');
			if (!env) return null;
			return { mitigation: env, jitterMs: resolveJitter(env, spec.jitterMs) };
		},

		/**
		 * @param {{ from: string, to: string }} transition
		 * @returns {{ recovery: any, jitterMs: number } | null}
		 */
		onRecovered(transition) {
			const env = validateEnvelope(recoveryFn ? recoveryFn(transition) : staticRecovery, 'recovery');
			if (!env) return null;
			return { recovery: env, jitterMs: resolveJitter(env, spec.recoveryJitterMs) };
		}
	};
}
