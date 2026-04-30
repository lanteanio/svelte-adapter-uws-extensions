/**
 * Admission control for svelte-adapter-uws-extensions.
 *
 * Pressure-aware companion to the dependency circuit breaker. Where the
 * breaker answers "is the backend up?", admission control answers "are
 * we OK to take more work right now?" -- using `platform.pressure` from
 * the adapter (memory, publish rate, subscriber ratio) to gate
 * non-critical work before it ever reaches a backend.
 *
 * The reason-precedence math (memory > publish rate > subscribers) lives
 * in the adapter. This module just maps a class name to a block-list of
 * reasons and answers true/false against the current snapshot.
 *
 * @module svelte-adapter-uws-extensions/admission
 */

const VALID_REASONS = new Set(['NONE', 'PUBLISH_RATE', 'SUBSCRIBERS', 'MEMORY']);
const REASONS_FOR_DOC = 'PUBLISH_RATE, SUBSCRIBERS, MEMORY';

/**
 * @typedef {'NONE' | 'PUBLISH_RATE' | 'SUBSCRIBERS' | 'MEMORY'} PressureReason
 */

/**
 * @typedef {Object} PressureSnapshot
 * @property {boolean} active
 * @property {number} subscriberRatio
 * @property {number} publishRate
 * @property {number} memoryMB
 * @property {PressureReason} reason
 */

/**
 * @typedef {Array<PressureReason> | ((snapshot: PressureSnapshot) => boolean)} ClassRule
 */

/**
 * @typedef {Object} AdmissionControlOptions
 * @property {Record<string, ClassRule>} classes - Per-class block rule. Either a list of pressure reasons that should block this class, or a predicate that returns true to block.
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

/**
 * @typedef {Object} AdmissionControl
 * @property {(className: string, platform: { readonly pressure: PressureSnapshot }) => boolean} shouldAccept
 */

/**
 * Create an admission controller.
 *
 * @param {AdmissionControlOptions} options
 * @returns {AdmissionControl}
 *
 * @example
 * ```js
 * const ac = createAdmissionControl({
 *   classes: {
 *     critical:   ['MEMORY'],                              // refuse only on memory pressure
 *     normal:     ['MEMORY', 'PUBLISH_RATE'],              // refuse on memory or publish rate
 *     background: ['MEMORY', 'PUBLISH_RATE', 'SUBSCRIBERS'] // refuse on any pressure
 *   }
 * });
 *
 * // Per-request:
 * if (!ac.shouldAccept('background', platform)) {
 *   return new Response('busy', { status: 503 });
 * }
 * ```
 */
export function createAdmissionControl(options) {
	if (!options || typeof options !== 'object') {
		throw new Error('admission control: options object is required');
	}
	const { classes, metrics } = options;
	if (!classes || typeof classes !== 'object') {
		throw new Error('admission control: classes is required and must be an object');
	}
	const classNames = Object.keys(classes);
	if (classNames.length === 0) {
		throw new Error('admission control: classes must define at least one class');
	}

	/** @type {Map<string, Set<PressureReason> | ((snap: PressureSnapshot) => boolean)>} */
	const compiled = new Map();
	for (const name of classNames) {
		const rule = classes[name];
		if (Array.isArray(rule)) {
			for (const r of rule) {
				if (!VALID_REASONS.has(r)) {
					throw new Error(
						`admission control: invalid reason "${r}" in class "${name}". ` +
						`Expected one of: ${REASONS_FOR_DOC}`
					);
				}
			}
			compiled.set(name, new Set(rule));
		} else if (typeof rule === 'function') {
			compiled.set(name, rule);
		} else {
			throw new Error(
				`admission control: class "${name}" rule must be an array of reasons or a predicate function`
			);
		}
	}

	const mAccepted = metrics?.counter('admission_accepted_total', 'Admissions accepted', ['class']);
	const mRejected = metrics?.counter('admission_rejected_total', 'Admissions rejected', ['class', 'reason']);

	return {
		shouldAccept(className, platform) {
			const rule = compiled.get(className);
			if (rule === undefined) {
				throw new Error(`admission control: unknown class "${className}"`);
			}
			if (!platform || !platform.pressure) {
				throw new Error('admission control: platform.pressure is required (svelte-adapter-uws >= 0.5.0-next.1)');
			}
			const snap = platform.pressure;
			let blocked;
			if (typeof rule === 'function') {
				blocked = !!rule(snap);
			} else {
				blocked = rule.has(snap.reason);
			}
			if (blocked) {
				mRejected?.inc({ class: className, reason: snap.reason });
				return false;
			}
			mAccepted?.inc({ class: className });
			return true;
		}
	};
}
