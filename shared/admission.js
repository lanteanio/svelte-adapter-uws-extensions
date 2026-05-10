/**
 * Admission control for svelte-adapter-uws-extensions.
 *
 * Pressure-aware companion to the dependency circuit breaker. Where the
 * breaker answers "is the backend up?", admission control answers "are
 * we OK to take more work right now?" - using `platform.pressure` from
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
 * @typedef {{ clusterTopPublisher: { threshold: number } }} ClusterTopPublisherRule
 */

/**
 * @typedef {Array<PressureReason> | ((snapshot: PressureSnapshot) => boolean) | ClusterTopPublisherRule} ClassRule
 */

/**
 * @typedef {Object} AdmissionControlOptions
 * @property {Record<string, ClassRule>} classes - Per-class block rule. Either a list of pressure reasons that should block this class, a predicate that returns true to block, or a `{clusterTopPublisher: {threshold}}` object that consults a publish-rate aggregator on a per-topic basis.
 * @property {{ rateOf(topic: string): number } | null} [aggregator] - Cluster publish-rate aggregator (from `redis/publish-rate`). Required when any class uses the `clusterTopPublisher` rule shape.
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

/**
 * @typedef {Object} AdmissionControl
 * @property {(className: string, platform: { readonly pressure: PressureSnapshot }, opts?: { topic?: string }) => boolean} shouldAccept
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
	const { classes, metrics, aggregator } = options;
	if (!classes || typeof classes !== 'object') {
		throw new Error('admission control: classes is required and must be an object');
	}
	const classNames = Object.keys(classes);
	if (classNames.length === 0) {
		throw new Error('admission control: classes must define at least one class');
	}

	/** @type {Map<string, Set<PressureReason> | ((snap: PressureSnapshot) => boolean) | { kind: 'clusterTopPublisher', threshold: number }>} */
	const compiled = new Map();
	let needsAggregator = false;
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
		} else if (rule && typeof rule === 'object' && rule.clusterTopPublisher) {
			const cfg = rule.clusterTopPublisher;
			if (!cfg || typeof cfg.threshold !== 'number' || !Number.isFinite(cfg.threshold) || cfg.threshold < 0) {
				throw new Error(
					`admission control: class "${name}" clusterTopPublisher rule requires a non-negative numeric threshold`
				);
			}
			compiled.set(name, { kind: 'clusterTopPublisher', threshold: cfg.threshold });
			needsAggregator = true;
		} else {
			throw new Error(
				`admission control: class "${name}" rule must be an array of reasons, a predicate function, or a {clusterTopPublisher: {threshold}} object`
			);
		}
	}

	if (needsAggregator) {
		if (!aggregator || typeof aggregator.rateOf !== 'function') {
			throw new Error(
				'admission control: a clusterTopPublisher rule was configured but no aggregator was passed. ' +
				'Provide one via `createAdmissionControl({ aggregator: createPublishRateAggregator(...) })`.'
			);
		}
	}

	const mAccepted = metrics?.counter('admission_accepted_total', 'Admissions accepted', ['class']);
	const mRejected = metrics?.counter('admission_rejected_total', 'Admissions rejected', ['class', 'reason']);

	return {
		shouldAccept(className, platform, opts) {
			const rule = compiled.get(className);
			if (rule === undefined) {
				throw new Error(`admission control: unknown class "${className}"`);
			}
			if (!platform || !platform.pressure) {
				throw new Error('admission control: platform.pressure is required (svelte-adapter-uws >= 0.5.0-next.1)');
			}
			const snap = platform.pressure;
			let blocked;
			let blockReason = snap.reason;
			if (typeof rule === 'function') {
				blocked = !!rule(snap);
			} else if (rule instanceof Set) {
				blocked = rule.has(snap.reason);
			} else if (rule && rule.kind === 'clusterTopPublisher') {
				const topic = opts && opts.topic;
				if (typeof topic !== 'string' || topic.length === 0) {
					throw new Error(
						`admission control: class "${className}" uses the clusterTopPublisher rule; ` +
						'shouldAccept must be called with a {topic: "..."} third argument.'
					);
				}
				const rate = aggregator.rateOf(topic);
				blocked = rate >= rule.threshold;
				if (blocked) blockReason = 'CLUSTER_TOP_PUBLISHER';
			}
			if (blocked) {
				mRejected?.inc({ class: className, reason: blockReason });
				return false;
			}
			mAccepted?.inc({ class: className });
			return true;
		}
	};
}
