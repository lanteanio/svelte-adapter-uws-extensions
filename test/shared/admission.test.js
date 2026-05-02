import { describe, it, expect, beforeEach } from 'vitest';
import { createAdmissionControl } from '../../shared/admission.js';
import { mockPlatform } from '../helpers/mock-platform.js';

describe('admission control', () => {
	let platform;

	beforeEach(() => {
		platform = mockPlatform();
	});

	describe('construction', () => {
		it('requires options', () => {
			expect(() => createAdmissionControl()).toThrow('options object is required');
			expect(() => createAdmissionControl(null)).toThrow('options object is required');
		});

		it('requires classes', () => {
			expect(() => createAdmissionControl({})).toThrow('classes is required');
			expect(() => createAdmissionControl({ classes: null })).toThrow('classes is required');
		});

		it('requires at least one class', () => {
			expect(() => createAdmissionControl({ classes: {} })).toThrow('at least one class');
		});

		it('rejects invalid reason strings', () => {
			expect(() => createAdmissionControl({
				classes: { critical: ['MEMOR'] }
			})).toThrow('invalid reason "MEMOR"');
		});

		it('rejects rules that are neither array nor function', () => {
			expect(() => createAdmissionControl({
				classes: { critical: 'MEMORY' }
			})).toThrow('must be an array of reasons');

			expect(() => createAdmissionControl({
				classes: { critical: 42 }
			})).toThrow('must be an array of reasons');
		});

		it('accepts valid reason arrays', () => {
			expect(() => createAdmissionControl({
				classes: {
					critical: ['MEMORY'],
					normal: ['MEMORY', 'PUBLISH_RATE'],
					background: ['MEMORY', 'PUBLISH_RATE', 'SUBSCRIBERS']
				}
			})).not.toThrow();
		});

		it('accepts predicate rules', () => {
			expect(() => createAdmissionControl({
				classes: { streaming: (snap) => snap.subscriberRatio > 50 }
			})).not.toThrow();
		});
	});

	describe('shouldAccept (array rule)', () => {
		let ac;

		beforeEach(() => {
			ac = createAdmissionControl({
				classes: {
					critical: ['MEMORY'],
					normal: ['MEMORY', 'PUBLISH_RATE'],
					background: ['MEMORY', 'PUBLISH_RATE', 'SUBSCRIBERS']
				}
			});
		});

		it('admits everything under no pressure', () => {
			expect(ac.shouldAccept('critical', platform)).toBe(true);
			expect(ac.shouldAccept('normal', platform)).toBe(true);
			expect(ac.shouldAccept('background', platform)).toBe(true);
		});

		it('rejects only background under SUBSCRIBERS pressure', () => {
			platform._setPressure({
				active: true, subscriberRatio: 100, publishRate: 0, memoryMB: 100, reason: 'SUBSCRIBERS'
			});
			expect(ac.shouldAccept('critical', platform)).toBe(true);
			expect(ac.shouldAccept('normal', platform)).toBe(true);
			expect(ac.shouldAccept('background', platform)).toBe(false);
		});

		it('rejects normal and background under PUBLISH_RATE pressure', () => {
			platform._setPressure({
				active: true, subscriberRatio: 0, publishRate: 50000, memoryMB: 100, reason: 'PUBLISH_RATE'
			});
			expect(ac.shouldAccept('critical', platform)).toBe(true);
			expect(ac.shouldAccept('normal', platform)).toBe(false);
			expect(ac.shouldAccept('background', platform)).toBe(false);
		});

		it('rejects everything under MEMORY pressure', () => {
			platform._setPressure({
				active: true, subscriberRatio: 0, publishRate: 0, memoryMB: 8000, reason: 'MEMORY'
			});
			expect(ac.shouldAccept('critical', platform)).toBe(false);
			expect(ac.shouldAccept('normal', platform)).toBe(false);
			expect(ac.shouldAccept('background', platform)).toBe(false);
		});
	});

	describe('shouldAccept (predicate rule)', () => {
		it('uses the predicate result', () => {
			const ac = createAdmissionControl({
				classes: {
					streaming: (snap) => snap.subscriberRatio > 50
				}
			});

			platform._setPressure({
				active: true, subscriberRatio: 60, publishRate: 0, memoryMB: 0, reason: 'SUBSCRIBERS'
			});
			expect(ac.shouldAccept('streaming', platform)).toBe(false);

			platform._setPressure({
				active: true, subscriberRatio: 30, publishRate: 0, memoryMB: 0, reason: 'SUBSCRIBERS'
			});
			expect(ac.shouldAccept('streaming', platform)).toBe(true);
		});

		it('treats truthy non-boolean predicate returns as block', () => {
			const ac = createAdmissionControl({
				classes: { stream: (snap) => snap.publishRate ? 'overload' : null }
			});

			platform._setPressure({
				active: true, subscriberRatio: 0, publishRate: 100, memoryMB: 0, reason: 'PUBLISH_RATE'
			});
			expect(ac.shouldAccept('stream', platform)).toBe(false);

			platform._setPressure({
				active: false, subscriberRatio: 0, publishRate: 0, memoryMB: 0, reason: 'NONE'
			});
			expect(ac.shouldAccept('stream', platform)).toBe(true);
		});
	});

	describe('error paths', () => {
		it('throws on unknown class name', () => {
			const ac = createAdmissionControl({ classes: { critical: ['MEMORY'] } });
			expect(() => ac.shouldAccept('crtical', platform)).toThrow('unknown class "crtical"');
		});

		it('throws when platform.pressure is missing', () => {
			const ac = createAdmissionControl({ classes: { critical: ['MEMORY'] } });
			expect(() => ac.shouldAccept('critical', null)).toThrow('platform.pressure is required');
			expect(() => ac.shouldAccept('critical', {})).toThrow('platform.pressure is required');
		});
	});

	describe('metrics integration', () => {
		it('counts accepted and rejected calls per class', async () => {
			const { createMetrics } = await import('../../prometheus/index.js');
			const metrics = createMetrics();
			const ac = createAdmissionControl({
				classes: {
					critical: ['MEMORY'],
					background: ['MEMORY', 'PUBLISH_RATE', 'SUBSCRIBERS']
				},
				metrics
			});

			ac.shouldAccept('critical', platform);   // accepted
			ac.shouldAccept('background', platform); // accepted

			platform._setPressure({
				active: true, subscriberRatio: 100, publishRate: 0, memoryMB: 0, reason: 'SUBSCRIBERS'
			});
			ac.shouldAccept('critical', platform);   // accepted (SUBSCRIBERS not in critical list)
			ac.shouldAccept('background', platform); // rejected with reason=SUBSCRIBERS

			const out = metrics.serialize();
			expect(out).toContain('# TYPE admission_accepted_total counter');
			expect(out).toContain('# TYPE admission_rejected_total counter');
			expect(out).toMatch(/admission_accepted_total\{class="critical"\} 2/);
			expect(out).toMatch(/admission_accepted_total\{class="background"\} 1/);
			expect(out).toMatch(/admission_rejected_total\{class="background",reason="SUBSCRIBERS"\} 1/);
		});

		it('does not require a metrics registry', () => {
			const ac = createAdmissionControl({
				classes: { critical: ['MEMORY'] }
			});
			expect(ac.shouldAccept('critical', platform)).toBe(true);
		});
	});

	describe('reason-precedence trust', () => {
		it('only the resolved reason is checked, not raw signal values', () => {
			// The adapter collapses concurrent signals into a single
			// most-urgent reason via its precedence math. The controller
			// must not second-guess that resolution by inspecting the
			// raw numeric fields and inferring a different reason.
			const ac = createAdmissionControl({
				classes: { critical: ['MEMORY'] }
			});

			// memoryMB high but the resolved reason is PUBLISH_RATE.
			platform._setPressure({
				active: true, subscriberRatio: 0, publishRate: 50000, memoryMB: 7000, reason: 'PUBLISH_RATE'
			});
			// critical class is not in PUBLISH_RATE's block list, so accept.
			expect(ac.shouldAccept('critical', platform)).toBe(true);
		});
	});

	describe('clusterTopPublisher rule', () => {
		function makeAggregator(rateMap = {}) {
			return {
				rateOf: (topic) => rateMap[topic] || 0
			};
		}

		it('rejects when the cluster-wide rate for the topic is at or above threshold', () => {
			const ac = createAdmissionControl({
				classes: { hot: { clusterTopPublisher: { threshold: 1000 } } },
				aggregator: makeAggregator({ 'org:42:audit': 1500, 'chat:room1': 500 })
			});

			expect(ac.shouldAccept('hot', platform, { topic: 'org:42:audit' })).toBe(false);
			expect(ac.shouldAccept('hot', platform, { topic: 'chat:room1' })).toBe(true);
		});

		it('accepts when the topic is unknown to the aggregator', () => {
			const ac = createAdmissionControl({
				classes: { hot: { clusterTopPublisher: { threshold: 100 } } },
				aggregator: makeAggregator({})
			});
			expect(ac.shouldAccept('hot', platform, { topic: 'cold-topic' })).toBe(true);
		});

		it('throws if shouldAccept is called without a topic', () => {
			const ac = createAdmissionControl({
				classes: { hot: { clusterTopPublisher: { threshold: 100 } } },
				aggregator: makeAggregator({})
			});
			expect(() => ac.shouldAccept('hot', platform)).toThrow(/topic/);
			expect(() => ac.shouldAccept('hot', platform, {})).toThrow(/topic/);
			expect(() => ac.shouldAccept('hot', platform, { topic: '' })).toThrow(/topic/);
		});

		it('throws at construction time if no aggregator is provided', () => {
			expect(() => createAdmissionControl({
				classes: { hot: { clusterTopPublisher: { threshold: 100 } } }
			})).toThrow(/aggregator/);
		});

		it('throws on a non-numeric or negative threshold', () => {
			expect(() => createAdmissionControl({
				classes: { hot: { clusterTopPublisher: { threshold: 'lots' } } },
				aggregator: makeAggregator({})
			})).toThrow(/threshold/);
			expect(() => createAdmissionControl({
				classes: { hot: { clusterTopPublisher: { threshold: -1 } } },
				aggregator: makeAggregator({})
			})).toThrow(/threshold/);
		});

		it('records admission_rejected_total with reason="CLUSTER_TOP_PUBLISHER"', async () => {
			const { createMetrics } = await import('../../prometheus/index.js');
			const metrics = createMetrics();
			const ac = createAdmissionControl({
				classes: { hot: { clusterTopPublisher: { threshold: 100 } } },
				aggregator: makeAggregator({ 'hot-topic': 5000 }),
				metrics
			});
			ac.shouldAccept('hot', platform, { topic: 'hot-topic' });
			ac.shouldAccept('hot', platform, { topic: 'cold-topic' });

			const out = await metrics.serialize();
			expect(out).toMatch(/admission_rejected_total\{class="hot",reason="CLUSTER_TOP_PUBLISHER"\}\s+1/);
			expect(out).toMatch(/admission_accepted_total\{class="hot"\}\s+1/);
		});

		it('coexists with reason-array and predicate rules', () => {
			const aggregator = makeAggregator({ 'hot': 9999 });
			const ac = createAdmissionControl({
				classes: {
					critical: ['MEMORY'],
					streaming: (snap) => snap.subscriberRatio > 50,
					hot: { clusterTopPublisher: { threshold: 100 } }
				},
				aggregator
			});

			platform._setPressure({
				active: true, subscriberRatio: 10, publishRate: 0, memoryMB: 100, reason: 'NONE'
			});
			expect(ac.shouldAccept('critical', platform)).toBe(true);
			expect(ac.shouldAccept('streaming', platform)).toBe(true);
			expect(ac.shouldAccept('hot', platform, { topic: 'hot' })).toBe(false);
		});
	});
});
