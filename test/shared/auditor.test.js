import { describe, it, expect, vi } from 'vitest';
import { createConsistencyAuditor } from '../../src/shared/auditor.js';

function constantSnapshot(snap) {
	return () => ({ ...snap, total: (snap.connections || []).length });
}

describe('createConsistencyAuditor (extensions)', () => {
	it('rejects a missing snapshot or assert function', () => {
		expect(() => createConsistencyAuditor({ assert: () => {} })).toThrow(/snapshot/);
		expect(() => createConsistencyAuditor({ snapshot: () => ({}) })).toThrow(/assert/);
	});

	it('requires a fatal sink when hardCategories is set', () => {
		expect(() => createConsistencyAuditor({
			snapshot: () => ({}),
			assert: () => {},
			hardCategories: ['subs.shape']
		})).toThrow(/fatal/);
	});

	it('surfaces a seeded invariant violation through the soft assert by default', () => {
		const softAssert = vi.fn();
		const auditor = createConsistencyAuditor({
			snapshot: constantSnapshot({ connections: [{ id: 1, subscribed: ['a'], bookkeeping: null }] }),
			assert: softAssert
		});
		expect(auditor.runOnce()).toEqual([{ category: 'subs.shape', context: { ws: 1 } }]);
		expect(softAssert).toHaveBeenCalledWith(false, 'subs.shape', { ws: 1 });
		expect(auditor.stats.fatals).toBe(0);
	});

	it('keeps a hard-tier violation soft first, escalates on persistence', () => {
		const softAssert = vi.fn();
		const fatalFn = vi.fn();
		const auditor = createConsistencyAuditor({
			snapshot: constantSnapshot({ connections: [{ id: 1, subscribed: ['a'], bookkeeping: null }] }),
			assert: softAssert,
			fatal: fatalFn,
			hardCategories: ['subs.shape']
		});
		auditor.runOnce();
		expect(fatalFn).not.toHaveBeenCalled();
		auditor.runOnce();
		expect(fatalFn).toHaveBeenCalledWith(false, 'subs.shape', { ws: 1 });
		expect(auditor.stats.fatals).toBe(1);
	});

	it('advances a round-robin window so a bounded snapshot covers everything', () => {
		const offsets = [];
		const auditor = createConsistencyAuditor({
			snapshot: ({ offset }) => { offsets.push(offset); return { connections: [], total: 5 }; },
			assert: () => {},
			maxPerTick: 2
		});
		auditor.runOnce();
		auditor.runOnce();
		auditor.runOnce();
		auditor.runOnce();
		expect(offsets).toEqual([0, 2, 4, 0]);
	});
});
