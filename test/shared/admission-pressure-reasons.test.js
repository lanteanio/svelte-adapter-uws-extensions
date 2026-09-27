import { describe, expect, it } from 'vitest';
import { PRESSURE_REASON_CODES } from 'svelte-adapter-uws/observability';
import { createAdmissionControl, PRESSURE_REASONS } from '../../src/shared/admission.js';
import { mockPlatform } from '../helpers/mock-platform.js';

describe('admission pressure-reason contract', () => {
	it('is the adapter\'s published reason list, in its order, and sheds every configured reason', () => {
		expect(PRESSURE_REASONS).toEqual(Object.keys(PRESSURE_REASON_CODES));
		expect(PRESSURE_REASONS).toContain('PSI');
		expect(Object.isFrozen(PRESSURE_REASONS)).toBe(true);

		const platform = mockPlatform();
		const admission = createAdmissionControl({
			classes: { all: PRESSURE_REASONS }
		});
		for (const reason of PRESSURE_REASONS) {
			platform._setPressure({ reason, active: reason !== 'NONE' });
			expect(admission.shouldAccept('all', platform), reason).toBe(false);
		}
	});

	it('a class that lists every reason but NONE admits an idle platform and sheds under any active pressure', () => {
		const platform = mockPlatform();
		const admission = createAdmissionControl({
			classes: { background: PRESSURE_REASONS.filter((reason) => reason !== 'NONE') }
		});
		platform._setPressure({ reason: 'NONE', active: false });
		expect(admission.shouldAccept('background', platform)).toBe(true);
		for (const reason of PRESSURE_REASONS.filter((reason) => reason !== 'NONE')) {
			platform._setPressure({ reason, active: true });
			expect(admission.shouldAccept('background', platform), reason).toBe(false);
		}
	});

	it.each(['PSI', 'CPU_QUOTA', 'CAPACITY'])('accepts %s at construction and enforces it at runtime', (reason) => {
		const platform = mockPlatform();
		const admission = createAdmissionControl({ classes: { urgent: [reason] } });
		platform._setPressure({ reason: 'NONE', active: false });
		expect(admission.shouldAccept('urgent', platform)).toBe(true);
		platform._setPressure({ reason, active: true });
		expect(admission.shouldAccept('urgent', platform)).toBe(false);
	});
});
