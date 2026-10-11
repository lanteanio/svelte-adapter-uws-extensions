import { expect, it } from 'vitest';
import { createMetrics } from '../../src/prometheus/index.js';

it('retracts unknown samples while keeping the gauge registered', () => {
	const metrics = createMetrics();
	const gauge = metrics.gauge('memory_wall_bytes', 'Available memory wall');
	gauge.remove();
	gauge.set(1024);
	expect(metrics.serialize()).toContain('memory_wall_bytes 1024\n');
	gauge.remove();
	gauge.remove();
	expect(metrics.serialize()).toContain('# TYPE memory_wall_bytes gauge\n');
	expect(metrics.serialize()).not.toMatch(/^memory_wall_bytes /m);
	expect(metrics.gauge('memory_wall_bytes', 'Available memory wall')).toBe(gauge);
	gauge.set(2048);
	expect(metrics.serialize()).toContain('memory_wall_bytes 2048\n');
	for (const value of [undefined, NaN, Infinity]) expect(() => gauge.set(value)).toThrow();
});

it('removes only a validated label set and frees its series capacity', () => {
	const metrics = createMetrics();
	const gauge = metrics.gauge('worker_bytes', 'Worker usage', ['worker'], 2);
	gauge.set({ worker: 'one' }, 10);
	gauge.set({ worker: 'two' }, 20);
	expect(() => gauge.remove()).toThrow(/missing required labels/);
	expect(() => gauge.remove({ other: 'one' })).toThrow(/unexpected label/);
	gauge.remove({ worker: 'one' });
	gauge.remove({ worker: 'absent' });
	gauge.set({ worker: 'three' }, 30);
	const output = metrics.serialize();
	expect(output).not.toContain('worker="one"');
	expect(output).toContain('worker_bytes{worker="two"} 20');
	expect(output).toContain('worker_bytes{worker="three"} 30');
});
