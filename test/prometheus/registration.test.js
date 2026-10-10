import { describe, expect, it } from 'vitest';
import { createMetrics } from '../../src/prometheus/index.js';

describe('metric registration descriptors', () => {
	it.each(['counter', 'gauge', 'histogram'])('%s refuses help and cap drift without changing existing samples', (kind) => {
		const metrics = createMetrics();
		const metric = metrics[kind]('shared', 'Original', { maxSeries: 2 });
		if (kind === 'counter') metric.inc();
		else if (kind === 'gauge') metric.set(7);
		else metric.observe(7);
		const before = metrics.serialize();
		expect(() => metrics[kind]('shared', 'Changed', { maxSeries: 2 })).toThrow('different help');
		expect(() => metrics[kind]('shared', 'Original', { maxSeries: 3 })).toThrow('different maxSeries');
		expect(metrics.serialize()).toBe(before);
	});

	it('refuses histogram bucket, sign policy and bucket cap drift', () => {
		const metrics = createMetrics();
		const options = { buckets: [0.1, 1], allowNegative: false, maxBuckets: 3 };
		const histogram = metrics.histogram('duration_seconds', 'Duration', options);
		expect(() => metrics.histogram('duration_seconds', 'Duration', { ...options, buckets: [10, 100] })).toThrow('different buckets');
		expect(() => metrics.histogram('duration_seconds', 'Duration', { ...options, allowNegative: true })).toThrow('different allowNegative');
		expect(() => metrics.histogram('duration_seconds', 'Duration', { ...options, maxBuckets: 4 })).toThrow('different maxBuckets');
		histogram.observe(0.05);
		expect(metrics.serialize()).toContain('duration_seconds_bucket{le="0.1"} 1');
	});

	it('normalizes labels, buckets, defaults and both factory forms', () => {
		const metrics = createMetrics({ maxSeries: 4, maxBuckets: 5, defaultBuckets: [1, 0.1] });
		const histogram = metrics.histogram('latency', 'Latency', ['b', 'a']);
		expect(metrics.histogram('latency', 'Latency', {
			labelNames: ['a', 'b'], buckets: [0.1, 1, 0.1], allowNegative: false, maxSeries: 4, maxBuckets: 5
		})).toBe(histogram);
	});

	it.each(['counter', 'gauge', 'histogram'])('%s validates a repeated descriptor and owns its label array', (kind) => {
		const metrics = createMetrics();
		const labels = ['route'];
		const metric = metrics[kind]('shared', 'Shared', labels);
		labels[0] = 'tenant';
		expect(metrics[kind]('shared', 'Shared', ['route'])).toBe(metric);
		expect(() => metrics[kind]('shared', 'Shared', { labelNames: ['route'], maxSeries: 0 })).toThrow('maxSeries');
		expect(() => metrics[kind]('shared', 'Shared', ['route', 'route'])).toThrow('duplicate label');
	});
});
it.each(['counter', 'gauge', 'histogram'])('reserves drop accounting before a user %s can replace it', (kind) => {
	const metrics = createMetrics({ prefix: 'app_', maxSeries: 1 });
	expect(() => metrics[kind]('prometheus_series_dropped_total', 'User metric')).toThrow('reserved');
	const requests = metrics.counter('requests_total', 'Requests', ['route']);
	requests.inc({ route: 'first' });
	requests.inc({ route: 'second' });
	expect(metrics.serialize()).toContain('app_prometheus_series_dropped_total{metric="app_requests_total"} 1');
	expect(() => metrics[kind]('prometheus_series_dropped_total', 'User metric')).toThrow('reserved');
	requests.inc({ route: 'third' });
	expect(metrics.serialize()).toContain('app_prometheus_series_dropped_total{metric="app_requests_total"} 2');
});
