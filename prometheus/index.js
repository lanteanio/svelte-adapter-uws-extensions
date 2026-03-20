/**
 * Prometheus metrics extension for svelte-adapter-uws-extensions.
 *
 * Exposes extension metrics in Prometheus text exposition format.
 * No external dependencies -- the text format is trivial to produce.
 *
 * Zero overhead when not enabled: extensions check for the metrics object
 * with optional chaining, so the engine short-circuits on a single
 * pointer check when metrics is not provided.
 *
 * @module svelte-adapter-uws-extensions/prometheus
 */

const METRIC_NAME_RE = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;
const LABEL_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function validateMetricName(name) {
	if (!METRIC_NAME_RE.test(name)) {
		throw new Error(`invalid Prometheus metric name "${name}" (must match [a-zA-Z_:][a-zA-Z0-9_:]*)`);
	}
}

function validateLabelName(name) {
	if (!LABEL_NAME_RE.test(name)) {
		throw new Error(`invalid Prometheus label name "${name}" (must match [a-zA-Z_][a-zA-Z0-9_]*)`);
	}
	if (name.startsWith('__')) {
		throw new Error(`invalid Prometheus label name "${name}" (__ prefix is reserved)`);
	}
}

function escapeHelp(text) {
	return text.replace(/\\/g, '\\\\').replace(/\n/g, '\\n');
}

function assertFinite(value, method) {
	if (typeof value !== 'number' || !Number.isFinite(value)) {
		throw new Error(`${method}: value must be a finite number, got ${value}`);
	}
}

/**
 * Serialize a label set into a stable string key for Map lookups.
 * Labels are sorted to ensure {a="1",b="2"} and {b="2",a="1"} hit the same entry.
 */
function labelKey(labels) {
	if (!labels) return '';
	const keys = Object.keys(labels);
	if (keys.length === 0) return '';
	keys.sort();
	let out = '';
	for (let i = 0; i < keys.length; i++) {
		if (i > 0) out += ',';
		out += keys[i] + '="' + String(labels[keys[i]]).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n') + '"';
	}
	return out;
}

/**
 * Format a number for Prometheus output.
 * Handles +Inf, -Inf, and NaN.
 */
function validateLabels(labels, labelNames, metricName) {
	if (labelNames.length === 0) {
		if (labels && Object.keys(labels).length > 0) {
			const keys = Object.keys(labels);
			throw new Error(`unexpected label "${keys[0]}" for metric "${metricName}" (no labels declared)`);
		}
		return;
	}
	if (!labels) {
		throw new Error(`missing required labels for metric "${metricName}" (expected: ${labelNames.join(', ')})`);
	}
	const keys = Object.keys(labels);
	for (let i = 0; i < keys.length; i++) {
		if (!labelNames.includes(keys[i])) {
			throw new Error(`unexpected label "${keys[i]}" for metric "${metricName}" (expected: ${labelNames.join(', ')})`);
		}
	}
	for (let i = 0; i < labelNames.length; i++) {
		if (!(labelNames[i] in labels)) {
			throw new Error(`missing required label "${labelNames[i]}" for metric "${metricName}"`);
		}
	}
}

function formatValue(v) {
	if (v === Infinity) return '+Inf';
	if (v === -Infinity) return '-Inf';
	if (Number.isNaN(v)) return 'NaN';
	return String(v);
}

class Counter {
	constructor(name, help, labelNames) {
		validateMetricName(name);
		if (typeof help !== 'string') {
			throw new Error(`metric "${name}": help must be a string`);
		}
		if (labelNames != null) {
			if (!Array.isArray(labelNames)) {
				throw new Error(`metric "${name}": labelNames must be an array`);
			}
			const seen = new Set();
			for (let i = 0; i < labelNames.length; i++) {
				validateLabelName(labelNames[i]);
				if (seen.has(labelNames[i])) {
					throw new Error(`metric "${name}": duplicate label name "${labelNames[i]}"`);
				}
				seen.add(labelNames[i]);
			}
		}
		this.name = name;
		this.help = help;
		this.labelNames = labelNames || [];
		this.samples = new Map();
	}

	inc(labels, n) {
		if (typeof labels === 'number') {
			n = labels;
			labels = undefined;
		}
		const val = n ?? 1;
		assertFinite(val, 'counter.inc()');
		if (val < 0) throw new Error('counter value must not be negative');
		validateLabels(labels, this.labelNames, this.name);
		const key = labelKey(labels);
		this.samples.set(key, {
			labels: labels || null,
			value: (this.samples.get(key)?.value || 0) + val
		});
	}

	serialize() {
		let out = '# HELP ' + this.name + ' ' + escapeHelp(this.help) + '\n';
		out += '# TYPE ' + this.name + ' counter\n';
		for (const [key, sample] of this.samples) {
			const lbl = key ? '{' + key + '}' : '';
			out += this.name + lbl + ' ' + formatValue(sample.value) + '\n';
		}
		return out;
	}
}

class Gauge {
	constructor(name, help, labelNames) {
		validateMetricName(name);
		if (typeof help !== 'string') {
			throw new Error(`metric "${name}": help must be a string`);
		}
		if (labelNames != null) {
			if (!Array.isArray(labelNames)) {
				throw new Error(`metric "${name}": labelNames must be an array`);
			}
			const seen = new Set();
			for (let i = 0; i < labelNames.length; i++) {
				validateLabelName(labelNames[i]);
				if (seen.has(labelNames[i])) {
					throw new Error(`metric "${name}": duplicate label name "${labelNames[i]}"`);
				}
				seen.add(labelNames[i]);
			}
		}
		this.name = name;
		this.help = help;
		this.labelNames = labelNames || [];
		this.samples = new Map();
		this.collectFn = null;
	}

	set(labels, n) {
		if (typeof labels === 'number') {
			n = labels;
			labels = undefined;
		}
		assertFinite(n, 'gauge.set()');
		validateLabels(labels, this.labelNames, this.name);
		const key = labelKey(labels);
		this.samples.set(key, { labels: labels || null, value: n });
	}

	inc(labels, n) {
		if (typeof labels === 'number') {
			n = labels;
			labels = undefined;
		}
		const val = n ?? 1;
		assertFinite(val, 'gauge.inc()');
		validateLabels(labels, this.labelNames, this.name);
		const key = labelKey(labels);
		const current = this.samples.get(key)?.value || 0;
		this.samples.set(key, { labels: labels || null, value: current + val });
	}

	dec(labels, n) {
		if (typeof labels === 'number') {
			n = labels;
			labels = undefined;
		}
		const val = n ?? 1;
		assertFinite(val, 'gauge.dec()');
		validateLabels(labels, this.labelNames, this.name);
		const key = labelKey(labels);
		const current = this.samples.get(key)?.value || 0;
		this.samples.set(key, { labels: labels || null, value: current - val });
	}

	collect(fn) {
		this.collectFn = fn;
	}

	serialize() {
		if (this.collectFn) {
			this.samples.clear();
			this.collectFn();
		}
		let out = '# HELP ' + this.name + ' ' + escapeHelp(this.help) + '\n';
		out += '# TYPE ' + this.name + ' gauge\n';
		for (const [key, sample] of this.samples) {
			const lbl = key ? '{' + key + '}' : '';
			out += this.name + lbl + ' ' + formatValue(sample.value) + '\n';
		}
		return out;
	}
}

const DEFAULT_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000];

class Histogram {
	constructor(name, help, labelNames, buckets) {
		validateMetricName(name);
		if (typeof help !== 'string') {
			throw new Error(`metric "${name}": help must be a string`);
		}
		if (labelNames != null) {
			if (!Array.isArray(labelNames)) {
				throw new Error(`metric "${name}": labelNames must be an array`);
			}
			const seen = new Set();
			for (let i = 0; i < labelNames.length; i++) {
				validateLabelName(labelNames[i]);
				if (seen.has(labelNames[i])) {
					throw new Error(`metric "${name}": duplicate label name "${labelNames[i]}"`);
				}
				seen.add(labelNames[i]);
			}
		}
		this.name = name;
		this.help = help;
		this.labelNames = labelNames || [];
		const rawBuckets = buckets || DEFAULT_BUCKETS;
		for (let i = 0; i < rawBuckets.length; i++) {
			if (typeof rawBuckets[i] !== 'number' || !Number.isFinite(rawBuckets[i])) {
				throw new Error(`metric "${name}": bucket values must be finite numbers, got ${rawBuckets[i]}`);
			}
		}
		this.buckets = [...new Set(rawBuckets)].sort((a, b) => a - b);
		// per label-key: { counts: number[], sum: number, count: number }
		this.samples = new Map();
	}

	observe(labels, value) {
		if (typeof labels === 'number') {
			value = labels;
			labels = undefined;
		}
		assertFinite(value, 'histogram.observe()');
		validateLabels(labels, this.labelNames, this.name);
		const key = labelKey(labels);
		let state = this.samples.get(key);
		if (!state) {
			state = {
				labels: labels || null,
				counts: new Array(this.buckets.length).fill(0),
				sum: 0,
				count: 0
			};
			this.samples.set(key, state);
		}
		state.sum += value;
		state.count++;
		for (let i = 0; i < this.buckets.length; i++) {
			if (value <= this.buckets[i]) state.counts[i]++;
		}
	}

	serialize() {
		let out = '# HELP ' + this.name + ' ' + escapeHelp(this.help) + '\n';
		out += '# TYPE ' + this.name + ' histogram\n';
		for (const [, state] of this.samples) {
			const baseLabels = state.labels ? labelKey(state.labels) : '';
			for (let i = 0; i < this.buckets.length; i++) {
				const le = formatValue(this.buckets[i]);
				const lbl = baseLabels
					? '{' + baseLabels + ',le="' + le + '"}'
					: '{le="' + le + '"}';
				out += this.name + '_bucket' + lbl + ' ' + state.counts[i] + '\n';
			}
			const infLabel = baseLabels
				? '{' + baseLabels + ',le="+Inf"}'
				: '{le="+Inf"}';
			out += this.name + '_bucket' + infLabel + ' ' + state.count + '\n';

			const sumLabel = baseLabels ? '{' + baseLabels + '}' : '';
			out += this.name + '_sum' + sumLabel + ' ' + formatValue(state.sum) + '\n';
			out += this.name + '_count' + sumLabel + ' ' + state.count + '\n';
		}
		return out;
	}
}

/**
 * @typedef {Object} MetricsOptions
 * @property {string} [prefix=''] - Prefix for all metric names
 * @property {(topic: string) => string} [mapTopic] - Map topic names to bounded label values for cardinality control
 * @property {number[]} [defaultBuckets] - Default histogram buckets
 */

/**
 * Create a Prometheus metrics registry.
 *
 * @param {MetricsOptions} [options]
 *
 * @example
 * ```js
 * import { createMetrics } from 'svelte-adapter-uws-extensions/prometheus';
 *
 * const metrics = createMetrics({ prefix: 'myapp_' });
 *
 * // Pass to extensions:
 * const presence = createPresence(redis, { metrics, key: 'id' });
 *
 * // Mount the endpoint:
 * app.get('/metrics', metrics.handler);
 * ```
 */
export function createMetrics(options = {}) {
	const prefix = options.prefix || '';
	if (options.mapTopic != null && typeof options.mapTopic !== 'function') {
		throw new Error('createMetrics: mapTopic must be a function');
	}
	const mapTopic = options.mapTopic || ((t) => t);
	const defaultBuckets = options.defaultBuckets || DEFAULT_BUCKETS;
	const registry = new Map();

	function counter(name, help, labelNames) {
		const fullName = prefix + name;
		const existing = registry.get(fullName);
		if (existing) {
			if (!(existing instanceof Counter)) throw new Error(`metric "${fullName}" already registered as a different type`);
			return existing;
		}
		const c = new Counter(fullName, help, labelNames);
		registry.set(fullName, c);
		return c;
	}

	function gauge(name, help, labelNames) {
		const fullName = prefix + name;
		const existing = registry.get(fullName);
		if (existing) {
			if (!(existing instanceof Gauge)) throw new Error(`metric "${fullName}" already registered as a different type`);
			return existing;
		}
		const g = new Gauge(fullName, help, labelNames);
		registry.set(fullName, g);
		return g;
	}

	function histogram(name, help, labelNames, buckets) {
		const fullName = prefix + name;
		const existing = registry.get(fullName);
		if (existing) {
			if (!(existing instanceof Histogram)) throw new Error(`metric "${fullName}" already registered as a different type`);
			return existing;
		}
		const h = new Histogram(fullName, help, labelNames, buckets || defaultBuckets);
		registry.set(fullName, h);
		return h;
	}

	function serialize() {
		let out = '';
		for (const c of registry.values()) {
			const s = c.serialize();
			if (s) out += s;
		}
		return out;
	}

	function handler(res, req) {
		res.writeStatus('200 OK');
		res.writeHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
		res.end(serialize());
	}

	return {
		counter,
		gauge,
		histogram,
		serialize,
		handler,
		mapTopic
	};
}
