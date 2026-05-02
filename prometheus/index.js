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

/**
 * Default histogram buckets for `connectionMetricsHook`. Tuned for the
 * realistic distribution of WebSocket session shapes:
 *   - Duration: short reconnect (1s) -> idle dashboard tab (1 day).
 *   - Messages: silent reconnect (1) -> chatty game (100k).
 *   - Bytes: keepalive only (1KB) -> bulk transfer (256MB).
 */
const DEFAULT_DURATION_BUCKETS = [1, 10, 60, 300, 1800, 3600, 14400, 86400];
const DEFAULT_MESSAGE_BUCKETS = [1, 10, 100, 1000, 10000, 100000];
const DEFAULT_BYTE_BUCKETS = [1024, 65536, 1048576, 16777216, 268435456];

/**
 * Wire `platform.pressure.topPublishers` into per-topic publish-rate gauges.
 *
 * Uses `gauge.collect()` so values are scraped on demand from the adapter's
 * pressure snapshot rather than continuously accounted on the publish hot
 * path. Read every Prometheus scrape; otherwise free.
 *
 * @param {{ pressure?: { topPublishers?: Array<{topic: string, messagesPerSec: number, bytesPerSec: number}> } }} platform
 * @param {ReturnType<typeof createMetrics>} metrics
 * @param {{ topN?: number, mapTopic?: (topic: string) => string }} [options]
 */
export function wirePublishRateMetrics(platform, metrics, options = {}) {
	if (!platform || typeof platform !== 'object') {
		throw new TypeError('wirePublishRateMetrics: platform is required');
	}
	if (!metrics || typeof metrics.gauge !== 'function') {
		throw new TypeError('wirePublishRateMetrics: metrics registry is required');
	}
	const topN = options.topN ?? 10;
	if (!Number.isInteger(topN) || topN < 1) {
		throw new TypeError('wirePublishRateMetrics: topN must be a positive integer');
	}
	const mapTopic = options.mapTopic ?? metrics.mapTopic ?? ((t) => t);

	const rateGauge = metrics.gauge(
		'ws_topic_publish_rate',
		'Top publishers by messages/sec, sampled from platform.pressure',
		['topic']
	);
	const bytesGauge = metrics.gauge(
		'ws_topic_publish_bytes',
		'Top publishers by bytes/sec, sampled from platform.pressure',
		['topic']
	);

	function snapshot() {
		const top = platform.pressure && Array.isArray(platform.pressure.topPublishers)
			? platform.pressure.topPublishers
			: [];
		return top.length > topN ? top.slice(0, topN) : top;
	}

	rateGauge.collect(() => {
		for (const t of snapshot()) {
			rateGauge.set({ topic: mapTopic(t.topic) }, t.messagesPerSec);
		}
	});
	bytesGauge.collect(() => {
		for (const t of snapshot()) {
			bytesGauge.set({ topic: mapTopic(t.topic) }, t.bytesPerSec);
		}
	});
}

/**
 * Returns a `close` hook that emits per-connection histograms + a close-code
 * counter from the adapter's close-ctx telemetry. Composes with a user-
 * provided close hook by passing `userClose` as the second argument.
 *
 * @param {ReturnType<typeof createMetrics>} metrics
 * @param {((ws: any, ctx: any) => void | Promise<void>) | undefined} [userClose]
 */
export function connectionMetricsHook(metrics, userClose) {
	if (!metrics || typeof metrics.histogram !== 'function') {
		throw new TypeError('connectionMetricsHook: metrics registry is required');
	}
	if (userClose !== undefined && typeof userClose !== 'function') {
		throw new TypeError('connectionMetricsHook: userClose must be a function if provided');
	}

	const durationHist = metrics.histogram(
		'ws_connection_duration_seconds',
		'Connection duration in seconds at close',
		[],
		DEFAULT_DURATION_BUCKETS
	);
	const msgsInHist = metrics.histogram(
		'ws_connection_messages_in',
		'Messages received per connection at close',
		[],
		DEFAULT_MESSAGE_BUCKETS
	);
	const msgsOutHist = metrics.histogram(
		'ws_connection_messages_out',
		'Messages sent per connection at close',
		[],
		DEFAULT_MESSAGE_BUCKETS
	);
	const bytesInHist = metrics.histogram(
		'ws_connection_bytes_in',
		'Bytes received per connection at close',
		[],
		DEFAULT_BYTE_BUCKETS
	);
	const bytesOutHist = metrics.histogram(
		'ws_connection_bytes_out',
		'Bytes sent per connection at close',
		[],
		DEFAULT_BYTE_BUCKETS
	);
	const closeCounter = metrics.counter(
		'ws_connection_close_total',
		'Connections closed by close code',
		['code']
	);

	return async function close(ws, ctx) {
		if (ctx) {
			if (typeof ctx.duration === 'number' && ctx.duration >= 0) {
				durationHist.observe(ctx.duration / 1000);
			}
			if (typeof ctx.messagesIn === 'number' && ctx.messagesIn >= 0) {
				msgsInHist.observe(ctx.messagesIn);
			}
			if (typeof ctx.messagesOut === 'number' && ctx.messagesOut >= 0) {
				msgsOutHist.observe(ctx.messagesOut);
			}
			if (typeof ctx.bytesIn === 'number' && ctx.bytesIn >= 0) {
				bytesInHist.observe(ctx.bytesIn);
			}
			if (typeof ctx.bytesOut === 'number' && ctx.bytesOut >= 0) {
				bytesOutHist.observe(ctx.bytesOut);
			}
			if (ctx.code !== undefined && ctx.code !== null) {
				closeCounter.inc({ code: String(ctx.code) });
			}
		}
		if (userClose) await userClose(ws, ctx);
	};
}
