/**
 * Prometheus metrics extension for svelte-adapter-uws-extensions.
 *
 * Exposes extension metrics in Prometheus text exposition format.
 * No external dependencies - the text format is trivial to produce.
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
 * Default per-metric series cap. A "series" in Prometheus is one
 * (metric, labelset) combination - each unique labelset creates a new
 * sample row in `/metrics` output. Past ~10k label combinations per
 * metric, Grafana queries time out, prometheus scrape volume balloons,
 * and the metric becomes operationally useless. This default protects
 * against unbounded-cardinality leaks (e.g. labeling by client IP,
 * user-controlled topic, or request ID without `mapTopic` containment).
 * Pass `Infinity` to opt out per-metric or registry-wide; pass a smaller
 * value when you know the legitimate cardinality is bounded.
 */
const DEFAULT_MAX_SERIES = 10_000;

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

/**
 * Validate the resolved `maxSeries` value. Must be a positive integer
 * or Infinity (the "no cap" opt-out).
 *
 * @param {unknown} value
 * @param {string} metricName
 */
function validateMaxSeries(value, metricName) {
	if (value === Infinity) return;
	if (!Number.isInteger(value) || value < 1) {
		throw new Error(`metric "${metricName}": maxSeries must be a positive integer or Infinity, got ${value}`);
	}
}

class Counter {
	constructor(name, help, labelNames, maxSeries) {
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
		validateMaxSeries(maxSeries, name);
		this.name = name;
		this.help = help;
		this.labelNames = labelNames || [];
		this.maxSeries = maxSeries;
		this.samples = new Map();
		/**
		 * Callback invoked when a new label-key would exceed `maxSeries`.
		 * Set by `createMetrics()` so the registry-level dropped-series
		 * counter can be lazily registered and labeled by metric name.
		 * @type {((metricName: string) => void) | null}
		 */
		this._onDrop = null;
		/** Latch so the warn-on-cap log fires once per metric, not per drop. */
		this._warned = false;
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
		const existing = this.samples.get(key);
		if (!existing && this.samples.size >= this.maxSeries) {
			recordSeriesDrop(this);
			return;
		}
		this.samples.set(key, {
			labels: labels || null,
			value: (existing?.value || 0) + val
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
	constructor(name, help, labelNames, maxSeries) {
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
		validateMaxSeries(maxSeries, name);
		this.name = name;
		this.help = help;
		this.labelNames = labelNames || [];
		this.maxSeries = maxSeries;
		this.samples = new Map();
		this.collectFn = null;
		this._onDrop = null;
		this._warned = false;
	}

	set(labels, n) {
		if (typeof labels === 'number') {
			n = labels;
			labels = undefined;
		}
		assertFinite(n, 'gauge.set()');
		validateLabels(labels, this.labelNames, this.name);
		const key = labelKey(labels);
		if (!this.samples.has(key) && this.samples.size >= this.maxSeries) {
			recordSeriesDrop(this);
			return;
		}
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
		if (!this.samples.has(key) && this.samples.size >= this.maxSeries) {
			recordSeriesDrop(this);
			return;
		}
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
		if (!this.samples.has(key) && this.samples.size >= this.maxSeries) {
			recordSeriesDrop(this);
			return;
		}
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

/**
 * De-facto bucket count cap across mature Prometheus client libraries
 * (Go's `client_golang` warns past ~100, Java's Micrometer soft-caps at
 * 64, Python's `prometheus_client` has no hard cap but practitioners
 * recommend staying under 32). 32 is generous for any realistic latency
 * / size / duration distribution; past that, each labelset's
 * per-histogram state expands linearly (each bucket is one counter +
 * one wire-format `_bucket` line at scrape time), and the percentile
 * resolution gain past 32 well-chosen buckets is marginal. Pass
 * `Infinity` to opt out per-metric or registry-wide.
 */
const DEFAULT_MAX_BUCKETS = 32;

class Histogram {
	constructor(name, help, labelNames, buckets, allowNegative, maxSeries, maxBuckets) {
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
		validateMaxSeries(maxSeries, name);
		if (maxBuckets !== Infinity && (!Number.isInteger(maxBuckets) || maxBuckets < 1)) {
			throw new Error(`metric "${name}": maxBuckets must be a positive integer or Infinity, got ${maxBuckets}`);
		}
		this.name = name;
		this.help = help;
		this.labelNames = labelNames || [];
		this.maxSeries = maxSeries;
		const rawBuckets = buckets || DEFAULT_BUCKETS;
		if (!Array.isArray(rawBuckets)) {
			throw new Error(`metric "${name}": buckets must be an array`);
		}
		if (rawBuckets.length > maxBuckets) {
			throw new Error(
				`metric "${name}": buckets length ${rawBuckets.length} exceeds maxBuckets ${maxBuckets}. ` +
				'Mature Prometheus clients soft-cap at 32; percentile-resolution gains past that are marginal ' +
				'while per-labelset state and scrape volume grow linearly. Pass a smaller bucket array, or ' +
				'override `maxBuckets` per-metric (7th positional arg to `histogram()`) or registry-wide ' +
				'(`createMetrics({ maxBuckets })`).'
			);
		}
		for (let i = 0; i < rawBuckets.length; i++) {
			if (typeof rawBuckets[i] !== 'number' || !Number.isFinite(rawBuckets[i])) {
				throw new Error(`metric "${name}": bucket values must be finite numbers, got ${rawBuckets[i]}`);
			}
		}
		this.buckets = [...new Set(rawBuckets)].sort((a, b) => a - b);
		// Histograms are non-negative by default (Prometheus convention).
		// Negative observations corrupt `_sum` and break histogram bucket
		// monotonicity (`rate()` queries return wrong values), so reject
		// them unless the caller explicitly opts in. Signed-observation
		// use cases (latency skew, p&l, temperature deltas) pass true.
		this.allowNegative = allowNegative === true;
		// per label-key: { counts: number[], sum: number, count: number }
		this.samples = new Map();
		this._onDrop = null;
		this._warned = false;
	}

	observe(labels, value) {
		if (typeof labels === 'number') {
			value = labels;
			labels = undefined;
		}
		assertFinite(value, 'histogram.observe()');
		if (!this.allowNegative && value < 0) {
			throw new Error('histogram value must not be negative (pass `allowNegative: true` at histogram() registration to opt in)');
		}
		validateLabels(labels, this.labelNames, this.name);
		const key = labelKey(labels);
		let state = this.samples.get(key);
		if (!state) {
			if (this.samples.size >= this.maxSeries) {
				recordSeriesDrop(this);
				return;
			}
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
 * Drop callback called from `Counter.inc` / `Gauge.set/inc/dec` /
 * `Histogram.observe` when adding a new (metric, labelset) combination
 * would push `samples.size` past the metric's `maxSeries` cap. Routes
 * to the registry-level dropped-series counter (registered lazily on
 * first drop) and warn-once-per-metric to console.
 *
 * @param {Counter | Gauge | Histogram} metric
 */
function recordSeriesDrop(metric) {
	if (metric._onDrop) metric._onDrop(metric.name);
	if (!metric._warned) {
		metric._warned = true;
		console.warn(
			`[prometheus] metric "${metric.name}" hit maxSeries cap ` +
			`${metric.maxSeries} - subsequent unique labelsets dropped. ` +
			'See `prometheus_series_dropped_total{metric="' + metric.name + '"}` ' +
			'for ongoing drop count. Bump `maxSeries` if legitimate, or use ' +
			'`mapTopic` to bound the label cardinality.'
		);
	}
}

/**
 * @typedef {Object} MetricsOptions
 * @property {string} [prefix=''] - Prefix for all metric names
 * @property {(topic: string) => string} [mapTopic] - Map topic names to bounded label values for cardinality control
 * @property {number[]} [defaultBuckets] - Default histogram buckets
 * @property {number} [maxSeries=10_000] - Default per-metric series cap (label combinations).
 *   Past the cap, new labelsets are dropped, a warn-once log fires per metric, and the
 *   built-in `prometheus_series_dropped_total{metric}` counter (lazily registered on first
 *   drop) tracks the ongoing drop rate. Pass `Infinity` to disable. Individual metrics
 *   can override via the per-factory `maxSeries` argument.
 * @property {number} [maxBuckets=32] - Maximum number of buckets a histogram may declare.
 *   Each labelset's per-histogram state grows linearly with bucket count (one counter per
 *   bucket + one `_bucket` line per scrape); mature client libraries soft-cap around 32.
 *   Throws at registration if the bucket array exceeds the cap. Pass `Infinity` to disable.
 *   Individual histograms can override via the per-factory `maxBuckets` argument.
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
 * // Mount the endpoint. RECOMMENDED: bind `/metrics` behind a network
 * // barrier (private scrape port, internal LB, sidecar) rather than
 * // exposing it next to public traffic. When that is not available,
 * // wrap with `metrics.authedHandler(predicate)`:
 * app.get('/metrics', metrics.handler);
 * //
 * // OR (auth helper for same-listener mounts):
 * // const token = process.env.METRICS_SCRAPE_TOKEN;
 * // app.get('/metrics', metrics.authedHandler(
 * //   (res, req) => req.getHeader('x-scrape-token') === token
 * // ));
 * ```
 */
export function createMetrics(options = {}) {
	const prefix = options.prefix || '';
	if (options.mapTopic != null && typeof options.mapTopic !== 'function') {
		throw new Error('createMetrics: mapTopic must be a function');
	}
	const mapTopic = options.mapTopic || ((t) => t);
	const defaultBuckets = options.defaultBuckets || DEFAULT_BUCKETS;
	const defaultMaxSeries = options.maxSeries ?? DEFAULT_MAX_SERIES;
	validateMaxSeries(defaultMaxSeries, '<registry default>');
	const defaultMaxBuckets = options.maxBuckets ?? DEFAULT_MAX_BUCKETS;
	if (defaultMaxBuckets !== Infinity && (!Number.isInteger(defaultMaxBuckets) || defaultMaxBuckets < 1)) {
		throw new Error(`createMetrics: maxBuckets must be a positive integer or Infinity, got ${defaultMaxBuckets}`);
	}
	const registry = new Map();

	/**
	 * Lazily-registered counter that records each (metric, drop) event.
	 * Created on first drop so a healthy registry never adds the extra
	 * series-dropped lines to its `/metrics` output. Itself has
	 * `maxSeries: Infinity` so a high-cardinality drop pattern cannot
	 * cause the drop counter itself to start dropping.
	 * @type {Counter | null}
	 */
	let droppedCounter = null;
	const droppedCounterName = prefix + 'prometheus_series_dropped_total';

	/**
	 * Wire the per-metric `_onDrop` hook to a lazy registration of the
	 * registry-level dropped-series counter. Called from the factories
	 * after each metric is created.
	 *
	 * @param {Counter | Gauge | Histogram} metric
	 */
	function attachDropHook(metric) {
		metric._onDrop = (metricName) => {
			if (!droppedCounter) {
				droppedCounter = new Counter(
					droppedCounterName,
					'Number of (metric, labelset) combinations dropped because the metric reached its maxSeries cap.',
					['metric'],
					Infinity
				);
				registry.set(droppedCounterName, droppedCounter);
			}
			droppedCounter.inc({ metric: metricName });
		};
	}

	function counter(name, help, labelNames, maxSeries) {
		const fullName = prefix + name;
		const existing = registry.get(fullName);
		if (existing) {
			if (!(existing instanceof Counter)) throw new Error(`metric "${fullName}" already registered as a different type`);
			return existing;
		}
		const c = new Counter(fullName, help, labelNames, maxSeries ?? defaultMaxSeries);
		attachDropHook(c);
		registry.set(fullName, c);
		return c;
	}

	function gauge(name, help, labelNames, maxSeries) {
		const fullName = prefix + name;
		const existing = registry.get(fullName);
		if (existing) {
			if (!(existing instanceof Gauge)) throw new Error(`metric "${fullName}" already registered as a different type`);
			return existing;
		}
		const g = new Gauge(fullName, help, labelNames, maxSeries ?? defaultMaxSeries);
		attachDropHook(g);
		registry.set(fullName, g);
		return g;
	}

	function histogram(name, help, labelNames, buckets, allowNegative, maxSeries, maxBuckets) {
		const fullName = prefix + name;
		const existing = registry.get(fullName);
		if (existing) {
			if (!(existing instanceof Histogram)) throw new Error(`metric "${fullName}" already registered as a different type`);
			return existing;
		}
		const h = new Histogram(
			fullName,
			help,
			labelNames,
			buckets || defaultBuckets,
			allowNegative,
			maxSeries ?? defaultMaxSeries,
			maxBuckets ?? defaultMaxBuckets
		);
		attachDropHook(h);
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

	/**
	 * Wrap `handler` with a caller-supplied auth predicate. The predicate
	 * receives `(res, req)` and returns truthy to allow the request or
	 * falsy to deny. May be synchronous or return a Promise. When the
	 * predicate denies, the response is a `401 Unauthorized` with a
	 * minimal text body and no metrics payload.
	 *
	 * This is an *opt-in* wrapper. The default `handler` does no auth -
	 * the recommended deployment shape is to bind `/metrics` behind a
	 * network barrier (private port, internal load balancer, sidecar
	 * scrape target) rather than relying on app-layer auth. Use this
	 * helper when the metrics endpoint lives on the same listener as
	 * public traffic and a network barrier is not available.
	 *
	 * Predicate exceptions are caught and treated as denial (500-level
	 * info is not leaked back; the response is the same 401 shape).
	 *
	 * @param {(res: any, req: any) => boolean | Promise<boolean>} predicate
	 * @returns {(res: any, req: any) => Promise<void>}
	 *
	 * @example
	 * ```js
	 * // Token-based auth from env
	 * const expectedToken = process.env.METRICS_SCRAPE_TOKEN;
	 * app.get('/metrics', metrics.authedHandler((res, req) => {
	 *   return req.getHeader('x-scrape-token') === expectedToken;
	 * }));
	 * ```
	 *
	 * @example
	 * ```js
	 * // IP-allowlist (when behind a non-CDN reverse proxy that surfaces
	 * // the real client IP via the upgrade hook's userData chain).
	 * const SCRAPE_ALLOWLIST = new Set(['10.0.0.5', '10.0.0.6']);
	 * app.get('/metrics', metrics.authedHandler((res, req) => {
	 *   const ip = Buffer.from(res.getRemoteAddressAsText()).toString();
	 *   return SCRAPE_ALLOWLIST.has(ip);
	 * }));
	 * ```
	 */
	function authedHandler(predicate) {
		if (typeof predicate !== 'function') {
			throw new Error('createMetrics.authedHandler: predicate must be a function');
		}
		return async function authedMetricsHandler(res, req) {
			// uWS requires onAborted before any async work so a client
			// hang-up does not leave us calling res.* on a dead socket.
			let aborted = false;
			res.onAborted(() => { aborted = true; });

			let allowed;
			try {
				allowed = await predicate(res, req);
			} catch {
				allowed = false;
			}
			if (aborted) return;

			res.cork(() => {
				if (allowed) {
					res.writeStatus('200 OK');
					res.writeHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
					res.end(serialize());
				} else {
					res.writeStatus('401 Unauthorized');
					res.writeHeader('Content-Type', 'text/plain; charset=utf-8');
					res.end('Unauthorized\n');
				}
			});
		};
	}

	return {
		counter,
		gauge,
		histogram,
		serialize,
		handler,
		authedHandler,
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
 * Wire cluster-wide publish-rate gauges from a publish-rate aggregator
 * (`redis/publish-rate`). Mirrors `wirePublishRateMetrics` but at the
 * cluster layer: the gauges scrape the aggregator's merged top-N at
 * collect time, no continuous accounting. Both wirers can be active
 * simultaneously - the local view shows hot-shard pressure, the
 * cluster view shows global capacity.
 *
 * @param {{ topPublishers: Array<{topic: string, messagesPerSec: number, bytesPerSec: number}> }} aggregator
 * @param {ReturnType<typeof createMetrics>} metrics
 * @param {{ topN?: number, mapTopic?: (topic: string) => string }} [options]
 */
export function wireClusterPublishRateMetrics(aggregator, metrics, options = {}) {
	if (!aggregator || typeof aggregator !== 'object') {
		throw new TypeError('wireClusterPublishRateMetrics: aggregator is required');
	}
	if (!metrics || typeof metrics.gauge !== 'function') {
		throw new TypeError('wireClusterPublishRateMetrics: metrics registry is required');
	}
	const topN = options.topN ?? 10;
	if (!Number.isInteger(topN) || topN < 1) {
		throw new TypeError('wireClusterPublishRateMetrics: topN must be a positive integer');
	}
	const mapTopic = options.mapTopic ?? metrics.mapTopic ?? ((t) => t);

	const rateGauge = metrics.gauge(
		'cluster_topic_publish_rate',
		'Top publishers by cluster-wide messages/sec, summed across instances',
		['topic']
	);
	const bytesGauge = metrics.gauge(
		'cluster_topic_publish_bytes',
		'Top publishers by cluster-wide bytes/sec, summed across instances',
		['topic']
	);

	function snapshot() {
		const top = Array.isArray(aggregator.topPublishers) ? aggregator.topPublishers : [];
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

// Re-export the assertion-metrics wirer for discoverability alongside
// the other `wire*Metrics` factories. Source-of-truth lives in
// `shared/assert.js` so the assert helper owns its observability
// surface end-to-end.
export { wireAssertionMetrics } from '../shared/assert.js';

