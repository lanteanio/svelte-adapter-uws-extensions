export interface MetricsOptions {
	/** Prefix for all metric names. @default '' */
	prefix?: string;
	/** Map topic names to bounded label values for cardinality control. @default identity */
	mapTopic?: (topic: string) => string;
	/** Default histogram buckets. @default [1, 5, 10, 25, 50, 100, 250, 500, 1000] */
	defaultBuckets?: number[];
	/** Default per-metric series (labelset) cap. Pass `Infinity` to disable. @default 10000 */
	maxSeries?: number;
	/** Default maximum number of buckets a histogram may declare. Pass `Infinity` to disable. @default 32 */
	maxBuckets?: number;
}

/** Options-object form of the counter/gauge factory third argument. */
export interface MetricOptions {
	/** Label names for this metric. */
	labelNames?: string[];
	/** Per-metric series (labelset) cap; overrides the registry default. */
	maxSeries?: number;
}

/** Options-object form of the histogram factory third argument. */
export interface HistogramOptions extends MetricOptions {
	/** Bucket upper bounds. */
	buckets?: number[];
	/**
	 * Allow negative observations (off by default; Prometheus convention -
	 * negative values corrupt `_sum` and break bucket monotonicity).
	 */
	allowNegative?: boolean;
	/** Maximum number of buckets this histogram may declare; overrides the registry default. */
	maxBuckets?: number;
}

export interface Counter {
	/** Increment the counter. */
	inc(labels?: Record<string, string>, n?: number): void;
	inc(n?: number): void;
}

export interface Gauge {
	/** Set the gauge to an absolute value. */
	set(labels: Record<string, string>, n: number): void;
	set(n: number): void;
	/** Increment the gauge. */
	inc(labels?: Record<string, string>, n?: number): void;
	inc(n?: number): void;
	/** Decrement the gauge. */
	dec(labels?: Record<string, string>, n?: number): void;
	dec(n?: number): void;
	/** Register a callback that runs at serialize time to set gauge values. */
	collect(fn: () => void): void;
}

export interface Histogram {
	/** Record an observed value. */
	observe(labels: Record<string, string>, value: number): void;
	observe(value: number): void;
}

export interface MetricsRegistry {
	/** Create a counter metric (positional form). */
	counter(name: string, help: string, labelNames?: string[], maxSeries?: number): Counter;
	/** Create a counter metric (options form). */
	counter(name: string, help: string, options: MetricOptions): Counter;
	/** Create a gauge metric (positional form). */
	gauge(name: string, help: string, labelNames?: string[], maxSeries?: number): Gauge;
	/** Create a gauge metric (options form). */
	gauge(name: string, help: string, options: MetricOptions): Gauge;
	/**
	 * Create a histogram metric. Observations must be non-negative by
	 * default (Prometheus convention; negative values corrupt `_sum` and
	 * break histogram bucket monotonicity). Pass `allowNegative = true`
	 * to opt in for signed-observation use cases (latency skew, profit
	 * & loss, temperature deltas).
	 *
	 * The seven-positional form is unwieldy when you only want a later
	 * argument; prefer the options form `histogram(name, help, { maxBuckets })`.
	 */
	histogram(name: string, help: string, labelNames?: string[], buckets?: number[], allowNegative?: boolean, maxSeries?: number, maxBuckets?: number): Histogram;
	/** Create a histogram metric (options form). */
	histogram(name: string, help: string, options: HistogramOptions): Histogram;
	/** Serialize all metrics in Prometheus text exposition format. */
	serialize(): string;
	/** uWebSockets.js HTTP handler for the /metrics endpoint. */
	handler(res: any, req: any): void;
	/** Map a topic name through the cardinality control function. */
	mapTopic(topic: string): string;
}

/**
 * Create a Prometheus metrics registry.
 */
export function createMetrics(options?: MetricsOptions): MetricsRegistry;

export interface PublishRateMetricsOptions {
	/** Cap the gauge cardinality at the top N publishers. @default 10 */
	topN?: number;
	/**
	 * Override the registry's `mapTopic` for these gauges. Used to map
	 * arbitrary topic names to bounded label values when topic identifiers
	 * carry user IDs / session IDs.
	 */
	mapTopic?: (topic: string) => string;
}

/**
 * Wire `platform.pressure.topPublishers` into per-topic publish-rate gauges
 * (`ws_topic_publish_rate{topic="..."}`, `ws_topic_publish_bytes{topic="..."}`).
 *
 * Uses `gauge.collect()` so values are scraped on demand from the adapter's
 * pressure snapshot rather than continuously accounted on the publish hot
 * path. Read every Prometheus scrape; otherwise free.
 *
 * Requires `svelte-adapter-uws >= 0.5.0-next.4` for the `topPublishers` field
 * on the pressure snapshot.
 */
export function wirePublishRateMetrics(
	platform: {
		pressure?: {
			topPublishers?: Array<{
				topic: string;
				messagesPerSec: number;
				bytesPerSec: number;
			}>;
		};
	},
	metrics: MetricsRegistry,
	options?: PublishRateMetricsOptions
): void;

/**
 * Wire cluster-wide publish-rate gauges from a publish-rate aggregator
 * (`redis/publish-rate`). Mirrors `wirePublishRateMetrics` but at the
 * cluster layer: the gauges scrape the aggregator's merged top-N at
 * collect time, no continuous accounting.
 *
 * Emits:
 *   - `cluster_topic_publish_rate{topic="..."}` (gauge)
 *   - `cluster_topic_publish_bytes{topic="..."}` (gauge)
 *
 * Both wirers (per-instance + cluster) can be active simultaneously --
 * the local view shows hot-shard pressure, the cluster view shows
 * global capacity.
 */
export function wireClusterPublishRateMetrics(
	aggregator: {
		topPublishers: Array<{
			topic: string;
			messagesPerSec: number;
			bytesPerSec: number;
		}>;
	},
	metrics: MetricsRegistry,
	options?: PublishRateMetricsOptions
): void;

/**
 * Returns a `close` hook that emits per-connection histograms and a
 * close-code counter from the adapter's close-ctx telemetry. Composes
 * with a user-provided close hook by passing it as the second argument.
 *
 * Emitted metrics:
 *   - `ws_connection_duration_seconds` (histogram)
 *   - `ws_connection_messages_in` / `ws_connection_messages_out` (histograms)
 *   - `ws_connection_bytes_in` / `ws_connection_bytes_out` (histograms)
 *   - `ws_connection_close_total{code}` (counter)
 *
 * Requires `svelte-adapter-uws >= 0.5.0-next.4` - the duration / messages /
 * bytes fields are only allocated when a `close` hook is registered.
 */
export function connectionMetricsHook(
	metrics: MetricsRegistry,
	userClose?: (ws: any, ctx: any) => void | Promise<void>
): (ws: any, ctx: any) => Promise<void>;

/**
 * Bind the production-assertion violation counter
 * (`extensions_assertion_violations_total`) to a metrics registry.
 * Re-exported from the shared assertion module - the runtime export
 * lives on this subpath.
 */
export { wireAssertionMetrics } from '../shared/assert.js';
