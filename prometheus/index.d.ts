export interface MetricsOptions {
	/** Prefix for all metric names. @default '' */
	prefix?: string;
	/** Map topic names to bounded label values for cardinality control. @default identity */
	mapTopic?: (topic: string) => string;
	/** Default histogram buckets. @default [1, 5, 10, 25, 50, 100, 250, 500, 1000] */
	defaultBuckets?: number[];
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
	/** Create a counter metric. */
	counter(name: string, help: string, labelNames?: string[]): Counter;
	/** Create a gauge metric. */
	gauge(name: string, help: string, labelNames?: string[]): Gauge;
	/** Create a histogram metric. */
	histogram(name: string, help: string, labelNames?: string[], buckets?: number[]): Histogram;
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
 * Requires `svelte-adapter-uws >= 0.5.0-next.4` -- the duration / messages /
 * bytes fields are only allocated when a `close` hook is registered.
 */
export function connectionMetricsHook(
	metrics: MetricsRegistry,
	userClose?: (ws: any, ctx: any) => void | Promise<void>
): (ws: any, ctx: any) => Promise<void>;
