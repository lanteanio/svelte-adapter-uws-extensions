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
