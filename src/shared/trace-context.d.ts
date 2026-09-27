import type { TraceContext } from 'svelte-adapter-uws/observability';

export type { TraceContext };

/** Span attributes shared by every Redis producer and consumer operation. */
export const REDIS_TRACE_ATTRIBUTES: Readonly<{ 'messaging.system': 'redis' }>;
/** Span attributes shared by every Postgres producer and consumer operation. */
export const POSTGRES_TRACE_ATTRIBUTES: Readonly<{ 'messaging.system': 'postgresql' }>;

export interface TraceSpanLike {
	readonly traceContext?: TraceContext | (() => TraceContext | null);
	readonly context?: TraceContext | (() => TraceContext | null);
	spanContext?(): { traceId: string; spanId: string; traceFlags?: number };
	setAttribute?(name: string, value: string | number | boolean): void;
	recordException?(error: unknown): void;
	end?(): void;
}

export interface TraceOperationOptions {
	readonly kind?: 'server' | 'consumer' | 'producer' | 'internal';
	readonly parent?: TraceContext | null;
	readonly attributes?: Readonly<Record<string, string | number | boolean>>;
}

/**
 * The part of the Platform tracing surface extensions reads. Every member is
 * optional: a Platform without a tracing provider is detected at runtime and
 * does no trace work.
 */
export interface TracePlatform {
	readonly requestId?: string | null;
	readonly traceContext?: TraceContext | null;
	readonly trace?: {
		readonly enabled?: boolean;
		current?(): TraceContext | null;
		run?<T>(
			name: string,
			options: TraceOperationOptions,
			fn: (span: TraceSpanLike | null) => T
		): T;
		withContext?<T>(context: TraceContext | null, fn: () => T): T;
	};
}

export function normalizeTraceContext(value: unknown): TraceContext | null;
export function injectTraceContext<T extends Headers | Record<string, unknown>>(
	carrier: T,
	context?: unknown
): T;
/** The Platform's live context, validated. */
export function liveTraceContext(platform: unknown): TraceContext | null;
/**
 * `options.traceContext` as passed to a job or task producer: `undefined`
 * when absent, `null` when present but null or invalid (opts the call out of
 * tracing), otherwise the validated context.
 */
export function explicitTraceContext(options: unknown): TraceContext | null | undefined;
/** The context carried by one of this package's own envelopes or rows (exact lowercase fields). */
export function carriedTraceContext(carrier: unknown): TraceContext | null;
/** True when the Platform has a tracing provider configured. */
export function traceEnabled(platform: unknown): boolean;
export function runTraceOperation<T>(
	platform: unknown,
	name: string,
	kind: 'producer' | 'consumer',
	parent: TraceContext | null,
	attributes: Readonly<Record<string, string | number | boolean>>,
	fn: (context: TraceContext | null) => T
): T;
export function withTraceContext<T>(
	platform: unknown,
	context: TraceContext | null,
	fn: () => T
): T;
