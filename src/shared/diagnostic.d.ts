import type {
	DataClass,
	DiagnosticInput,
	DiagnosticRecord,
	OperationalEventSink,
	TelemetryLevel
} from 'svelte-adapter-uws/observability';

export type { DataClass, DiagnosticInput, DiagnosticRecord, OperationalEventSink, TelemetryLevel };

/** The `source` every record emitted by this package carries. */
export declare const PACKAGE_SOURCE: 'svelte-adapter-uws-extensions';
export declare const DIAGNOSTIC_SCHEMA_VERSION: 1;
/** Identical to the adapter's prefix, so one parser reads both packages' lines. */
export declare const DIAGNOSTIC_PREFIX: 'lantean/diagnostic';
export declare const TELEMETRY_LEVELS: readonly TelemetryLevel[];
export declare const DATA_CLASSES: readonly DataClass[];

export declare function normalizeDiagnosticMessage(value: unknown, fallback?: string): string;
export declare function isDiagnosticDotName(value: string): boolean;
export declare function isDiagnosticSeverity(value: unknown): value is TelemetryLevel;
export declare function createDiagnostic<T = unknown>(input: DiagnosticInput<T>): DiagnosticRecord<T>;
export declare function formatDiagnostic<T = unknown>(input: DiagnosticInput<T>): string;

/** Bounded projection of a caught value: name, code, and message only. */
export declare function diagnosticError(error: unknown): { name: string; code: string | null; message: string };

/**
 * Install the process-wide operational event sink. The registry is the same
 * one `svelte-adapter-uws/observability` uses, so a sink installed through
 * either package receives records from the adapter, this package, and
 * svelte-realtime. Pass null to restore console output. The returned disposer
 * removes only this registration.
 */
export declare function setOperationalEventSink(sink: OperationalEventSink | null): () => void;
export declare function emitOperationalEvent<T = unknown>(input: DiagnosticInput<T>): DiagnosticRecord<T> | null;

/**
 * Emit a failure indexed in docs/errors.md. The registry entry supplies the
 * component, event, severity, and message; a caught error is projected to
 * name, code, and message (connection strings redacted) under
 * `attributes.error`, with the arguments a backend error repeats cut. Every
 * occurrence of a string listed in `redact` is replaced with `<redacted>` in
 * that message; `redact: true` replaces the whole message.
 */
export declare function emitRegisteredFailure(id: string, options?: {
	error?: unknown;
	redact?: Iterable<unknown> | string | true;
	detail?: string;
	attributes?: Readonly<Record<string, unknown>>;
	dataClass?: DataClass;
}): DiagnosticRecord | null;
