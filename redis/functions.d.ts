import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface FunctionLibraryOptions {
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface FunctionCallOptions {
	/** Keys passed to the function. */
	keys?: ReadonlyArray<string>;
	/** Args passed to the function. */
	args?: ReadonlyArray<string | number>;
}

export interface FunctionLibrary {
	/** Library name parsed from the `#!lua name=<libname>` shebang. */
	readonly name: string;

	/**
	 * Load (or replace) the library on the server via
	 * `FUNCTION LOAD REPLACE`. Runs `INFO server` on first call and
	 * throws on Redis < 7. Idempotent across calls.
	 */
	load(): Promise<void>;

	/**
	 * Invoke a registered function via `FCALL <funcName> <numkeys> <keys...> <args...>`.
	 * Note: FCALL keys on function name, not library name -- function
	 * names are global across the server.
	 */
	call(funcName: string, opts?: FunctionCallOptions): Promise<unknown>;

	/** Drop the library via `FUNCTION DELETE`. */
	delete(): Promise<void>;
}

/**
 * Create a Redis Functions library handle. `code` must start with
 * `#!lua name=<libname>`; the library name is parsed from the
 * shebang. Requires Redis 7+.
 */
export function createFunctionLibrary(
	client: RedisClient,
	code: string,
	options?: FunctionLibraryOptions
): FunctionLibrary;
