export class CircuitBrokenError extends Error {
	readonly name: 'CircuitBrokenError';
}

export interface CircuitBreakerOptions {
	/** Consecutive failures before breaking. @default 5 */
	failureThreshold?: number;
	/** Ms before transitioning from broken to probing. @default 30000 */
	resetTimeout?: number;
	/**
	 * In-flight probes admitted per probing window; the first success closes
	 * the circuit, any failure re-opens it. @default 1
	 */
	probeConcurrency?: number;
	/** Called on state transitions. */
	onStateChange?: (from: string, to: string) => void;
}

export interface CircuitBreaker {
	/**
	 * State is partitioned by an optional string `key`, so one key (e.g. a tenant) can
	 * break without tripping the others. The default key `''` is the single global
	 * breaker - every method is byte-identical for callers that pass no key.
	 */
	/** State of the default (`''`) key. */
	readonly state: 'healthy' | 'broken' | 'probing';
	/** True only when the default key's state is healthy. */
	readonly isHealthy: boolean;
	/** Default key's consecutive failure count. */
	readonly failures: number;
	/** State for a given key (default `''`). */
	stateOf(key?: string): 'healthy' | 'broken' | 'probing';
	/** Failure count for a given key (default `''`). */
	failuresOf(key?: string): number;
	/** Throws CircuitBrokenError if the circuit for `key` (default `''`) is broken. */
	guard(key?: string): void;
	/** Record a successful operation for `key`. May transition to healthy. */
	success(key?: string): void;
	/** Record a failed operation for `key`. May transition to broken. */
	failure(err?: any, key?: string): void;
	/** Force `key` (default `''`) back to healthy state. */
	reset(key?: string): void;
	/**
	 * Register a state-transition listener. Multiple subscribers are
	 * supported and the constructor-time `onStateChange` callback (if
	 * any) is itself one of them. Returns an unsubscribe function.
	 * Listener errors are swallowed so one bad listener cannot break
	 * the others.
	 */
	subscribe(handler: (from: string, to: string) => void): () => void;
	/** Clear internal timers. */
	destroy(): void;
}

/**
 * Run an async operation through a breaker. Guards before, records
 * success/failure after. Pass null/undefined breaker to skip. The optional `key`
 * partitions the breaker state (default `''` = the global breaker).
 */
export function withBreaker<T>(breaker: CircuitBreaker | null | undefined, fn: () => Promise<T>, key?: string): Promise<T>;

/**
 * Create a circuit breaker.
 */
export function createCircuitBreaker(options?: CircuitBreakerOptions): CircuitBreaker;
