export class CircuitBrokenError extends Error {
	readonly name: 'CircuitBrokenError';
}

export interface CircuitBreakerOptions {
	/** Consecutive failures before breaking. @default 5 */
	failureThreshold?: number;
	/** Ms before transitioning from broken to probing. @default 30000 */
	resetTimeout?: number;
	/** Called on state transitions. */
	onStateChange?: (from: string, to: string) => void;
}

export interface CircuitBreaker {
	/** Current state. */
	readonly state: 'healthy' | 'broken' | 'probing';
	/** True only when state is healthy. */
	readonly isHealthy: boolean;
	/** Current consecutive failure count. */
	readonly failures: number;
	/** Throws CircuitBrokenError if the circuit is broken. */
	guard(): void;
	/** Record a successful operation. May transition to healthy. */
	success(): void;
	/** Record a failed operation. May transition to broken. */
	failure(err?: any): void;
	/** Force back to healthy state. */
	reset(): void;
	/** Clear internal timers. */
	destroy(): void;
}

/**
 * Run an async operation through a breaker. Guards before, records
 * success/failure after. Pass null/undefined breaker to skip.
 */
export function withBreaker<T>(breaker: CircuitBreaker | null | undefined, fn: () => Promise<T>): Promise<T>;

/**
 * Create a circuit breaker.
 */
export function createCircuitBreaker(options?: CircuitBreakerOptions): CircuitBreaker;
