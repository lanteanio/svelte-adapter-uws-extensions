import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface PubSubBusOptions {
	/** Redis channel name for pub/sub messages. @default 'uws:pubsub' */
	channel?: string;
	/**
	 * Topic used for auto-emitted `degraded` / `recovered` events on the
	 * local platform when the breaker leaves or returns to the healthy
	 * state. Set to `null` or `false` to disable auto-emission. Requires
	 * a `breaker` to do anything.
	 *
	 * @default '__realtime'
	 */
	systemChannel?: string | null | false;
	/**
	 * Called once when the breaker leaves the healthy state. Useful when
	 * you want to react server-side (log, alert) without parsing the
	 * auto-emitted event. Requires a `breaker`.
	 */
	onDegraded?: () => void;
	/**
	 * Called once when the breaker returns to the healthy state. Requires
	 * a `breaker`.
	 */
	onRecovered?: () => void;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface PubSubBus {
	/**
	 * Returns a new Platform whose publish() and batch() send to Redis + local.
	 * Use this wrapped platform everywhere you call publish().
	 */
	wrap(platform: Platform): Platform;

	/**
	 * Start the Redis subscriber. Incoming messages from other instances
	 * are forwarded to the local platform.publish(). Call once at startup.
	 * Idempotent.
	 */
	activate(platform: Platform): Promise<void>;

	/** Stop the Redis subscriber and clean up. */
	deactivate(): Promise<void>;
}

/**
 * Create a Redis-backed pub/sub bus for cross-instance message distribution.
 */
export function createPubSubBus(client: RedisClient, options?: PubSubBusOptions): PubSubBus;
