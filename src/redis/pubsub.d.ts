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

	/**
	 * Reject inbound bus envelopes larger than this many bytes before
	 * `JSON.parse` runs. Defends against a hostile co-tenant or
	 * compromised peer flooding the bus with oversized payloads.
	 * @default 1048576 (1 MB)
	 */
	maxEnvelopeBytes?: number;

	/**
	 * When `false` (default), inbound envelopes addressed to `__`-prefixed
	 * topics are dropped; the configured `systemChannel` remains in an
	 * explicit allowlist so the bus's own degraded / recovered events
	 * still flow. Closes the bus-injection class in shared-Redis
	 * deployments where a foreign publisher could otherwise inject
	 * forged `__signal:*` / `__rpc` / plugin-internal frames. Apps that
	 * legitimately bus-relay user-defined `__`-prefixed topics (rare)
	 * can opt back in via `allowSystemTopics: true`.
	 * @default false
	 */
	allowSystemTopics?: boolean;
}

export interface PubSubBus {
	/**
	 * Returns a new Platform whose `publish()` / `batch()` / `publishBatched()`
	 * send to Redis + local. Other Platform methods (`send`, `sendCoalesced`,
	 * `request`, `pressure`, etc.) pass through unchanged.
	 *
	 * `publishBatched` ships one Redis envelope per call regardless of batch
	 * size; receivers fan out via local `platform.publishBatched` so each
	 * subscriber sees one wire frame per batch (post-coalesce).
	 *
	 * Use this wrapped platform everywhere you call `publish()` or
	 * `publishBatched()`.
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

	/**
	 * Ready-made WebSocket hooks. Destructure `const { open } = bus.hooks`
	 * into your `hooks.ws.js` for a one-line wiring that:
	 *   1. activates the Redis subscriber (idempotent; only the first call
	 *      opens the subscriber), and
	 *   2. subscribes the connection to the bus's `systemChannel` via the
	 *      platform-trust path, so `degraded` / `recovered` events reach
	 *      it. Skipped when `systemChannel` is `null` or `false`.
	 *
	 * Subscribing through `platform.subscribe` (server-trust) intentionally
	 * bypasses the wire-level `__`-prefix gate that the adapter enforces
	 * on client-sent subscribe frames.
	 */
	hooks: {
		open(ws: any, ctx: { platform: Platform }): Promise<void>;
	};
}

/**
 * Create a Redis-backed pub/sub bus for cross-instance message distribution.
 */
export function createPubSubBus(client: RedisClient, options?: PubSubBusOptions): PubSubBus;
