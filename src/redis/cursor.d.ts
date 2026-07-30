import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisCursorOptions {
	/**
	 * Minimum ms between broadcasts per user per topic.
	 * Trailing-edge timer ensures the final position is always sent.
	 * Default 16 (60Hz) matches the world-state tick rate so an individual
	 * cursor's motion stays smooth at the per-peer wire rate.
	 * @default 16
	 */
	throttle?: number;

	/**
	 * World-state tick rate, in ms. Per-topic aggregate cap on broadcasts:
	 * each topic emits at most one frame per window, carrying the latest
	 * position for every cursor that moved. Bandwidth per peer scales with
	 * active-mover count, not with mover-count times per-mover rate.
	 * Default 16 (60Hz) suits typical small-to-medium rooms; raise to 33
	 * (30Hz) for high-density rooms where wire bytes matter.
	 * 0 disables the tick; per-cursor `throttle` then governs broadcast rate.
	 * @default 16
	 */
	topicThrottle?: number;

	/**
	 * Extract user-identifying data from userData.
	 * Broadcast alongside cursor data so other clients know who the cursor belongs to.
	 * The default recursively drops internal keys, credentials, common personal
	 * data, and request transport metadata. An explicit callback can intentionally
	 * admit fields; its result still passes through `stripInternal()`.
	 * @default privacy projection
	 */
	select?: (userData: any) => any;

	/**
	 * How often to flush coalesced cursor positions to Redis HSET, in ms.
	 * The wire/relay path runs on the per-flush cadence (`topicThrottle`) and
	 * does not wait for HSET; this timer only governs the Redis snapshot used
	 * for new-joiner reconcile and cross-instance startup reconcile. 100ms
	 * staleness on the reconcile path is fine for cursors.
	 * 0 disables coalescing and reverts to per-flush HSET (legacy behavior).
	 * @default 100
	 */
	snapshotIntervalMs?: number;

	/**
	 * TTL in seconds for Redis hash entries.
	 * Entries are refreshed on every snapshot tick. Stale cursors from crashed
	 * instances are cleaned up automatically after this period.
	 * @default 30
	 */
	ttl?: number;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;

	/**
	 * Reject inbound cursor envelopes larger than this many bytes before
	 * `JSON.parse` runs. The inner topic is always validated against the
	 * `__` denylist (the module constructs its own `__cursor:` wrapper
	 * prefix), so no `allowSystemTopics` knob is exposed here.
	 * @default 1048576 (1 MB)
	 */
	maxEnvelopeBytes?: number;

	/**
	 * Extract a `{x, y}` coordinate from cursor `data` for the `minMove`
	 * jitter filter. Defaults to reading finite `data.x` / `data.y`. Return
	 * `null` (or throw) to opt a single frame out of the filter - it is then
	 * always delivered, never dropped. Only consulted when `minMove > 0`.
	 * @default reads finite data.x / data.y
	 */
	position?: (data: any) => { x: number; y: number } | null;

	/**
	 * Minimum movement (Chebyshev distance in the units `position` returns)
	 * from the last broadcast position before a cursor move is fanned out and
	 * relayed across instances. A burst of sub-threshold wobble around a point
	 * is dropped at ingest; when movement stops, a debounced settle delivers
	 * the final resting position once (so a still cursor is never stranded at a
	 * stale point, and an exact repeat stays dropped). Measured against what
	 * the subscriber last saw, so a slow drift still delivers every `minMove`
	 * units. 0 (default) disables the filter. For integer-pixel cursor data,
	 * `minMove: 1` drops exact-repeat frames at no visual cost; raise to 2-4 to
	 * suppress sub-pixel wobble from high-DPI input. Mirrors the in-memory
	 * cursor plugin's `minMove`.
	 * @default 0
	 */
	minMove?: number;
}

export interface CursorEntry {
	/** Unique connection key. */
	key: string;
	/** Selected user data. */
	user: any;
	/** Latest cursor/position data. */
	data: any;
}

/**
 * Thrown by `attach()` when the websocket closes before `platform.subscribe`
 * can land. Same shape as `presence.WsClosedError`; catch on `err.code ===
 * 'WS_CLOSED'` for cross-feature handling.
 */
export class WsClosedError extends Error {
	name: 'WsClosedError';
	code: 'WS_CLOSED';
	operation: string;
	topic: string;
}

/**
 * Thrown by `attach()` when `platform.checkSubscribe` refuses the topic.
 * Authorization runs before anything is granted, so nothing is subscribed
 * or emitted when this throws. Catch on `err.code === 'SUBSCRIBE_DENIED'`
 * and surface `err.reason` to the caller.
 */
export class SubscribeDeniedError extends Error {
	name: 'SubscribeDeniedError';
	code: 'SUBSCRIBE_DENIED';
	operation: string;
	topic: string;
	reason: string;
}

export interface RedisCursorTracker {
	/**
	 * Opt this connection into receiving cursor updates for `topic`.
	 * Subscribes the connection to the internal `__cursor:` channel via
	 * the platform-trust path (which intentionally bypasses the wire-level
	 * `__`-prefix gate) and sends the current cursor state. Call from your
	 * "join room" RPC, mirroring `presence.join`.
	 *
	 * Without `attach`, the publishes in `update` fan out to an empty
	 * subscriber set and no client ever sees a cursor frame.
	 *
	 * Authorization runs FIRST: nothing is subscribed, granted or emitted
	 * when the platform refuses the topic. The membership `attach` grants is
	 * also what gates the inbound `cursor` / `cursor-viewport` frames in
	 * `hooks.message`, so a socket that never attached cannot write into the
	 * room.
	 *
	 * @throws {SubscribeDeniedError} (`err.code === 'SUBSCRIBE_DENIED'`, with
	 *   `err.reason` carrying the platform's denial reason) if
	 *   `platform.checkSubscribe` refuses `topic`.
	 * @throws {WsClosedError} (`err.code === 'WS_CLOSED'`) if the websocket
	 *   has already closed by the time the underlying `ws.subscribe` runs.
	 *   No state to roll back (the throw fires before `snapshot()` could
	 *   allocate the connection's identity); callers do not need to
	 *   compensate. The follow-up `snapshot()` call is skipped
	 *   when this throws. Snapshot-send failures on an already-subscribed
	 *   connection are NOT thrown - cursor frames are self-recovering via
	 *   the next bulk tick. Note: this module uses the uWS-native
	 *   `ws.subscribe` for `__cursor:*` topics (mirroring presence's
	 *   `__presence:*` path), not `platform.subscribe` - as of adapter 0.5.5
	 *   `platform.subscribe` swallows closed-ws throws and returns the same
	 *   sentinel on success and on close, which would silently break this
	 *   throw contract.
	 */
	attach(ws: any, topic: string, platform: Platform): Promise<void>;

	/**
	 * Stop this connection from receiving cursor updates for `topic`. Safe
	 * to call on a closed connection. uWS releases subscriptions on
	 * disconnect automatically, so `detach` is only needed when a
	 * still-connected user leaves a room.
	 */
	detach(ws: any, topic: string, platform: Platform): void;

	/**
	 * Broadcast a cursor position update. Throttled per user per topic.
	 * Call this from your `message` hook when you receive cursor data.
	 */
	update(ws: any, topic: string, data: any, platform: Platform): void;

	/**
	 * Remove a connection's cursor state from a specific topic, or all topics if omitted.
	 * Call this from your `close` hook.
	 */
	remove(ws: any, platform: Platform, topic?: string): Promise<void>;

	/**
	 * Right-to-erasure: remove a user's cursors from every topic across the
	 * cluster, matching the userId against `value.user` (a string equal to the
	 * userId, or an object whose `id`/`userId` equals it). Returns cursor
	 * entries removed.
	 */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;

	/**
	 * Send the current state for a topic to a single connection as ordered
	 * events on `__cursor:{topic}`: `time` (`{t}`, the server clock seed),
	 * `you` (`{key}`, the connection's own roster key - allocated here
	 * without announcing a join, so a pure viewer is never broadcast to
	 * others), then `catalog` (`[{key, user}, ...]`) followed by `bulk`
	 * (`[{key, data}, ...]`). An empty board still receives `time` + `you`.
	 * Folded into `attach` for typical use; exposed for advanced callers
	 * that want to resend a snapshot without re-subscribing.
	 */
	snapshot(ws: any, topic: string, platform: Platform): Promise<void>;

	/**
	 * Get current cursor positions for a topic across all instances.
	 */
	list(topic: string): Promise<CursorEntry[]>;

	/** Clear all cursor state (local and Redis). */
	clear(): Promise<void>;

	/** Stop the Redis subscriber and clear local timers. */
	destroy(): void;

	/**
	 * Snapshot of scheduler health. Always available, near-zero cost.
	 *
	 * - `flushes`: total tick-driven flushes since tracker creation.
	 * - `driftMeanMs`: mean (target_deadline - actual_fire_time) across
	 *   all tick-driven flushes. 0 means perfect cadence; values >
	 *   `topicThrottle` indicate sustained event-loop saturation or CPU
	 *   contention (consider a dedicated-CPU instance, or raise
	 *   `topicThrottle`).
	 * - `driftMaxMs`: largest single observed late fire. Useful for
	 *   spotting one-off GC pauses vs. sustained drift.
	 * - `dirtyTopicsCurrent`: topics with pending coalesced entries right
	 *   now. Should hover near zero in healthy operation.
	 * - `activeTopicsTotal`: topics with at least one local cursor.
	 * - `jitterDropped`: cursor moves dropped by the `minMove` jitter filter
	 *   before they reached the flush scheduler. Always 0 when `minMove` is 0
	 *   (the default).
	 *
	 * Leading-edge synchronous flushes are not counted in drift stats -
	 * they fire on the call thread, not via the scheduler.
	 */
	stats(): {
		flushes: number;
		driftMeanMs: number;
		driftMaxMs: number;
		dirtyTopicsCurrent: number;
		activeTopicsTotal: number;
		jitterDropped: number;
	};

	/**
	 * Ready-made WebSocket hooks for cursor tracking.
	 *
	 * `message` handles incoming `{ type: 'cursor', topic, data }` messages.
	 * `close` removes the connection's cursors from all topics.
	 *
	 * `subscribe` fires for an inbound `__cursor:*` subscribe frame only where
	 * the app opted into `allowSystemTopicSubscribe`; the adapter's wire-level
	 * `__`-prefix gate denies it otherwise. When it does fire it authorizes
	 * the topic exactly as `attach()` does, then grants membership and emits
	 * the snapshot. Calling `tracker.attach(ws, topic, platform)` from the
	 * app's "join room" RPC remains the path that does not depend on that
	 * opt-in.
	 *
	 * It resolves to the platform's denial reason when authorization fails,
	 * and to `undefined` otherwise. Wrapping it means RETURNING that value:
	 * the adapter reads anything that is not `false` or a string as ALLOW, so
	 * swallowing it withholds the snapshot while still subscribing the socket
	 * to the broadcast channel - the larger half of what the gate prevents.
	 *
	 * @example
	 * ```js
	 * import { cursor } from '$lib/server/cursor';
	 * export const { message, close } = cursor.hooks;
	 * ```
	 */
	hooks: {
		subscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<string | undefined>;
		message(ws: any, ctx: { data: any; platform: Platform }): void;
		close(ws: any, ctx: { platform: Platform }): Promise<void>;
	};
}

/**
 * Create a Redis-backed cursor tracker.
 */
export function createCursor(client: RedisClient, options?: RedisCursorOptions): RedisCursorTracker;
