/**
 * Bounded-by-default capacity caps for svelte-adapter-uws-extensions.
 *
 * Single source of truth for every Map / Set / Array of factory-or-module-
 * level scope. Mirrors the adapter's `files/utils.js` capacity-cap pattern,
 * scaled one tier up: where the adapter caps per-connection at 1M, we cap
 * per-instance at 10M (an instance can hold ~1M concurrent uWS connections
 * with ~10 entries of state per connection on average). Cluster-wide warn-
 * only caps land at 100M, beyond which the in-memory index itself becomes
 * a real memory concern (~3.2GB on each instance maintaining the index).
 *
 * Smaller structural caps (Redis duplicates, task handlers, breaker
 * listeners, aggregator instances) stay in their natural ballpark because
 * they are configuration / wiring limits, not state-driven growth.
 *
 * @module svelte-adapter-uws-extensions/shared/caps
 */

// - Per-instance state caps -------------------------------------------------
// Maps / Sets that grow with local connections, local subscriptions, or
// local in-flight operations. One instance's worth of state. Reject-new on
// saturation (or evict-with-flush for throttle/debounce-shaped buffers).

/** Sessions tracked by the connection registry on this instance. */
export const MAX_REGISTRY_SESSIONS_PER_INSTANCE = 10_000_000;

/** In-flight `registry.request(...)` calls awaiting a reply. */
export const MAX_REGISTRY_PENDING_REQUESTS = 10_000_000;

/** Topics this instance follows on the sharded bus. */
export const MAX_SHARDED_BUS_TOPICS = 10_000_000;

/** Local ws entries tracked by `createPresence`. */
export const MAX_PRESENCE_WS = 10_000_000;

/** Local topics tracked by `createPresence`. */
export const MAX_PRESENCE_TOPICS = 10_000_000;

/** Local ws entries tracked by `createCursor`. */
export const MAX_CURSOR_WS = 10_000_000;

/** Local topics tracked by `createCursor`. */
export const MAX_CURSOR_TOPICS = 10_000_000;

/** Local member entries tracked by `createGroups`. */
export const MAX_GROUPS_LOCAL_MEMBERS = 10_000_000;

/** Worker controllers spawned by the task runner harness. */
export const MAX_WORKER_CONTROLLERS = 10_000_000;

// - Cluster-wide warn-only caps ---------------------------------------------
// Maps that mirror cluster-wide cardinality (every user across every
// instance, etc). Eviction would corrupt routing, so saturation is warn-only:
// a single structured `console.warn` fires the first time the cap is crossed
// so ops can identify the leak shape before OOM.

/** Cluster-wide userId -> instanceId index for `registry.sendTo(...)`. */
export const MAX_REGISTRY_USER_INDEX = 100_000_000;

/** Per-attribute-key bucket size in the registry secondary index. */
export const MAX_REGISTRY_INDEX_VALUES_PER_KEY = 10_000_000;

/** Local presence ws*topic*key tuples. */
export const MAX_PRESENCE_TOPIC_KEY_INDEX = 100_000_000;

/** Sync-only presence subscribers. */
export const MAX_PRESENCE_SYNC_OBSERVERS = 1_000_000;

/** In-flight HGETALL coalesce on the presence module. */
export const MAX_PRESENCE_INFLIGHT_HGETALL = 1_000_000;

/** Per-topic dirty cursor entries before flush-and-evict. */
export const MAX_CURSOR_DIRTY_PER_TOPIC = 1_000_000;

// - Per-tick microtask batch caps -------------------------------------------
// Outbound batches that are drained every microtask. Crossing the cap
// signals something allocated the entire cap's worth in one tick (leak).

/** Channels in one sharded-bus microtask batch. */
export const MAX_SHARDED_BUS_BATCH_CHANNELS_PER_TICK = 1_000_000;

/** Entries in one pubsub relay microtask batch. */
export const MAX_PUBSUB_RELAY_BATCH_PER_TICK = 1_000_000;

// - Structural caps ---------------------------------------------------------
// Configuration / wiring limits, not state-driven. Smaller numbers because
// the legitimate scale is genuinely smaller (a Node process should never
// have 1K distinct ioredis duplicate connections; a deployment should never
// have 10K named task handlers).

/** ioredis `client.duplicate()` connections owned by one client wrapper. */
export const MAX_REDIS_DUPLICATES_PER_CLIENT = 1_000;

/** Sibling instances broadcasting on the publish-rate aggregator channel. */
export const MAX_AGGREGATOR_REMOTE_INSTANCES = 10_000;

/** `register(name, handler)` entries on the task runner. */
export const MAX_TASK_HANDLERS = 10_000;

/** `breaker.subscribe(handler)` listeners on a single breaker. */
export const MAX_BREAKER_LISTENERS = 10_000;

/**
 * Defense-in-depth cap on the idempotency-store key after framework-level
 * namespacing. Matches the worst-case `rpc:<path>:<256-char-user-key>` /
 * `task:<name>:<256-char-user-key>` shape with headroom; framework
 * boundaries (realtime `live.idempotent`, extensions `tasks.run`) cap the
 * user-supplied portion at 256.
 */
export const MAX_IDEMPOTENCY_KEY_LENGTH = 1024;
