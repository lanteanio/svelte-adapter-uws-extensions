/**
 * Bounded-by-default capacity caps. See `shared/caps.js` for the full
 * rationale on per-conn (adapter) vs per-instance (extensions) vs cluster-
 * wide (warn-only) tier values.
 */

export const MAX_REGISTRY_SESSIONS_PER_INSTANCE: number;
export const MAX_REGISTRY_PENDING_REQUESTS: number;
export const MAX_REGISTRY_USER_INDEX: number;
export const MAX_REGISTRY_INDEX_VALUES_PER_KEY: number;
export const MAX_SHARDED_BUS_TOPICS: number;
export const MAX_SHARDED_BUS_BATCH_CHANNELS_PER_TICK: number;
export const MAX_PUBSUB_RELAY_BATCH_PER_TICK: number;
export const MAX_PRESENCE_WS: number;
export const MAX_PRESENCE_TOPICS: number;
export const MAX_PRESENCE_TOPIC_KEY_INDEX: number;
export const MAX_PRESENCE_SYNC_OBSERVERS: number;
export const MAX_PRESENCE_INFLIGHT_HGETALL: number;
export const MAX_CURSOR_WS: number;
export const MAX_CURSOR_TOPICS: number;
export const MAX_CURSOR_DIRTY_PER_TOPIC: number;
export const MAX_GROUPS_LOCAL_MEMBERS: number;
export const MAX_WORKER_CONTROLLERS: number;
export const MAX_REDIS_DUPLICATES_PER_CLIENT: number;
export const MAX_AGGREGATOR_REMOTE_INSTANCES: number;
export const MAX_TASK_HANDLERS: number;
export const MAX_BREAKER_LISTENERS: number;
