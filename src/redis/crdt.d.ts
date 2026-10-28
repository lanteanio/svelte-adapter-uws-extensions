import type { RedisClient } from './index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface CrdtClusterOptions {
	/**
	 * Per-topic persist-lease TTL (ms). The lease holder is the sole snapshot
	 * writer for a topic; set it above the document's `debounceWait` so the
	 * holder keeps renewing across scheduled stores and the lease only rotates
	 * when the holder goes quiet or dies. Default 6000 (suits the 2s document
	 * debounce default). A newly acquired lease waits up to a third of it for
	 * a sync round with the peers before the write is allowed.
	 */
	persistLeaseMs?: number;
	/**
	 * Reject inbound relay messages larger than this BEFORE JSON.parse.
	 * Default 1048576. A larger envelope (a big update or full document
	 * state) is sent as parts that each fit, so every instance must use the
	 * same value.
	 */
	maxEnvelopeBytes?: number;
	/**
	 * Optional circuit breaker. While it is open no relay is published; every
	 * topic whose traffic it refused is resynced with the peers once it closes.
	 */
	breaker?: CircuitBreaker;
	/**
	 * Observe a relay-subscriber failure (the subscriber tears down, is opened
	 * again, and resyncs), and a relay envelope too large to send even in
	 * parts (code `RELAY_ENVELOPE_TOO_LARGE`, once per held document;
	 * `acquirePersist` then allows a write only while no other instance can
	 * hold edits it would erase, see there).
	 */
	onError?: (err: unknown) => void;
}

/** Inbound relay handlers the realtime layer registers via `onMessage`. */
export interface CrdtClusterHandlers {
	/** A peer applied an update: apply it to the local replica and fan it out. */
	onUpdate?: (declKey: string, topic: string, bytes: number[]) => void;
	/**
	 * A peer cold-joined or is resyncing: if this instance holds the topic,
	 * reply with the updates it lacks. The coordinator also calls it to read
	 * the local replica's full state (an empty state vector, a local
	 * `fromInstance`), so the reply must be sent synchronously.
	 */
	onSyncRequest?: (declKey: string, topic: string, stateVector: number[], fromInstance: string) => void;
	/** A peer answered this instance's sync request: apply the catch-up diff and fan it out. */
	onSyncReply?: (declKey: string, topic: string, bytes: number[]) => void;
}

export interface CrdtPersistLease {
	/** Whether this instance holds the lease and may persist the topic. */
	acquired: boolean;
	/** True for a new lease until its first successful result, never for a later renewal or denial. */
	fresh: boolean;
}

/**
 * The cluster coordinator for conflict-free documents. Pure transport: it
 * relays applied updates so every instance's replica converges, answers
 * cold-join sync requests, resyncs the replicas after the relay lost traffic,
 * and gates snapshot persistence to one writer per topic. The replica itself lives in the adapter and is owned by the realtime
 * layer.
 */
export interface CrdtCluster {
	/** This instance's relay identity (stable for the coordinator's life). */
	readonly instanceId: string;
	/** Register the inbound handlers and start the relay subscriber (once). */
	onMessage(handlers: CrdtClusterHandlers): void;
	/** Relay one applied update to peers so their replicas converge. */
	relayUpdate(declKey: string, topic: string, bytes: number[]): void;
	/** Broadcast a cold-join sync request carrying this replica's state vector. */
	requestSync(declKey: string, topic: string, stateVector: number[]): void;
	/** Answer a peer's sync request with the updates it lacks (targeted at one instance). */
	sendSyncReply(declKey: string, topic: string, bytes: number[], toInstance: string): void;
	/**
	 * Try to become (or stay) the sole snapshot writer for a topic. Resolves
	 * true when this instance holds the per-topic persist lease and may write;
	 * false when another instance holds it (skip). A newly acquired lease, and
	 * one renewed while a resync of the topic runs, first waits for a sync
	 * round with the peers (at most a third of `persistLeaseMs`). Rejects on a
	 * Redis error, and with code `CRDT_RELAY_PENDING` while local edits to the
	 * topic have not reached the peers (a relay that failed, or one Redis has
	 * not confirmed within that window) or while the replica has not synced
	 * with them after the relay lost traffic (a sync round the breaker refused,
	 * or whose request failed or was not confirmed within that window), so the
	 * caller's persistence schedule retries and keeps the replica loaded. A
	 * stored snapshot is replaced, not merged, so for a topic whose local
	 * state is too large to push even in parts the write is allowed only
	 * after a resync round in which every peer answered that it holds the
	 * same state or nothing; every other subscriber of the relay channel
	 * counts as a peer (on a Redis Cluster, `PUBSUB NUMSUB` summed over every
	 * node), and one that does not answer holds the write back, retryably.
	 * Once a peer holds a different state, or a sync request or reply
	 * for the topic cannot be sent, the sender and the requester it answered
	 * reject with the same code until they restart, its `cause` the
	 * `RELAY_ENVELOPE_TOO_LARGE` error, and keep the replica loaded.
	 */
	acquirePersist(topic: string): Promise<boolean>;
	/**
	 * The same lease and convergence barrier as `acquirePersist`, plus a
	 * `fresh` signal held until the new lease's first successful result, even
	 * when a renewal retries a rejected barrier. On a fresh result the document
	 * layer must merge the persisted snapshot before its first replacement
	 * write; later renewals and denied acquisitions return false.
	 */
	acquirePersistDetailed(topic: string): Promise<CrdtPersistLease>;
	/** Tear down the relay subscriber. Idempotent. */
	destroy(): void;
}

/**
 * Create the cluster coordinator for `live.doc` / `live.map` / `live.array`.
 * Attach it as `platform.crdt = createCrdtCluster(client)` (the same wiring as
 * the other Redis plugins); `bus.wrap` forwards it and the realtime layer
 * detects `platform.crdt` to route relay / cold-join / persist-gating through
 * it. Without it, `live.doc` runs single-instance.
 */
export function createCrdtCluster(client: RedisClient, options?: CrdtClusterOptions): CrdtCluster;
