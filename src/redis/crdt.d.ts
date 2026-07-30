import type { RedisClient } from './index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface CrdtClusterOptions {
	/**
	 * Per-topic persist-lease TTL (ms). The lease holder is the sole snapshot
	 * writer for a topic; set it above the document's `debounceWait` so the
	 * holder keeps renewing across scheduled stores and the lease only rotates
	 * when the holder goes quiet or dies. Default 6000 (suits the 2s document
	 * debounce default).
	 */
	persistLeaseMs?: number;
	/** Reject inbound relay envelopes larger than this BEFORE JSON.parse. Default 1048576. */
	maxEnvelopeBytes?: number;
	/** Optional circuit breaker; when open, outbound relays are skipped. */
	breaker?: CircuitBreaker;
	/** Observe a relay-subscriber failure (the subscriber tears down so a later relay re-subscribes). */
	onError?: (err: unknown) => void;
}

/** Inbound relay handlers the realtime layer registers via `onMessage`. */
export interface CrdtClusterHandlers {
	/** A peer applied an update: apply it to the local replica and fan it out. */
	onUpdate?: (declKey: string, topic: string, bytes: number[]) => void;
	/** A peer cold-joined: if this instance holds the topic, reply with the updates it lacks. */
	onSyncRequest?: (declKey: string, topic: string, stateVector: number[], fromInstance: string) => void;
	/** A peer answered this instance's cold-join request: apply the catch-up diff and fan it out. */
	onSyncReply?: (declKey: string, topic: string, bytes: number[]) => void;
}

/**
 * The cluster coordinator for conflict-free documents. Pure transport: it
 * relays applied updates so every instance's replica converges, answers
 * cold-join sync requests, and gates snapshot persistence to one writer per
 * topic. The replica itself lives in the adapter and is owned by the realtime
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
	 * false when another instance holds it (skip). Rejects on a Redis error so
	 * the caller's persistence schedule retries.
	 */
	acquirePersist(topic: string): Promise<boolean>;
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
