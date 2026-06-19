import type { RedisClient } from './index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface SmoothClusterOptions {
	/**
	 * Per-topic ownership-lease TTL (ms). The lease holder is the sole tick
	 * authority for a topic; the realtime layer renews it (`renewOwner`) on a
	 * cadence well inside this TTL while it holds live entities, so the lease
	 * only rotates when the holder goes quiet or dies. Default 10000 (leaves
	 * room for a renew at ~TTL/3 to survive a GC pause plus a Redis blip).
	 */
	leaseMs?: number;
	/** Reject inbound relay envelopes larger than this BEFORE JSON.parse. Default 1048576. */
	maxEnvelopeBytes?: number;
	/** Optional circuit breaker; when open, outbound relays are skipped. */
	breaker?: CircuitBreaker;
	/** Observe a relay-subscriber failure (the subscriber tears down so a later relay re-subscribes). */
	onError?: (err: unknown) => void;
}

/** Inbound relay handlers the realtime layer registers via `onMessage`. */
export interface SmoothClusterHandlers {
	/** Owner: enqueue a forwarded command batch (one envelope, intra-batch order preserved). */
	onCommand?: (wireTopic: string, identity: string, originInstance: string, batch: unknown[]) => void;
	/** Owner: ensure the entity and answer the cold-join sync via `sendSyncReply`. */
	onSync?: (wireTopic: string, identity: string, originInstance: string, corr: string) => void;
	/** Requester: resolve the pending sync awaiting this `corr` with the owner's catalog. */
	onSyncReply?: (wireTopic: string, corr: string, payload: unknown) => void;
	/** Every instance: re-emit the owner's broadcast to local subscribers (deduped by `seq` per `ownerInstance`, author excluded). */
	onBroadcast?: (wireTopic: string, event: string, data: unknown, excludeIdentity: string | undefined, seq: number, ownerInstance: string) => void;
	/** The commanding client's instance: deliver the ack to its local socket. */
	onAck?: (wireTopic: string, identity: string, payload: unknown) => void;
	/** Owner: drop the departed client's surrogate and broadcast the entity removal. */
	onLeave?: (wireTopic: string, identity: string, originInstance: string) => void;
}

/**
 * The cluster coordinator for server-authoritative smooth entities. Pure
 * transport: it forwards a topic's commands to the single instance that owns
 * the topic's tick, relays that owner's broadcasts/acks/events back to every
 * instance's local clients, and gates tick ownership to one instance per topic
 * via a Redis lease. The authority itself lives in the adapter and is owned by
 * the realtime layer.
 */
export interface SmoothCluster {
	/** This instance's relay identity (stable for the coordinator's life). */
	readonly instanceId: string;
	/** Register the inbound handlers and start the relay subscriber (once). */
	onMessage(handlers: SmoothClusterHandlers): void;
	/** Forward a client's command batch to the topic's owner (fire-and-forget, one envelope). */
	relayCommand(wireTopic: string, identity: string, originInstance: string, batch: unknown[]): void;
	/** Ask the topic's owner for the entity catalog (correlation request answered via `sendSyncReply`). */
	requestSync(wireTopic: string, identity: string, originInstance: string, corr: string): void;
	/** Answer a sync request with the catalog, targeted at the requester. */
	sendSyncReply(wireTopic: string, corr: string, toInstance: string, payload: unknown): void;
	/** Relay one of the owner's broadcasts (update / event / remove) to every other instance. */
	relayBroadcast(wireTopic: string, event: string, data: unknown, excludeIdentity: string | undefined, seq: number): void;
	/** Relay an ack to the single instance the commanding client is on. */
	relayAck(wireTopic: string, identity: string, toInstance: string, payload: unknown): void;
	/** Tell the owner a client left so it drops the surrogate and broadcasts the removal. */
	relayLeave(wireTopic: string, identity: string, originInstance: string): void;
	/**
	 * Become (or stay) the topic's tick owner. Resolves true when this instance
	 * holds the lease and may tick; false when another instance holds it. Claims
	 * a free or expired lease, or renews when already ours. Rejects on a Redis
	 * error so the caller can decide how to degrade.
	 */
	acquireOwner(wireTopic: string): Promise<boolean>;
	/**
	 * Renew this instance's ownership lease WITHOUT acquiring a free one.
	 * Resolves true when the lease was ours and refreshed; false when we no
	 * longer own it. Rejects on a Redis error.
	 */
	renewOwner(wireTopic: string): Promise<boolean>;
	/**
	 * Release this instance's ownership lease (compare-and-delete) so a sibling
	 * takes over within a renew cycle. Best-effort: never throws; resolves true
	 * when released, false when not ours or on error.
	 */
	releaseOwner(wireTopic: string): Promise<boolean>;
	/**
	 * Diagnostic read of the topic's current owner instance id. Best-effort:
	 * never throws; resolves null when unowned or on error.
	 */
	currentOwner(wireTopic: string): Promise<string | null>;
	/** Tear down the relay subscriber. Idempotent. */
	destroy(): void;
}

/**
 * Create the cluster coordinator for `live.smooth`. Attach it as
 * `platform.smooth = createSmoothCluster(client)` (the same wiring as the other
 * Redis plugins); `bus.wrap` forwards it and the realtime layer detects
 * `platform.smooth` to route ownership / command-forward / broadcast-relay
 * through it. Without it, `live.smooth` runs single-instance (correct only when
 * every client of a topic lands on one instance).
 */
export function createSmoothCluster(client: RedisClient, options?: SmoothClusterOptions): SmoothCluster;
