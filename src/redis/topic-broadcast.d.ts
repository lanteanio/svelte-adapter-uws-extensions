import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

/** One per-subscriber outcome carried back from an instance's local fan-out. */
export type TopicReplyOutcome<TReply = unknown> =
	| { ok: true; reply: TReply }
	| { ok: false; error: string };

export interface TopicBroadcastOptions {
	/**
	 * Prefix prepended to the coordinator's channel and presence key (stacks
	 * with the client's own `keyPrefix`). Lets two logical coordinators on
	 * one ioredis client stay separate.
	 * @default ''
	 */
	keyPrefix?: string;

	/**
	 * Default whole-fan-out budget for `broadcast(...)` when the call omits
	 * `timeoutMs`. The hard ceiling: the call resolves no later than this
	 * even if a recorded instance never answers.
	 * @default 5000
	 */
	requestTimeoutMs?: number;

	/**
	 * Presence refresh interval in ms. Each tick re-stamps this instance's
	 * score in the presence set and evicts instances stale beyond the TTL.
	 * @default 10000
	 */
	heartbeat?: number;

	/**
	 * How long (ms) an instance counts as live after its last heartbeat.
	 * Default `heartbeat * 3` so a GC pause inside one missed beat does not
	 * drop a live instance from the early-completion set.
	 */
	presenceTtlMs?: number;

	/**
	 * Bound inbound envelopes before JSON.parse and outbound encoded bytes.
	 * Oversized requests reject with RangeError before serving any local
	 * subscribers. Oversized replies are withheld and reported, so the
	 * requester may wait until its timeout.
	 * @default 1048576
	 */
	maxEnvelopeBytes?: number;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;

	/** Observe a relay-subscriber failure (it tears down so a later broadcast re-subscribes). */
	onError?(err: unknown): void;
}

export interface TopicBroadcast {
	/** Stable id for this coordinator (this instance's relay identity). */
	readonly instanceId: string;

	/**
	 * Register the local-serve handler and start the relay subscriber +
	 * presence heartbeat. The handler runs `platform.requestTopic` over this
	 * instance's own subscribers and returns their per-subscriber outcomes.
	 */
	onRequest(
		fn: (
			topic: string,
			event: string,
			data: unknown,
			opts: { timeoutMs: number }
		) => Promise<Array<TopicReplyOutcome>>
	): void;

	/**
	 * Broadcast a request to every subscriber of `topic` across the cluster
	 * and resolve with the flat list of per-subscriber outcomes - this
	 * instance's own subscribers first, then every other live instance's.
	 * Resolves as soon as every live instance has answered, or at `timeoutMs`,
	 * whichever comes first; a non-answering subscriber/instance simply does
	 * not contribute (partial-success).
	 * Rejects with RangeError for an oversized request, or Error for an invalid
	 * topic or event, before serving local subscribers or publishing to Redis.
	 */
	broadcast<TReply = unknown>(
		topic: string,
		event: string,
		data?: unknown,
		opts?: { timeoutMs?: number }
	): Promise<Array<TopicReplyOutcome<TReply>>>;

	/** Tear down the subscriber + heartbeat and drop this instance's presence. Idempotent. */
	destroy(): Promise<void>;
}

/**
 * Create the topic-broadcast cluster coordinator. Attach it to the platform
 * (`platform.topicBroadcast = createTopicBroadcast(client)`); `bus.wrap`
 * forwards it and the realtime `live.push({ topic })` layer detects it to fan a
 * broadcast-with-reply across the cluster.
 */
export function createTopicBroadcast(
	client: RedisClient,
	options?: TopicBroadcastOptions
): TopicBroadcast;
