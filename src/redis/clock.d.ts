/**
 * Cluster clock: multi-source fused time with a non-decreasing clamp, a
 * cluster-consistent reading anchored on the shared Redis server clock, and an
 * optional leader-stamped timestamp authority for clustered event ordering.
 *
 * - `now()` - robust local wall estimate: the median of the available sources
 *   {local wall, Redis-corrected, NTP-corrected}, never regressing.
 * - `consistent()` - the local wall clock corrected onto the shared Redis
 *   server clock; every instance converges to the same reference within
 *   round-trip error.
 * - `stamp()` - leader-authoritative event timestamp when a leader handle is
 *   configured (the leader publishes its clock offset against the shared
 *   Redis reference; followers apply it to their own corrected reading,
 *   cached with a TTL); otherwise `consistent()`.
 *
 * The first sample runs at construction by default - the automatic startup
 * drift check (warn at 100 ms, trip at 500 ms). Wire `tripped()` into
 * admission control via the `{clockTripped}` rule.
 */
import type { RedisClient } from './index.js';
import type { CircuitBreaker } from '../shared/breaker.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { NtpSource } from './ntp-source.js';

export interface ClusterClockOptions {
	/** Background sampling cadence in milliseconds. @default 30000 */
	intervalMs?: number;
	/** Redis TIME reads per round (median-reduced to reject jitter). @default 5 */
	samples?: number;
	/** Absolute Redis skew (ms) at or above which onWarn fires (below tripMs). @default 100 */
	warnMs?: number;
	/** Absolute Redis skew (ms) at or above which onTrip fires and tripped() reads true. @default 500 */
	tripMs?: number;
	/** Max pairwise source disagreement (ms) at or above which onDrift fires. @default 100 */
	driftThresholdMs?: number;
	/** Take the first sample at construction (the startup drift check). @default true */
	immediate?: boolean;
	/**
	 * Optional third clock source (one read per sampling round). Off by
	 * default; a failed read drops the source from the median until it reads
	 * again.
	 */
	ntp?: NtpSource;
	/**
	 * Leader election handle (`createLeader(...)` or any `{ isLeader() }`)
	 * enabling leader-stamped `stamp()`: only the leader assigns event
	 * timestamps from its own clock; followers reproduce it through the
	 * shared Redis reference.
	 */
	leader?: { isLeader: () => boolean };
	/**
	 * Key the leader publishes its clock offset under. Resolved RELATIVE to
	 * the client key prefix; pass the bare name.
	 * @default 'clock:leader-offset'
	 */
	leaderKey?: string;
	/**
	 * Also read the UNPREFIXED `leaderKey` when the prefixed one is absent,
	 * for a rolling upgrade from a release that wrote it raw. Off by default:
	 * the unprefixed key is the shared name the prefixing exists to escape,
	 * so a follower reading it would adopt whatever other app on the same
	 * Redis publishes there. Left off, a follower ahead of its leader
	 * degrades to `consistent()` for one key lifetime.
	 * @default false
	 */
	legacyLeaderKeyFallback?: boolean;
	/**
	 * Freshness bound (ms) for both the published offset (PX) and a
	 * follower's cached copy; a staler cache falls back to `consistent()`.
	 * @default intervalMs * 3
	 */
	leaderTtlMs?: number;
	/** Called after every successful round with the signed median Redis skew (positive = local ahead). */
	onSample?: (skewMs: number) => void;
	onWarn?: (skewMs: number) => void;
	onTrip?: (skewMs: number) => void;
	/** Called when two sources disagree by driftThresholdMs or more. */
	onDrift?: (drift: { driftMs: number; sources: { redis: number; ntp: number | null } }) => void;
	/** Redis/NTP/leader read-write failures. The last good state is retained. */
	onError?: (err: Error) => void;
	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface ClusterClock {
	/** Fused multi-source wall clock, non-decreasing. Before the first sample: the seam wall clock. */
	now(): number;
	/** Cluster-consistent reading (Redis-anchored), non-decreasing. Before the first sample: falls back to now(). */
	consistent(): number;
	/** Leader-stamped event timestamp (leader mode), else consistent(). Non-decreasing. */
	stamp(): number;
	/** True once the first successful sample landed. */
	ready(): boolean;
	/** Most recent signed Redis skew (ms), or null before the first sample. */
	skew(): number | null;
	/** True while the last sampled |skew| >= tripMs. For the admission {clockTripped} rule. */
	tripped(): boolean;
	/** Most recent max pairwise source disagreement (ms), or null before the first sample. */
	drift(): number | null;
	/** Run one round now; resolves with the signed Redis skew (null on failure). */
	sample(): Promise<number | null>;
	/** Stop the interval and await any in-flight round. Idempotent. */
	stop(): Promise<void>;
}

/** Create a cluster clock. Starts sampling immediately; call stop() on shutdown. */
export function createClusterClock(client: RedisClient, options?: ClusterClockOptions): ClusterClock;

/**
 * Attach a cluster clock to the platform as the `clusterClock` convention
 * (`platform.clusterClock = { now, consistent, stamp, ready, tripped }`).
 * `bus.wrap` forwards it, mirroring `clockFence`.
 */
export function attachClusterClock<T>(platform: T, clock: ClusterClock): T;
