// Type declarations for the redis/postgres deterministic simulation backends
// (svelte-adapter-uws-extensions/sim).

/** Seeded fault spec applied to a store's cross-instance delivery channel. */
export interface SimFaults {
	/** Probability in [0,1] that a relayed message is dropped. */
	drop?: number;
	/** Probability in [0,1] that a relayed message is re-delivered. */
	duplicate?: number;
	/** Probability in [0,1] that a relayed message has one byte flipped. */
	corrupt?: number;
	/** Fixed delay (ms) or a [min,max] range sampled per message. */
	delayMs?: number | [number, number];
	/** Probability in [0,1] that a message gets independent jitter (reorder). */
	reorder?: number;
	/** Cap on the sampled jitter window (ms). */
	maxJitterMs?: number;
}

/** A connected sim client (from the adapter's in-memory app). */
export interface SimClientFacade {
	readonly state: 'connecting' | 'open' | 'rejected' | 'closed';
	frames(): Array<{ payload: string | Uint8Array; isBinary: boolean }>;
	texts(): string[];
	json(): any[];
	send(obj: unknown): boolean;
	subscribe(topic: string, ref?: number | string): boolean;
	unsubscribe(topic: string): boolean;
	close(code?: number, reason?: string): void;
}

/** One instance's handle within a scenario. */
export interface SimRedisInstanceApi {
	connect(opts?: { headers?: Record<string, string>; query?: string }): SimClientFacade;
	clients(): SimClientFacade[];
	publish(topic: string, event: string, data?: unknown, options?: unknown): boolean;
	publishBatched(messages: unknown[]): void;
	/** The bus-wrapped platform (publishing through it relays cross-instance). */
	platform: any;
	/** The raw createTestServer platform. */
	rawPlatform: any;
	/** The replay tracker constructed against the shared client (when plugins includes 'replay'). */
	replay?: any;
	/** The presence tracker constructed against the shared client (when plugins includes 'presence'). */
	presence?: any;
}

export interface SimRedisApi {
	now(): number;
	instances: number;
	instance(i: number): SimRedisInstanceApi;
	advance(rounds?: number): Promise<void>;
}

export interface SimRedisConfig {
	seed?: string;
	/** Number of server instances (default 2). */
	instances?: number;
	clients?: number;
	topics?: string[];
	steps?: number;
	startEpoch?: number;
	tz?: string;
	/** Redis pub/sub channel (default 'uws:sim'). */
	channel?: string;
	/** Faults applied to the per-instance client wire channel. */
	faults?: SimFaults;
	/** Faults applied to the cross-instance redis pub/sub relay. */
	relayFaults?: SimFaults;
	/** Stateful plugins to wire per instance against the shared client. */
	plugins?: Array<'replay' | 'presence'>;
	/** Per-plugin options keyed by plugin name. */
	pluginOptions?: Record<string, any>;
	/** The createTestServer handler (merged with plugin hooks; plugin hooks win). */
	handler?: Record<string, any>;
	scenario?: (api: SimRedisApi, opts: { instances: number; clients: number; topics: string[] }) => void | Promise<void>;
	gitCommit?: string;
}

export interface SimPgInstanceApi {
	connect(opts?: { headers?: Record<string, string>; query?: string }): SimClientFacade;
	clients(): SimClientFacade[];
	platform: any;
	server: any;
}

export interface SimPgApi {
	now(): number;
	instances: number;
	/** The ONE shared mock-pg client (drive advisory locks / SKIP LOCKED claims through it). */
	client: any;
	instance(i: number): SimPgInstanceApi;
	/** Emit a change across the cluster via pg_notify (every instance's bridge relays it). */
	notify(topic: string, event: string, data?: unknown): Promise<void>;
	advance(rounds?: number): Promise<void>;
	advanceTime(ms: number): Promise<void>;
}

export interface SimPgConfig {
	seed?: string;
	instances?: number;
	clients?: number;
	topics?: string[];
	steps?: number;
	startEpoch?: number;
	tz?: string;
	/** LISTEN/NOTIFY channel (default 'sim_changes'). */
	channel?: string;
	faults?: SimFaults;
	/** Faults applied to the cross-instance LISTEN/NOTIFY relay. */
	relayFaults?: SimFaults;
	handler?: Record<string, any>;
	scenario?: (api: SimPgApi, opts: { instances: number; clients: number; topics: string[] }) => void | Promise<void>;
	gitCommit?: string;
}

export interface SimResult {
	seed: string;
	gitCommit: string | null;
	config: Record<string, any>;
	steps: number;
	virtualTimeMs: number;
	invariantViolations: Array<{ category: string; context: any }>;
	metrics: { instances: number; clients: number; framesDelivered: number };
	/** Per-instance client frames, sorted by instance. */
	clusterFrames: Array<{ instance: number; clients: any[][] }>;
	finalState: Array<{ instance: number; topicCounts: Record<string, number>; openConnections: number }>;
	/** True only on a replay result whose frames + state + metrics matched the reproducer. */
	reproduced?: boolean;
}

export function runRedisSim(config?: SimRedisConfig): Promise<SimResult>;
export function replayRedisSim(reproducer: SimResult): Promise<SimResult>;
export function runPgSim(config?: SimPgConfig): Promise<SimResult>;
export function replayPgSim(reproducer: SimResult): Promise<SimResult>;

export const DEFAULT_REDIS_SEED: string;
export const DEFAULT_PG_SEED: string;

// Convenience re-exports (the doubles the runners build on).
export { mockRedisClient } from './testing/index.js';
export { mockPgClient } from './testing/index.js';
