/** The precomputed client mitigation broadcast when a failure class degrades. */
export interface Mitigation {
	/** Stream topics to treat as unavailable while degraded. */
	streams?: string[];
	/** RPC paths to treat as unavailable while degraded. */
	rpcs?: string[];
	/** How long (ms) the client should hold off before retrying. */
	retryAfterMs?: number;
	/** Ready-to-render notice text. */
	bannerCopy?: string;
}

/** The optional recovery hint broadcast when the failure class recovers. */
export interface RecoveryHint {
	/** The client should refetch affected streams. */
	refetch?: boolean;
	/** The client should clear any local cache for the affected data. */
	clearCache?: boolean;
	/** Ready-to-render notice text. */
	bannerCopy?: string;
}

export interface DegradationTransition {
	from: string;
	to: string;
}

export interface DegradationSpec {
	/**
	 * The envelope broadcast on degrade - a static mitigation, or a function of the
	 * breaker transition for dynamic policies.
	 */
	mitigation?: Mitigation | ((transition: DegradationTransition) => Mitigation | null);
	/**
	 * De-herd window (ms) for the degraded push. Default `min(retryAfterMs / 4, 5000)`
	 * when the mitigation sets `retryAfterMs`, else 0 (no jitter). Max 60000.
	 */
	jitterMs?: number;
	/** Optional envelope broadcast on recover. */
	recovery?: RecoveryHint | ((transition: DegradationTransition) => RecoveryHint | null);
	/** De-herd window (ms) for the recovered push. Max 60000. */
	recoveryJitterMs?: number;
}

export interface DegradationPolicy {
	/** Resolve the degraded mitigation + its de-herd window for a transition. */
	onDegraded(transition: DegradationTransition): { mitigation: Mitigation; jitterMs: number } | null;
	/** Resolve the recovery hint + its de-herd window for a transition. */
	onRecovered(transition: DegradationTransition): { recovery: RecoveryHint; jitterMs: number } | null;
}

/**
 * Create a degradation policy. Pass it to `createPubSubBus(client, { breaker,
 * degradationPolicy })`: on a breaker transition the bus ships the resolved envelope on
 * the `degraded` / `recovered` system event, de-herded with the resolved jitter window
 * so the clients' resulting retries spread across the cooldown instead of spiking at t+0.
 */
export function createDegradationPolicy(spec?: DegradationSpec): DegradationPolicy;
