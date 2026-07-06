/**
 * Cluster-shared delivery controls for svelte-realtime's outbound webhooks: a
 * retry budget and an endpoint-ejection circuit breaker backed by Redis, so a
 * fleet of instances shares one retry budget and one view of a failing endpoint
 * instead of each deciding in isolation.
 *
 * Both implement the interface svelte-realtime's outbound-webhook delivery
 * consumes (the adapter's `plugins/webhooks` `RetryBudget` / `WebhookBreaker`
 * shapes). Wire them:
 *
 *   import { createRetryBudget, createWebhookBreaker } from 'svelte-adapter-uws-extensions/redis/webhook-controls';
 *   configureWebhooks({
 *     budget: createRetryBudget(redisClient),
 *     breaker: createWebhookBreaker(redisClient)
 *   });
 *
 * Needs svelte-realtime >= 0.6.0-next.72 / svelte-adapter-uws >= 0.6.0-next.61,
 * which added the delivery-control hooks. The single-instance defaults ship in
 * the adapter (`createRetryBudget` / `createWebhookBreaker` from
 * `svelte-adapter-uws/plugins/webhooks`); these are their cluster substitutes,
 * keyed by the webhook registration id so each endpoint's state is isolated.
 *
 * @module svelte-adapter-uws-extensions/redis/webhook-controls
 */

import { withBreaker } from '../shared/breaker.js';
import { evalCached } from '../shared/eval-cached.js';
import { monotonicNow } from '../shared/runtime.js';
import { CONSUME_SCRIPT } from './token-bucket-script.js';

/** Cap on distinct locally-cached breaker keys before the oldest is evicted. */
const MAX_BREAKER_KEYS = 4096;

/**
 * Thrown by a cluster breaker's `guard` when an endpoint's circuit is open.
 * Mirrors the adapter's error code (`WEBHOOK_CIRCUIT_OPEN`) so a consumer sees
 * the same contract whether the in-process or the cluster breaker is wired; the
 * class is defined locally to keep this module free of an adapter runtime import
 * (extensions references the adapter for types only).
 */
export class WebhookCircuitOpenError extends Error {
	constructor(key) {
		super('outbound webhook: endpoint circuit open' + (key ? ' (' + key + ')' : ''));
		this.name = 'WebhookCircuitOpenError';
		/** @type {'WEBHOOK_CIRCUIT_OPEN'} */
		this.code = 'WEBHOOK_CIRCUIT_OPEN';
	}
}

/**
 * Create a Redis-backed retry budget: a windowed token bucket shared across the
 * fleet. `take(key)` consumes one token, resolving to whether a retry may
 * proceed; up to `capacity` retries are allowed per `intervalMs` window PER
 * endpoint (keyed by the webhook id). Reuses the audited token-bucket script the
 * rate limiter uses (windowed refill, not the continuous bucket of the adapter's
 * in-process default - both satisfy the `take` contract). A backend blip rejects
 * `take`; the delivery path treats that as fail-open (a retry throttle must never
 * wedge delivery when Redis is down).
 *
 * @param {import('./index.js').RedisClient} client
 * @param {{ capacity?: number, intervalMs?: number, breaker?: object }} [options]
 */
export function createRetryBudget(client, options = {}) {
	const capacity = options.capacity ?? 100;
	const intervalMs = options.intervalMs ?? 10000;
	if (!Number.isFinite(capacity) || capacity < 1) {
		throw new Error('redis retry budget: capacity must be a number >= 1');
	}
	if (!Number.isInteger(intervalMs) || intervalMs < 1) {
		throw new Error('redis retry budget: intervalMs must be a positive integer');
	}
	const redis = client.redis;
	const b = options.breaker;

	return {
		async take(key) {
			const bk = client.key('whbudget:{' + String(key ?? '') + '}');
			// CONSUME_SCRIPT returns [allowed, remaining, resetMs]; cost 1, no ban
			// (blockDuration 0), no emergency scale.
			const res = await withBreaker(b, () => evalCached(redis, CONSUME_SCRIPT, 1, bk, capacity, intervalMs, 1, 0));
			return Array.isArray(res) && Number(res[0]) === 1;
		}
	};
}

/**
 * Create a Redis-backed endpoint-ejection breaker shared across the fleet.
 * Failures increment a shared per-endpoint counter (TTL `resetMs`); when the
 * combined count across the fleet reaches `failureThreshold` the endpoint's
 * circuit opens and deliveries fast-fail (`guard` throws), healing after a probe
 * delivery succeeds. `guard` is SYNCHRONOUS (the delivery path calls it inline),
 * so it reads a small local view of the circuit that each `failure`/`success`
 * round-trip refreshes from the shared counter: an instance learns an endpoint is
 * ejected on its next `failure()`, and with leader-gated fan-out (one instance
 * delivers) that view is authoritative. After `resetMs` the local view allows one
 * half-open probe; a probe success closes the circuit, a probe failure re-opens
 * it. `failure`/`success` return a promise that never rejects (a backend blip
 * degrades to no ejection, never a broken delivery path) - awaitable in tests,
 * ignored by the delivery caller.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {{ failureThreshold?: number, resetMs?: number, breaker?: object }} [options]
 */
export function createWebhookBreaker(client, options = {}) {
	const failureThreshold = options.failureThreshold ?? 5;
	const resetMs = options.resetMs ?? 30000;
	if (!Number.isInteger(failureThreshold) || failureThreshold < 1) {
		throw new Error('redis webhook breaker: failureThreshold must be a positive integer');
	}
	if (!Number.isInteger(resetMs) || resetMs < 1) {
		throw new Error('redis webhook breaker: resetMs must be a positive integer');
	}
	const redis = client.redis;
	const b = options.breaker;

	/** Local view for the synchronous guard: key -> { open, until, probing }. */
	const local = new Map();
	function view(key) {
		const k = String(key ?? '');
		let v = local.get(k);
		if (!v) {
			if (local.size >= MAX_BREAKER_KEYS) {
				const oldest = local.keys().next().value;
				if (oldest !== undefined) local.delete(oldest);
			}
			v = { open: false, until: 0, probing: false };
			local.set(k, v);
		}
		return v;
	}
	function openLocal(key) {
		const v = view(key);
		v.open = true;
		v.until = monotonicNow() + resetMs;
		v.probing = false;
	}
	function closeLocal(key) {
		const v = view(key);
		v.open = false;
		v.until = 0;
		v.probing = false;
	}

	const failKey = (key) => client.key('whbreaker:fail:' + String(key ?? ''));

	return {
		stateOf(key) {
			const v = local.get(String(key ?? ''));
			if (!v || !v.open) return 'healthy';
			if (v.probing) return 'probing';
			return monotonicNow() >= v.until ? 'probing' : 'broken';
		},

		guard(key) {
			const v = local.get(String(key ?? ''));
			if (!v || !v.open) return; // healthy or not-yet-seen -> allow
			if (monotonicNow() < v.until) throw new WebhookCircuitOpenError(key); // still open
			if (v.probing) throw new WebhookCircuitOpenError(key); // a probe is already out
			v.probing = true; // reset window elapsed: allow exactly one probe
		},

		success(key) {
			closeLocal(key); // heal the local view immediately (guard sees it now)
			return withBreaker(b, () => redis.del(failKey(key))).then(() => {}, () => {});
		},

		failure(_err, key) {
			const v = local.get(String(key ?? ''));
			const wasProbing = !!(v && v.probing);
			return withBreaker(b, () => redis.incr(failKey(key)))
				.then((n) => withBreaker(b, () => redis.pexpire(failKey(key), resetMs)).then(() => {
					// A failed half-open probe re-opens regardless of count; otherwise
					// open once the SHARED count reaches the threshold.
					if (wasProbing || Number(n) >= failureThreshold) openLocal(key);
				}))
				.then(() => {}, () => {});
		},

		destroy() {
			local.clear();
		}
	};
}
