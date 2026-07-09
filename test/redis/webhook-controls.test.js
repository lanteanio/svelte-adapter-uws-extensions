// Cluster-shared outbound-webhook delivery controls (redis/webhook-controls.js):
// a Redis retry budget (reusing the token-bucket script) and a shared-count
// endpoint-ejection breaker with a synchronous local guard. Exercised against
// the in-process Redis double; the breaker's local reset countdown is driven
// through a fake monotonic clock installed on the runtime seam.

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import {
	createRetryBudget,
	createWebhookBreaker,
	WebhookCircuitOpenError
} from '../../src/redis/webhook-controls.js';

describe('redis createRetryBudget', () => {
	let client;
	beforeEach(() => { client = mockRedisClient('test:'); });

	it('validates its options', () => {
		expect(() => createRetryBudget(client, { capacity: 0 })).toThrow(/capacity/);
		expect(() => createRetryBudget(client, { intervalMs: 0 })).toThrow(/intervalMs/);
	});

	it('drains capacity then denies within a window', async () => {
		const budget = createRetryBudget(client, { capacity: 2, intervalMs: 60000 });
		expect(await budget.take('k')).toBe(true);
		expect(await budget.take('k')).toBe(true);
		expect(await budget.take('k')).toBe(false);
	});

	it('keeps endpoints isolated', async () => {
		const budget = createRetryBudget(client, { capacity: 1, intervalMs: 60000 });
		expect(await budget.take('a')).toBe(true);
		expect(await budget.take('a')).toBe(false);
		expect(await budget.take('b')).toBe(true); // b's bucket is untouched by a
	});
});

describe('redis createWebhookBreaker', () => {
	let client;
	let clock;
	beforeEach(() => {
		client = mockRedisClient('test:');
		clock = 0;
		setRuntimeEnv({ clock: { monotonic: () => clock } });
	});
	afterEach(() => { resetRuntimeEnv(); });

	it('validates its options', () => {
		expect(() => createWebhookBreaker(client, { failureThreshold: 0 })).toThrow(/failureThreshold/);
		expect(() => createWebhookBreaker(client, { resetMs: 0 })).toThrow(/resetMs/);
		expect(() => createWebhookBreaker(client, { probeConcurrency: 0 })).toThrow(/probeConcurrency/);
		expect(() => createWebhookBreaker(client, { probeConcurrency: 1.5 })).toThrow(/probeConcurrency/);
	});

	it('stays healthy below the threshold', async () => {
		const br = createWebhookBreaker(client, { failureThreshold: 3, resetMs: 1000 });
		await br.failure(new Error('x'), 'k');
		await br.failure(new Error('x'), 'k');
		expect(() => br.guard('k')).not.toThrow();
		expect(br.stateOf('k')).toBe('healthy');
	});

	it('opens once the shared count reaches the threshold', async () => {
		const br = createWebhookBreaker(client, { failureThreshold: 3, resetMs: 1000 });
		await br.failure(new Error('x'), 'k');
		await br.failure(new Error('x'), 'k');
		await br.failure(new Error('x'), 'k');
		expect(br.stateOf('k')).toBe('broken');
		expect(() => br.guard('k')).toThrow(WebhookCircuitOpenError);
	});

	it('heals on success', async () => {
		const br = createWebhookBreaker(client, { failureThreshold: 1, resetMs: 1000 });
		await br.failure(new Error('x'), 'k');
		expect(() => br.guard('k')).toThrow();
		await br.success('k');
		expect(br.stateOf('k')).toBe('healthy');
		expect(() => br.guard('k')).not.toThrow();
	});

	it('shares the failure count across instances on one client', async () => {
		const brA = createWebhookBreaker(client, { failureThreshold: 3, resetMs: 1000 });
		const brB = createWebhookBreaker(client, { failureThreshold: 3, resetMs: 1000 });
		await brA.failure(new Error('x'), 'k'); // shared count 1
		await brA.failure(new Error('x'), 'k'); // shared count 2
		await brB.failure(new Error('x'), 'k'); // shared count 3 -> B crosses, opens locally
		expect(() => brB.guard('k')).toThrow(WebhookCircuitOpenError);
		expect(() => brA.guard('k')).not.toThrow(); // A only observed counts 1,2 so far
		await brA.failure(new Error('x'), 'k'); // shared count 4 -> A now opens too
		expect(() => brA.guard('k')).toThrow(WebhookCircuitOpenError);
	});

	it('allows one half-open probe after resetMs and closes on success', async () => {
		const br = createWebhookBreaker(client, { failureThreshold: 1, resetMs: 1000 });
		await br.failure(new Error('x'), 'k'); // opens, until = 1000
		expect(() => br.guard('k')).toThrow();
		clock = 500;
		expect(() => br.guard('k')).toThrow(); // window not elapsed
		clock = 1000;
		expect(() => br.guard('k')).not.toThrow(); // one probe allowed
		expect(br.stateOf('k')).toBe('probing');
		expect(() => br.guard('k')).toThrow(); // only one
		await br.success('k');
		expect(br.stateOf('k')).toBe('healthy');
	});

	it('re-opens when the half-open probe fails', async () => {
		const br = createWebhookBreaker(client, { failureThreshold: 1, resetMs: 1000 });
		await br.failure(new Error('x'), 'k'); // open, until 1000
		clock = 1000;
		br.guard('k'); // probe allowed -> probing
		await br.failure(new Error('x'), 'k'); // probe failed -> re-open, until 2000
		expect(() => br.guard('k')).toThrow(WebhookCircuitOpenError); // 1000 < 2000
		clock = 2000;
		expect(() => br.guard('k')).not.toThrow();
	});

	it('probeConcurrency admits that many half-open probes per reset window', async () => {
		const br = createWebhookBreaker(client, { failureThreshold: 1, resetMs: 1000, probeConcurrency: 2 });
		await br.failure(new Error('x'), 'k'); // opens, until = 1000
		clock = 1000;
		expect(() => br.guard('k')).not.toThrow(); // probe 1
		expect(br.stateOf('k')).toBe('probing');
		expect(() => br.guard('k')).not.toThrow(); // probe 2
		expect(() => br.guard('k')).toThrow(WebhookCircuitOpenError); // budget spent
		await br.success('k'); // first probe reply closes the circuit
		expect(br.stateOf('k')).toBe('healthy');
		expect(() => br.guard('k')).not.toThrow();
	});

	it('probeConcurrency: a failed probe re-opens and clears the remaining budget', async () => {
		const br = createWebhookBreaker(client, { failureThreshold: 1, resetMs: 1000, probeConcurrency: 2 });
		await br.failure(new Error('x'), 'k'); // open, until 1000
		clock = 1000;
		br.guard('k'); // probe 1 out, one budget slot left
		await br.failure(new Error('x'), 'k'); // probe failed -> re-open, until 2000
		// The leftover slot must not leak a request through the re-opened circuit.
		expect(() => br.guard('k')).toThrow(WebhookCircuitOpenError);
		clock = 2000;
		expect(() => br.guard('k')).not.toThrow(); // fresh window, fresh budget
	});
});
