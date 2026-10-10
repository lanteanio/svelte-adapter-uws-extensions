import { afterEach, expect, it, vi } from 'vitest';
import { spawnSync } from 'node:child_process';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { createMetrics, wireBreakerMetrics } from '../../src/prometheus/index.js';
import { createPgClient } from '../../src/postgres/index.js';

afterEach(() => vi.useRealTimers());

it('exposes healthy idle state, keyed transitions and timer recovery without identifiers', async () => {
	vi.useFakeTimers();
	const metrics = createMetrics(), breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
	const stop = wireBreakerMetrics(breaker, metrics);
	expect(metrics.serialize()).toContain('circuit_breaker_states{state="healthy"} 1');
	expect(metrics.serialize()).toContain('circuit_breaker_transitions_total{from="healthy",to="broken"} 0');
	breaker.failure(new Error('private backend address'), 'private-user-123');
	let output = metrics.serialize();
	expect(output).toContain('circuit_breaker_states{state="healthy"} 1');
	expect(output).toContain('circuit_breaker_states{state="broken"} 1');
	expect(output).not.toMatch(/private-user|private backend/);
	await vi.advanceTimersByTimeAsync(50);
	expect(metrics.serialize()).toContain('circuit_breaker_states{state="probing"} 1');
	breaker.success('private-user-123');
	output = metrics.serialize();
	expect(output).toContain('circuit_breaker_states{state="healthy"} 2');
	expect(output).toContain('circuit_breaker_transitions_total{from="probing",to="healthy"} 1');
	stop(); breaker.destroy();
});

it('deduplicates repeated bindings and removes only the final reference', () => {
	const metrics = createMetrics(), a = createCircuitBreaker({ failureThreshold: 1 }), b = createCircuitBreaker();
	const stopA = wireBreakerMetrics(a, metrics), stopAgain = wireBreakerMetrics(a, metrics), stopB = wireBreakerMetrics(b, metrics);
	expect(metrics.serialize()).toContain('circuit_breaker_states{state="healthy"} 2');
	a.failure();
	expect(metrics.serialize()).toContain('circuit_breaker_transitions_total{from="healthy",to="broken"} 1');
	stopA(); stopA();
	expect(metrics.serialize()).toContain('circuit_breaker_states{state="broken"} 1');
	stopAgain(); a.reset();
	expect(metrics.serialize()).toContain('circuit_breaker_transitions_total{from="broken",to="healthy"} 0');
	stopB();
	expect(metrics.serialize()).toContain('circuit_breaker_states{state="healthy"} 0');
	const stopNew = wireBreakerMetrics(a, metrics);
	expect(metrics.serialize()).toContain('circuit_breaker_states{state="healthy"} 1');
	stopNew(); a.destroy(); b.destroy();
});

it('scrapes current partitions after state eviction rather than accumulating stale states', () => {
	const metrics = createMetrics(), breaker = createCircuitBreaker();
	const stop = wireBreakerMetrics(breaker, metrics);
	for (let i = 0; i < 10010; i++) breaker.stateOf('private-' + i);
	const output = metrics.serialize();
	expect(output).toContain('circuit_breaker_states{state="healthy"} 10000');
	expect(output).not.toContain('private-');
	stop(); breaker.destroy();
});

it('bounds labels from structural custom breakers', () => {
	let listener;
	const breaker = { state: 'healthy', subscribe(fn) { listener = fn; return () => {}; } };
	const metrics = createMetrics(), stop = wireBreakerMetrics(breaker, metrics);
	listener('private-tenant', 'broken');
	expect(metrics.serialize()).not.toContain('private-tenant');
	expect(metrics.serialize()).toContain('circuit_breaker_states{state="healthy"} 1');
	stop();
	expect(() => wireBreakerMetrics({}, metrics)).toThrow(TypeError);
});

it('does not retain an otherwise unreachable breaker through registry wiring', () => {
	const breakerUrl = new URL('../../src/shared/breaker.js', import.meta.url).href;
	const metricsUrl = new URL('../../src/prometheus/index.js', import.meta.url).href;
	const source = `
		import { createCircuitBreaker } from ${JSON.stringify(breakerUrl)};
		import { createMetrics, wireBreakerMetrics } from ${JSON.stringify(metricsUrl)};
		import assert from 'node:assert/strict';
		const metrics = createMetrics(); let ref;
		(function register() { const breaker = createCircuitBreaker(); ref = new WeakRef(breaker); wireBreakerMetrics(breaker, metrics); })();
		for (let i=0;i<10;i++) { await new Promise(resolve => setImmediate(resolve)); global.gc(); }
		assert.equal(ref.deref(), undefined);
		assert.match(metrics.serialize(), /circuit_breaker_states\{state="healthy"\} 0/);
	`;
	const result = spawnSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', source], { encoding: 'utf8', timeout: 30000 });
	expect(result.error).toBeUndefined();
	expect(result.status, result.stderr).toBe(0);
}, 35000);

it('scrapes actual pool utilization once per shared pool and removes ended pools', () => {
	const metrics = createMetrics();
	const pool = { query: async () => ({ rows: [] }), totalCount: 5, idleCount: 2, waitingCount: 3, options: { max: 10 }, ended: false };
	const other = { query: pool.query, totalCount: 2, idleCount: 2, waitingCount: 1, options: { max: 4 } };
	createPgClient({ pool, metrics }); createPgClient({ pool, metrics }); createPgClient({ pool: other, metrics });
	let output = metrics.serialize();
	expect(output).toContain('postgres_pool_connections{state="active"} 3');
	expect(output).toContain('postgres_pool_connections{state="idle"} 4');
	expect(output).toContain('postgres_pool_capacity 14');
	expect(output).toContain('postgres_pool_waiters 4');
	pool.ended = true;
	output = metrics.serialize();
	expect(output).toContain('postgres_pool_connections{state="active"} 0');
	expect(output).toContain('postgres_pool_connections{state="idle"} 2');
	expect(output).toContain('postgres_pool_capacity 4');
	expect(output).toContain('postgres_pool_waiters 1');
});
