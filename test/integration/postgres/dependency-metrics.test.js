import { expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createMetrics, wireBreakerMetrics } from '../../../src/prometheus/index.js';
import { createCircuitBreaker, withBreaker } from '../../../src/shared/breaker.js';

it('measures direct native checkout utilization and stops counting an ended pool', async () => {
	const metrics = createMetrics();
	const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, metrics, options: { max: 2 } });
	createPgClient({ pool: client.pool, autoShutdown: false, metrics: { ...metrics } });
	const other = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, metrics: { ...metrics }, options: { max: 1 } });
	let first, second, third;
	try {
		expect(metrics.serialize()).toContain('postgres_pool_capacity 3');
		first = await client.pool.connect(); second = await client.pool.connect(); third = await other.pool.connect();
		expect(metrics.serialize()).toContain('postgres_pool_connections{state="active"} 3');
		const pending = client.query('SELECT 42 AS value');
		expect(metrics.serialize()).toContain('postgres_pool_waiters 1');
		first.release(); first = null;
		await expect(pending).resolves.toMatchObject({ rows: [{ value: 42 }] });
		expect(metrics.serialize()).toContain('postgres_pool_connections{state="active"} 2');
		expect(metrics.serialize()).toContain('postgres_pool_connections{state="idle"} 1');
	} finally { first?.release(); second?.release(); third?.release(); await Promise.all([client.end(), other.end()]); }
	const output = metrics.serialize();
	expect(output).toContain('postgres_pool_capacity 0');
	expect(output).toContain('postgres_pool_connections{state="active"} 0');
	expect(output).toContain('postgres_pool_connections{state="idle"} 0');
});

it('distinguishes a real database query error from an idle healthy breaker', async () => {
	const metrics = createMetrics(), breaker = createCircuitBreaker({ failureThreshold: 1 });
	const stop = wireBreakerMetrics(breaker, metrics);
	const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, metrics });
	try {
		expect(metrics.serialize()).toContain('circuit_breaker_states{state="healthy"} 1');
		await expect(withBreaker(breaker, () => client.query('SELECT 1 / 0'))).rejects.toHaveProperty('code', '22012');
		const output = metrics.serialize();
		expect(output).toContain('circuit_breaker_states{state="broken"} 1');
		expect(output).toContain('circuit_breaker_transitions_total{from="healthy",to="broken"} 1');
		expect(output).toContain('postgres_operation_duration_seconds_count{operation="query",result="error"} 1');
		expect(output).not.toContain('SELECT');
	} finally { stop(); breaker.destroy(); await client.end(); }
});
