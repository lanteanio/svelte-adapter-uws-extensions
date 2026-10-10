import { getEventListeners } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPgClient } from '../../src/postgres/index.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { createCircuitBreaker, withBreaker } from '../../src/shared/breaker.js';

function deferred() {
	let resolve, reject;
	const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
	return { promise, resolve, reject };
}
function poolWith(pending) {
	return { query: vi.fn(() => pending.promise), connect: vi.fn(() => pending.promise), end: vi.fn(), waitingCount: 0 };
}
const clients = [];
function owned(options = {}) {
	const client = createPgClient({ connectionString: 'postgres://localhost/example', autoShutdown: false, ...options });
	clients.push(client);
	return client;
}
afterEach(async () => { resetRuntimeEnv(); await Promise.all(clients.splice(0).map((client) => client.end())); });

describe('PostgreSQL pool admission and deadlines', () => {
	it('configures finite owned-pool defaults and explicit zero opt-outs', () => {
		const bounded = owned();
		expect(bounded.pool.options).toMatchObject({ connectionTimeoutMillis: 10000, query_timeout: 30000, statement_timeout: 30000 });
		const input = { connectionTimeoutMillis: 0, query_timeout: 0, statement_timeout: 0, max: 2 };
		const unlimited = owned({ options: input, maxInFlight: 0 });
		expect(unlimited.pool.options).toMatchObject(input);
		expect(input).toEqual({ connectionTimeoutMillis: 0, query_timeout: 0, statement_timeout: 0, max: 2 });
		expect(owned({ options: { query_timeout: undefined } }).pool.options.query_timeout).toBe(30000);
	});

	it.each([-1, 1.2, Infinity, NaN, '100'])('rejects invalid admission limit %s', (maxInFlight) => {
		expect(() => owned({ maxInFlight })).toThrow('maxInFlight');
	});

	it.each(['connectionTimeoutMillis', 'query_timeout', 'statement_timeout'])('validates owned %s without reconfiguring supplied pools', (name) => {
		expect(() => owned({ options: { [name]: -1 } })).toThrow(name);
		expect(() => owned({ options: { [name]: 2147483648 } })).toThrow(name);
		const pool = poolWith(deferred());
		const client = createPgClient({ pool, options: { [name]: -1 } });
		expect(client.pool).toBe(pool);
		expect(pool).not.toHaveProperty('options');
	});

	it('rejects null supplied pool clearly', () => {
		expect(() => createPgClient({ pool: null })).toThrow('pool must be a pg.Pool instance');
	});

	it('bounds a 20,000-call owned burst before entering the driver queue', async () => {
		const client = owned();
		const pending = deferred();
		const query = vi.spyOn(client.pool, 'query').mockReturnValue(pending.promise);
		const calls = Array.from({ length: 20000 }, () => client.query('SELECT 1').catch((error) => error.code));
		expect(query).toHaveBeenCalledTimes(100);
		pending.resolve({ rows: [] });
		const results = await Promise.all(calls);
		expect(results.filter((result) => result === 'PG_CAPACITY_EXCEEDED')).toHaveLength(19900);
		await expect(client.query('SELECT 2')).resolves.toEqual({ rows: [] });
	});

	it('shares query/checkout admission and frees it only on actual settlement', async () => {
		const pending = deferred();
		const pool = poolWith(pending);
		const client = createPgClient({ pool, maxInFlight: 1 });
		const first = client.query({ text: 'SELECT $1', values: [1] });
		await expect(client.connect()).rejects.toMatchObject({ code: 'PG_CAPACITY_EXCEEDED' });
		expect(pool.connect).not.toHaveBeenCalled();
		pending.resolve({ rows: [{ value: 1 }] });
		await first;
		pool.connect.mockResolvedValue({ query: vi.fn(), release: vi.fn() });
		const lease = await client.connect();
		// Checkout settlement releases admission, not the database connection.
		await expect(client.query('SELECT 2')).resolves.toBeDefined();
		expect(lease.release).not.toHaveBeenCalled();
		lease.release();
	});

	it('keeps default external ownership and unbounded admission unchanged', async () => {
		const pending = deferred();
		const pool = poolWith(pending);
		const client = createPgClient({ pool });
		const calls = Array.from({ length: 200 }, () => client.query('SELECT 1'));
		expect(pool.query).toHaveBeenCalledTimes(200);
		await client.end();
		expect(pool.end).not.toHaveBeenCalled();
		pending.resolve({ rows: [] });
		await Promise.all(calls);
		await expect(client.query('SELECT 2')).resolves.toBeDefined();
	});

	it.each(['query', 'connect'])('does not admit pre-aborted %s and preserves the caller reason', async (operation) => {
		const pool = poolWith(deferred());
		const client = createPgClient({ pool, maxInFlight: 1 });
		const controller = new AbortController();
		controller.abort();
		const result = operation === 'query' ? client.query('SELECT 1', undefined, { signal: controller.signal }) : client.connect({ signal: controller.signal });
		await expect(result).rejects.toBe(controller.signal.reason);
		expect(controller.signal.reason.name).toBe('AbortError');
		expect(pool[operation]).not.toHaveBeenCalled();
	});

	it.each(['resolve', 'reject'])('retains capacity after query cancellation until the driver can %s', async (settle) => {
		const pending = deferred();
		const pool = poolWith(pending);
		const client = createPgClient({ pool, maxInFlight: 1 });
		const controller = new AbortController();
		const operation = client.query('SELECT 1', undefined, { signal: controller.signal });
		controller.abort(new Error('caller left'));
		await expect(operation).rejects.toThrow('caller left');
		await expect(client.query('SELECT 2')).rejects.toMatchObject({ code: 'PG_CAPACITY_EXCEEDED' });
		pending[settle](settle === 'resolve' ? { rows: [] } : new Error('late backend failure'));
		await Promise.resolve();
		pool.query.mockResolvedValue({ rows: [] });
		await expect(client.query('SELECT 3')).resolves.toEqual({ rows: [] });
	});

	it('releases an abandoned checkout exactly once after late acquisition', async () => {
		const pending = deferred();
		const pool = poolWith(pending);
		const client = createPgClient({ pool, maxInFlight: 1 });
		const controller = new AbortController();
		const operation = client.connect({ signal: controller.signal });
		controller.abort();
		await expect(operation).rejects.toHaveProperty('name', 'AbortError');
		await expect(client.connect()).rejects.toHaveProperty('code', 'PG_CAPACITY_EXCEEDED');
		const acquired = { release: vi.fn(), query: vi.fn() };
		pending.resolve(acquired);
		await Promise.resolve();
		expect(acquired.release).toHaveBeenCalledTimes(1);
		controller.abort();
		pool.connect.mockResolvedValue(acquired);
		await expect(client.connect()).resolves.toBe(acquired);
		expect(acquired.release).toHaveBeenCalledTimes(1);
	});

	it('does not release a successfully returned checkout on a later abort', async () => {
		const acquired = { release: vi.fn(), query: vi.fn() };
		const pool = { query: vi.fn(), connect: vi.fn().mockResolvedValue(acquired) };
		const client = createPgClient({ pool, maxInFlight: 1 });
		const controller = new AbortController();
		await client.connect({ signal: controller.signal });
		controller.abort();
		expect(acquired.release).not.toHaveBeenCalled();
	});

	it('cleans up admission after a synchronous driver failure', async () => {
		const pool = { query: vi.fn(() => { throw new Error('failed postgres://user:secret@host/db'); }) };
		const client = createPgClient({ pool, maxInFlight: 1 });
		await expect(client.query('SELECT 1')).rejects.toThrow('postgres://user:***@host/db');
		pool.query.mockResolvedValue({ rows: [] });
		await expect(client.query('SELECT 2')).resolves.toEqual({ rows: [] });
	});

	it('reports actual outstanding work after cancellation and native timeouts with bounded labels', async () => {
		const metrics = createMetrics();
		const pending = deferred();
		const pool = poolWith(pending);
		pool.waitingCount = 7;
		const client = createPgClient({ pool, maxInFlight: 1, metrics });
		const controller = new AbortController();
		const operation = client.query('SECRET SQL', undefined, { signal: controller.signal });
		controller.abort();
		await expect(operation).rejects.toHaveProperty('name', 'AbortError');
		expect(metrics.serialize()).toContain('postgres_operations_in_flight{operation="query"} 1');
		expect(metrics.serialize()).toContain('postgres_pool_waiters 7');
		await expect(client.query('SELECT 2')).rejects.toHaveProperty('code', 'PG_CAPACITY_EXCEEDED');
		pending.reject(new Error('Query read timeout'));
		await Promise.resolve();
		const output = metrics.serialize();
		expect(output).toContain('postgres_operations_in_flight{operation="query"} 0');
		expect(output).toContain('postgres_operation_timeouts_total{operation="query"} 1');
		expect(output).toContain('postgres_operation_duration_seconds_count{operation="query",result="timeout"} 1');
		expect(output).toContain('postgres_admission_rejected_total{operation="query",reason="aborted"} 1');
		expect(output).toContain('postgres_admission_rejected_total{operation="query",reason="capacity"} 1');
		expect(output).not.toContain('SECRET SQL');
	});

	it('aggregates shared-registry driver waiters once per pool', () => {
		const metrics = createMetrics();
		const one = poolWith(deferred()), two = poolWith(deferred());
		one.waitingCount = 3; two.waitingCount = 4;
		createPgClient({ pool: one, metrics });
		createPgClient({ pool: two, metrics });
		createPgClient({ pool: one, metrics });
		expect(metrics.serialize()).toContain('postgres_pool_waiters 7');
		one.waitingCount = 0; two.waitingCount = 1;
		expect(metrics.serialize()).toContain('postgres_pool_waiters 1');
	});

	it('does not leak admission if a foreign signal cannot install its listener', async () => {
		const pool = { query: vi.fn().mockResolvedValue({ rows: [] }) };
		const client = createPgClient({ pool, maxInFlight: 1 });
		const signal = { aborted: false, addEventListener() { throw new Error('listener refused'); }, removeEventListener() { throw new Error('cleanup refused'); } };
		await expect(client.query('SELECT 1', undefined, { signal })).rejects.toThrow('listener refused');
		expect(pool.query).not.toHaveBeenCalled();
		await expect(client.query('SELECT 2')).resolves.toEqual({ rows: [] });
	});

	it('removes abort listeners on success and rejection', async () => {
		for (const succeeds of [true, false]) {
			const pending = deferred();
			const client = createPgClient({ pool: poolWith(pending), maxInFlight: 1 });
			const controller = new AbortController();
			const result = client.query('SELECT 1', undefined, { signal: controller.signal }).catch((error) => error);
			expect(getEventListeners(controller.signal, 'abort')).toHaveLength(1);
			if (succeeds) pending.resolve({ rows: [] });
			else pending.reject(new Error('driver failed'));
			await result;
			expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
		}
	});

	it('handles a late failed checkout after abort without an unhandled rejection', async () => {
		const pending = deferred();
		const pool = poolWith(pending);
		const client = createPgClient({ pool, maxInFlight: 1 });
		const controller = new AbortController();
		const result = client.connect({ signal: controller.signal });
		controller.abort();
		await expect(result).rejects.toHaveProperty('name', 'AbortError');
		pending.reject(new Error('late checkout failure'));
		await Promise.resolve();
		const acquired = { release: vi.fn() };
		pool.connect.mockResolvedValue(acquired);
		await expect(client.connect()).resolves.toBe(acquired);
		expect(acquired.release).not.toHaveBeenCalled();
	});

	it('observes seconds and avoids clock reads without metrics', async () => {
		let time = 1000;
		const clock = vi.fn(() => time);
		setRuntimeEnv({ clock: { monotonic: clock } });
		const pending = deferred();
		const pool = poolWith(pending);
		const metrics = createMetrics();
		const measured = createPgClient({ pool, metrics, maxInFlight: 1 });
		const result = measured.query('SELECT 1');
		time += 1500;
		pending.resolve({ rows: [] });
		await result;
		expect(metrics.serialize()).toContain('postgres_operation_duration_seconds_sum{operation="query",result="ok"} 1.5');
		clock.mockClear();
		await createPgClient({ pool, maxInFlight: 1 }).query('SELECT 2');
		await createPgClient({ pool }).query('SELECT 3');
		expect(clock).not.toHaveBeenCalled();
	});

	it('lets a caller deadline trip a breaker for a never-settling external query without releasing admission', async () => {
		const pool = poolWith(deferred());
		const client = createPgClient({ pool, maxInFlight: 1 });
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		try {
			await expect(withBreaker(breaker, () => client.query('SELECT 1', undefined, { signal: AbortSignal.timeout(20) }))).rejects.toHaveProperty('name', 'TimeoutError');
			expect(breaker.state).toBe('broken');
			await expect(withBreaker(breaker, () => client.query('SELECT 2'))).rejects.toHaveProperty('name', 'CircuitBrokenError');
			expect(pool.query).toHaveBeenCalledTimes(1);
			await expect(client.query('SELECT 3')).rejects.toHaveProperty('code', 'PG_CAPACITY_EXCEEDED');
		} finally { breaker.destroy(); }
	});
});
