import { afterEach, expect, it, vi } from 'vitest';
import { createPgClient } from '../../src/postgres/index.js';
import { createJobQueue } from '../../src/postgres/jobs.js';
import { createReplay } from '../../src/postgres/replay.js';
import { createIdempotencyStore } from '../../src/postgres/idempotency.js';
import { createAlarmStore } from '../../src/postgres/alarm-store.js';
import { createDeadLetter } from '../../src/postgres/dead-letter.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from '../helpers/runtime-clock.js';

const cleanup = [];
afterEach(async () => { for (const close of cleanup.splice(0)) await close(); releaseRuntimeClock(); vi.useRealTimers(); });
function heldClient() {
	let release;
	const pool = { query: vi.fn().mockImplementationOnce(() => new Promise(resolve => { release = resolve; })).mockResolvedValue({ rows: [{ id: '1' }], rowCount: 1 }) };
	const client = createPgClient({ pool, maxInFlight: 1 });
	const held = client.query('held');
	cleanup.push(async () => { release({ rows: [] }); await held; });
	return { client, pool, async settle() { release({ rows: [] }); await held; } };
}
const variants = [
	['jobs', createJobQueue, store => store.enqueue('work', {})],
	['replay', createReplay, store => store.seq('room')],
	['replay gap', createReplay, store => store.gap('room', 1)],
	['replay delivery', createReplay, store => store.replay({}, 'room', 0, { checkSubscribe: () => null, send() {} })],
	['idempotency', createIdempotencyStore, store => store.acquire('key')],
	['alarms', createAlarmStore, store => store.get('room')],
	['dead-letter', createDeadLetter, store => store.count()],
	['tasks', createTaskRunner, store => store.counts()]
];
it.each(variants)('keeps local %s capacity failures out of dependency health', async (_name, factory, operation) => {
	const { client, pool } = heldClient();
	const breaker = createCircuitBreaker({ failureThreshold: 5 });
	breaker.failure(new Error('previous real database failure'));
	const store = factory(client, { breaker, autoMigrate: false, cleanupInterval: 0, recoveryInterval: 0, dispatchInterval: 0 });
	cleanup.push(() => { store.destroy?.(); breaker.destroy(); });
	for (let i = 0; i < 6; i++) await expect(operation(store)).rejects.toMatchObject({ code: 'PG_CAPACITY_EXCEEDED' });
	expect(breaker.state).toBe('healthy');
	expect(breaker.failures).toBe(1);
	expect(pool.query).toHaveBeenCalledTimes(1);
});

it.each(variants)('returns the half-open %s probe when local admission refuses work', async (_name, factory, operation) => {
	vi.useFakeTimers(); installFakeRuntimeClock();
	const { client } = heldClient();
	const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 10 });
	const store = factory(client, { breaker, autoMigrate: false, cleanupInterval: 0, recoveryInterval: 0, dispatchInterval: 0 });
	cleanup.push(() => { store.destroy?.(); breaker.destroy(); });
	breaker.failure(new Error('database unavailable'));
	await vi.advanceTimersByTimeAsync(10);
	for (let i = 0; i < 3; i++) await expect(operation(store)).rejects.toMatchObject({ code: 'PG_CAPACITY_EXCEEDED' });
	expect(breaker.state).toBe('probing');
	expect(breaker.failures).toBe(1);
});

it('does not report a local capacity rejection to a custom breaker', async () => {
	const { client } = heldClient();
	const breaker = { guard: vi.fn(), success: vi.fn(), failure: vi.fn() };
	const jobs = createJobQueue(client, { breaker, autoMigrate: false });
	await expect(jobs.enqueue('work', {})).rejects.toMatchObject({ code: 'PG_CAPACITY_EXCEEDED' });
	expect(breaker.guard).toHaveBeenCalledOnce();
	expect(breaker.success).not.toHaveBeenCalled();
	expect(breaker.failure).not.toHaveBeenCalled();
});

it('still reports actual query errors and recovers after successful admitted work', async () => {
	const { client, pool, settle } = heldClient();
	const breaker = createCircuitBreaker();
	cleanup.push(() => breaker.destroy());
	const jobs = createJobQueue(client, { breaker, autoMigrate: false });
	await settle();
	pool.query.mockRejectedValueOnce(Object.assign(new Error('connection reset'), { code: 'ECONNRESET' }));
	await expect(jobs.enqueue('work', {})).rejects.toMatchObject({ code: 'ECONNRESET' });
	expect(breaker.failures).toBe(1);
	await jobs.enqueue('work', {});
	expect(breaker.failures).toBe(0);
});
