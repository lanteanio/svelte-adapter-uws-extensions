import { it, expect, beforeAll, afterEach, afterAll } from 'vitest';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { LockLostError } from '../../src/redis/lock.js';

/** Run the same public failure paths against the double and PostgreSQL. */
export function taskErrorCodeContract(makeClient) {
	let client;
	let runners = [];
	beforeAll(async () => { client = await makeClient(); });
	afterEach(() => {
		for (const runner of runners) runner.destroy();
		runners = [];
	});
	afterAll(async () => { await client?.end?.(); });
	function create(options = {}) {
		const runner = createTaskRunner(client, {
			table: 'svti_error_code_contract', recoveryInterval: 0,
			cleanupInterval: 0, dispatchInterval: 0, ...options
		});
		runners.push(runner);
		return runner;
	}

	it('identifies an unregistered handler without parsing its message', async () => {
		await expect(create().run('missing', { input: null })).rejects.toMatchObject({
			name: 'UnknownTaskError', code: 'UNKNOWN_TASK', taskName: 'missing'
		});
	});
	it('identifies an already-owned idempotency slot', async () => {
		const runner = create({ idempotency: {
			async acquire() { return { acquired: false, pending: true }; },
			async purge() {}, async clear() {}
		} });
		runner.register('owned', async () => null);
		await expect(runner.run('owned', { input: null, idempotencyKey: 'busy' })).rejects.toMatchObject({
			name: 'TaskInFlightError', code: 'TASK_IN_FLIGHT', idempotencyKey: 'busy'
		});
	});
	it('distinguishes a missing row from a wait deadline while preserving Error names', async () => {
		const runner = create();
		const missing = '00000000-0000-4000-8000-000000000000';
		await expect(runner.await(missing)).rejects.toMatchObject({
			name: 'Error', code: 'TASK_NOT_FOUND', message: `postgres tasks: task "${missing}" not found`
		});
		const taskId = await runner.enqueue('pending', { input: null });
		await expect(runner.await(taskId, { timeout: 1, pollInterval: 1 })).rejects.toMatchObject({
			name: 'Error', code: 'TASK_AWAIT_TIMEOUT',
			message: `postgres tasks: await timeout for task "${taskId}" (waited 1ms, status=pending)`
		});
	});
	it.each(['inline', 'worker'])('preserves a %s handler code through events and a separate runner', async mode => {
		const events = [];
		const writer = create({ onStateChange: event => events.push(event) });
		const reader = create();
		if (mode === 'worker') {
			writer.register('coded', null, { worker: new URL('./workers/throws-coded-error.js', import.meta.url) });
		} else {
			writer.register('coded', async () => { throw new LockLostError('inline-resource'); });
		}
		const received = await writer.run('coded', { input: null }).catch(error => error);
		expect(received).toBeInstanceOf(Error);
		expect(received).toMatchObject({ name: 'LockLostError', code: 'LOCK_LOST' });
		const failed = events.find(event => event.newStatus === 'failed');
		expect(failed?.error).toMatchObject({ name: received.name, message: received.message, code: 'LOCK_LOST' });
		const persisted = await reader.await(failed.taskId).catch(error => error);
		expect(persisted).toBeInstanceOf(Error);
		expect(persisted).not.toBeInstanceOf(LockLostError);
		expect(persisted).toMatchObject({ name: received.name, message: received.message, code: 'LOCK_LOST' });
	});
}
