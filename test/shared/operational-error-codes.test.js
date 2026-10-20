import { describe, it, expect, afterEach, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createWorkerPool } from '../../src/postgres/_tasks-worker-pool.js';
import { registerPgLifecycle, onPgShutdown } from '../../src/postgres/_client-lifecycle.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { closeConnection } from '../../src/shared/redis-subscriber.js';

vi.mock('../../src/shared/caps.js', async original => ({
	...(await original()), MAX_REGISTRY_PENDING_REQUESTS: 2,
	MAX_SHARDED_BUS_TOPICS: 2, MAX_CURSOR_TOPICS: 2, MAX_CURSOR_WS: 2
}));
vi.mock('node:worker_threads', async original => {
	const { EventEmitter } = await import('node:events');
	return { ...(await original()), Worker: class extends EventEmitter {
		static instances = [];
		constructor() { super(); this.constructor.instances.push(this); }
		postMessage() {}
		unref() {}
		terminate() { this.emit('exit', 0); return Promise.resolve(0); }
	} };
});

const cleanup = [];
afterEach(async () => {
	for (const fn of cleanup.splice(0).reverse()) await fn();
	vi.restoreAllMocks();
});
const outcome = promise => promise.catch(error => error);
function registry() {
	const client = mockRedisClient('codes:');
	const value = createConnectionRegistry(client, { identify: ws => ws.getUserData().userId, sessionIdentify: ws => ws.getUserData().sid, heartbeat: 60000 });
	cleanup.push(() => value.destroy());
	return { client, value };
}
async function route(client, method, target, instanceId = 'absent-owner') {
	await client.redis.hset(client.key((method === 'request' ? 'conns:' : 'regsess:') + target), 'instanceId', instanceId, 'sessionId', 'transport', 'ts', Date.now());
}

describe.each(['request', 'requestSession'])('registry %s operational failures', method => {
	it.each(['missing', 'local socket missing'])('identifies %s without logging the target', async kind => {
		const { client, value } = registry();
		const target = 'private-target';
		if (kind !== 'missing') await route(client, method, target, value.instanceId);
		const error = await outcome(value[method](target, 'event'));
		expect(error).toMatchObject({ name: 'Error', code: 'REGISTRY_TARGET_OFFLINE' });
		expect(error.message).not.toContain(target);
	});
	it('distinguishes a remote deadline from offline admission', async () => {
		const { client, value } = registry();
		await route(client, method, 'private-target');
		const error = await outcome(value[method]('private-target', 'event', null, { timeoutMs: 5 }));
		expect(error).toMatchObject({ name: 'Error', code: 'REGISTRY_REQUEST_TIMEOUT' });
		expect(error.message).not.toContain('private-target');
	});
	it('identifies a remote request made after destroy', async () => {
		const { client, value } = registry();
		await route(client, method, 'private-target');
		await value.destroy();
		await expect(value[method]('private-target', 'event')).rejects.toMatchObject({ name: 'Error', code: 'REGISTRY_DESTROYED' });
	});
	it('identifies capacity refusal and terminal rejection of admitted requests', async () => {
		const { client, value } = registry();
		await route(client, method, 'private-target');
		const publish = vi.spyOn(client.redis, 'publish');
		const first = outcome(value[method]('private-target', 'event', null, { timeoutMs: 60000 }));
		const second = outcome(value[method]('private-target', 'event', null, { timeoutMs: 60000 }));
		await vi.waitFor(() => expect(publish).toHaveBeenCalledTimes(2));
		await expect(value[method]('private-target', 'event')).rejects.toMatchObject({ code: 'REGISTRY_PENDING_LIMIT' });
		await value.destroy();
		for (const error of await Promise.all([first, second])) expect(error).toMatchObject({ code: 'REGISTRY_DESTROYED' });
	});
});

it('distinguishes a remote handler failure without changing the wire envelope', async () => {
	const { client, value } = registry();
	const owner = createConnectionRegistry(client, { identify: () => 'user', heartbeat: 60000 });
	cleanup.push(() => owner.destroy());
	const ws = mockWs({});
	ws.getUserData()[WS_SESSION_ID] = 'transport';
	const platform = mockPlatform();
	platform.request = async () => { throw new Error('handler rejected'); };
	await owner.hooks.open(ws, { platform });
	await expect(value.request('user', 'event')).rejects.toMatchObject({ code: 'REGISTRY_REMOTE_ERROR', message: 'handler rejected' });
});

function workerPool() {
	const pool = createWorkerPool({ path: 'file:///unused-handler.js', pool: { size: 1, idleTimeout: 0 } }, 'job');
	cleanup.push(() => pool.destroy());
	return pool;
}
it('preserves RangeError while exposing worker admission capacity', () => {
	const pool = workerPool();
	const permit = pool.reserveThread(1);
	try {
		expect(() => pool.reserveThread(1)).toThrow(expect.objectContaining({ name: 'RangeError', code: 'TASK_WORKER_CAPACITY_EXCEEDED' }));
	} finally { permit.release(); }
});
it('identifies worker admission and queued execution after shutdown', async () => {
	const pool = workerPool();
	const active = outcome(pool.run({ input: null }));
	const queued = outcome(pool.run({ input: null }));
	pool.destroy();
	for (const error of await Promise.all([active, queued, outcome(pool.run({ input: null }))])) expect(error).toMatchObject({ code: 'TASK_RUNNER_DESTROYED' });
	try { pool.reserve(1); throw new Error('expected refusal'); } catch (error) { expect(error.code).toBe('TASK_RUNNER_DESTROYED'); }
});
it('distinguishes an expired worker reservation from a capacity refusal', async () => {
	const pool = workerPool();
	const permit = pool.reserve(1);
	permit.release();
	await expect(pool.run({ input: null }, permit)).rejects.toMatchObject({ code: 'TASK_WORKER_ADMISSION_EXPIRED' });
});
it('identifies a reservation already executing and an unexpected worker exit', async () => {
	const pool = workerPool();
	const permit = pool.reserve(1);
	const pending = outcome(pool.run({ input: null }, permit));
	await expect(pool.run({ input: null }, permit)).rejects.toMatchObject({ code: 'TASK_WORKER_RESERVATION_IN_USE' });
	const { Worker } = await import('node:worker_threads');
	Worker.instances.at(-1).emit('exit', 1);
	expect(await pending).toMatchObject({ code: 'TASK_WORKER_EXITED' });
});
it('codes the worker abort fallback while preserving supplied abort reasons', async () => {
	const pool = workerPool();
	await expect(pool.run({ input: null, signal: { aborted: true } })).rejects.toMatchObject({ code: 'OPERATION_ABORTED' });
	const reason = new Error('caller cancelled');
	await expect(pool.run({ input: null, signal: { aborted: true, reason } })).rejects.toBe(reason);
});
it('identifies a PostgreSQL consumer created during shutdown', () => {
	const client = {};
	registerPgLifecycle(client, { closing: true, stoppers: new Set() });
	expect(() => onPgShutdown(client, () => {})).toThrow(expect.objectContaining({ code: 'PG_CLIENT_CLOSING' }));
});
it('identifies incomplete erasure while retaining failures and successful counts', async () => {
	const cause = new Error('backend refused');
	const forget = createForgetStore({ completed: { purgeUser: async () => 3 }, refused: { purgeUser: async () => { throw cause; } } });
	const error = await outcome(forget.purgeUser(null, 'private-user'));
	expect(error).toMatchObject({ code: 'ERASURE_INCOMPLETE', partialCounts: { completed: 3 }, failures: [cause] });
	expect(error.message).not.toContain('private-user');
});
it('codes rejected queued commands when a reconnecting subscriber is ended', () => {
	let rejected;
	const connection = new EventEmitter();
	Object.assign(connection, { status: 'reconnecting', disconnect() {}, flushQueue(error) { rejected = error; }, setStatus(status) { this.status = status; } });
	closeConnection(connection);
	expect(rejected).toMatchObject({ code: 'REDIS_CONNECTION_CLOSED', message: 'Connection is closed.' });
});
it('identifies the sharded topic bound after successful follows', async () => {
	const bus = createShardedBus(mockRedisClient());
	cleanup.push(() => bus.deactivate());
	await bus.activate(mockPlatform());
	await bus.follow('one');
	await bus.follow('two');
	await expect(bus.follow('private-third')).rejects.toMatchObject({ code: 'SHARDED_TOPIC_CAPACITY_EXCEEDED' });
});
it('identifies the cursor topic bound without including the topic', async () => {
	const cursor = createCursor(mockRedisClient());
	cleanup.push(() => cursor.destroy());
	const ws = mockWs({});
	const platform = mockPlatform();
	cursor.update(ws, 'one', { x: 1, y: 1 }, platform);
	cursor.update(ws, 'two', { x: 1, y: 1 }, platform);
	try { cursor.update(ws, 'private-third', { x: 1, y: 1 }, platform); throw new Error('expected refusal'); }
	catch (error) { expect(error.code).toBe('CURSOR_TOPIC_CAPACITY_EXCEEDED'); expect(error.message).not.toContain('private-third'); }
});
it('identifies the cursor connection-state bound', () => {
	const cursor = createCursor(mockRedisClient());
	cleanup.push(() => cursor.destroy());
	for (let i = 0; i < 2; i++) cursor.update(mockWs({}), 'room', { x: i, y: i }, mockPlatform());
	expect(() => cursor.update(mockWs({}), 'room', { x: 3, y: 3 }, mockPlatform())).toThrow(expect.objectContaining({ code: 'CURSOR_CONNECTION_CAPACITY_EXCEEDED' }));
});
it('codes a Cluster publisher outage delivered to the breaker', async () => {
	const client = mockRedisClient();
	client.redis.nodes = () => [];
	const failure = vi.fn();
	const breaker = { guard() {}, success() {}, failure, onChange() { return () => {}; }, get isHealthy() { return true; } };
	const bus = createPubSubBus(client, { breaker });
	cleanup.push(() => bus.deactivate());
	bus.wrap(mockPlatform()).publish('room', 'event', null);
	await vi.waitFor(() => expect(failure).toHaveBeenCalled());
	expect(failure.mock.calls[0][0]).toMatchObject({ code: 'REDIS_CLUSTER_PUBLISHER_UNAVAILABLE' });
});
it.each(['follow', 'followBatch'])('codes %s before bus activation', async method => {
	const bus = createShardedBus(mockRedisClient());
	cleanup.push(() => bus.deactivate());
	await expect(bus[method](method === 'follow' ? 'room' : ['room'])).rejects.toMatchObject({ code: 'BUS_NOT_ACTIVE' });
});
it.each([createPubSubBus, createShardedBus])('preserves RangeError for refused relay envelopes', async createBus => {
	const bus = createBus(mockRedisClient(), { maxEnvelopeBytes: 320 });
	cleanup.push(() => bus.deactivate());
	expect(() => bus.wrap(mockPlatform()).publishBatched([{ topic: 'room', event: 'data', data: 'x'.repeat(500) }])).toThrow(expect.objectContaining({ name: 'RangeError', code: 'RELAY_ENVELOPE_TOO_LARGE' }));
});
