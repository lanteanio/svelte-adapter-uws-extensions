import { describe, expect, it } from 'vitest';
import { AsyncLocalStorage } from 'node:async_hooks';
import { EventEmitter } from 'node:events';
import { trace, resolveTraceContext } from '../../node_modules/svelte-adapter-uws/src/runtime/tracing.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createNotifyBridge } from '../../src/postgres/notify.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

const activation = { traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01' };
const producer = { traceparent: '00-fedcba9876543210fedcba9876543210-fedcba9876543210-01' };
const carriers = [undefined, 'invalid', producer.traceparent];

function recordingPlatform() {
	const platform = mockPlatform();
	const spans = [];
	// Use the installed adapter's context storage and null-boundary behavior.
	platform.trace = {
		...trace,
		enabled: true,
		run(name, options, fn) {
			const parent = options.parent ?? trace.current();
			spans.push({ name, parent });
			return trace.withContext(parent, () => fn(null));
		}
	};
	Object.defineProperty(platform, 'traceContext', {
		get: () => resolveTraceContext({ connectionTraceContext: activation })
	});
	return { platform, spans };
}

function observeDelivery(platform) {
	const observed = [];
	const descendants = [];
	const capture = () => {
		observed.push([platform.trace.current(), platform.traceContext]);
		descendants.push(Promise.resolve().then(() => [platform.trace.current(), platform.traceContext]));
		platform.trace.run('nested.publish', { kind: 'producer', parent: null }, () => {});
	};
	for (const method of ['publish', 'publishBatched']) {
		const original = platform[method].bind(platform);
		platform[method] = (...args) => { capture(); return original(...args); };
	}
	return { observed, descendants };
}

function captureSubscriberContext(client, platform) {
	const opened = [];
	const duplicate = client.redis.duplicate.bind(client.redis);
	client.redis.duplicate = (...args) => {
		opened.push([trace.current(), platform.traceContext]);
		const sub = duplicate(...args);
		const on = sub.on.bind(sub);
		sub.on = (event, fn) => {
			const resume = AsyncLocalStorage.snapshot();
			return on(event, (...values) => resume(fn, ...values));
		};
		return sub;
	};
	return opened;
}

for (const sharded of [false, true]) {
	for (const batched of [false, true]) {
		describe(`${sharded ? 'sharded' : 'pubsub'} ${batched ? 'batched' : 'single'} consumer boundary`, () => {
			it.each(carriers)('isolates delivery carrying %s across async descendants', async (traceparent) => {
				const { platform, spans } = recordingPlatform();
				const { observed, descendants } = observeDelivery(platform);
				const client = mockRedisClient();
				const opened = captureSubscriberContext(client, platform);
				const bus = (sharded ? createShardedBus : createPubSubBus)(client);
				await trace.withContext(activation, () => bus.activate(platform));
				if (sharded) await bus.follow('chat');
				try {
					expect(opened).toEqual([[null, null]]);
					const body = { topic: 'chat', event: 'message', data: 1 };
					const envelope = { instanceId: 'peer', traceparent, ...(batched ? { batch: [body] } : body) };
					if (sharded) await client.redis.spublish('uws:sharded:chat', JSON.stringify(envelope));
					else await client.redis.publish('uws:pubsub', JSON.stringify(envelope));
					const expected = traceparent === producer.traceparent ? producer : null;
					expect(platform.published).toHaveLength(1);
					expect(observed).toEqual([[expected, expected]]);
					expect(await Promise.all(descendants)).toEqual([[expected, expected]]);
					expect(spans.filter((s) => s.name === 'nested.publish')).toEqual([{ name: 'nested.publish', parent: expected }]);
					expect(spans.filter((s) => s.name.endsWith('.receive'))).toEqual(expected === null ? [] : [{
						name: sharded ? 'redis.sharded-pubsub.receive' : 'redis.pubsub.receive', parent: producer
					}]);
				} finally { await bus.deactivate(); }
			});
		});
	}
}

for (const sharded of [false, true]) {
	it.each(['ambient', 'connection fallback'])(`clears a later %s outside ${sharded ? 'sharded' : 'pubsub'} socket startup`, async (origin) => {
		const { platform, spans } = recordingPlatform();
		const { observed, descendants } = observeDelivery(platform);
		const client = mockRedisClient();
		const bus = (sharded ? createShardedBus : createPubSubBus)(client);
		await trace.withContext(activation, () => bus.activate(platform));
		if (sharded) await bus.follow('chat');
		try {
			const message = JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'message', data: 1 });
			const emit = () => sharded
				? client.redis.spublish('uws:sharded:chat', message)
				: client.redis.publish('uws:pubsub', message);
			// This emitter deliberately does not preserve the socket's startup
			// context: a later callback still has to clear its actual parent.
			if (origin === 'ambient') await trace.withContext(producer, emit);
			else await emit();
			expect(platform.published).toHaveLength(1);
			expect(observed).toEqual([[null, null]]);
			expect(await Promise.all(descendants)).toEqual([[null, null]]);
			expect(spans.filter((span) => span.name === 'nested.publish')).toEqual([{ name: 'nested.publish', parent: null }]);
		} finally { await bus.deactivate(); }
	});
}

function notificationClient() {
	const connection = new EventEmitter();
	let resume;
	connection.connect = async () => { resume = AsyncLocalStorage.snapshot(); };
	connection.query = async () => ({ rows: [{ acquired: true }] });
	connection.end = async () => {};
	return {
		createClient: () => connection,
		// The session probe goes out through the query route and comes back on
		// the pinned session, outside any traced context.
		query: async (text, values) => {
			if (/pg_notify/.test(text)) queueMicrotask(() => connection.emit('notification', { channel: values[0], payload: values[1] }));
			return { rows: [], rowCount: 0 };
		},
		emit: (payload) => resume(() => connection.emit('notification', { channel: 'changes', payload }))
	};
}

for (const advisory of [false, true]) {
	describe(`notify ${advisory ? 'advisory' : 'all'} consumer boundary`, () => {
		it.each(carriers)('isolates delivery and bus relay carrying %s', async (traceparent) => {
			const { platform, spans } = recordingPlatform();
			const { observed, descendants } = observeDelivery(platform);
			const redis = mockRedisClient();
			const bus = createPubSubBus(redis);
			await bus.activate(platform);
			const client = notificationClient();
			const bridge = createNotifyBridge(client, {
				channel: 'changes', ...(advisory ? { multiListener: 'advisory', lockId: 42 } : {})
			});
			await trace.withContext(activation, () => bridge.activate(bus.wrap(platform)));
			try {
				client.emit(JSON.stringify({ topic: 'chat', event: 'message', data: 1, traceparent }));
				const expected = traceparent === producer.traceparent ? producer : null;
				expect(platform.published).toHaveLength(1);
				expect(observed).toEqual([[expected, expected]]);
				expect(await Promise.all(descendants)).toEqual([[expected, expected]]);
				expect(spans.filter((s) => s.name === 'postgres.notify.receive')).toEqual(expected === null ? [] : [{ name: 'postgres.notify.receive', parent: producer }]);
				if (advisory) expect(spans.find((s) => s.name === 'redis.pubsub.publish').parent).toEqual(expected);
			} finally { await bridge.deactivate(); await bus.deactivate(); }
		});
	});
}

for (const recovery of [false, true]) {
	describe(`task ${recovery ? 'recovery' : 'dispatch'} consumer boundary`, () => {
		it.each(carriers)('isolates a durable attempt carrying %s from its timer', async (traceparent) => {
			const { platform, spans } = recordingPlatform();
			const ticks = new Map();
			setRuntimeEnv({ timers: {
				setInterval: (fn, ms) => {
					const resume = AsyncLocalStorage.snapshot();
					ticks.set(ms, () => resume(fn));
					return { unref() {} };
				},
				clearInterval() {}
			} });
			const client = mockPgClient();
			const runner = trace.withContext(activation, () => createTaskRunner(client, {
				platform, recoveryInterval: recovery ? 1000 : 0,
				dispatchInterval: recovery ? 0 : 1000, cleanupInterval: 0
			}));
			const observed = [];
			runner.register('worker', async ({ traceContext }) => {
				observed.push([traceContext, platform.trace.current(), platform.traceContext]);
				await Promise.resolve();
				observed.push([traceContext, platform.trace.current(), platform.traceContext]);
				platform.trace.run('nested.publish', { kind: 'producer', parent: null }, () => {});
				return 'done';
			});
			client._getTaskRows().set('pending-work', {
				id: 'pending-work', name: 'worker', input: null, idempotency_key: null,
				status: recovery ? 'running' : 'pending', result: null, error: null,
				fence: 'expired', fence_expires_at: Date.now() - 10000, attempts: 1,
				created_at: Date.now() - 60000, updated_at: Date.now() - 60000, traceparent
			});
			try {
				await ticks.get(1000)();
				await expect.poll(() => client._getTaskRows().get('pending-work').status).toBe('committed');
				const expected = traceparent === producer.traceparent ? producer : null;
				expect(observed).toEqual([[expected, expected, expected], [expected, expected, expected]]);
				expect(spans.find((s) => s.name === 'postgres.tasks.execute').parent).toEqual(expected);
				expect(spans.find((s) => s.name === 'nested.publish').parent).toEqual(expected);
			} finally { runner.destroy(); resetRuntimeEnv(); }
		});
	});
}
