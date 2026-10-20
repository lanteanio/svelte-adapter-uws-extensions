// The pub/sub bus's open hook when the bus cannot activate: Redis refuses the
// SUBSCRIBE, or the breaker is open. The hook is the application's
// `hooks.ws.open`, and adapters call it without awaiting it: one that rejects
// is an unhandled rejection on an adapter that leaves it alone, and a closed
// connection (1011) on one that catches it. Local delivery works without the
// bus, so a connection must stay open, the failure is reported once per
// failure streak, and the bus activates on a later open or when the breaker
// lets a probe through.
import { describe, it, expect, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { registeredRecord } from '../helpers/diagnostic-line.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';

let uWS;
try {
	uWS = (await import('uWebSockets.js')).default;
} catch {
	uWS = null;
}
const describeUWS = uWS ? describe : describe.skip;

const EVENT = 'redis.pubsub.subscribe.failed';
const REMOTE = JSON.stringify({ instanceId: 'other-instance', topic: '__realtime', event: 'remote', data: 1 });

/** @type {Array<() => unknown>} */
const cleanups = [];
afterEach(async () => {
	for (const fn of cleanups.splice(0).reverse()) {
		try { await fn(); } catch { /* already closed */ }
	}
	vi.restoreAllMocks();
});

const settle = (ms = 40) => new Promise((r) => setTimeout(r, ms));

/** The failure lines printed so far. */
function reports() {
	const lines = [];
	const capture = (line) => {
		if (typeof line === 'string' && line.includes('event=' + EVENT)) lines.push(line);
	};
	vi.spyOn(console, 'warn').mockImplementation(capture);
	vi.spyOn(console, 'error').mockImplementation(capture);
	return lines;
}

/** A client whose subscriber connections refuse SUBSCRIBE while `refuse.on`. */
function refusingClient(refuse) {
	const client = mockRedisClient('open-activation:');
	const subscribers = [];
	const duplicate = client.duplicate.bind(client);
	client.duplicate = (overrides) => {
		const sub = duplicate(overrides);
		const subscribe = sub.subscribe.bind(sub);
		sub.subscribe = (...channels) => (refuse.on ? Promise.reject(new Error('subscribe refused')) : subscribe(...channels));
		sub.disconnect = vi.fn();
		subscribers.push(sub);
		return sub;
	};
	return { client, subscribers };
}

/** A client that records every JSON frame it receives and how it closed. */
async function connect(url) {
	const { WebSocket } = await import('ws');
	const ws = new WebSocket(url);
	const state = { messages: [], closed: null, ws };
	ws.on('message', (raw, isBinary) => {
		if (isBinary) return;
		try { state.messages.push(JSON.parse(raw.toString())); } catch { /* not JSON */ }
	});
	ws.on('close', (code) => { state.closed = code; });
	await new Promise((resolve, reject) => {
		ws.on('open', resolve);
		ws.on('error', reject);
	});
	cleanups.push(() => ws.close());
	return state;
}

async function serve(bus) {
	const { createTestServer } = await import('svelte-adapter-uws/testing');
	const server = await createTestServer({ handler: { open: (ws, ctx) => bus.hooks.open(ws, ctx), subscribe: () => null } });
	cleanups.push(() => server.close(), () => bus.deactivate());
	return server;
}

describe('pub/sub bus open hook when activation fails', () => {
	it('resolves, so an adapter that does not catch it sees no unhandled rejection', async () => {
		const lines = reports();
		const refuse = { on: true };
		const { client } = refusingClient(refuse);
		const bus = createPubSubBus(client);
		cleanups.push(() => bus.deactivate());
		const platform = mockPlatform();
		const escaped = [];
		const onUnhandled = (reason) => { escaped.push(reason); };
		process.on('unhandledRejection', onUnhandled);
		try {
			await expect(bus.hooks.open({ id: 1 }, { platform })).resolves.toBeUndefined();
			await expect(bus.hooks.open({ id: 2 }, { platform })).resolves.toBeUndefined();
			await settle(5);
			expect(escaped).toEqual([]);
		} finally {
			process.off('unhandledRejection', onUnhandled);
		}
		expect(lines).toHaveLength(1);
		const record = registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PUBSUB_SUBSCRIBE);
		expect(record.event).toBe(EVENT);
	});

	it('resolves while the breaker is open', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		cleanups.push(() => breaker.destroy());
		const bus = createPubSubBus(mockRedisClient('open-activation:'), { breaker });
		cleanups.push(() => bus.deactivate());
		breaker.failure(new Error('down'));
		reports();
		await expect(bus.hooks.open({ id: 1 }, { platform: mockPlatform() })).resolves.toBeUndefined();
	});

	it('retries on a timer while no connection opens', async () => {
		vi.useFakeTimers();
		try {
			const lines = reports();
			const refuse = { on: true };
			const { client } = refusingClient(refuse);
			const bus = createPubSubBus(client);
			cleanups.push(() => bus.deactivate());
			const platform = mockPlatform();
			await bus.hooks.open({ id: 1 }, { platform });
			refuse.on = false;
			await vi.advanceTimersByTimeAsync(1000);
			await client.redis.publish('uws:pubsub', REMOTE);
			expect(platform.published.filter((p) => p.event === 'remote')).toHaveLength(1);
			expect(lines).toHaveLength(1);
		} finally {
			vi.useRealTimers();
		}
	});

	it('stops retrying after deactivate()', async () => {
		vi.useFakeTimers();
		try {
			reports();
			const refuse = { on: true };
			const { client, subscribers } = refusingClient(refuse);
			const bus = createPubSubBus(client);
			await bus.hooks.open({ id: 1 }, { platform: mockPlatform() });
			await bus.deactivate();
			await vi.advanceTimersByTimeAsync(120000);
			expect(subscribers).toHaveLength(1);
		} finally {
			vi.useRealTimers();
		}
	});
});

describeUWS('pub/sub bus open hook through the real adapter when activation fails', () => {
	it('keeps the connection open, reports once, and activates on a later open', async () => {
		const lines = reports();
		const refuse = { on: true };
		const { client } = refusingClient(refuse);
		const bus = createPubSubBus(client);
		const server = await serve(bus);

		const first = await connect(server.wsUrl);
		const second = await connect(server.wsUrl);
		await settle();
		expect(first.closed).toBe(null);
		expect(second.closed).toBe(null);
		expect(lines).toHaveLength(1);

		refuse.on = false;
		const third = await connect(server.wsUrl);
		await settle();
		await client.redis.publish('uws:pubsub', REMOTE);
		await settle();
		for (const c of [first, second, third]) {
			expect(c.closed).toBe(null);
			expect(c.messages.filter((m) => m.topic === '__realtime' && m.event === 'remote')).toHaveLength(1);
		}
	});

	it('keeps the connection open while the breaker is open and activates when it lets a probe through', async () => {
		reports();
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
		cleanups.push(() => breaker.destroy());
		const client = mockRedisClient('open-activation:');
		const bus = createPubSubBus(client, { breaker });
		const server = await serve(bus);
		breaker.failure(new Error('down'));

		const conn = await connect(server.wsUrl);
		await settle(10);
		expect(conn.closed).toBe(null);
		expect(breaker.state).toBe('broken');

		await settle(150);
		expect(breaker.state).toBe('healthy');
		await client.redis.publish('uws:pubsub', REMOTE);
		await settle();
		expect(conn.closed).toBe(null);
		expect(conn.messages.filter((m) => m.topic === '__realtime' && m.event === 'remote')).toHaveLength(1);
		expect(conn.messages.filter((m) => m.topic === '__realtime' && m.event === 'recovered')).toHaveLength(1);
	});
});
