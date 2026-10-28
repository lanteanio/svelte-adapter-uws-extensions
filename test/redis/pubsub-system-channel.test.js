// The pub/sub bus's open hook on the adapter's real test server: a real
// uWebSockets.js server and WS client from svelte-adapter-uws/testing. The
// bus subscribes each connection to its systemChannel with the adapter's
// tracked server-side helper, independently of application subscribe hooks.
import { describe, it, expect, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

let uWS;
try {
	uWS = (await import('uWebSockets.js')).default;
} catch {
	uWS = null;
}
const describeUWS = uWS ? describe : describe.skip;

const EVENT = 'redis.pubsub.system-channel.refused';

/** @type {Array<() => unknown>} */
const cleanups = [];
afterEach(async () => {
	for (const fn of cleanups.splice(0).reverse()) {
		try { await fn(); } catch { /* already closed */ }
	}
	vi.restoreAllMocks();
});

const settle = () => new Promise((r) => setTimeout(r, 40));

/** A client that records every JSON frame it receives. */
async function connect(url) {
	const { WebSocket } = await import('ws');
	const ws = new WebSocket(url);
	const messages = [];
	ws.on('message', (raw, isBinary) => {
		if (isBinary) return;
		try { messages.push(JSON.parse(raw.toString())); } catch { /* not JSON */ }
	});
	await new Promise((resolve, reject) => {
		ws.on('open', resolve);
		ws.on('error', reject);
	});
	cleanups.push(() => ws.close());
	return { messages };
}

/** The bus's open hook and the given subscribe hook on a real test server. */
async function start(subscribe) {
	const { createTestServer } = await import('svelte-adapter-uws/testing');
	const breaker = createCircuitBreaker({ failureThreshold: 1 });
	const bus = createPubSubBus(mockRedisClient('system-channel:'), { breaker });
	const server = await createTestServer({ handler: { open: (ws, ctx) => bus.hooks.open(ws, ctx), subscribe } });
	cleanups.push(() => server.close(), () => bus.deactivate());
	return { server, breaker };
}

/** The refusal lines printed so far. */
function refusals() {
	const lines = [];
	vi.spyOn(console, 'warn').mockImplementation((line) => {
		if (typeof line === 'string' && line.includes('event=' + EVENT)) lines.push(line);
	});
	return lines;
}

describeUWS('pub/sub bus systemChannel through the real adapter', () => {
	it('a subscribe hook that allows the channel gets degraded delivered, and nothing is reported', async () => {
		const lines = refusals();
		const { server, breaker } = await start(() => null);
		const client = await connect(server.wsUrl);
		await settle();

		breaker.failure();
		await settle();

		expect(client.messages.filter((m) => m.topic === '__realtime' && m.event === 'degraded')).toHaveLength(1);
		expect(lines).toEqual([]);
	});

	it('an application subscribe refusal does not block the bus-owned channel', async () => {
		const lines = refusals();
		let asked = 0;
		const { server, breaker } = await start(() => { asked++; return 'FORBIDDEN'; });
		const client = await connect(server.wsUrl);
		await settle();

		expect(asked).toBe(0);
		expect(lines).toEqual([]);
		breaker.failure();
		await settle();
		expect(client.messages.filter((m) => m.topic === '__realtime' && m.event === 'degraded')).toHaveLength(1);
	});

	it('an application subscribeBatch refusal does not block the bus-owned channel', async () => {
		const lines = refusals();
		const { createTestServer } = await import('svelte-adapter-uws/testing');
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const bus = createPubSubBus(mockRedisClient('system-channel:'), { breaker });
		let asked = 0;
		const server = await createTestServer({
			handler: {
				open: (ws, ctx) => bus.hooks.open(ws, ctx),
				subscribe: () => null,
				subscribeBatch: () => { asked++; return {}; }
			}
		});
		cleanups.push(() => server.close(), () => bus.deactivate());
		const client = await connect(server.wsUrl);
		await settle();

		expect(asked).toBe(0);
		expect(lines).toEqual([]);
		breaker.failure();
		await settle();
		expect(client.messages.filter((m) => m.topic === '__realtime' && m.event === 'degraded')).toHaveLength(1);
	});
});
