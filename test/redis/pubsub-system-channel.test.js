// The pub/sub bus's open hook on the adapter's real test server: a real
// uWebSockets.js server and WS client from svelte-adapter-uws/testing. The
// bus subscribes each connection to its systemChannel with the platform's
// subscribe, which runs the application's subscribe hook, so that hook decides
// whether a connection receives the bus's degraded / recovered events.
import { describe, it, expect, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
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

	it('a subscribe hook that refuses the channel is reported once, and that connection gets no degraded', async () => {
		const lines = refusals();
		// Refuse the first two connections' systemChannel subscribe and allow
		// the third, which shows degraded is published on this server.
		let asked = 0;
		const { server, breaker } = await start((ws, topic) => (topic === '__realtime' && asked++ < 2 ? 'FORBIDDEN' : null));
		const refused = await connect(server.wsUrl);
		await settle();
		await connect(server.wsUrl);
		await settle();
		const allowed = await connect(server.wsUrl);
		await settle();

		expect(asked).toBe(3);
		expect(lines).toHaveLength(1);
		expect(registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PUBSUB_SYSTEM_CHANNEL_REFUSED).attributes).toEqual({ reason: 'FORBIDDEN' });

		breaker.failure();
		await settle();
		expect(allowed.messages.filter((m) => m.topic === '__realtime' && m.event === 'degraded')).toHaveLength(1);
		expect(refused.messages.filter((m) => m.topic === '__realtime')).toEqual([]);
	});

	it('a subscribeBatch hook decides the systemChannel subscribe when the app exports one', async () => {
		const lines = refusals();
		const { createTestServer } = await import('svelte-adapter-uws/testing');
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const bus = createPubSubBus(mockRedisClient('system-channel:'), { breaker });
		const server = await createTestServer({
			handler: {
				open: (ws, ctx) => bus.hooks.open(ws, ctx),
				subscribe: () => null,
				subscribeBatch: (ws, topics) => Object.fromEntries(topics.map((t) => [t, t === '__realtime' ? false : true]))
			}
		});
		cleanups.push(() => server.close(), () => bus.deactivate());
		await connect(server.wsUrl);
		await settle();

		expect(lines).toHaveLength(1);
		expect(registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PUBSUB_SYSTEM_CHANNEL_REFUSED).attributes).toEqual({ reason: 'FORBIDDEN' });
	});
});
