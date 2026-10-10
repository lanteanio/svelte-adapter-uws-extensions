/**
 * Integration tests for subscriber connections whose server stops answering,
 * against a real Redis server and, with INTEGRATION_BACKEND=cluster, a real
 * Redis Cluster.
 *
 * CLIENT PAUSE ... ALL makes a server hold every command it receives while
 * its connections stay open, which is what a subscriber sees of a host that
 * died without closing its sockets: nothing arrives, and nothing says so.
 * These tests pin that the subscriber notices within the PING timeout,
 * reports it, and delivers again: on a cluster from another node while the
 * paused one still holds everything, on a standalone server once it answers.
 *
 * The PING rounds run on demand through the runtime seam; the 10 second
 * timeout is real.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { captureRegistered } from '../../helpers/diagnostic-line.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 5000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(50);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

// Longer than the PING timeout, so the subscriber gives up while the server
// still holds its commands.
const PAUSE_MS = 14000;

describe('subscriber connections whose server stops answering (integration)', () => {
	let admin;
	/** @type {Array<() => Promise<unknown>>} */
	const cleanups = [];
	let sink = null;
	const unhandled = vi.fn();
	/** @type {Array<{ cb: () => unknown, cleared: boolean }>} */
	let intervals = [];

	/** The server connections: every node of a Cluster, or the one server. */
	const servers = () => (isClusterBackend() ? admin.redis.nodes('all') : [admin.redis]);
	/** The one server connection for which `count(server)` is positive. */
	async function serverWith(count) {
		const found = [];
		for (const server of servers()) {
			if ((await count(server)) > 0) found.push(server);
		}
		expect(found).toHaveLength(1);
		return found[0];
	}
	const numsub = async (server, channel) => Number((await server.call('PUBSUB', 'NUMSUB', channel))[1]);
	/** Ids of the server's connections holding a channel. */
	const subscriberIds = async (server) => String(await server.call('CLIENT', 'LIST', 'TYPE', 'pubsub'))
		.split('\n')
		.filter((line) => / sub=[1-9]/.test(line))
		.map((line) => /^id=(\d+)/.exec(line)?.[1]);

	/** Hold every command on `server` for PAUSE_MS; the test waits it out before the next one. */
	async function pause(server) {
		await server.call('CLIENT', 'PAUSE', String(PAUSE_MS), 'ALL');
		// Answered once the pause is over.
		cleanups.unshift(() => server.ping());
		return Date.now() + PAUSE_MS;
	}
	/** One PING round of every subscriber, as its 30 second interval would run it. */
	const pingRound = () => {
		for (const t of intervals) if (!t.cleared) t.cb();
	};
	const silences = () => sink.records.filter((r) => r.event === 'redis.subscriber.silent');

	beforeAll(async () => {
		admin = createBackendClient({ keyPrefix: 'inttest-live:' });
		if (isClusterBackend()) await admin.redis.ping();
		process.on('unhandledRejection', unhandled);
	});

	beforeEach(() => {
		intervals = [];
		setRuntimeEnv({ timers: {
			setInterval: (cb, ms) => { const t = { cb, ms, cleared: false, unref() {} }; intervals.push(t); return t; },
			clearInterval: (t) => { if (t) t.cleared = true; }
		} });
		sink = captureRegistered();
	});

	afterEach(async () => {
		sink?.stop();
		sink = null;
		while (cleanups.length > 0) await cleanups.shift()().catch(() => {});
		resetRuntimeEnv();
	});

	afterAll(async () => {
		process.off('unhandledRejection', unhandled);
		await admin.quit();
	});

	it('pub/sub bus: a subscriber whose server stops answering is reported and delivers again', async () => {
		const channel = 'inttest-live:pubsub:' + Date.now();
		// Exercise the PING watchdog after the shorter command deadline, without
		// the independent socket deadline closing the connection first.
		const client = createBackendClient({ keyPrefix: 'inttest-live:', options: { socketTimeout: undefined } });
		cleanups.push(() => client.quit());
		const platform = mockPlatform();
		const bus = createPubSubBus(client, { channel });
		cleanups.push(() => bus.deactivate());
		await bus.activate(platform);
		const holder = await serverWith((server) => numsub(server, channel));
		const [before] = await subscriberIds(holder);
		expect(before).toBeDefined();

		const pausedUntil = await pause(holder);
		pingRound();
		await waitFor(() => silences().length > 0, 12000);
		expect(silences()[0].attributes).toEqual({ owner: 'pubsub', timeoutMs: 10000 });

		// On a cluster another node takes the channel while the paused one still
		// holds every command; a standalone server delivers again once it answers.
		const publisher = isClusterBackend() ? admin.redis.nodes('master').find((n) => n !== holder) : admin.redis;
		const envelope = JSON.stringify({ instanceId: 'remote', topic: 'chat', event: 'back', data: 1 });
		await waitFor(async () => {
			await publisher.publish(channel, envelope);
			return platform.published.some((p) => p.event === 'back');
		}, PAUSE_MS + 5000);
		if (isClusterBackend()) {
			expect(Date.now()).toBeLessThan(pausedUntil);
		} else {
			// Delivered by a new connection: the silent one was closed, not waited out.
			expect(await subscriberIds(holder)).not.toContain(before);
		}
		expect(silences()).toHaveLength(1);
		expect(unhandled).not.toHaveBeenCalled();
	}, 45000);
});
