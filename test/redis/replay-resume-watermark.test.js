// End-to-end recovery-barrier contract: the REAL adapter message loop (a real
// uWebSockets.js server + a real WS client via svelte-adapter-uws/testing)
// wired to the REAL extensions resume hook over the Redis replay store. A
// publish that lands DURING the async resume window (past the backend read,
// before the connection subscribes to live) must reach the client EXACTLY
// once: the store's gap-fill covers it and reports the covered watermark, so
// the adapter's cutover flush skips the live frame it buffered for the same
// seq. The window is made deterministic by gating the store's Redis read (the
// exact I/O boundary where real network latency opens the window) on a pair
// of deferreds: the test publishes while the read is held, then releases it.
import { describe, it, expect, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createReplay } from '../../src/redis/replay.js';

let uWS;
try {
	uWS = (await import('uWebSockets.js')).default;
} catch {
	uWS = null;
}
const describeUWS = uWS ? describe : describe.skip;

/** @type {Array<{ close: () => Promise<void> | void }>} */
const servers = [];
/** @type {Array<{ close: () => void }>} */
const clients = [];

afterEach(async () => {
	for (const c of clients.splice(0)) { try { c.close(); } catch { /* already closed */ } }
	for (const s of servers.splice(0)) { try { await s.close(); } catch { /* already closed */ } }
});

function deferred() {
	let resolve;
	let reject;
	const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
	return { promise, resolve, reject };
}

async function connectClient(url) {
	const { WebSocket } = await import('ws');
	const ws = new WebSocket(url);
	const messages = [];
	const waiters = [];
	ws.on('message', (raw, isBinary) => {
		if (isBinary) return;
		let m;
		try { m = JSON.parse(raw.toString()); } catch { return; }
		messages.push(m);
		for (const w of waiters.slice()) {
			if (w.pred(m)) { waiters.splice(waiters.indexOf(w), 1); w.resolve(m); }
		}
	});
	await new Promise((resolve, reject) => {
		ws.on('open', resolve);
		ws.on('error', reject);
	});
	const client = {
		ws,
		messages,
		send: (frame) => ws.send(JSON.stringify(frame)),
		waitFor(pred, timeoutMs = 2000) {
			const existing = messages.find(pred);
			if (existing) return Promise.resolve(existing);
			return new Promise((resolve, reject) => {
				const w = { pred, resolve };
				waiters.push(w);
				setTimeout(() => {
					const i = waiters.indexOf(w);
					if (i >= 0) { waiters.splice(i, 1); reject(new Error('waitFor timeout')); }
				}, timeoutMs);
			});
		},
		close: () => { try { ws.close(); } catch { /* already closed */ } }
	};
	clients.push(client);
	return client;
}

// Let queued micro/macrotasks settle so a "did not arrive" assertion is real.
const settle = () => new Promise((r) => setTimeout(r, 40));

// Gate the NEXT store read on the mock redis so the resume hook parks mid-read.
// Both backends read through one pipeline (sorted-set: probe + range + counter;
// stream: range + counter, all in one RTT), so a single gate on the pipeline
// exec covers both. The gate resolves `entered` when the read is reached and
// holds it until `release` fires - standing in for the real-world network
// round trip to Redis, which is exactly where the recovery-barrier window
// lives.
function gateNextPipelineExec(client) {
	const entered = deferred();
	const release = deferred();
	const real = client.redis.pipeline.bind(client.redis);
	let armed = true;
	client.redis.pipeline = () => {
		const p = real();
		if (!armed) return p;
		armed = false;
		return new Proxy(p, {
			get(target, prop) {
				if (prop === 'exec') {
					return async () => { entered.resolve(); await release.promise; return target.exec(); };
				}
				return target[prop];
			}
		});
	};
	return { entered, release };
}

const BACKENDS = [
	{ name: 'sorted-set', options: {}, gate: gateNextPipelineExec },
	{ name: 'stream', options: { storage: 'stream' }, gate: gateNextPipelineExec }
];

describeUWS('resume covered watermark through the real adapter barrier', () => {
	for (const backend of BACKENDS) {
		it(`${backend.name}: a publish landing in the resume window reaches the client exactly once`, async () => {
			const { createTestServer } = await import('svelte-adapter-uws/testing');
			const client = mockRedisClient('t:');
			const replay = createReplay(client, backend.options);
			const server = await createTestServer({ handler: { resume: replay.resumeHook() } });
			servers.push(server);
			const c = await connectClient(server.wsUrl);

			// History the client missed while offline.
			await replay.publish(server.platform, 'room', 'tick', { n: 1 });
			await replay.publish(server.platform, 'room', 'tick', { n: 2 });

			const gate = backend.gate(client);
			c.send({ type: 'subscribe', topic: 'room', ref: 1, recover: { offset: 0 } });
			await gate.entered.promise;   // the hook is parked on the store read
			// Lands in the window: stored as seq 3 AND fanned out live, where the
			// adapter's barrier buffers it for this still-resuming connection.
			await replay.publish(server.platform, 'room', 'tick', { n: 3 });
			gate.release.resolve();
			await c.waitFor((m) => m.type === 'subscribed' && m.topic === 'room');

			// The gap-fill read ran after the release, so it covered seq 3 and the
			// hook reported { room: 3 }: the buffered live frame is skipped.
			const msgs = c.messages.filter((m) => m.topic === '__replay:room' && m.event === 'msg');
			expect(msgs.map((m) => m.data.seq)).toEqual([1, 2, 3]);
			await settle();
			expect(c.messages.filter((m) => m.topic === 'room' && m.event === 'tick')).toHaveLength(0);

			// Post-cutover the live path flows normally, still exactly once.
			await replay.publish(server.platform, 'room', 'tick', { n: 4 });
			const live = await c.waitFor((m) => m.topic === 'room' && m.event === 'tick');
			expect(live.data).toEqual({ n: 4 });
			expect(c.messages.filter((m) => m.topic === 'room' && m.event === 'tick')).toHaveLength(1);
		});
	}

	for (const backend of BACKENDS) {
		it(`${backend.name}: an offset above the counter never becomes a trusted watermark (in-window publish still arrives)`, async () => {
			const { createTestServer } = await import('svelte-adapter-uws/testing');
			const client = mockRedisClient('t:');
			const replay = createReplay(client, backend.options);
			const server = await createTestServer({ handler: { resume: replay.resumeHook() } });
			servers.push(server);
			const c = await connectClient(server.wsUrl);

			// The counter is at 2; the client claims 999. The offset is
			// client-controlled wire input (and the same shape arises honestly
			// when a failover to a lagging replica regresses the counter with
			// the epoch unchanged), so it must never become a trusted watermark.
			await replay.publish(server.platform, 'room', 'tick', { n: 1 });
			await replay.publish(server.platform, 'room', 'tick', { n: 2 });

			const gate = backend.gate(client);
			c.send({ type: 'subscribe', topic: 'room', ref: 1, recover: { offset: 999 } });
			await gate.entered.promise;
			// Seq 3 lands in the window - BELOW the claimed offset, so a trusted
			// 999 watermark would skip it at flush with no truncated marker.
			await replay.publish(server.platform, 'room', 'tick', { n: 3 });
			gate.release.resolve();
			await c.waitFor((m) => m.type === 'subscribed' && m.topic === 'room');
			await settle();

			// The unverifiable claim reported nothing, so the adapter fell back
			// to its conservative pre-window floor and the buffered live frame
			// flushed: the in-window publish is delivered, never silently lost.
			const gapFill = c.messages.filter((m) => m.topic === '__replay:room' && m.event === 'msg');
			expect(gapFill).toHaveLength(0);
			const live = c.messages.filter((m) => m.topic === 'room' && m.event === 'tick');
			expect(live).toHaveLength(1);
			expect(live[0].data).toEqual({ n: 3 });
		});
	}

	it('contrast: swallowing the watermark degrades to at-least-once (the duplicate the report exists to remove)', async () => {
		const { createTestServer } = await import('svelte-adapter-uws/testing');
		const client = mockRedisClient('t:');
		const replay = createReplay(client);
		const hook = replay.resumeHook();
		// Same real hook, same real store - but the return value is dropped, the
		// way a non-reporting backend behaves. The adapter then falls back to its
		// conservative pre-window floor and re-delivers the window frame.
		const server = await createTestServer({
			handler: { resume: async (ws, ctx) => { await hook(ws, ctx); } }
		});
		servers.push(server);
		const c = await connectClient(server.wsUrl);

		await replay.publish(server.platform, 'room', 'tick', { n: 1 });
		await replay.publish(server.platform, 'room', 'tick', { n: 2 });

		const gate = gateNextPipelineExec(client);
		c.send({ type: 'subscribe', topic: 'room', ref: 1, recover: { offset: 0 } });
		await gate.entered.promise;
		await replay.publish(server.platform, 'room', 'tick', { n: 3 });
		gate.release.resolve();
		await c.waitFor((m) => m.type === 'subscribed' && m.topic === 'room');
		await settle();

		// Seq 3 arrives twice: once via the gap-fill, once as the flushed live
		// frame. This pins WHY the watermark is load-bearing - remove the report
		// and the exactly-once test above would fail exactly here.
		const msgs = c.messages.filter((m) => m.topic === '__replay:room' && m.event === 'msg');
		expect(msgs.map((m) => m.data.seq)).toEqual([1, 2, 3]);
		expect(c.messages.filter((m) => m.topic === 'room' && m.event === 'tick')).toHaveLength(1);
	});

	it('batch resume: each topic dedups against its own watermark', async () => {
		const { createTestServer } = await import('svelte-adapter-uws/testing');
		const client = mockRedisClient('t:');
		const replay = createReplay(client);
		const server = await createTestServer({ handler: { resume: replay.resumeHook() } });
		servers.push(server);
		const c = await connectClient(server.wsUrl);

		// Topic a: the client lags (missed 1..2). Topic b: the client is current.
		await replay.publish(server.platform, 'a', 'tick', { n: 1 });
		await replay.publish(server.platform, 'a', 'tick', { n: 2 });
		await replay.publish(server.platform, 'b', 'tick', { n: 1 });

		// The batch resume runs both gap-fills concurrently; the gate parks the
		// FIRST store read (topic a - fills start in lastSeenSeqs order), so a's
		// read happens after the in-window publishes while b's read has already
		// returned when they land.
		const gate = gateNextPipelineExec(client);
		c.send({ type: 'subscribe-batch', topics: ['a', 'b'], ref: 1, recover: { a: { offset: 0 }, b: { offset: 1 } } });
		await gate.entered.promise;
		await replay.publish(server.platform, 'a', 'tick', { n: 3 }); // covered by a's late read -> skipped at flush
		await replay.publish(server.platform, 'b', 'tick', { n: 2 }); // past b's watermark -> delivered by the flush
		gate.release.resolve();
		await c.waitFor((m) => m.type === 'subscribed' && m.topic === 'a');
		await c.waitFor((m) => m.type === 'subscribed' && m.topic === 'b');
		await settle();

		// Topic a: gap-fill delivered 1..3, the buffered live seq-3 frame skipped.
		const aMsgs = c.messages.filter((m) => m.topic === '__replay:a' && m.event === 'msg');
		expect(aMsgs.map((m) => m.data.seq)).toEqual([1, 2, 3]);
		expect(c.messages.filter((m) => m.topic === 'a' && m.event === 'tick')).toHaveLength(0);

		// Topic b: nothing to gap-fill (watermark = the client's floor), and the
		// in-window publish arrives exactly once via the barrier flush.
		const bMsgs = c.messages.filter((m) => m.topic === '__replay:b' && m.event === 'msg');
		expect(bMsgs).toHaveLength(0);
		const bLive = c.messages.filter((m) => m.topic === 'b' && m.event === 'tick');
		expect(bLive).toHaveLength(1);
		expect(bLive[0].data).toEqual({ n: 2 });
	});
});
