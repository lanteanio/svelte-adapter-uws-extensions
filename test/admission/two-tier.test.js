// Pins the contract that the adapter's handshake-path admission and the
// extensions' message-path admission compose without overlap. The
// adapter's own upgrade-admission-wiring test covers the handshake tier
// in isolation; the shared/admission unit tests cover the message tier
// in isolation. This file is the layered case: a connection storm hits
// a server that has both tiers wired, and the test asserts that each
// tier sheds at the layer it owns.

import { describe, it, expect, afterEach } from 'vitest';
import { createAdmissionControl } from '../../shared/admission.js';

let uWS;
try {
	uWS = (await import('uWebSockets.js')).default;
} catch {
	uWS = null;
}

const describeUWS = uWS ? describe : describe.skip;

let server;

async function attemptUpgrade(url) {
	const { WebSocket } = await import('ws');
	return await new Promise((resolve) => {
		const ws = new WebSocket(url);
		const result = { opened: false, status: null, ws: null };
		ws.on('open', () => { result.opened = true; result.ws = ws; resolve(result); });
		ws.on('unexpected-response', (_req, res) => { result.status = res.statusCode; resolve(result); });
		ws.on('error', () => {
			if (result.status === null && !result.opened) resolve(result);
		});
	});
}

function makePressureRef(initial) {
	let snapshot = initial;
	const platform = {
		get pressure() { return snapshot; }
	};
	return {
		platform,
		set(next) { snapshot = next; }
	};
}

const NO_PRESSURE = {
	active: false,
	reason: 'NONE',
	subscriberRatio: 0,
	publishRate: 0,
	memoryMB: 0
};

const MEMORY_PRESSURE = {
	active: true,
	reason: 'MEMORY',
	subscriberRatio: 0,
	publishRate: 0,
	memoryMB: 1024
};

describeUWS('two-tier admission (handshake + message)', () => {
	afterEach(async () => {
		await server?.close();
		server = null;
	});

	it('handshake tier sheds the surplus; message tier is consulted only on connections that survived', async () => {
		const ac = createAdmissionControl({
			classes: { background: ['MEMORY', 'PUBLISH_RATE', 'SUBSCRIBERS'] }
		});
		const pressure = makePressureRef(NO_PRESSURE);
		const messageDecisions = [];

		const { createTestServer } = await import('svelte-adapter-uws/testing');
		server = await createTestServer({
			upgradeAdmission: { maxConcurrent: 2 },
			handler: {
				upgrade: async () => {
					await new Promise((r) => setTimeout(r, 60));
					return {};
				},
				message: (ws, ctx) => {
					const accepted = ac.shouldAccept('background', pressure.platform);
					messageDecisions.push(accepted);
					const text = typeof ctx.data === 'string'
						? ctx.data
						: new TextDecoder().decode(ctx.data);
					ws.send(JSON.stringify({ echo: text, accepted }));
				}
			}
		});

		const burst = await Promise.all(
			Array.from({ length: 12 }, () => attemptUpgrade(server.wsUrl))
		);

		const opened = burst.filter((r) => r.opened);
		const shed = burst.filter((r) => r.status === 503);

		expect(shed.length).toBeGreaterThanOrEqual(8);
		expect(opened.length).toBeLessThanOrEqual(4);
		expect(opened.length + shed.length).toBe(burst.length);
		expect(messageDecisions).toHaveLength(0);

		for (const r of burst) r.ws?.close();
	});

	it('message tier sheds on accepted connections when pressure flips, independent of the handshake tier', async () => {
		const ac = createAdmissionControl({
			classes: { background: ['MEMORY', 'PUBLISH_RATE', 'SUBSCRIBERS'] }
		});
		const pressure = makePressureRef(NO_PRESSURE);

		const { createTestServer } = await import('svelte-adapter-uws/testing');
		server = await createTestServer({
			upgradeAdmission: { maxConcurrent: 16 },
			handler: {
				message: (ws, ctx) => {
					const accepted = ac.shouldAccept('background', pressure.platform);
					const text = typeof ctx.data === 'string'
						? ctx.data
						: new TextDecoder().decode(ctx.data);
					ws.send(JSON.stringify({ echo: text, accepted }));
				}
			}
		});

		const fresh = await attemptUpgrade(server.wsUrl);
		expect(fresh.opened).toBe(true);

		const replies = [];
		fresh.ws.on('message', (raw) => {
			replies.push(JSON.parse(raw.toString()));
		});

		fresh.ws.send(JSON.stringify({ kind: 'ping', n: 1 }));
		await new Promise((r) => setTimeout(r, 30));
		expect(replies.at(-1)).toMatchObject({ accepted: true });

		pressure.set(MEMORY_PRESSURE);

		fresh.ws.send(JSON.stringify({ kind: 'ping', n: 2 }));
		await new Promise((r) => setTimeout(r, 30));
		expect(replies.at(-1)).toMatchObject({ accepted: false });

		pressure.set(NO_PRESSURE);

		fresh.ws.send(JSON.stringify({ kind: 'ping', n: 3 }));
		await new Promise((r) => setTimeout(r, 30));
		expect(replies.at(-1)).toMatchObject({ accepted: true });

		fresh.ws.close();
	});

	it('the two tiers are configured independently; a permissive message rule cannot rescue a handshake-shed connection', async () => {
		const ac = createAdmissionControl({
			classes: { critical: ['MEMORY'] }
		});
		const pressure = makePressureRef(NO_PRESSURE);

		const { createTestServer } = await import('svelte-adapter-uws/testing');
		server = await createTestServer({
			upgradeAdmission: { maxConcurrent: 1 },
			handler: {
				upgrade: async () => {
					await new Promise((r) => setTimeout(r, 80));
					return {};
				},
				message: (ws) => {
					const accepted = ac.shouldAccept('critical', pressure.platform);
					ws.send(JSON.stringify({ accepted }));
				}
			}
		});

		const burst = await Promise.all(
			Array.from({ length: 6 }, () => attemptUpgrade(server.wsUrl))
		);

		const shed = burst.filter((r) => r.status === 503);
		expect(shed.length).toBeGreaterThan(0);

		for (const r of shed) {
			expect(r.opened).toBe(false);
			expect(r.ws).toBeNull();
		}

		for (const r of burst) r.ws?.close();
	});
});
