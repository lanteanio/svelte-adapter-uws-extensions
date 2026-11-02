import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { createBackendClient } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createCircuitBreaker } from '../../../src/shared/breaker.js';
import { createDegradationPolicy } from '../../../src/shared/degradation.js';
import { invalidHealthEvents } from '../../helpers/health-envelope-corpus.js';

describe('health controls through Redis and the native WebSocket transport', () => {
	let client;
	beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-health-envelope:' }); });
	afterAll(async () => { await client.quit(); });

	it('drops malformed controls and keeps genuine transitions and well-formed peer controls', async () => {
		const channel = 'inttest-health-envelope:' + randomUUID();
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const degradationPolicy = createDegradationPolicy({
			mitigation: ['local', { custom: 'degraded' }],
			recovery: ['local', { custom: 'recovered' }]
		});
		const bus = createPubSubBus(client, { channel, breaker, degradationPolicy });
		let readyResolve, readyReject;
		const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
		let server, ws;
		const frames = [];
		try {
			server = await createTestServer({ handler: {
				async open(socket, ctx) {
					try {
						await bus.hooks.open(socket, ctx);
						ctx.platform.subscribe(socket, 'application');
						readyResolve();
					} catch (error) { readyReject(error); }
				}
			} });
			ws = new WebSocket(server.wsUrl);
			ws.on('message', (raw, binary) => { if (!binary) { try { frames.push(JSON.parse(raw.toString())); } catch { /* non-envelope */ } } });
			await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
			await ready;
			// Pin a publisher connection so the barrier follows its earlier controls.
			const publisher = typeof client.redis.nodes === 'function' ? client.redis.nodes('master')[0] : client.redis;
			const publish = envelope => publisher.publish(channel, JSON.stringify({ instanceId: 'peer', ...envelope }));
			for (const [, event, data] of invalidHealthEvents()) await publish({ topic: '__realtime', event, data });
			await publish({ topic: 'application', event: 'barrier', data: { n: 1 } });
			await expect.poll(() => frames.some(frame => frame.topic === 'application' && frame.event === 'barrier'), { timeout: 5000 }).toBe(true);
			expect(frames.filter(frame => frame.topic === '__realtime')).toHaveLength(0);

			breaker.failure(new Error('synthetic backend fault'));
			await expect.poll(() => frames.some(frame => frame.event === 'degraded'), { timeout: 5000 }).toBe(true);
			expect(frames.find(frame => frame.event === 'degraded').data.mitigation).toEqual(['local', { custom: 'degraded' }]);
			// success() only recovers a probing breaker; reset() is a public transition.
			breaker.reset();
			await expect.poll(() => frames.some(frame => frame.event === 'recovered'), { timeout: 5000 }).toBe(true);
			expect(frames.find(frame => frame.event === 'recovered').data.recovery).toEqual(['local', { custom: 'recovered' }]);
			// Shape checks do not authenticate a publisher with valid Redis access.
			await publish({ topic: '__realtime', event: 'degraded', data: { at: 2, mitigation: { messageKey: 'service.unavailable' } } });
			await publish({ topic: '__realtime', event: 'recovered', data: { at: 3, recovery: { refetch: false } } });
			await expect.poll(() => frames.some(frame => frame.event === 'recovered' && frame.data?.at === 3), { timeout: 5000 }).toBe(true);
			expect(frames.some(frame => frame.event === 'degraded' && frame.data?.at === 2)).toBe(true);
			await publish({ topic: '__realtime', event: 'degraded', data: { at: 4, mitigation: ['peer', { custom: 'degraded' }] } });
			await publish({ topic: '__realtime', event: 'recovered', data: { at: 5, recovery: ['peer', { custom: 'recovered' }] } });
			await expect.poll(() => frames.some(frame => frame.event === 'recovered' && frame.data?.at === 5), { timeout: 5000 }).toBe(true);
			expect(frames.find(frame => frame.event === 'degraded' && frame.data?.at === 4).data.mitigation).toEqual(['peer', { custom: 'degraded' }]);
			expect(frames.find(frame => frame.event === 'recovered' && frame.data?.at === 5).data.recovery).toEqual(['peer', { custom: 'recovered' }]);
		} finally {
			await bus.deactivate();
			breaker.destroy();
			if (ws && ws.readyState !== WebSocket.CLOSED) await new Promise(resolve => { ws.once('close', resolve); ws.terminate(); });
			if (server) await server.close();
		}
	});
});
