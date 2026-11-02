import { afterEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { WebSocket } from 'ws';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createCursor } from '../../../src/redis/cursor.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';

const sockets = [], servers = [], trackers = [], clients = [];
afterEach(async () => {
	for (const socket of sockets.splice(0)) socket.terminate();
	const results = await Promise.allSettled(servers.splice(0).map(server => server.close()));
	for (const tracker of trackers.splice(0)) tracker.destroy();
	resetRuntimeEnv();
	for (const client of clients.splice(0)) {
		results.push(...await Promise.allSettled([resetBackendKeys(client)]));
		results.push(...await Promise.allSettled([client.quit()]));
	}
	const failures = results.filter(result => result.status === 'rejected').map(result => result.reason);
	if (failures.length) throw new AggregateError(failures, 'Cursor mover contract cleanup failed');
});

async function waitFor(read, description) {
	const deadline = performance.now() + 15000;
	while (!await read()) {
		if (performance.now() > deadline) throw new Error('Timed out waiting for ' + description);
		await new Promise(resolve => setTimeout(resolve, 2));
	}
}

// Only the extension's existing clock/timer seam is virtual. Redis, sockets,
// the native adapter platform and every wire frame retain their real paths.
function clock() {
	let now = Date.now(), sequence = 0;
	const pending = new Map();
	function set(callback, delay, interval, args) {
		const handle = { id: ++sequence, unref() { return this; } };
		pending.set(handle, { callback, at: now + Math.max(0, delay), interval, args });
		return handle;
	}
	setRuntimeEnv({
		clock: { now: () => now, monotonic: () => now },
		timers: {
			set: (callback, delay, ...args) => set(callback, delay, 0, args),
			setInterval: (callback, delay, ...args) => set(callback, delay, delay, args),
			clear: handle => pending.delete(handle), clearInterval: handle => pending.delete(handle)
		}
	});
	return {
		advance(ms) {
			const until = now + ms;
			let steps = 0;
			for (;;) {
				let selected;
				for (const entry of pending) if (entry[1].at <= until && (!selected || entry[1].at < selected[1].at)) selected = entry;
				if (!selected) break;
				if (++steps > 1000) throw new Error('Manual extension clock exceeded its finite callback bound');
				const [handle, timer] = selected;
				pending.delete(handle); now = timer.at;
				if (timer.interval) pending.set(handle, { ...timer, at: now + timer.interval });
				timer.callback(...timer.args);
			}
			now = until;
		}
	};
}

it('bounds a receiver to 64 local and Redis peer movers per tick while preserving FIFO and latest queued values', async () => {
	const identity = randomUUID(), topic = 'movers-' + identity, prefix = 'cursor-movers:' + identity + ':';
	const time = clock();
	const start = async (name, topicThrottle) => {
		const client = createBackendClient({ keyPrefix: prefix }); clients.push(client);
		const tracker = createCursor(client, { binary: false, throttle: 0, topicThrottle, snapshotIntervalMs: 0 }); trackers.push(tracker);
		const server = await createTestServer({ handler: {
			upgrade: ({ headers }) => ({ id: headers['x-id'] }),
			subscribe(_ws, value) { return value === topic ? null : 'FORBIDDEN'; },
			async open(ws, { platform }) { await tracker.attach(ws, topic, platform); platform.send(ws, topic, 'attached', { id: ws.getUserData().id }); },
			message(ws, context) {
				if (context.msg?.type !== 'cursor') return;
				tracker.hooks.message(ws, { ...context, data: context.msg });
				context.platform.send(ws, topic, 'input-fence', { ref: context.msg.ref });
			},
			close: tracker.hooks.close
		} }); servers.push(server);
		const connect = async id => {
			const socket = server.track(new WebSocket(server.wsUrl, { headers: { 'x-id': id } })); sockets.push(socket);
			const frames = [], state = { error: null };
			const receive = frame => { if (frame.type === 'batch') { for (const entry of frame.events) receive(entry); } else frames.push(frame); };
			socket.on('error', error => { state.error = error; });
			socket.on('message', (raw, binary) => { try { expect(binary).toBe(false); receive(JSON.parse(raw.toString())); } catch (error) { state.error = error; } });
			await waitFor(() => { if (state.error) throw state.error; return socket.readyState === WebSocket.OPEN && frames.some(frame => frame.event === 'attached' && frame.data.id === id); }, name + ' attachment');
			return { socket, frames, state, id };
		};
		return { client, tracker, server, connect };
	};
	const receiver = await start('receiver', 16), origin = await start('origin', 0);
	const observer = await receiver.connect('observer');
	const local = [], peer = [];
	for (let index = 0; index < 32; index++) local.push(await receiver.connect('local-' + index));
	for (let index = 0; index < 35; index++) peer.push(await origin.connect('peer-' + index));
	const send = async (client, version = 1) => {
		const ref = randomUUID();
		client.socket.send(JSON.stringify({ type: 'cursor', topic, ref, data: { mover: client.id, version, x: version, y: Number(client.id.split('-')[1]) } }));
		await waitFor(() => { if (client.state.error) throw client.state.error; return client.frames.some(frame => frame.event === 'input-fence' && frame.data.ref === ref); }, client.id + ' input processing');
	};
	// Each input is a real WebSocket task. The frozen receiver clock prevents
	// transport and event-loop timing from deciding which slice it sees.
	for (const client of local) await send(client);
	for (const client of peer.slice(0, 34)) await send(client);
	await send(local[0], 2); await send(peer[31], 2);
	await send(peer[34]);
	await waitFor(() => observer.frames.some(frame => frame.event === 'join' && frame.data.user.id === peer[34].id), 'ordered peer JOIN fence after the preceding UPDATEs');
	const positions = frames => frames.filter(frame => frame.topic === '__cursor:' + topic && ['update', 'bulk'].includes(frame.event))
		.flatMap(frame => frame.event === 'bulk' ? frame.data : [frame.data]).map(entry => entry.data);
	expect(positions(observer.frames)).toEqual([]);
	const nativeObserver = [...receiver.server.wsConnections].find(ws => ws.getUserData().id === 'observer');
	expect(nativeObserver).toBeDefined();
	let offset = observer.frames.length;
	const slices = [];
	for (let tick = 0; tick < 4; tick++) {
		time.advance(tick === 0 ? 0 : 16);
		receiver.server.platform.send(nativeObserver, topic, 'tick-fence', { tick });
		await waitFor(() => { if (observer.state.error) throw observer.state.error; return observer.frames.some(frame => frame.event === 'tick-fence' && frame.data.tick === tick); }, 'native receiver tick fence');
		const end = observer.frames.findIndex(frame => frame.event === 'tick-fence' && frame.data.tick === tick) + 1;
		const slice = positions(observer.frames.slice(offset, end)); offset = end;
		expect(slice.length).toBeLessThanOrEqual(64); slices.push(slice);
		if (tick === 0) {
			expect(slice.map(value => value.mover)).toEqual([...local.map(client => client.id), ...peer.slice(0, 32).map(client => client.id)]);
			expect(slice[0].version).toBe(2); expect(slice[63].version).toBe(2);
			expect(receiver.tracker.stats().moversDeferred).toBeGreaterThanOrEqual(2);
		}
		if (slices.flat().length === 67) break;
	}
	const delivered = slices.flat();
	expect(delivered.map(value => value.mover)).toEqual([...local.map(client => client.id), ...peer.map(client => client.id)]);
	expect(new Set(delivered.map(value => value.mover)).size).toBe(67);
	expect(delivered.filter(value => value.version === 2).map(value => value.mover)).toEqual([local[0].id, peer[31].id]);
	expect(slices[0]).toHaveLength(64);
}, 45000);
