import assert from 'node:assert/strict';
import dgram from 'node:dgram';
import { afterEach, beforeEach, describe, it, vi } from 'vitest';
import { createNtpSource } from '../../src/redis/ntp-source.js';

const clients = new Map(), servers = new Set(), failures = [], handlers = new Set();
let creatingServer = false;
beforeEach(() => {
	const create = dgram.createSocket;
	vi.spyOn(dgram, 'createSocket').mockImplementation((...args) => {
		const socket = create(...args);
		if (!creatingServer) { clients.set(socket, false); socket.once('close', () => clients.set(socket, true)); }
		return socket;
	});
});
afterEach(async () => {
	for (const socket of servers) socket.removeAllListeners('message');
	let drainTimer;
	try {
		await Promise.race([
			Promise.allSettled([...handlers]),
			new Promise((_, reject) => { drainTimer = setTimeout(() => reject(new Error('Loopback responder cleanup deadline')), 1500); })
		]);
	} catch (error) { failures.push(error); }
	finally { clearTimeout(drainTimer); }
	await new Promise(resolve => setImmediate(resolve));
	const leaked = [...clients].filter(([, closed]) => !closed).map(([socket]) => socket);
	const close = socket => new Promise(resolve => { try { socket.close(resolve); } catch { resolve(); } });
	await Promise.all([...servers, ...leaked].map(close));
	servers.clear(); clients.clear(); vi.restoreAllMocks();
	const errors = failures.splice(0);
	assert.equal(leaked.length, 0, 'each completed source read must close its actual UDP socket');
	if (errors.length) throw new AggregateError(errors, 'Loopback NTP responder failed');
});
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function server(onRequest) {
	creatingServer = true;
	let socket;
	try { socket = dgram.createSocket('udp4'); } finally { creatingServer = false; }
	servers.add(socket); socket.on('error', error => failures.push(error));
	await new Promise((resolve, reject) => { socket.once('listening', resolve); socket.once('error', reject); socket.bind(0, '127.0.0.1'); });
	if (onRequest) socket.on('message', (packet, peer) => {
		const handling = Promise.resolve().then(() => onRequest(packet, peer, socket)).catch(error => failures.push(error)).finally(() => handlers.delete(handling));
		handlers.add(handling);
	});
	return socket;
}
function reply(packet, epoch = 1_700_000_000_500, { version = 4, leap = 0, stratum = 2 } = {}) {
	assert.equal(packet.length, 48); assert.equal(packet[0], 0x23);
	assert.notDeepEqual(packet.subarray(40, 48), Buffer.alloc(8));
	const bytes = Buffer.alloc(48); bytes[0] = (leap << 6) | (version << 3) | 4; bytes[1] = stratum;
	packet.copy(bytes, 24, 40, 48);
	bytes.writeUInt32BE(Math.floor(epoch / 1000) + 2208988800, 40);
	bytes.writeUInt32BE(Math.round((epoch % 1000) / 1000 * 0x100000000) >>> 0, 44);
	return bytes;
}
function send(socket, bytes, peer) {
	assert.equal(peer.address, '127.0.0.1');
	return new Promise((resolve, reject) => socket.send(bytes, peer.port, peer.address, error => error ? reject(error) : resolve()));
}
function source(socket, extra = {}) { return createNtpSource({ host: '127.0.0.1', port: socket.address().port, timeoutMs: 1000, ...extra }); }

describe('NTP source over local UDP only', () => {
	for (const version of [3, 4]) it('accepts a correlated synchronized version ' + version + ' reply', async () => {
		const responder = await server((packet, peer, socket) => send(socket, reply(packet, undefined, { version }), peer));
		assert.equal(await source(responder, { host: 'localhost' }).read(), 1_700_000_000_500);
	});
	it('ignores a correct-looking response from a different UDP endpoint', async () => {
		const other = await server(); let settled = false;
		const responder = await server(async (packet, peer, socket) => {
			await send(other, reply(packet, 1_900_000_000_000), peer);
			await delay(20); assert.equal(settled, false, 'wrong peer must not settle the source');
			await send(socket, reply(packet), peer);
		});
		const reading = source(responder).read().then(value => { settled = true; return value; });
		assert.equal(await reading, 1_700_000_000_500);
	});
	it('ignores the intended peer until its originate timestamp matches', async () => {
		let settled = false;
		const responder = await server(async (packet, peer, socket) => {
			const wrong = reply(packet, 1_900_000_000_000); wrong[31] ^= 1;
			await send(socket, wrong, peer);
			await delay(20); assert.equal(settled, false, 'wrong originate must not settle the source');
			await send(socket, reply(packet), peer);
		});
		assert.equal(await source(responder).read().then(value => { settled = true; return value; }), 1_700_000_000_500);
	});
	it('keeps simultaneous requests isolated when replies cross their sockets', async () => {
		const requests = [];
		const responder = await server(async (packet, peer, socket) => {
			requests.push({ packet, peer }); if (requests.length !== 2) return;
			assert.notDeepEqual(requests[0].packet.subarray(40, 48), requests[1].packet.subarray(40, 48));
			await send(socket, reply(requests[0].packet, 1_900_000_000_000), requests[1].peer);
			await send(socket, reply(requests[1].packet, 1_900_000_000_000), requests[0].peer);
			await delay(10);
			for (const request of requests) await send(socket, reply(request.packet), request.peer);
		});
		const ntp = source(responder);
		assert.deepEqual(await Promise.all([ntp.read(), ntp.read()]), [1_700_000_000_500, 1_700_000_000_500]);
	});
	it('times out an unanswered request and permits a later valid read', async () => {
		let calls = 0;
		const responder = await server((packet, peer, socket) => ++calls === 1 ? undefined : send(socket, reply(packet), peer));
		const ntp = source(responder, { timeoutMs: 40 });
		await assert.rejects(ntp.read(), /no reply/);
		assert.equal(await ntp.read(), 1_700_000_000_500); assert.equal(calls, 2);
	});
	for (const [name, header] of [['unsynchronized stratum', { stratum: 16 }], ['unsupported version', { version: 0 }], ['unsynchronized LI', { leap: 3 }]]) {
		it('rejects a matching actual-peer reply with ' + name, async () => {
			const responder = await server((packet, peer, socket) => send(socket, reply(packet, undefined, header), peer));
			await assert.rejects(source(responder).read(), /stratum|version|unsynchronized/);
		});
	}
});
