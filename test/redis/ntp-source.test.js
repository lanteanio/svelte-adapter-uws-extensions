import { describe, it, expect, afterEach, vi } from 'vitest';
import dgram from 'node:dgram';
import { EventEmitter } from 'node:events';
import { createNtpSource } from '../../src/redis/ntp-source.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

afterEach(() => { resetRuntimeEnv(); vi.restoreAllMocks(); });

/** Seconds between the NTP era (1900) and the Unix epoch (1970). */
const NTP_UNIX_OFFSET_SECONDS = 2_208_988_800;

/** Craft a well-formed 48-byte SNTP server reply for a given epoch-ms. */
function replyFor(epochMs, { mode = 4, stratum = 2, version = 4, leap = 0 } = {}) {
	const buf = Buffer.alloc(48);
	buf[0] = (leap << 6) | (version << 3) | mode;
	buf[1] = stratum;
	const seconds = Math.floor(epochMs / 1000) + NTP_UNIX_OFFSET_SECONDS;
	const fraction = Math.round(((epochMs % 1000) / 1000) * 0x100000000);
	buf.writeUInt32BE(seconds, 40);
	buf.writeUInt32BE(fraction >>> 0, 44);
	return buf;
}

function fakeTransport(replyOrFn, echo = true) {
	const calls = [];
	return {
		calls,
		async request(packet, host, port, timeoutMs) {
			calls.push({ packet, host, port, timeoutMs });
			const reply = await (typeof replyOrFn === 'function' ? replyOrFn(packet) : replyOrFn);
			if (!echo || !Buffer.isBuffer(reply) || reply.length < 32) return reply;
			const result = Buffer.from(reply);
			packet.copy(result, 24, 40, 48);
			return result;
		}
	};
}

describe('ntp-source - validation', () => {
	it('requires a host', () => {
		expect(() => createNtpSource()).toThrow('host');
		expect(() => createNtpSource({ host: '' })).toThrow('host');
	});
	it('validates port, timeout, and transport shape', () => {
		expect(() => createNtpSource({ host: 'x', port: 0 })).toThrow('port');
		expect(() => createNtpSource({ host: 'x', port: 70000 })).toThrow('port');
		expect(() => createNtpSource({ host: 'x', timeoutMs: 0 })).toThrow('timeoutMs');
		expect(() => createNtpSource({ host: 'x', transport: {} })).toThrow('transport');
	});
});

function datagramFixture() {
	const timers = new Map(), sockets = [];
	setRuntimeEnv({ timers: {
		set(fn, ms) { const handle = {}; timers.set(handle, { fn, ms }); return handle; },
		clear(handle) { timers.delete(handle); }
	} });
	class Socket extends EventEmitter {
		closed = 0;
		packet = null;
		connect(port, host, callback) { this.peer = { address: '192.0.2.1', port }; this.connected = callback; this.host = host; }
		remoteAddress() { return this.peer; }
		send(packet, callback) { this.packet = Buffer.from(packet); callback(null); }
		close() { this.closed++; }
	}
	vi.spyOn(dgram, 'createSocket').mockImplementation(() => { const socket = new Socket(); sockets.push(socket); return socket; });
	const reply = socket => { const bytes = replyFor(1_700_000_000_500); socket.packet.copy(bytes, 24, 40, 48); return bytes; };
	return { timers, sockets, reply };
}

describe('ntp-source - default transport lifecycle', () => {
	it('ignores a different address/port/request without replacing the original timeout', async () => {
		const { timers, sockets, reply } = datagramFixture();
		let settled = false;
		const reading = createNtpSource({ host: 'configured.example', port: 1234, timeoutMs: 20 }).read().then(value => { settled = true; return value; });
		const socket = sockets[0], timer = [...timers.keys()][0];
		socket.connected();
		expect(socket.host).toBe('configured.example');
		socket.emit('message', reply(socket), { address: '192.0.2.2', port: 1234 });
		socket.emit('message', reply(socket), { address: '192.0.2.1', port: 1235 });
		socket.emit('message', replyFor(1_700_000_000_000), socket.peer);
		socket.emit('message', Buffer.alloc(20), socket.peer);
		await Promise.resolve(); await Promise.resolve();
		expect(settled).toBe(false); expect([...timers.keys()]).toEqual([timer]); expect(timers.get(timer).ms).toBe(20);
		socket.emit('message', reply(socket), socket.peer);
		expect(await reading).toBe(1_700_000_000_500); expect(socket.closed).toBe(1); expect(timers.size).toBe(0);
	});
	it('closes on timeout and does not send after a late connect callback', async () => {
		const { timers, sockets } = datagramFixture();
		const reading = createNtpSource({ host: 'configured.example', timeoutMs: 20 }).read();
		const rejection = expect(reading).rejects.toThrow('no reply');
		[...timers.values()][0].fn(); await rejection;
		const socket = sockets[0]; socket.connected(); socket.emit('error', new Error('late error'));
		expect(socket.packet).toBeNull(); expect(socket.closed).toBe(1); expect(timers.size).toBe(0);
	});
	for (const failure of ['connect callback', 'connect throw', 'send callback', 'send throw', 'error event']) it('cleans up after ' + failure, async () => {
		const { timers, sockets } = datagramFixture(), expected = new Error(failure);
		if (failure === 'connect throw') dgram.createSocket.mockImplementationOnce(() => {
			const socket = new EventEmitter(); socket.closed = 0; socket.close = () => { socket.closed++; };
			socket.connect = () => { throw expected; }; sockets.push(socket); return socket;
		});
		const reading = createNtpSource({ host: 'configured.example' }).read();
		const rejected = expect(reading).rejects.toBe(expected), socket = sockets[0];
		if (failure === 'connect callback') socket.connected(expected);
		else if (failure === 'send callback') { socket.send = (_packet, callback) => callback(expected); socket.connected(); }
		else if (failure === 'send throw') { socket.send = () => { throw expected; }; socket.connected(); }
		else if (failure === 'error event') socket.emit('error', expected);
		await rejected; expect(socket.closed).toBe(1); expect(timers.size).toBe(0);
		socket.emit('error', new Error('late second error'));
	});
});

describe('ntp-source - read', () => {
	it('sends a 48-byte mode-3 client request to the configured server', async () => {
		const local = 1_700_000_000_123;
		setRuntimeEnv({ clock: { wallEpoch: () => local } });
		const t = fakeTransport(replyFor(1_700_000_000_000));
		const src = createNtpSource({ host: 'time.example', port: 1230, timeoutMs: 500, transport: t });
		await src.read();
		expect(t.calls).toHaveLength(1);
		const { packet, host, port, timeoutMs } = t.calls[0];
		expect(packet.length).toBe(48);
		expect(packet[0]).toBe(0x23); // LI=0, VN=4, Mode=3 (client)
		expect(packet.subarray(1, 40).every((b) => b === 0)).toBe(true);
		expect(packet.subarray(40, 48).equals(Buffer.alloc(8))).toBe(false);
		const sentEpoch = (packet.readUInt32BE(40) - NTP_UNIX_OFFSET_SECONDS) * 1000 + packet.readUInt32BE(44) / 0x100000000 * 1000;
		expect(Math.abs(sentEpoch - local)).toBeLessThan(1);
		expect(host).toBe('time.example');
		expect(port).toBe(1230);
		expect(timeoutMs).toBe(500);
	});

	it('parses the transmit timestamp to epoch milliseconds (incl. the fraction)', async () => {
		const src = createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_500)) });
		expect(await src.read()).toBe(1_700_000_000_500);
	});

	it('rejects a broadcast reply to this unicast request', async () => {
		const src = createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { mode: 5 })) });
		await expect(src.read()).rejects.toThrow('mode');
	});

	it('rejects a kiss-of-death (stratum 0) reply', async () => {
		const src = createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { stratum: 0 })) });
		await expect(src.read()).rejects.toThrow('kiss-of-death');
	});

	it('rejects a wrong-mode reply', async () => {
		const src = createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { mode: 3 })) });
		await expect(src.read()).rejects.toThrow('mode');
	});

	it('rejects a short reply', async () => {
		const src = createNtpSource({ host: 'x', transport: fakeTransport(Buffer.alloc(20)) });
		await expect(src.read()).rejects.toThrow('48-byte');
	});

	it('rejects a zero transmit timestamp', async () => {
		const buf = Buffer.alloc(48);
		buf[0] = 0x24;
		buf[1] = 2;
		const src = createNtpSource({ host: 'x', transport: fakeTransport(buf) });
		await expect(src.read()).rejects.toThrow('zero transmit');
	});

	it('propagates a transport rejection (timeout path)', async () => {
		const src = createNtpSource({
			host: 'x',
			transport: fakeTransport(() => Promise.reject(new Error('ntp: no reply from x:123 within 2000ms')))
		});
		await expect(src.read()).rejects.toThrow('no reply');
	});

	for (const version of [3, 4]) it('accepts supported version ' + version, async () => {
		expect(await createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { version })) }).read()).toBe(1_700_000_000_000);
	});
	for (const version of [0, 1, 2, 5, 6, 7]) it('rejects unsupported version ' + version, async () => {
		await expect(createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { version })) }).read()).rejects.toThrow('version');
	});
	for (const stratum of [1, 15]) it('accepts synchronized stratum ' + stratum, async () => {
		expect(await createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { stratum })) }).read()).toBe(1_700_000_000_000);
	});
	for (const stratum of [16, 17, 255]) it('rejects unsynchronized or reserved stratum ' + stratum, async () => {
		await expect(createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { stratum })) }).read()).rejects.toThrow('stratum');
	});
	for (const leap of [1, 2]) it('accepts a synchronized leap warning ' + leap, async () => {
		expect(await createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { leap })) }).read()).toBe(1_700_000_000_000);
	});
	it('rejects the unsynchronized leap indicator', async () => {
		await expect(createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { leap: 3 })) }).read()).rejects.toThrow('unsynchronized');
	});
	it('rejects an originate timestamp that does not echo the request', async () => {
		await expect(createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000), false) }).read()).rejects.toThrow('originate');
	});
	it('does not let a mutable injected packet replace the expected timestamp', async () => {
		const transport = fakeTransport(packet => { packet.fill(0, 40); return replyFor(1_700_000_000_000); });
		await expect(createNtpSource({ host: 'x', transport }).read()).rejects.toThrow('originate');
	});
	it('allocates unique timestamps for outstanding reads even if the clock and RNG repeat', async () => {
		setRuntimeEnv({ clock: { wallEpoch: () => 1_700_000_000_123 }, rng: { bytes: n => Buffer.alloc(n) } });
		const pending = [];
		const src = createNtpSource({ host: 'x', transport: { request(packet) { return new Promise(resolve => pending.push({ packet, resolve })); } } });
		const reads = [src.read(), src.read(), src.read()];
		const timestamps = pending.map(({ packet }) => packet.subarray(40, 48).toString('hex'));
		for (const { packet, resolve } of pending.reverse()) { const reply = replyFor(1_700_000_000_000); packet.copy(reply, 24, 40, 48); resolve(reply); }
		expect(await Promise.all(reads)).toEqual([1_700_000_000_000, 1_700_000_000_000, 1_700_000_000_000]);
		expect(new Set(timestamps).size).toBe(3);
	});
});
