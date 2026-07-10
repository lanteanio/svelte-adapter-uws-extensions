import { describe, it, expect } from 'vitest';
import { createNtpSource } from '../../src/redis/ntp-source.js';

/** Seconds between the NTP era (1900) and the Unix epoch (1970). */
const NTP_UNIX_OFFSET_SECONDS = 2_208_988_800;

/** Craft a well-formed 48-byte SNTP server reply for a given epoch-ms. */
function replyFor(epochMs, { mode = 4, stratum = 2 } = {}) {
	const buf = Buffer.alloc(48);
	buf[0] = 0x20 | mode; // LI=0, VN=4
	buf[1] = stratum;
	const seconds = Math.floor(epochMs / 1000) + NTP_UNIX_OFFSET_SECONDS;
	const fraction = Math.round(((epochMs % 1000) / 1000) * 0x100000000);
	buf.writeUInt32BE(seconds, 40);
	buf.writeUInt32BE(fraction >>> 0, 44);
	return buf;
}

function fakeTransport(replyOrFn) {
	const calls = [];
	return {
		calls,
		request(packet, host, port, timeoutMs) {
			calls.push({ packet, host, port, timeoutMs });
			return typeof replyOrFn === 'function' ? replyOrFn(packet) : Promise.resolve(replyOrFn);
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

describe('ntp-source - read', () => {
	it('sends a 48-byte mode-3 client request to the configured server', async () => {
		const t = fakeTransport(replyFor(1_700_000_000_000));
		const src = createNtpSource({ host: 'time.example', port: 1230, timeoutMs: 500, transport: t });
		await src.read();
		expect(t.calls).toHaveLength(1);
		const { packet, host, port, timeoutMs } = t.calls[0];
		expect(packet.length).toBe(48);
		expect(packet[0]).toBe(0x23); // LI=0, VN=4, Mode=3 (client)
		expect(packet.slice(1).every((b) => b === 0)).toBe(true);
		expect(host).toBe('time.example');
		expect(port).toBe(1230);
		expect(timeoutMs).toBe(500);
	});

	it('parses the transmit timestamp to epoch milliseconds (incl. the fraction)', async () => {
		const src = createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_500)) });
		expect(await src.read()).toBe(1_700_000_000_500);
	});

	it('accepts a broadcast (mode 5) reply', async () => {
		const src = createNtpSource({ host: 'x', transport: fakeTransport(replyFor(1_700_000_000_000, { mode: 5 })) });
		expect(await src.read()).toBe(1_700_000_000_000);
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
});
