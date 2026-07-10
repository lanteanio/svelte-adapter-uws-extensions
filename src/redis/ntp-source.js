/**
 * Minimal SNTP client: a third clock source for the cluster clock.
 *
 * Speaks just enough of the SNTPv4 wire (RFC 4330) to ask one server for its
 * transmit timestamp: a 48-byte mode-3 client request, one mode-4 reply, the
 * transmit timestamp field converted from the NTP era (seconds since 1900 +
 * 32-bit binary fraction) to epoch milliseconds. No poll loop, no filtering,
 * no server selection - the cluster clock owns cadence and median-fusion; this
 * module owns exactly one read.
 *
 * Off by default everywhere: the cluster clock only queries NTP when handed a
 * source, and nothing constructs one implicitly. Built on `node:dgram`, so it
 * adds no dependency.
 *
 * Determinism: the reply timeout is armed through this package's runtime seam
 * (`setTimer`), and the UDP transport is injectable - a simulation or unit
 * test passes `transport: { request }` returning a canned reply, so no real
 * socket ever opens and the read reproduces under a virtual clock.
 *
 * @module svelte-adapter-uws-extensions/redis/ntp-source
 */

import dgram from 'node:dgram';
import { setTimer, clearTimer } from '../shared/runtime.js';

/** Seconds between the NTP era (1900-01-01) and the Unix epoch (1970-01-01). */
const NTP_UNIX_OFFSET_SECONDS = 2_208_988_800;

const DEFAULT_PORT = 123;
const DEFAULT_TIMEOUT_MS = 2_000;

/**
 * @typedef {Object} NtpTransport
 * @property {(packet: Buffer, host: string, port: number, timeoutMs: number) => Promise<Buffer>} request -
 *   Send one request datagram and resolve with the first reply datagram (or
 *   reject on error/timeout). The default transport opens a fresh UDP socket
 *   per read and closes it when settled.
 */

/**
 * @typedef {Object} NtpSourceOptions
 * @property {string} host - NTP server hostname or IP. Required.
 * @property {number} [port=123]
 * @property {number} [timeoutMs=2000] - Reply deadline per read.
 * @property {NtpTransport} [transport] - Injectable wire (tests/simulations).
 */

/**
 * @typedef {Object} NtpSource
 * @property {() => Promise<number>} read - One SNTP round trip; resolves with
 *   the server clock as epoch milliseconds. Rejects on timeout, a malformed
 *   reply, or a kiss-of-death (stratum 0) reply.
 */

/** One-socket-per-read UDP transport with a seam-armed timeout. */
function createDgramTransport() {
	return {
		request(packet, host, port, timeoutMs) {
			return new Promise((resolve, reject) => {
				const socket = dgram.createSocket('udp4');
				let timer = null;
				let settled = false;
				const settle = (fn, arg) => {
					if (settled) return;
					settled = true;
					if (timer) { clearTimer(timer); timer = null; }
					try { socket.close(); } catch { /* already closed */ }
					fn(arg);
				};
				timer = setTimer(() => settle(reject, new Error(`ntp: no reply from ${host}:${port} within ${timeoutMs}ms`)), timeoutMs);
				socket.once('error', (err) => settle(reject, err));
				socket.once('message', (msg) => settle(resolve, msg));
				socket.send(packet, port, host, (err) => { if (err) settle(reject, err); });
			});
		}
	};
}

/**
 * Parse an SNTP reply's transmit timestamp into epoch milliseconds.
 * @param {Buffer} reply
 * @returns {number}
 */
function parseTransmitEpochMs(reply) {
	if (!Buffer.isBuffer(reply) || reply.length < 48) {
		throw new Error('ntp: reply shorter than the 48-byte SNTP header');
	}
	const mode = reply[0] & 0x07;
	if (mode !== 4 && mode !== 5) {
		throw new Error(`ntp: unexpected reply mode ${mode} (want 4 server / 5 broadcast)`);
	}
	const stratum = reply[1];
	if (stratum === 0) {
		// RFC 4330: stratum 0 is a kiss-of-death packet - the server refuses.
		throw new Error('ntp: kiss-of-death reply (stratum 0) - server refused');
	}
	const seconds = reply.readUInt32BE(40);
	const fraction = reply.readUInt32BE(44);
	if (seconds === 0) {
		throw new Error('ntp: reply carries a zero transmit timestamp');
	}
	return (seconds - NTP_UNIX_OFFSET_SECONDS) * 1000 + Math.round((fraction / 0x100000000) * 1000);
}

/**
 * Create an SNTP clock source for `createClusterClock({ ntp })`.
 *
 * @param {NtpSourceOptions} options
 * @returns {NtpSource}
 *
 * @example
 * ```js
 * import { createNtpSource } from 'svelte-adapter-uws-extensions/redis/ntp-source';
 * const ntp = createNtpSource({ host: 'pool.ntp.org' });
 * const clock = createClusterClock(redis, { ntp });
 * ```
 */
export function createNtpSource(options = {}) {
	if (!options || typeof options.host !== 'string' || options.host.length === 0) {
		throw new Error('ntp: host is required');
	}
	const host = options.host;
	const port = options.port ?? DEFAULT_PORT;
	if (!Number.isInteger(port) || port < 1 || port > 65535) {
		throw new Error('ntp: port must be an integer in 1..65535');
	}
	const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
	if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
		throw new Error('ntp: timeoutMs must be a positive number (ms)');
	}
	if (options.transport !== undefined && (typeof options.transport !== 'object' || typeof options.transport.request !== 'function')) {
		throw new Error('ntp: transport must expose request(packet, host, port, timeoutMs) => Promise<Buffer>');
	}
	const transport = options.transport ?? createDgramTransport();

	return {
		async read() {
			// LI=0, VN=4, Mode=3 (client). Everything else zero: an SNTP client
			// request needs no reference/origin timestamps for a transmit read.
			const packet = Buffer.alloc(48);
			packet[0] = 0x23;
			const reply = await transport.request(packet, host, port, timeoutMs);
			return parseTransmitEpochMs(reply);
		}
	};
}
