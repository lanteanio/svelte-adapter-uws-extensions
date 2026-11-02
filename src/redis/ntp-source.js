/**
 * Minimal SNTP client: a third clock source for the cluster clock.
 *
 * Speaks just enough of the SNTPv4 wire (RFC 4330) to ask one server for its
 * transmit timestamp: a 48-byte mode-3 client request, one mode-4 reply, the
 * transmit timestamp field converted from the NTP era (seconds since 1900 +
 * 32-bit binary fraction) to epoch milliseconds. No poll loop, no sample filter,
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
import { setTimer, clearTimer, wallEpoch, randomBytes } from '../shared/runtime.js';
import { defineOptions, option, timerCeiling } from '../shared/options.js';

/** Seconds between the NTP era (1900-01-01) and the Unix epoch (1970-01-01). */
const NTP_UNIX_OFFSET_SECONDS = 2_208_988_800;

const DEFAULT_PORT = 123;
const DEFAULT_TIMEOUT_MS = 2_000;
const FRACTION_NONCE_BITS = 22;
const FRACTION_NONCE_SIZE = 2 ** FRACTION_NONCE_BITS;
const FRACTION_NONCE_MASK = FRACTION_NONCE_SIZE - 1;
const outstandingTransmits = new Set();

/** A real millisecond-resolution timestamp with unpredictable insignificant bits. */
function clientPacket() {
	const epochMs = wallEpoch();
	if (!Number.isFinite(epochMs)) throw new Error('ntp: local clock is not finite');
	const seconds = Math.floor(epochMs / 1000);
	const significant = Math.floor((epochMs - seconds * 1000) * 1.024) * FRACTION_NONCE_SIZE;
	let nonce = Buffer.from(randomBytes(4)).readUInt32BE(0) & FRACTION_NONCE_MASK;
	const transmit = Buffer.alloc(8);
	transmit.writeUInt32BE((seconds + NTP_UNIX_OFFSET_SECONDS) >>> 0, 0);
	const zeroBucket = transmit.readUInt32BE(0) === 0 && significant === 0;
	// RFC 5905 section 6 recommends randomizing non-significant fraction bits.
	// Ten significant bits retain the runtime clock's millisecond resolution.
	// Probe a bounded number of slots if simultaneous reads collide, including
	// under a deterministic simulation RNG. A pending read never shares its token.
	const attempts = Math.min(outstandingTransmits.size + 1, FRACTION_NONCE_SIZE);
	for (let attempt = 0; attempt < attempts; attempt++) {
		if (zeroBucket && nonce === 0) nonce = 1;
		transmit.writeUInt32BE(significant + nonce, 4);
		const key = transmit.toString('hex');
		if (key !== '0000000000000000' && !outstandingTransmits.has(key)) {
			outstandingTransmits.add(key);
			const packet = Buffer.alloc(48);
			packet[0] = 0x23; // LI=0, VN=4, mode=3 (client).
			transmit.copy(packet, 40);
			return { packet, transmit, key };
		}
		nonce = (nonce + 1) & FRACTION_NONCE_MASK;
	}
	throw new Error('ntp: no unique transmit timestamp available');
}

/**
 * @typedef {Object} NtpTransport
 * @property {(packet: Buffer, host: string, port: number, timeoutMs: number) => Promise<Buffer>} request -
 *   Send one request datagram and resolve with a reply from that peer (or
 *   reject on error/timeout). Reply bytes 24..31 must echo request bytes 40..47.
 *   The default transport opens a fresh connected IPv4 UDP socket per read and
 *   closes it when settled; a custom transport owns its peer/timeout cleanup.
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
 * @property {() => Promise<number>} read - One unicast SNTP round trip; resolves
 *   with synchronized NTPv3/v4 server time as epoch milliseconds. Rejects on
 *   timeout, invalid synchronization/header fields or request correlation.
 */

/** One-socket-per-read UDP transport with a seam-armed timeout. */
function createDgramTransport() {
	return {
		request(packet, host, port, timeoutMs) {
			return new Promise((resolve, reject) => {
				const socket = dgram.createSocket('udp4');
				let timer = null;
				let settled = false, peer;
				const transmit = Buffer.from(packet.subarray(40, 48));
				const settle = (fn, arg) => {
					if (settled) return;
					settled = true;
					if (timer) { clearTimer(timer); timer = null; }
					try { socket.close(); } catch { /* already closed */ }
					fn(arg);
				};
				timer = setTimer(() => settle(reject, new Error(`ntp: no reply from ${host}:${port} within ${timeoutMs}ms`)), timeoutMs);
				socket.on('error', (err) => settle(reject, err));
				socket.on('message', (msg, rinfo) => {
					if (settled || !peer || rinfo.address !== peer.address || rinfo.port !== peer.port) return;
					if (msg.length < 48 || !msg.subarray(24, 32).equals(transmit)) return;
					settle(resolve, msg);
				});
				try {
					// Connected UDP fixes the resolved destination for this request and
					// filters other peers in the kernel; retain the explicit tuple check.
					socket.connect(port, host, (err) => {
						if (settled) return;
						if (err) { settle(reject, err); return; }
						try {
							peer = socket.remoteAddress();
							socket.send(packet, (error) => { if (error) settle(reject, error); });
						} catch (error) { settle(reject, error); }
					});
				} catch (error) { settle(reject, error); }
			});
		}
	};
}

/**
 * Parse an SNTP reply's transmit timestamp into epoch milliseconds.
 * @param {Buffer} reply
 * @returns {number}
 */
function parseTransmitEpochMs(reply, transmit) {
	if (!Buffer.isBuffer(reply) || reply.length < 48) {
		throw new Error('ntp: reply shorter than the 48-byte SNTP header');
	}
	const version = (reply[0] >>> 3) & 0x07;
	if (version !== 3 && version !== 4) throw new Error(`ntp: unsupported reply version ${version}`);
	const mode = reply[0] & 0x07;
	if (mode !== 4) {
		throw new Error(`ntp: unexpected reply mode ${mode} (want 4 server)`);
	}
	if ((reply[0] >>> 6) === 3) throw new Error('ntp: server leap indicator is unsynchronized');
	const stratum = reply[1];
	if (stratum === 0) {
		// RFC 4330: stratum 0 is a kiss-of-death packet - the server refuses.
		throw new Error('ntp: kiss-of-death reply (stratum 0) - server refused');
	}
	if (stratum > 15) throw new Error(`ntp: unsynchronized or reserved stratum ${stratum}`);
	if (!reply.subarray(24, 32).equals(transmit)) throw new Error('ntp: reply originate timestamp does not match the request');
	const seconds = reply.readUInt32BE(40);
	const fraction = reply.readUInt32BE(44);
	if (seconds === 0) {
		throw new Error('ntp: reply carries a zero transmit timestamp');
	}
	return (seconds - NTP_UNIX_OFFSET_SECONDS) * 1000 + Math.round((fraction / 0x100000000) * 1000);
}

const validateOptions = defineOptions({
	factory: 'createNtpSource',
	label: 'ntp',
	types: 'NtpSourceOptions in redis/ntp-source.d.ts'
}, {
	host: option.known,
	port: option.known,
	// The default transport arms its reply timeout with this value.
	timeoutMs: timerCeiling(),
	transport: option.known
});

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
	options = validateOptions(options);
	const host = options.host;
	const port = options.port ?? DEFAULT_PORT;
	if (!Number.isInteger(port) || port < 1 || port > 65535) {
		throw new Error('ntp: port must be an integer in 1..65535');
	}
	const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
	if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
		throw new Error('ntp: timeoutMs must be a positive number (ms)');
	}
	if (options.transport != null && (typeof options.transport !== 'object' || typeof options.transport.request !== 'function')) {
		throw new Error('ntp: transport must expose request(packet, host, port, timeoutMs) => Promise<Buffer>');
	}
	const transport = options.transport ?? createDgramTransport();

	return {
		async read() {
			const { packet, transmit, key } = clientPacket();
			try {
				const reply = await transport.request(packet, host, port, timeoutMs);
				return parseTransmitEpochMs(reply, transmit);
			} finally { outstandingTransmits.delete(key); }
		}
	};
}
