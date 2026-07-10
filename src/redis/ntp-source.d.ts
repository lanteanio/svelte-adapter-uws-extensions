/**
 * Minimal SNTP client: a third clock source for the cluster clock
 * (`redis/clock`). One 48-byte mode-3 request, one mode-4 reply, the transmit
 * timestamp converted to epoch milliseconds. Off by default everywhere; built
 * on `node:dgram` (no dependency). The transport is injectable so tests and
 * simulations never open a real socket.
 */

/** Injectable wire: send one request datagram, resolve with the first reply. */
export interface NtpTransport {
	request(packet: Buffer, host: string, port: number, timeoutMs: number): Promise<Buffer>;
}

export interface NtpSourceOptions {
	/** NTP server hostname or IP. Required. */
	host: string;
	/** @default 123 */
	port?: number;
	/** Reply deadline per read, in milliseconds. @default 2000 */
	timeoutMs?: number;
	/** Injectable wire (tests/simulations). Defaults to a one-socket-per-read UDP transport. */
	transport?: NtpTransport;
}

export interface NtpSource {
	/**
	 * One SNTP round trip; resolves with the server clock as epoch
	 * milliseconds. Rejects on timeout, a malformed reply, or a kiss-of-death
	 * (stratum 0) reply.
	 */
	read(): Promise<number>;
}

/** Create an SNTP clock source for `createClusterClock({ ntp })`. */
export function createNtpSource(options: NtpSourceOptions): NtpSource;
