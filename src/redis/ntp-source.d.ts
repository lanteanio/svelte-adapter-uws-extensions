/**
 * Minimal SNTP client: a third clock source for the cluster clock
 * (`redis/clock`). One 48-byte mode-3 request, one mode-4 reply, the transmit
 * timestamp converted to epoch milliseconds. Off by default everywhere; built
 * on `node:dgram` (no dependency). The transport is injectable so tests and
 * simulations never open a real socket.
 */

/**
 * Injectable wire: send one request datagram and resolve with a reply from the
 * configured peer. A canned reply must copy request bytes 40..47 into its
 * originate timestamp (bytes 24..31). Implementations own their timeout/error
 * cleanup; the default transport uses one connected IPv4 UDP socket per read.
 */
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
	 * One unicast SNTP round trip; resolves with synchronized NTPv3/v4 server
	 * time as epoch milliseconds. Only mode-4 replies with a matching originate
	 * timestamp, LI 0..2 and stratum 1..15 are accepted. The default transport
	 * ignores unrelated peers/requests until its original deadline. Rejects on
	 * timeout, malformed/unsynchronized replies or request-correlation failure.
	 */
	read(): Promise<number>;
}

/** Create an SNTP clock source for `createClusterClock({ ntp })`. */
export function createNtpSource(options: NtpSourceOptions): NtpSource;
