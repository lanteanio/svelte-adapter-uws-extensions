import { describe, it, expect } from 'vitest';
import { isPrivateOrLoopbackAddress, isAddressHeaderConfigured } from '../../src/shared/client-ip.js';

describe('shared/client-ip', () => {
	describe('isPrivateOrLoopbackAddress', () => {
		it('classifies IPv4 loopback / private / link-local / unspecified as true', () => {
			for (const ip of [
				'127.0.0.1', '127.1.2.3',
				'10.0.0.1', '10.255.255.255',
				'172.16.0.1', '172.31.255.255', '172.17.0.1',
				'192.168.0.1', '192.168.1.1',
				'169.254.1.1',
				'0.0.0.0'
			]) {
				expect(isPrivateOrLoopbackAddress(ip), ip).toBe(true);
			}
		});

		it('classifies routable IPv4 as false', () => {
			for (const ip of [
				'1.2.3.4', '8.8.8.8', '203.0.113.7',
				'172.15.0.1', '172.32.0.1', // just outside 172.16.0.0/12
				'192.167.0.1', '192.169.0.1', // just outside 192.168.0.0/16
				'169.253.0.1', '169.255.0.1' // just outside 169.254.0.0/16
			]) {
				expect(isPrivateOrLoopbackAddress(ip), ip).toBe(false);
			}
		});

		it('classifies IPv6 loopback / ULA / link-local / unspecified as true', () => {
			for (const ip of ['::1', '::', 'fe80::1', 'FE80::abcd', 'febf::1', 'fc00::1', 'fd12:3456::1']) {
				expect(isPrivateOrLoopbackAddress(ip), ip).toBe(true);
			}
		});

		it('classifies global-unicast IPv6 as false', () => {
			for (const ip of ['2001:4860:4860::8888', '2606:4700:4700::1111', 'fec0::1']) {
				expect(isPrivateOrLoopbackAddress(ip), ip).toBe(false);
			}
		});

		it('handles IPv4-mapped IPv6 by the embedded tail', () => {
			expect(isPrivateOrLoopbackAddress('::ffff:127.0.0.1')).toBe(true);
			expect(isPrivateOrLoopbackAddress('::ffff:10.0.0.1')).toBe(true);
			expect(isPrivateOrLoopbackAddress('::ffff:8.8.8.8')).toBe(false);
		});

		it('strips a zone id and surrounding brackets', () => {
			expect(isPrivateOrLoopbackAddress('fe80::1%eth0')).toBe(true);
			expect(isPrivateOrLoopbackAddress('[::1]')).toBe(true);
			expect(isPrivateOrLoopbackAddress('  127.0.0.1  ')).toBe(true);
		});

		it('treats sentinels and unusable input as a collapse signature', () => {
			for (const v of ['', 'unknown', 'UNKNOWN', null, undefined, 42, {}]) {
				expect(isPrivateOrLoopbackAddress(v)).toBe(true);
			}
		});

		it('returns false for malformed dotted quads (not coincidentally private)', () => {
			for (const ip of ['256.1.1.1', '10.0.0', '10.0.0.0.0', '10.0.0.x', 'not-an-ip']) {
				expect(isPrivateOrLoopbackAddress(ip), ip).toBe(false);
			}
		});
	});

	describe('isAddressHeaderConfigured', () => {
		it('is false for an empty / blank env', () => {
			expect(isAddressHeaderConfigured({})).toBe(false);
			expect(isAddressHeaderConfigured({ ADDRESS_HEADER: '' })).toBe(false);
			expect(isAddressHeaderConfigured({ ADDRESS_HEADER: '   ' })).toBe(false);
		});

		it('detects the unprefixed ADDRESS_HEADER', () => {
			expect(isAddressHeaderConfigured({ ADDRESS_HEADER: 'x-forwarded-for' })).toBe(true);
		});

		it('detects an adapter-envPrefix form (*_ADDRESS_HEADER)', () => {
			expect(isAddressHeaderConfigured({ MYAPP_ADDRESS_HEADER: 'x-real-ip' })).toBe(true);
		});

		it('ignores unrelated env names', () => {
			expect(isAddressHeaderConfigured({ ADDRESS_HEADERS: 'x', SOME_HEADER: 'y' })).toBe(false);
		});
	});
});
