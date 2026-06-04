/**
 * keySlot() parity with the live Redis CLUSTER KEYSLOT.
 *
 * The sharded bus resolves a channel's hash slot client-side (CRC16 of the
 * hash-tag) instead of a CLUSTER KEYSLOT round trip. A key's slot is
 * topology-independent, so this is a pure equality check against the server's
 * own computation - the definitive proof the client-side CRC16 matches Redis
 * byte-for-byte, including multi-byte UTF-8 channel names. Cluster-only (it needs
 * a cluster client to ask). The CRC16 conformance unit test in
 * test/shared/cluster.test.js runs everywhere and pins the algorithm offline.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
import { keySlot } from '../../../shared/cluster.js';

describe.runIf(isClusterBackend())('keySlot parity with CLUSTER KEYSLOT (cluster only)', () => {
	let client;

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-keyslot:' });
	});

	afterAll(async () => {
		await client.quit();
	});

	it('equals the live CLUSTER KEYSLOT for channel names, hash tags, and edge cases', async () => {
		// Multi-byte channel built from code points so this source stays ASCII;
		// it exercises the UTF-8 byte hashing path against the live server.
		const multibyte = 'uws:sharded:multibyte-' + String.fromCodePoint(0xe9, 0xe8, 0x4f60, 0x597d);
		const keys = [
			'uws:sharded:room1',
			'uws:sharded:room-42',
			'uws:sharded:' + 'x'.repeat(256),
			'uws:sharded:{co-located}:a',
			'uws:sharded:{co-located}:b',
			multibyte,
			'{user1000}.following',
			'foo{}{bar}',
			'foo{{bar}}zap',
			'foo{bar}{zap}',
			'123456789'
		];
		for (const k of keys) {
			const live = Number(await client.redis.cluster('KEYSLOT', k));
			expect(keySlot(k), `slot mismatch for ${JSON.stringify(k)}`).toBe(live);
		}
		// And co-located channels share a slot (the server agrees above).
		expect(keySlot('uws:sharded:{co-located}:a')).toBe(keySlot('uws:sharded:{co-located}:b'));
	});
});
