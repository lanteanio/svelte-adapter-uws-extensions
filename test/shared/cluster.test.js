import { describe, it, expect } from 'vitest';
import { keySlot, isCluster, execMultiSlot } from '../../src/shared/cluster.js';

describe('keySlot', () => {
	it('matches the CRC-16/XMODEM conformance vector Redis uses', () => {
		// "123456789" is the standard CRC-16/XMODEM check value, 0x31C3 = 12739 -
		// the exact CRC Redis Cluster uses, and 12739 < 16384 so it is the slot.
		// This pins keySlot to CLUSTER KEYSLOT without needing a live server.
		expect(keySlot('123456789')).toBe(12739);
	});

	it('hashes only the hash-tag content when one is present', () => {
		// Co-location: anything sharing a {tag} maps to the same slot as the tag.
		expect(keySlot('{user1000}.following')).toBe(keySlot('user1000'));
		expect(keySlot('{user1000}.followers')).toBe(keySlot('user1000'));
		expect(keySlot('foo')).toBe(keySlot('{foo}'));
		expect(keySlot('uws:sharded:{room}:a')).toBe(keySlot('uws:sharded:{room}:b'));
	});

	it('follows the Redis hash-tag edge cases exactly', () => {
		// First '{' paired with the first '}' after it; inner braces are content.
		expect(keySlot('foo{{bar}}zap')).toBe(keySlot('{bar'));
		expect(keySlot('foo{bar}{zap}')).toBe(keySlot('bar'));
		// An empty first tag means the WHOLE key is hashed (no skip to the next
		// brace), so it must not collapse onto the inner tag's content.
		expect(keySlot('foo{}{bar}')).not.toBe(keySlot('bar'));
		// '{}' is hashed literally, not as an empty string.
		expect(keySlot('{}')).not.toBe(keySlot(''));
		expect(keySlot('')).toBe(0);
	});

	it('returns a valid slot in [0, 16383] for arbitrary keys', () => {
		// A multi-byte string (built from code points to keep this source ASCII)
		// exercises the UTF-8 byte hashing path.
		const multibyte = String.fromCodePoint(0xe9, 0xe8, 0x4f60, 0x597d);
		const keys = ['', 'a', 'uws:sharded:room-42', '{t}:x', multibyte, 'x'.repeat(2048)];
		for (const k of keys) {
			const slot = keySlot(k);
			expect(Number.isInteger(slot)).toBe(true);
			expect(slot).toBeGreaterThanOrEqual(0);
			expect(slot).toBeLessThan(16384);
		}
	});

	it('is deterministic', () => {
		expect(keySlot('uws:sharded:room1')).toBe(keySlot('uws:sharded:room1'));
	});
});

describe('isCluster', () => {
	it('is true only for an object exposing a nodes() method', () => {
		expect(isCluster({ nodes: () => [] })).toBe(true);
		expect(isCluster({})).toBe(false);
		expect(isCluster(null)).toBe(false);
		expect(isCluster(undefined)).toBe(false);
		expect(isCluster('redis')).toBe(false);
		expect(isCluster(42)).toBe(false);
	});
});

describe('execMultiSlot', () => {
	it('returns [] for no commands without touching the client', async () => {
		let touched = false;
		const redis = { pipeline() { touched = true; return { exec: async () => [] }; } };
		expect(await execMultiSlot(redis, [])).toEqual([]);
		expect(touched).toBe(false);
	});

	it('batches into a single pipeline on a standalone client', async () => {
		const calls = [];
		let execCount = 0;
		const pipe = {
			set(...a) { calls.push(['set', ...a]); return this; },
			expire(...a) { calls.push(['expire', ...a]); return this; },
			async exec() { execCount++; return calls.map(() => [null, 'OK']); }
		};
		const redis = { pipeline: () => pipe }; // no nodes() => standalone
		const res = await execMultiSlot(redis, [['set', 'k', 'v'], ['expire', 'k', 10]]);
		expect(execCount).toBe(1);
		expect(calls).toEqual([['set', 'k', 'v'], ['expire', 'k', 10]]);
		expect(res).toEqual([[null, 'OK'], [null, 'OK']]);
	});

	it('issues each command individually on a cluster, index-aligned [err, value]', async () => {
		const redis = {
			nodes: () => [], // marks this as a Cluster
			async set(k) { return 'set:' + k; },
			async del() { throw new Error('boom'); }
		};
		const res = await execMultiSlot(redis, [['set', 'a'], ['del', 'b']]);
		expect(res[0]).toEqual([null, 'set:a']);
		expect(res[1][0]).toBeInstanceOf(Error);
		expect(res[1][1]).toBeUndefined();
	});
});
