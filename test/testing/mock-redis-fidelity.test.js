import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';

// A double that is looser than production lets a tenant-isolation or
// purge-completeness test pass against matching semantics Redis does not
// have. A double that is STRICTER hides prototype-sensitive consumer bugs.
// Both directions are failures, so these pin the double to real behavior.

describe('SCAN MATCH glob translation', () => {
	async function scanner(keys) {
		const client = mockRedisClient('');
		for (const k of keys) await client.redis.set(k, '1');
		return async (pattern) => {
			const out = [];
			let cur = '0';
			do {
				const [next, ks] = await client.redis.scan(cur, 'MATCH', pattern, 'COUNT', 1000);
				cur = next;
				out.push(...ks);
			} while (cur !== '0');
			return out.sort();
		};
	}

	it('treats regex metacharacters as literals', async () => {
		const scan = await scanner(['a.b', 'axb', 'a+b', 'aab', 'a(b)', 'a$b']);
		expect(await scan('a.b')).toEqual(['a.b']);
		expect(await scan('a+b')).toEqual(['a+b']);
		expect(await scan('a(b)')).toEqual(['a(b)']);
		expect(await scan('a$b')).toEqual(['a$b']);
	});

	it('matches * and ? including keys containing a newline', async () => {
		const scan = await scanner(['ab', 'a\nb', 'abc', 'b']);
		expect(await scan('a?')).toEqual(['ab']);
		expect(await scan('a?b')).toEqual(['a\nb']);
		expect((await scan('a*')).sort()).toEqual(['a\nb', 'ab', 'abc'].sort());
	});

	it('honors character classes, ranges and negation', async () => {
		const scan = await scanner(['a1', 'a2', 'a9', 'ab', 'aB']);
		expect(await scan('a[12]')).toEqual(['a1', 'a2']);
		expect(await scan('a[1-2]')).toEqual(['a1', 'a2']);
		expect(await scan('a[^0-9]')).toEqual(['aB', 'ab']);
	});

	it('treats an escaped class member as a literal, not a range endpoint', async () => {
		// '[a\-c]' is the three members {a, -, c}. Translated naively it
		// becomes the range 0x5C-0x63, which both misses 'x-y' and wrongly
		// matches 'xby' - a false POSITIVE in a purge-scope test.
		const scan = await scanner(['x-y', 'xay', 'xby', 'xcy']);
		expect(await scan('x[a\\-c]y')).toEqual(['x-y', 'xay', 'xcy']);
	});

	it('treats an ESCAPED ] as a class member', async () => {
		const scan = await scanner([']', 'a', 'b', 'ab]']);
		expect(await scan('[a\\]b]')).toEqual([']', 'a', 'b']);
	});

	// The cases below all disagreed with a real Redis 7 under the previous
	// regex translation; four of them made the double THROW rather than
	// answer. Every expectation here was read off a live server, and the
	// integration parity suite re-derives them from one on each run.

	it('closes the class on an UNESCAPED leading ], matching nothing', async () => {
		// Most glob dialects read a leading ']' as a literal member. Redis does
		// not - it breaks out of the class immediately, leaving it empty. The
		// old translation read it as a member, which made the double LOOSER
		// than production: the direction that reports a scope bug as fixed.
		const scan = await scanner([']', 'a', 'b', 'c']);
		expect(await scan('[]abc]')).toEqual([]);
		expect(await scan('[]]')).toEqual([]);
	});

	it('swaps a reversed range instead of throwing', async () => {
		// `[c-a]` is not an error to Redis; it swaps the endpoints and matches
		// a..c. Translated to a JS character class it raises `Range out of
		// order`, so a scan became a hard exception out of the double.
		const scan = await scanner(['a', 'b', 'c', 'd']);
		expect(await scan('[c-a]')).toEqual(['a', 'b', 'c']);
		expect(await scan('[z-a]')).toEqual(['a', 'b', 'c', 'd']); // a..z spans all four
		expect(await scan('[^c-a]')).toEqual(['d']);
	});

	it('reads [a-] as the range a..] with the endpoints swapped', async () => {
		// Not the two members {a, -}: a trailing '-' still forms a range, here
		// with ']' (0x5D) as the far endpoint, so it matches 0x5D..0x61.
		const scan = await scanner([']', '^', '_', '`', 'a', '-', 'b']);
		expect(await scan('[a-]')).toEqual([']', '^', '_', '`', 'a'].sort());
	});

	it('treats ! as an ordinary member, not a negation', async () => {
		const scan = await scanner(['!', 'a', 'b', 'c', 'd']);
		expect(await scan('[!abc]')).toEqual(['!', 'a', 'b', 'c']);
	});

	it('returns promptly on a pattern that backtracks catastrophically as a regex', async () => {
		// `*a*a*...*b` over a long non-matching subject took 148s translated to
		// a regex. The ported matcher stops the whole search the first time the
		// tail fails at every remaining offset.
		const scan = await scanner(['a'.repeat(64)]);
		const t0 = process.hrtime.bigint();
		expect(await scan('*a'.repeat(24) + '*b')).toEqual([]);
		expect(Number(process.hrtime.bigint() - t0) / 1e6).toBeLessThan(1000);
	});

	it('escapes a glob metacharacter with a backslash', async () => {
		const scan = await scanner(['k*z', 'k1z', 'kz']);
		expect(await scan('k\\*z')).toEqual(['k*z']);
		expect((await scan('k*z')).sort()).toEqual(['k*z', 'k1z', 'kz'].sort());
	});

	it('consumes an unterminated class to the end of the pattern', async () => {
		const scan = await scanner(['a', 'b', 'c', 'd', '[abc']);
		expect(await scan('[abc')).toEqual(['a', 'b', 'c']);
	});

	it('scopes a prefix scan to that prefix', async () => {
		const scan = await scanner(['tenant:a:1', 'tenant:a:2', 'tenant:b:1']);
		expect(await scan('tenant:a:*')).toEqual(['tenant:a:1', 'tenant:a:2']);
	});
});

describe('hash replies', () => {
	it('returns __proto__ as an ordinary own property, like ioredis', async () => {
		const client = mockRedisClient('');
		await client.redis.hset('h', '__proto__', 'plain', 'other', 'v');
		const all = await client.redis.hgetall('h');

		expect(Object.prototype.hasOwnProperty.call(all, '__proto__')).toBe(true);
		expect(all.__proto__).toBe('plain');
		expect(JSON.stringify(all)).toContain('__proto__');
		// ioredis builds a NORMAL object (defineProperty only for a key
		// already present), so a null-prototype reply would be safer than
		// production and would hide prototype-sensitive consumer bugs.
		expect(Object.getPrototypeOf(all)).toBe(Object.prototype);
		expect(typeof all.hasOwnProperty).toBe('function');
	});

	it('leaves ordinary hashes untouched', async () => {
		const client = mockRedisClient('');
		await client.redis.hset('h2', 'a', '1', 'b', '2');
		expect(await client.redis.hgetall('h2')).toEqual({ a: '1', b: '2' });
	});
});

describe('stream commands', () => {
	it('deletes the addressed entries and counts only the ones present', async () => {
		// XDEL was missing entirely, and the one caller - the streams replay
		// backend's right-to-erasure purge - wraps it in a best-effort
		// `catch {}`. So the erasure reported success having deleted nothing,
		// and no unit test could tell.
		const client = mockRedisClient('');
		await client.redis.xadd('s', '1-0', 'v', 'a');
		await client.redis.xadd('s', '2-0', 'v', 'b');
		await client.redis.xadd('s', '3-0', 'v', 'c');

		expect(await client.redis.xdel('s', '2-0')).toBe(1);
		expect(await client.redis.xlen('s')).toBe(2);
		expect((await client.redis.xrange('s', '-', '+')).map(([id]) => id)).toEqual(['1-0', '3-0']);

		// Only ids that were actually there count, as on a real server.
		expect(await client.redis.xdel('s', '2-0', '3-0', '99-0')).toBe(1);
		expect(await client.redis.xdel('missing', '1-0')).toBe(0);
	});

	it('addresses a bare millisecond id as <ms>-0', async () => {
		const client = mockRedisClient('');
		await client.redis.xadd('s', '7-0', 'v', 'a');
		expect(await client.redis.xdel('s', 7)).toBe(1);
		expect(await client.redis.xlen('s')).toBe(0);
	});

	it('does not rewind the last-id, so a purged id cannot be reused', async () => {
		// Real Redis keeps last_id across XDEL. Deriving the top from the tail
		// ENTRY instead would let a purge-then-republish reuse an id the server
		// refuses - the double being looser than production on the exact path
		// the purge test exercises.
		const client = mockRedisClient('');
		await client.redis.xadd('s', '5-0', 'v', 'a');
		await client.redis.xadd('s', '9-0', 'v', 'b');
		expect(await client.redis.xdel('s', '9-0')).toBe(1);

		await expect(client.redis.xadd('s', '9-0', 'v', 'c')).rejects.toThrow(/equal or smaller/);
		await expect(client.redis.xadd('s', '6-0', 'v', 'c')).rejects.toThrow(/equal or smaller/);
		// Still open above the retained top.
		expect(await client.redis.xadd('s', '10-0', 'v', 'c')).toBe('10-0');
	});

	it('DOES reset the last-id when the key itself is deleted', async () => {
		const client = mockRedisClient('');
		await client.redis.xadd('s', '9-0', 'v', 'a');
		expect(await client.redis.del('s')).toBe(1);
		// A re-created stream starts over - DEL drops the stream object, where
		// XDEL only removes entries from it.
		expect(await client.redis.xadd('s', '1-0', 'v', 'b')).toBe('1-0');
	});
});
