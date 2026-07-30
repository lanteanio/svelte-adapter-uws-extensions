import { describe, it, expect } from 'vitest';
import { capabilityCookie } from '../src/capability-cookie.js';

const SECRET = '9c6a2e5f8b3d10474edb92c05af6138b';

// cookieName, path and sameSite are concatenated straight into the
// Set-Cookie header, so they are validated at construction. Construction is
// the only place an operator sees the problem: a header the browser drops
// produces no error anywhere, and the capability check just degrades to
// permanently-absent.

describe('cookieName validation', () => {
	it('rejects bytes that would split the response or smuggle an attribute', () => {
		for (const bad of ['a\r\nb', 'a\nb', 'a;b']) {
			expect(() => capabilityCookie({ secret: SECRET, cookieName: bad })).toThrow('cookieName');
		}
	});

	it('rejects a name that is not an RFC 6265 token', () => {
		// These serialize into a header no browser accepts, so readCookie
		// would never match again.
		for (const bad of ['my cookie', 'a=b', 'a,b', 'a"b', 'a(b)', 'a@b', '']) {
			expect(() => capabilityCookie({ secret: SECRET, cookieName: bad })).toThrow('cookieName');
		}
	});

	it('accepts the full token charset', () => {
		for (const ok of ['cap', '__Host-cap', 'a.b_c-d', "a!#$%&'*+^`|~1"]) {
			expect(() => capabilityCookie({ secret: SECRET, cookieName: ok })).not.toThrow();
		}
	});
});

describe('path validation', () => {
	it('rejects header-unsafe and malformed paths', () => {
		for (const bad of ['/\r\nX-Injected: 1', '/; Domain=evil', '/a b', '/a,b', '/a"b', 'relative', '']) {
			expect(() => capabilityCookie({ secret: SECRET, path: bad })).toThrow('path');
		}
	});

	it('rejects NUL, the C0 controls, DEL and non-ASCII', () => {
		// These are the bytes a forbidden-character list misses. NUL and any
		// codepoint past U+00FF make the header layer itself throw (see the
		// header-layer test below), which is a per-request 500 on every
		// issue(); the rest produce a Path attribute no browser honours.
		for (const bad of ['/a\0b', '/a\x01b', '/a\x1fb', '/a\x7fb', '/café', '/日']) {
			expect(() => capabilityCookie({ secret: SECRET, path: bad })).toThrow('path');
		}
	});

	it('accepts ordinary paths', () => {
		for (const ok of ['/', '/app', '/a/b/c']) {
			expect(() => capabilityCookie({ secret: SECRET, path: ok })).not.toThrow();
		}
	});

	it('accepts the printable-ASCII path charset the RFC 6265 grammar allows', () => {
		// Narrower than path-value by exactly whitespace, ',' and '"'; every
		// other printable ASCII character stays legal, so widening the guard
		// did not turn a working config into a startup crash.
		for (const ok of ['/a-b.c_d~e', '/a%20b', '/a:b@c', "/a'b", '/a(b)', '/a=b&c', '/a+b', '/a$b!', '/a<b>c', '/a\\b', '/a`b', '/a?b', '/a#b', '/a[b]', '/a{b}', '/a|b^c']) {
			expect(() => capabilityCookie({ secret: SECRET, path: ok })).not.toThrow();
		}
	});

	it('emits a Set-Cookie value the real header layer accepts, for every accepted path', () => {
		// The constructor is not the point - the byte reaching the wire is.
		// Anything issue() produces has to survive Headers.append, which is
		// what SvelteKit's Response is backed by.
		for (const ok of ['/', '/app', '/a/b/c', '/a-b.c_d~e', "/a'b", '/a\\b', '/a[b]', '/a<b>c']) {
			const setCookies = [];
			const res = { headers: { append: (name, value) => setCookies.push([name, value]) } };
			capabilityCookie({ secret: SECRET, path: ok }).issue({ locals: { sessionId: 'sess-1' } }, res);
			expect(setCookies).toHaveLength(1);
			const [name, value] = setCookies[0];
			expect(value).toContain('; Path=' + ok + ';');
			expect(() => new Headers().append(name, value)).not.toThrow();
		}
	});

	it('is guarding against a header layer that really does throw on these bytes', () => {
		// Pins the reason the guard exists. Without it these two reach
		// serializeCookie and every issue() becomes a 500 - the failure the
		// construction-time check exists to move to startup.
		for (const bad of ['/a\0b', '/日']) {
			const value = 'cap=x; Path=' + bad + '; Max-Age=60; HttpOnly; SameSite=Lax';
			expect(() => new Headers().append('set-cookie', value)).toThrow();
		}
	});
});

describe('sameSite validation', () => {
	it('rejects a value that is not a SameSite mode', () => {
		expect(() => capabilityCookie({ secret: SECRET, sameSite: 'Bogus' })).toThrow('sameSite');
		expect(() => capabilityCookie({ secret: SECRET, sameSite: 42 })).toThrow('sameSite');
	});

	it('accepts any casing, because cookie attribute values are case-insensitive', () => {
		// Rejecting 'lax' would turn a config that worked yesterday into a
		// startup crash, and the serialized header is identical either way.
		for (const s of ['Strict', 'Lax', 'None', 'strict', 'lax', 'none', 'LAX']) {
			expect(() => capabilityCookie({ secret: SECRET, sameSite: s })).not.toThrow();
		}
	});

	it('normalizes the casing into the emitted header', () => {
		const setCookies = [];
		const res = { headers: { append: (name, value) => setCookies.push([name, value]) } };
		const event = { locals: { sessionId: 'sess-1' } };
		capabilityCookie({ secret: SECRET, sameSite: 'lax' }).issue(event, res);
		capabilityCookie({ secret: SECRET, sameSite: 'Lax' }).issue(event, res);

		expect(setCookies).toHaveLength(2);
		for (const [, value] of setCookies) expect(value).toContain('SameSite=Lax');
		// Same attribute string either way: the casing was normalized, not
		// passed through.
		const attrs = (v) => v.split(';').slice(1).join(';');
		expect(attrs(setCookies[0][1])).toBe(attrs(setCookies[1][1]));
	});
});

describe('sameSite lookup safety', () => {
	it('rejects prototype keys that a plain object lookup would resolve', () => {
		// An object literal inherits from Object.prototype, so `constructor`
		// resolves to the Object constructor and `__proto__` to the prototype
		// itself. Both are non-undefined, so both slip past an
		// `=== undefined` guard and get serialized into the Set-Cookie header.
		for (const bad of ['__proto__', 'constructor', 'prototype', 'valueOf', 'toString']) {
			expect(() => capabilityCookie({ secret: SECRET, sameSite: bad })).toThrow(/sameSite/);
		}
	});

	it('rejects an ordinary unknown value, so the guard is not prototype-specific', () => {
		expect(() => capabilityCookie({ secret: SECRET, sameSite: 'Loose' })).toThrow(/sameSite/);
	});
});
