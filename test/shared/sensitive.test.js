import { describe, it, expect } from 'vitest';
import { redactConnectionUrl, stripInternal } from '../../shared/sensitive.js';

describe('shared/sensitive: redactConnectionUrl', () => {
	it('replaces the password segment with *** for redis URLs', () => {
		expect(redactConnectionUrl('redis://:s3cret@redis.internal:6379'))
			.toBe('redis://:***@redis.internal:6379');
	});

	it('replaces the password segment for user:password URLs', () => {
		expect(redactConnectionUrl('redis://alice:hunter2@10.0.0.1:6379'))
			.toBe('redis://alice:***@10.0.0.1:6379');
	});

	it('replaces the password segment for postgres URLs', () => {
		expect(redactConnectionUrl('postgres://app:long-rotating-password@db.internal:5432/main'))
			.toBe('postgres://app:***@db.internal:5432/main');
	});

	it('preserves URLs with no userinfo segment', () => {
		expect(redactConnectionUrl('redis://redis.internal:6379'))
			.toBe('redis://redis.internal:6379');
		expect(redactConnectionUrl('redis://localhost'))
			.toBe('redis://localhost');
	});

	it('preserves URLs with userinfo but no password (user-only)', () => {
		// Postgres allows `postgres://user@host` - no password to redact.
		expect(redactConnectionUrl('postgres://app@db.internal:5432/main'))
			.toBe('postgres://app@db.internal:5432/main');
	});

	it('does not match `:password@` outside the host:port segment', () => {
		// A path-segment colon must not trigger the redaction regex.
		// rediss://host:6379/path:with:colons - preserved verbatim.
		expect(redactConnectionUrl('rediss://host:6379/path:with:colons'))
			.toBe('rediss://host:6379/path:with:colons');
	});

	it('preserves a trailing query string with `@` characters', () => {
		expect(redactConnectionUrl('postgres://app:secret@db:5432/main?application_name=foo@svc'))
			.toBe('postgres://app:***@db:5432/main?application_name=foo@svc');
	});

	it('returns "undefined" / "null" / "" string for non-string input (defensive)', () => {
		expect(redactConnectionUrl(undefined)).toBe('undefined');
		expect(redactConnectionUrl(null)).toBe('null');
		expect(redactConnectionUrl('')).toBe('');
		expect(redactConnectionUrl(42)).toBe('42');
	});

	it('does not modify URLs with embedded `@` in the password segment matched by the regex once', () => {
		// The standard URL-encoded form for `@` in password is `%40`. Plain
		// `@` characters in password segments are technically illegal, but
		// libraries commonly accept them by greedy matching to the last `@`.
		// Our regex stops at the first `@` after the userinfo separator, so
		// this collapses to `:***@host`.
		const out = redactConnectionUrl('redis://:weird%40pass@host:6379');
		expect(out).toBe('redis://:***@host:6379');
	});

	it('redacts passwords containing unescaped `@` (walks to last @ in authority)', () => {
		// pg / libpq accept unescaped `@` in passwords by greedy-matching to
		// the last `@` before the path. The old regex stopped at the first
		// `@` and leaked the tail of the password into the "host" segment.
		expect(redactConnectionUrl('postgres://user:p@ssword@db.internal:5432/main'))
			.toBe('postgres://user:***@db.internal:5432/main');
		expect(redactConnectionUrl('redis://:p@ss@host:6379'))
			.toBe('redis://:***@host:6379');
	});

	it('redacts passwords for IPv6 hosts (skips authority-terminator detection inside brackets)', () => {
		expect(redactConnectionUrl('redis://:secret@[::1]:6379'))
			.toBe('redis://:***@[::1]:6379');
		expect(redactConnectionUrl('redis://user:secret@[2001:db8::1]:6379'))
			.toBe('redis://user:***@[2001:db8::1]:6379');
		// IPv6 host with no userinfo passes through unchanged.
		expect(redactConnectionUrl('redis://[::1]:6379'))
			.toBe('redis://[::1]:6379');
	});

	it('redacts password-with-@ AND IPv6 host together', () => {
		expect(redactConnectionUrl('postgres://app:p@ss@word@[2001:db8::1]:5432/main'))
			.toBe('postgres://app:***@[2001:db8::1]:5432/main');
	});

	it('redacts `password=` query string parameters (case-insensitive)', () => {
		expect(redactConnectionUrl('postgres://host/db?password=hunter2'))
			.toBe('postgres://host/db?password=***');
		expect(redactConnectionUrl('postgres://host/db?PASSWORD=hunter2'))
			.toBe('postgres://host/db?PASSWORD=***');
	});

	it('redacts `pass=` and `pwd=` query parameters (alternate names libpq tolerates)', () => {
		expect(redactConnectionUrl('postgres://host/db?pass=hunter2'))
			.toBe('postgres://host/db?pass=***');
		expect(redactConnectionUrl('postgres://host/db?pwd=hunter2'))
			.toBe('postgres://host/db?pwd=***');
	});

	it('redacts password query param while preserving sibling params', () => {
		expect(redactConnectionUrl('postgres://host/db?sslmode=require&password=hunter2&application_name=foo'))
			.toBe('postgres://host/db?sslmode=require&password=***&application_name=foo');
	});

	it('redacts BOTH userinfo password and query-string password in the same URL', () => {
		expect(redactConnectionUrl('postgres://user:userpw@host:5432/db?password=querypw'))
			.toBe('postgres://user:***@host:5432/db?password=***');
	});

	it('does not match `password` as a substring of a non-password param', () => {
		// `application_name` and `oauth_password_grant` contain `password` /
		// `pass` but not as a top-level query key, so leave them alone.
		expect(redactConnectionUrl('postgres://host/db?application_name=foo'))
			.toBe('postgres://host/db?application_name=foo');
		// A param whose key STARTS with `pass` but is not exactly `pass` /
		// `password` should also be left alone (e.g. `passthrough=1`).
		expect(redactConnectionUrl('postgres://host/db?passthrough=1'))
			.toBe('postgres://host/db?passthrough=1');
	});

	it('does not redact `user:@host` (empty password)', () => {
		expect(redactConnectionUrl('redis://user:@host:6379'))
			.toBe('redis://user:@host:6379');
	});

	it('preserves fragment / hash after authority', () => {
		expect(redactConnectionUrl('redis://user:secret@host:6379/path#frag'))
			.toBe('redis://user:***@host:6379/path#frag');
	});

	it('does not match `?password=` inside a fragment (after #)', () => {
		// `#` ends the query region; `password=` inside the fragment is
		// not a query param and should not be touched.
		expect(redactConnectionUrl('redis://host/path#anchor?password=notreallyaparam'))
			.toBe('redis://host/path#anchor?password=notreallyaparam');
	});
});

describe('shared/sensitive: stripInternal still works (regression check)', () => {
	it('strips obvious credential keys', () => {
		const out = stripInternal({ id: 'u', token: 'abc', password: 'p', name: 'Alice' });
		expect(out).toEqual({ id: 'u', name: 'Alice' });
	});

	it('drops __proto__ keys (own property from JSON.parse) so Object.assign cannot trigger setPrototypeOf', () => {
		// JSON.parse exposes `__proto__` as an own enumerable property,
		// unlike object literals where it is a setter on Object.prototype.
		const tainted = JSON.parse('{"id":"u","__proto__":{"polluted":true}}');
		const out = stripInternal(tainted);
		expect(out).toEqual({ id: 'u' });
		// Spreading the result into a fresh target must not pollute the
		// shared prototype: defensive smoke check on the chain.
		const target = {};
		Object.assign(target, out);
		expect(/** @type {any} */ ({}).polluted).toBeUndefined();
	});

	it('drops constructor and prototype keys (defense-in-depth)', () => {
		const tainted = { id: 'u', constructor: 'forged', prototype: 'forged' };
		expect(stripInternal(tainted)).toEqual({ id: 'u' });
	});

	it('replaces Buffer with a "[bytes: N]" placeholder instead of leaking each byte', () => {
		// Without the binary-view branch, Object.keys(Buffer) returns the
		// stringified numeric indices and JSON.stringify would emit
		// `{"0":106,"1":119,"2":116,...}` - raw bytes leaking into logs.
		const tokenBytes = Buffer.from('jwt-token-bytes', 'utf8');
		const out = stripInternal({ id: 'u', payload: tokenBytes });
		expect(out).toEqual({ id: 'u', payload: '[bytes: 15]' });
	});

	it('replaces Uint8Array with the same placeholder', () => {
		const view = new Uint8Array([1, 2, 3, 4]);
		expect(stripInternal(view)).toBe('[bytes: 4]');
	});

	it('replaces other typed-array views (Int32Array, Float64Array)', () => {
		expect(stripInternal(new Int32Array([0, 1, 2]))).toBe('[bytes: 12]');
		expect(stripInternal(new Float64Array([1.5, 2.5]))).toBe('[bytes: 16]');
	});

	it('replaces DataView with the placeholder (also a binary view)', () => {
		const buf = new ArrayBuffer(8);
		expect(stripInternal(new DataView(buf))).toBe('[bytes: 8]');
	});

	it('replaces a raw ArrayBuffer with the placeholder', () => {
		expect(stripInternal(new ArrayBuffer(32))).toBe('[bytes: 32]');
	});

	it('JSON.stringify of the stripped payload is safe to ship to log aggregators', () => {
		// Smoke test: stringify must not contain the raw bytes of the
		// payload buffer, only the placeholder. The sensitive-named keys
		// are dropped entirely; the byte-bearing field uses a non-sensitive
		// name so we can observe the binary-view substitution.
		const out = stripInternal({
			user: 'alice',
			secret_token: 'will-be-stripped',
			profileImage: Buffer.from('imgdata', 'utf8')
		});
		const serialized = JSON.stringify(out);
		expect(serialized).not.toContain('imgdata');
		expect(serialized).not.toContain('will-be-stripped');
		expect(serialized).toContain('[bytes: 7]');
	});
});
