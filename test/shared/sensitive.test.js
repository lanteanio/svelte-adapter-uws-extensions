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
});

describe('shared/sensitive: stripInternal still works (regression check)', () => {
	it('strips obvious credential keys', () => {
		const out = stripInternal({ id: 'u', token: 'abc', password: 'p', name: 'Alice' });
		expect(out).toEqual({ id: 'u', name: 'Alice' });
	});
});
