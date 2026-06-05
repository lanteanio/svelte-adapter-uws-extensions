import { describe, it, expect, beforeEach, vi } from 'vitest';
import { capabilityCookie } from '../capability-cookie.js';

const COOKIE_NAME = 'sauws_cap';

/** Build a Response-like object that records appended Set-Cookie headers. */
function mockResponse() {
	const setCookies = [];
	return {
		setCookies,
		headers: {
			append(name, value) {
				if (String(name).toLowerCase() === 'set-cookie') setCookies.push(value);
			}
		}
	};
}

/** Build an event whose request carries a Cookie header (for refresh). */
function mockEvent({ cookie = null, locals = {} } = {}) {
	return {
		locals,
		request: {
			headers: {
				get(name) {
					if (String(name).toLowerCase() === 'cookie') return cookie;
					return null;
				}
			}
		}
	};
}

/** Pull the `name=value` portion (the cookie's wire value) out of a Set-Cookie. */
function cookieValueFrom(setCookie) {
	const first = setCookie.split(';')[0];
	return first; // already `name=value`
}

describe('capabilityCookie', () => {
	beforeEach(() => {
		vi.restoreAllMocks();
	});

	describe('construction validation', () => {
		it('throws without options', () => {
			expect(() => capabilityCookie()).toThrow('options object is required');
		});

		it('throws on empty / non-string secret', () => {
			expect(() => capabilityCookie({ secret: '' })).toThrow('secret must be a non-empty string');
			expect(() => capabilityCookie({ secret: 123 })).toThrow('secret must be a non-empty string');
		});

		it('throws on a bad previousSecret', () => {
			expect(() => capabilityCookie({ secret: 's', previousSecret: '' })).toThrow('previousSecret');
		});

		it('throws on non-positive ttlSeconds', () => {
			expect(() => capabilityCookie({ secret: 's', ttlSeconds: 0 })).toThrow('ttlSeconds must be a positive number');
		});

		it('returns the issue/refresh/verify API', () => {
			const cap = capabilityCookie({ secret: 's' });
			expect(typeof cap.issue).toBe('function');
			expect(typeof cap.refresh).toBe('function');
			expect(typeof cap.verify).toBe('function');
		});
	});

	describe('issue', () => {
		it('appends a signed Set-Cookie with the security attributes', () => {
			const cap = capabilityCookie({ secret: 'topsecret', ttlSeconds: 300 });
			const res = mockResponse();
			cap.issue(mockEvent(), res);

			expect(res.setCookies).toHaveLength(1);
			const sc = res.setCookies[0];
			expect(sc).toMatch(new RegExp('^' + COOKIE_NAME + '='));
			expect(sc).toContain('HttpOnly');
			expect(sc).toContain('Secure');
			expect(sc).toContain('SameSite=Lax');
			expect(sc).toContain('Path=/');
			expect(sc).toContain('Max-Age=300');
		});

		it('binds the cookie to a locals session id when present', () => {
			const cap = capabilityCookie({ secret: 's' });
			const res = mockResponse();
			cap.issue(mockEvent({ locals: { sessionId: 'sess-abc' } }), res);
			const value = cookieValueFrom(res.setCookies[0]);
			// The encoded sessionId is the first base64url field; decode it back.
			const raw = value.slice((COOKIE_NAME + '=').length);
			const sidField = raw.split('.')[0];
			const decodedSid = Buffer.from(sidField, 'base64url').toString('utf8');
			expect(decodedSid).toBe('sess-abc');
		});

		it('supports a plain setHeader-style response', () => {
			const cap = capabilityCookie({ secret: 's' });
			let stored = null;
			const res = {
				getHeader: () => stored,
				setHeader: (_, v) => { stored = v; }
			};
			cap.issue(mockEvent(), res);
			expect(typeof stored).toBe('string');
			expect(stored).toMatch(new RegExp('^' + COOKIE_NAME + '='));
		});
	});

	describe('verify - happy path', () => {
		it('accepts a freshly issued cookie', () => {
			const cap = capabilityCookie({ secret: 'topsecret' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			const header = cookieValueFrom(res.setCookies[0]);

			expect(cap.verify(header, { required: true })).toBe(true);
		});

		it('accepts the cookie alongside other cookies in the header', () => {
			const cap = capabilityCookie({ secret: 'topsecret' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			const capCookie = cookieValueFrom(res.setCookies[0]);
			const header = 'other=1; ' + capCookie + '; more=2';

			expect(cap.verify(header, { required: true })).toBe(true);
		});
	});

	describe('verify - required-ness keyed off posture', () => {
		it('an absent cookie passes when not required (normal posture)', () => {
			const cap = capabilityCookie({ secret: 's' });
			expect(cap.verify(null, { required: false })).toBe(true);
			expect(cap.verify(undefined)).toBe(true);
			expect(cap.verify('unrelated=1', { required: false })).toBe(true);
		});

		it('an absent cookie fails when required (elevated / siege posture)', () => {
			const cap = capabilityCookie({ secret: 's' });
			expect(cap.verify(null, { required: true })).toBe(false);
			expect(cap.verify('unrelated=1', { required: true })).toBe(false);
		});
	});

	describe('verify - rejection', () => {
		it('rejects a tampered signature even when not required', () => {
			const cap = capabilityCookie({ secret: 'topsecret' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			let header = cookieValueFrom(res.setCookies[0]);
			// Flip the last character of the signature.
			const last = header.slice(-1) === 'A' ? 'B' : 'A';
			header = header.slice(0, -1) + last;

			expect(cap.verify(header, { required: false })).toBe(false);
		});

		it('rejects a cookie signed by an unrelated secret', () => {
			const issuer = capabilityCookie({ secret: 'one' });
			const verifier = capabilityCookie({ secret: 'two' });
			const res = mockResponse();
			issuer.issue(mockEvent(), res);
			const header = cookieValueFrom(res.setCookies[0]);

			expect(verifier.verify(header, { required: true })).toBe(false);
		});

		it('rejects a structurally malformed cookie value', () => {
			const cap = capabilityCookie({ secret: 's' });
			expect(cap.verify(COOKIE_NAME + '=not.enough.parts', { required: true })).toBe(false);
			expect(cap.verify(COOKIE_NAME + '=garbage', { required: true })).toBe(false);
		});

		it('rejects an expired cookie', () => {
			const cap = capabilityCookie({ secret: 's', ttlSeconds: 60 });
			const t0 = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(t0);

			const res = mockResponse();
			cap.issue(mockEvent(), res);
			const header = cookieValueFrom(res.setCookies[0]);

			// Still valid before TTL.
			Date.now.mockReturnValue(t0 + 59000);
			expect(cap.verify(header, { required: true })).toBe(true);

			// Expired after TTL.
			Date.now.mockReturnValue(t0 + 61000);
			expect(cap.verify(header, { required: true })).toBe(false);
		});
	});

	describe('secret rotation window', () => {
		it('verifier with previousSecret accepts a cookie signed by the old secret', () => {
			const oldIssuer = capabilityCookie({ secret: 'old-secret' });
			const res = mockResponse();
			oldIssuer.issue(mockEvent(), res);
			const oldCookie = cookieValueFrom(res.setCookies[0]);

			// After rotation: current = new-secret, previous = old-secret.
			const rotated = capabilityCookie({ secret: 'new-secret', previousSecret: 'old-secret' });
			expect(rotated.verify(oldCookie, { required: true })).toBe(true);

			// And a cookie freshly issued under the new secret still verifies.
			const res2 = mockResponse();
			rotated.issue(mockEvent(), res2);
			expect(rotated.verify(cookieValueFrom(res2.setCookies[0]), { required: true })).toBe(true);
		});

		it('drops the old secret once the rotation window closes', () => {
			const oldIssuer = capabilityCookie({ secret: 'old-secret' });
			const res = mockResponse();
			oldIssuer.issue(mockEvent(), res);
			const oldCookie = cookieValueFrom(res.setCookies[0]);

			// previousSecret no longer configured: old cookies are now invalid.
			const current = capabilityCookie({ secret: 'new-secret' });
			expect(current.verify(oldCookie, { required: true })).toBe(false);
		});

		it('does not accept a cookie signed by neither current nor previous', () => {
			const strayIssuer = capabilityCookie({ secret: 'stray' });
			const res = mockResponse();
			strayIssuer.issue(mockEvent(), res);
			const strayCookie = cookieValueFrom(res.setCookies[0]);

			const rotated = capabilityCookie({ secret: 'new', previousSecret: 'old' });
			expect(rotated.verify(strayCookie, { required: true })).toBe(false);
		});
	});

	describe('refresh', () => {
		it('re-issues preserving the session id from a still-valid cookie', () => {
			const cap = capabilityCookie({ secret: 's' });
			const issueRes = mockResponse();
			cap.issue(mockEvent({ locals: { sessionId: 'keep-me' } }), issueRes);
			const issued = cookieValueFrom(issueRes.setCookies[0]);

			const refreshRes = mockResponse();
			cap.refresh(mockEvent({ cookie: issued }), refreshRes);

			expect(refreshRes.setCookies).toHaveLength(1);
			const refreshed = cookieValueFrom(refreshRes.setCookies[0]);
			const raw = refreshed.slice((COOKIE_NAME + '=').length);
			const sid = Buffer.from(raw.split('.')[0], 'base64url').toString('utf8');
			expect(sid).toBe('keep-me');
			expect(cap.verify(refreshed, { required: true })).toBe(true);
		});

		it('re-issues a fresh cookie when no valid cookie is presented', () => {
			const cap = capabilityCookie({ secret: 's' });
			const res = mockResponse();
			cap.refresh(mockEvent({ cookie: null }), res);
			expect(res.setCookies).toHaveLength(1);
			expect(cap.verify(cookieValueFrom(res.setCookies[0]), { required: true })).toBe(true);
		});

		it('a cookie refreshed across a rotation re-signs under the current secret', () => {
			const oldIssuer = capabilityCookie({ secret: 'old' });
			const issueRes = mockResponse();
			oldIssuer.issue(mockEvent({ locals: { sessionId: 's1' } }), issueRes);
			const oldCookie = cookieValueFrom(issueRes.setCookies[0]);

			const rotated = capabilityCookie({ secret: 'new', previousSecret: 'old' });
			const refreshRes = mockResponse();
			rotated.refresh(mockEvent({ cookie: oldCookie }), refreshRes);
			const refreshed = cookieValueFrom(refreshRes.setCookies[0]);

			// The refreshed cookie verifies under the current secret even with the
			// previous secret dropped.
			const currentOnly = capabilityCookie({ secret: 'new' });
			expect(currentOnly.verify(refreshed, { required: true })).toBe(true);
		});
	});

	describe('custom cookie name', () => {
		it('issues and verifies under a custom name', () => {
			const cap = capabilityCookie({ secret: 's', cookieName: 'cap2' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			expect(res.setCookies[0]).toMatch(/^cap2=/);
			expect(cap.verify(cookieValueFrom(res.setCookies[0]), { required: true })).toBe(true);
			// The default-name verifier does not see it.
			const other = capabilityCookie({ secret: 's' });
			expect(other.verify(cookieValueFrom(res.setCookies[0]), { required: true })).toBe(false);
		});
	});
});
