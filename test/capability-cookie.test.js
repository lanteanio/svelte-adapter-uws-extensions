import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { randomBytes, randomUUID } from 'node:crypto';
import { capabilityCookie } from '../src/capability-cookie.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from './helpers/runtime-clock.js';

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
		// issue/verify read wall time through the runtime clock; bind it to the
		// global Date.now so the expiry test's vi.spyOn(Date, 'now') drives it.
		installFakeRuntimeClock();
	});

	afterEach(() => {
		releaseRuntimeClock();
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
			expect(() => capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', previousSecret: '' })).toThrow('previousSecret');
		});

		it('refuses a secret weak enough to brute-force offline', () => {
			// The cookie's only security property is that a client cannot forge
			// the signature. An observer holds both the message and the tag, so a
			// weak secret is ground offline at memory speed with no network
			// involved - and a placeholder like 'dev' is exactly the value that
			// reaches production by accident.
			for (const weak of ['s', 'dev', 'changeme', 'hunter2', 'password']) {
				expect(() => capabilityCookie({ secret: weak })).toThrow(/at least 16 characters/);
			}
			// Length is not entropy.
			expect(() => capabilityCookie({ secret: 'x'.repeat(64) })).toThrow(/too few distinct characters/);
			expect(() => capabilityCookie({ secret: 'abababab'.repeat(8) })).toThrow(/too few distinct characters/);
		});

		it('accepts every shape a generated key actually takes', () => {
			// The SAME 128 bits is 32 chars as hex and 24 as base64, and a
			// 32-char floor would refuse the base64 form - a correctly
			// provisioned deployment that then cannot boot, since
			// capabilityCookie() runs at module scope in the documented setup.
			const generated = [
				randomBytes(16).toString('hex'),
				randomBytes(32).toString('hex'),
				randomBytes(16).toString('base64'),
				randomBytes(20).toString('base64url'),
				randomBytes(12).toString('hex'),
				randomUUID()
			];
			for (const s of generated) {
				expect(() => capabilityCookie({ secret: s }), s).not.toThrow();
			}
		});

		it('lets a deployment rotate AWAY from a weak secret', () => {
			// The floor must not apply to previousSecret. It only ever verifies,
			// never signs, and refusing the old value would leave "keep the weak
			// secret" and "sign every live session out" as the only options -
			// which is how a weak secret survives.
			expect(() => capabilityCookie({
				secret: randomBytes(32).toString('hex'),
				previousSecret: 'dev'
			})).not.toThrow();
		});

		it('throws on non-positive ttlSeconds', () => {
			expect(() => capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', ttlSeconds: 0 })).toThrow('ttlSeconds must be a positive number');
		});

		it('returns the issue/refresh/verify API', () => {
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
			expect(typeof cap.issue).toBe('function');
			expect(typeof cap.refresh).toBe('function');
			expect(typeof cap.verify).toBe('function');
		});
	});

	describe('issue', () => {
		it('appends a signed Set-Cookie with the security attributes', () => {
			const cap = capabilityCookie({ secret: 'b7e2d4a90c8f16352affe9d0c47b1836', ttlSeconds: 300 });
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
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
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
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
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
			const cap = capabilityCookie({ secret: 'b7e2d4a90c8f16352affe9d0c47b1836' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			const header = cookieValueFrom(res.setCookies[0]);

			expect(cap.verify(header, { required: true })).toBe(true);
		});

		it('accepts the cookie alongside other cookies in the header', () => {
			const cap = capabilityCookie({ secret: 'b7e2d4a90c8f16352affe9d0c47b1836' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			const capCookie = cookieValueFrom(res.setCookies[0]);
			const header = 'other=1; ' + capCookie + '; more=2';

			expect(cap.verify(header, { required: true })).toBe(true);
		});
	});

	describe('verify - required-ness keyed off posture', () => {
		it('an absent cookie passes when not required (normal posture)', () => {
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
			expect(cap.verify(null, { required: false })).toBe(true);
			expect(cap.verify(undefined)).toBe(true);
			expect(cap.verify('unrelated=1', { required: false })).toBe(true);
		});

		it('an absent cookie fails when required (elevated / siege posture)', () => {
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
			expect(cap.verify(null, { required: true })).toBe(false);
			expect(cap.verify('unrelated=1', { required: true })).toBe(false);
		});
	});

	describe('verify - rejection', () => {
		it('rejects a tampered signature even when not required', () => {
			const cap = capabilityCookie({ secret: 'b7e2d4a90c8f16352affe9d0c47b1836' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			let header = cookieValueFrom(res.setCookies[0]);
			// Flip the last character of the signature.
			const last = header.slice(-1) === 'A' ? 'B' : 'A';
			header = header.slice(0, -1) + last;

			expect(cap.verify(header, { required: false })).toBe(false);
		});

		it('rejects a cookie signed by an unrelated secret', () => {
			const issuer = capabilityCookie({ secret: '4d1b7f0a9c3e58267abdf14c0e9b3572' });
			const verifier = capabilityCookie({ secret: '5e2c8a1b0d4f69378bcea25d1fa04683' });
			const res = mockResponse();
			issuer.issue(mockEvent(), res);
			const header = cookieValueFrom(res.setCookies[0]);

			expect(verifier.verify(header, { required: true })).toBe(false);
		});

		it('rejects a structurally malformed cookie value', () => {
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
			expect(cap.verify(COOKIE_NAME + '=not.enough.parts', { required: true })).toBe(false);
			expect(cap.verify(COOKIE_NAME + '=garbage', { required: true })).toBe(false);
		});

		it('rejects an expired cookie', () => {
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', ttlSeconds: 60 });
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
			const oldIssuer = capabilityCookie({ secret: '6f3d9b2c1e5a074890dfb36e2ab15794' });
			const res = mockResponse();
			oldIssuer.issue(mockEvent(), res);
			const oldCookie = cookieValueFrom(res.setCookies[0]);

			// After rotation: current = new-secret, previous = old-secret.
			const rotated = capabilityCookie({ secret: '7a4e0c3d2f6b1859a1ec47f30bc268a5', previousSecret: '6f3d9b2c1e5a074890dfb36e2ab15794' });
			expect(rotated.verify(oldCookie, { required: true })).toBe(true);

			// And a cookie freshly issued under the new secret still verifies.
			const res2 = mockResponse();
			rotated.issue(mockEvent(), res2);
			expect(rotated.verify(cookieValueFrom(res2.setCookies[0]), { required: true })).toBe(true);
		});

		it('drops the old secret once the rotation window closes', () => {
			const oldIssuer = capabilityCookie({ secret: '6f3d9b2c1e5a074890dfb36e2ab15794' });
			const res = mockResponse();
			oldIssuer.issue(mockEvent(), res);
			const oldCookie = cookieValueFrom(res.setCookies[0]);

			// previousSecret no longer configured: old cookies are now invalid.
			const current = capabilityCookie({ secret: '7a4e0c3d2f6b1859a1ec47f30bc268a5' });
			expect(current.verify(oldCookie, { required: true })).toBe(false);
		});

		it('does not accept a cookie signed by neither current nor previous', () => {
			const strayIssuer = capabilityCookie({ secret: '8b5f1d4e3a7c2960b2fd58041cd379b6' });
			const res = mockResponse();
			strayIssuer.issue(mockEvent(), res);
			const strayCookie = cookieValueFrom(res.setCookies[0]);

			const rotated = capabilityCookie({ secret: 'd9a4b7c2e6f31850ab7dce49f2013c65', previousSecret: 'c1d8e3f6a9b40725d3ecfb82a6094d17' });
			expect(rotated.verify(strayCookie, { required: true })).toBe(false);
		});
	});

	describe('refresh', () => {
		it('re-issues preserving the session id from a still-valid cookie', () => {
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
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
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
			const res = mockResponse();
			cap.refresh(mockEvent({ cookie: null }), res);
			expect(res.setCookies).toHaveLength(1);
			expect(cap.verify(cookieValueFrom(res.setCookies[0]), { required: true })).toBe(true);
		});

		it('a cookie refreshed across a rotation re-signs under the current secret', () => {
			const oldIssuer = capabilityCookie({ secret: 'c1d8e3f6a9b40725d3ecfb82a6094d17' });
			const issueRes = mockResponse();
			oldIssuer.issue(mockEvent({ locals: { sessionId: 's1' } }), issueRes);
			const oldCookie = cookieValueFrom(issueRes.setCookies[0]);

			const rotated = capabilityCookie({ secret: 'd9a4b7c2e6f31850ab7dce49f2013c65', previousSecret: 'c1d8e3f6a9b40725d3ecfb82a6094d17' });
			const refreshRes = mockResponse();
			rotated.refresh(mockEvent({ cookie: oldCookie }), refreshRes);
			const refreshed = cookieValueFrom(refreshRes.setCookies[0]);

			// The refreshed cookie verifies under the current secret even with the
			// previous secret dropped.
			const currentOnly = capabilityCookie({ secret: 'd9a4b7c2e6f31850ab7dce49f2013c65' });
			expect(currentOnly.verify(refreshed, { required: true })).toBe(true);
		});
	});

	describe('custom cookie name', () => {
		it('issues and verifies under a custom name', () => {
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', cookieName: 'cap2' });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			expect(res.setCookies[0]).toMatch(/^cap2=/);
			expect(cap.verify(cookieValueFrom(res.setCookies[0]), { required: true })).toBe(true);
			// The default-name verifier does not see it.
			const other = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c' });
			expect(other.verify(cookieValueFrom(res.setCookies[0]), { required: true })).toBe(false);
		});
	});

	describe('metrics', () => {
		/** Recording registry matching the options.metrics contract. */
		function fakeRegistry() {
			const counters = new Map();
			return {
				counters,
				counter(name) {
					let c = counters.get(name);
					if (!c) {
						c = {
							series: new Map(),
							inc(labels) {
								const key = labels ? JSON.stringify(labels) : '';
								this.series.set(key, (this.series.get(key) || 0) + 1);
							}
						};
						counters.set(name, c);
					}
					return c;
				},
				gauge() { return { set() {} }; },
				misses(reason) {
					const c = counters.get('capability_cookie_misses_total');
					return c ? (c.series.get(JSON.stringify({ reason })) || 0) : 0;
				}
			};
		}

		it('counts an absent cookie as missing only when required', () => {
			const metrics = fakeRegistry();
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', metrics });

			// Optional posture: a first-time visitor is not a miss.
			expect(cap.verify(null, { required: false })).toBe(true);
			expect(metrics.misses('missing')).toBe(0);

			// Required posture: the absence is the rejection.
			expect(cap.verify(null, { required: true })).toBe(false);
			expect(cap.verify('unrelated=1', { required: true })).toBe(false);
			expect(metrics.misses('missing')).toBe(2);
			expect(metrics.misses('invalid')).toBe(0);
		});

		it('counts a presented-but-bad cookie as invalid regardless of required', () => {
			const metrics = fakeRegistry();
			const cap = capabilityCookie({ secret: 'b7e2d4a90c8f16352affe9d0c47b1836', metrics });
			const res = mockResponse();
			cap.issue(mockEvent(), res);
			let header = cookieValueFrom(res.setCookies[0]);
			const last = header.slice(-1) === 'A' ? 'B' : 'A';
			header = header.slice(0, -1) + last;

			expect(cap.verify(header, { required: false })).toBe(false);
			expect(cap.verify(header, { required: true })).toBe(false);
			expect(metrics.misses('invalid')).toBe(2);
			expect(metrics.misses('missing')).toBe(0);
		});

		it('counts an expired cookie as invalid, never as its own reason', () => {
			const metrics = fakeRegistry();
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', ttlSeconds: 60, metrics });
			const t0 = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(t0);

			const res = mockResponse();
			cap.issue(mockEvent(), res);
			const header = cookieValueFrom(res.setCookies[0]);

			Date.now.mockReturnValue(t0 + 61000);
			expect(cap.verify(header, { required: true })).toBe(false);
			expect(metrics.misses('invalid')).toBe(1);
			expect(metrics.counters.get('capability_cookie_misses_total').series.size).toBe(1);
		});

		it('does not count a valid verification', () => {
			const metrics = fakeRegistry();
			const cap = capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', metrics });
			const res = mockResponse();
			cap.issue(mockEvent(), res);

			expect(cap.verify(cookieValueFrom(res.setCookies[0]), { required: true })).toBe(true);
			expect(metrics.misses('missing')).toBe(0);
			expect(metrics.misses('invalid')).toBe(0);
		});

		it('registers the counter once at construction with the reason label', () => {
			const metrics = fakeRegistry();
			capabilityCookie({ secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c', metrics });
			expect(metrics.counters.has('capability_cookie_misses_total')).toBe(true);
		});

		it('contains a throwing emit: verify still returns its boolean and warns once', () => {
			const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
			try {
				const cap = capabilityCookie({
					secret: 'a3f9c1d7e5b2408695ecfa71d3b8402c',
					metrics: { counter: () => ({ inc() { throw new Error('emit boom'); } }) }
				});
				expect(cap.verify(null, { required: true })).toBe(false);
				expect(cap.verify(COOKIE_NAME + '=garbage', { required: false })).toBe(false);
				expect(errSpy).toHaveBeenCalledTimes(1);
			} finally {
				errSpy.mockRestore();
			}
		});
	});
});
