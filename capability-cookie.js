/**
 * Capability cookie for svelte-adapter-uws upgrade admission.
 *
 * Raises the per-request cost for a bot hitting `/ws` directly from "one TCP+TLS
 * handshake" to "load the HTML page, run its JS, present a short-lived signed
 * cookie". The HTML page response sets a signed cookie (an HMAC of
 * `sessionId | issuedAt | salt`); the WebSocket upgrade hook verifies it in O(1)
 * with one HMAC computation and a constant-time compare, before any backend
 * lookup. A client with no valid cookie is rejected at sub-microsecond cost.
 *
 * This stops commodity DDoS toolkits that target `/ws` directly. It does NOT stop
 * a headless-browser attacker that actually runs the page JS - that is a stated,
 * honest limitation, not a gap to paper over.
 *
 * Whether a missing cookie is fatal is decided by the caller per request via
 * `verify(cookie, { required })`. The intended wiring keys `required` off the
 * live server posture (optional in 'normal' so a fresh first-time visitor is
 * never locked out; required under pressure), but the posture is the caller's
 * concern - this module only takes the boolean.
 *
 * No Redis dependency: the whole point is to reject before any backend work.
 *
 * @module svelte-adapter-uws-extensions/capability-cookie
 */

import { createHmac, timingSafeEqual } from 'node:crypto';
import { randomBytes, now } from './shared/runtime.js';

/** Default cookie name. */
const DEFAULT_COOKIE_NAME = 'sauws_cap';

/** Default cookie lifetime, in seconds. */
const DEFAULT_TTL_SECONDS = 300;

/** Bytes of random salt mixed into every signature. */
const SALT_BYTES = 9; // 12 base64url chars, no padding

/**
 * Encode a string as base64url (no padding) so it survives a cookie value
 * unescaped and contains none of the `.` field delimiter.
 *
 * @param {string} s
 */
function b64u(s) {
	return Buffer.from(s, 'utf8').toString('base64url');
}

/**
 * @param {string} s
 */
function unb64u(s) {
	return Buffer.from(s, 'base64url').toString('utf8');
}

/**
 * Compute the signature over `sessionId | issuedAt | salt` with one secret.
 *
 * @param {string} secret
 * @param {string} sessionId
 * @param {number} issuedAt
 * @param {string} salt
 */
function sign(secret, sessionId, issuedAt, salt) {
	return createHmac('sha256', secret)
		.update(sessionId + '|' + issuedAt + '|' + salt)
		.digest('base64url');
}

/**
 * Constant-time string compare that never short-circuits on length, so an
 * attacker cannot probe signature length via timing.
 *
 * @param {string} a
 * @param {string} b
 */
function safeEqual(a, b) {
	const ba = Buffer.from(a, 'utf8');
	const bb = Buffer.from(b, 'utf8');
	if (ba.length !== bb.length) {
		// Still spend a compare so the early return does not leak length, then fail.
		timingSafeEqual(ba, ba);
		return false;
	}
	return timingSafeEqual(ba, bb);
}

/**
 * Read one cookie value out of a raw `Cookie` header string.
 *
 * @param {string | null | undefined} header
 * @param {string} name
 * @returns {string | null}
 */
function readCookie(header, name) {
	if (!header || typeof header !== 'string') return null;
	const prefix = name + '=';
	for (const part of header.split(';')) {
		const trimmed = part.trim();
		if (trimmed.startsWith(prefix)) {
			return trimmed.slice(prefix.length);
		}
	}
	return null;
}

/**
 * Append a `Set-Cookie` header to a Response-like object.
 *
 * Works with a standard `Response` (`headers.append`) and degrades to a plain
 * `setHeader` object so the module is not coupled to a specific server runtime.
 *
 * @param {any} response
 * @param {string} value
 */
function appendSetCookie(response, value) {
	const headers = response && response.headers;
	if (headers && typeof headers.append === 'function') {
		headers.append('set-cookie', value);
		return;
	}
	if (response && typeof response.setHeader === 'function') {
		const existing = response.getHeader ? response.getHeader('set-cookie') : undefined;
		if (existing == null) response.setHeader('set-cookie', value);
		else if (Array.isArray(existing)) response.setHeader('set-cookie', [...existing, value]);
		else response.setHeader('set-cookie', [existing, value]);
		return;
	}
	throw new Error('capability-cookie: response must expose headers.append or setHeader');
}

/**
 * @typedef {Object} CapabilityCookieOptions
 * @property {string} secret - HMAC secret. Required, non-empty.
 * @property {number} [ttlSeconds=300] - Cookie lifetime in seconds.
 * @property {string} [previousSecret] - Immediately-previous secret, accepted during a rotation window.
 * @property {string} [cookieName='sauws_cap'] - Cookie name.
 * @property {boolean} [secure=true] - Set the `Secure` attribute.
 * @property {'Strict' | 'Lax' | 'None'} [sameSite='Lax'] - SameSite policy.
 * @property {string} [path='/'] - Cookie path.
 */

/**
 * @typedef {Object} CapabilityCookie
 * @property {(event: any, response: any) => void} issue - Set the cookie on a fresh page response.
 * @property {(event: any, response: any) => void} refresh - Re-issue from a still-valid cookie; falls back to a fresh issue.
 * @property {(cookieHeader: string | null | undefined, opts?: { required?: boolean }) => boolean} verify - Validate a presented cookie.
 */

/**
 * Build the capability-cookie issuer/verifier.
 *
 * @param {CapabilityCookieOptions} options
 * @returns {CapabilityCookie}
 *
 * @example
 * ```js
 * import { capabilityCookie } from 'svelte-adapter-uws-extensions/capability-cookie';
 *
 * const cap = capabilityCookie({ secret: process.env.CAP_SECRET, ttlSeconds: 300 });
 *
 * // hooks.server.js - set on the HTML page response.
 * export const handle = async ({ event, resolve }) => {
 *   const response = await resolve(event);
 *   cap.issue(event, response);
 *   return response;
 * };
 *
 * // hooks.ws.js - verify before any backend work.
 * export function upgrade({ headers, platform }) {
 *   if (!cap.verify(headers.cookie, { required: platform.protection !== 'normal' })) return false;
 * }
 * ```
 */
export function capabilityCookie(options) {
	if (!options || typeof options !== 'object') {
		throw new Error('capability-cookie: options object is required');
	}
	const { secret, previousSecret } = options;
	if (typeof secret !== 'string' || secret.length === 0) {
		throw new Error('capability-cookie: secret must be a non-empty string');
	}
	if (previousSecret !== undefined && (typeof previousSecret !== 'string' || previousSecret.length === 0)) {
		throw new Error('capability-cookie: previousSecret must be a non-empty string when provided');
	}

	const ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
	if (typeof ttlSeconds !== 'number' || !Number.isFinite(ttlSeconds) || ttlSeconds <= 0) {
		throw new Error('capability-cookie: ttlSeconds must be a positive number');
	}

	const cookieName = options.cookieName ?? DEFAULT_COOKIE_NAME;
	if (typeof cookieName !== 'string' || cookieName.length === 0) {
		throw new Error('capability-cookie: cookieName must be a non-empty string');
	}

	const secure = options.secure ?? true;
	const sameSite = options.sameSite ?? 'Lax';
	const path = options.path ?? '/';
	const ttlMs = ttlSeconds * 1000;

	/**
	 * Resolve a stable session id for the request. Reuses an existing
	 * `event.locals.sessionId` / `event.locals.session?.id` when present so the
	 * cookie binds to the real session; otherwise mints a fresh random id (the
	 * cookie's job here is capability, not identity).
	 *
	 * @param {any} event
	 */
	function sessionIdFor(event) {
		const locals = event && event.locals;
		if (locals) {
			if (typeof locals.sessionId === 'string' && locals.sessionId) return locals.sessionId;
			if (locals.session && typeof locals.session.id === 'string' && locals.session.id) {
				return locals.session.id;
			}
		}
		return randomBytes(12).toString('base64url');
	}

	/**
	 * Build the signed cookie value `sessionId.issuedAt.salt.sig`.
	 *
	 * @param {string} sessionId
	 * @param {number} issuedAt
	 */
	function encode(sessionId, issuedAt) {
		const salt = randomBytes(SALT_BYTES).toString('base64url');
		const sig = sign(secret, sessionId, issuedAt, salt);
		return b64u(sessionId) + '.' + issuedAt + '.' + salt + '.' + sig;
	}

	/**
	 * Parse + verify a cookie value against the current OR previous secret.
	 * Returns the decoded fields on success, null otherwise.
	 *
	 * @param {string} value
	 */
	function decode(value) {
		if (typeof value !== 'string') return null;
		const parts = value.split('.');
		if (parts.length !== 4) return null;
		const [sidB64, issuedRaw, salt, presentedSig] = parts;

		let sessionId;
		try {
			sessionId = unb64u(sidB64);
		} catch {
			return null;
		}
		const issuedAt = Number(issuedRaw);
		if (!Number.isInteger(issuedAt) || issuedAt <= 0) return null;

		// Expiry is independent of the secret: an expired cookie is invalid even
		// under the current secret.
		if (now() - issuedAt > ttlMs) return null;

		const expectCurrent = sign(secret, sessionId, issuedAt, salt);
		if (safeEqual(expectCurrent, presentedSig)) {
			return { sessionId, issuedAt };
		}
		if (previousSecret) {
			const expectPrev = sign(previousSecret, sessionId, issuedAt, salt);
			if (safeEqual(expectPrev, presentedSig)) {
				return { sessionId, issuedAt };
			}
		}
		return null;
	}

	function serializeCookie(value) {
		let out =
			cookieName + '=' + value +
			'; Path=' + path +
			'; Max-Age=' + ttlSeconds +
			'; HttpOnly' +
			'; SameSite=' + sameSite;
		if (secure) out += '; Secure';
		return out;
	}

	return {
		issue(event, response) {
			const sessionId = sessionIdFor(event);
			appendSetCookie(response, serializeCookie(encode(sessionId, now())));
		},

		refresh(event, response) {
			// Re-issue from a still-valid presented cookie so a mid-session secret
			// rotation or near-expiry never strands a reconnect. Falls back to a
			// fresh issue when no valid cookie is present.
			const header =
				event && event.request && typeof event.request.headers?.get === 'function'
					? event.request.headers.get('cookie')
					: null;
			const presented = readCookie(header, cookieName);
			const decoded = presented ? decode(presented) : null;
			const sessionId = decoded ? decoded.sessionId : sessionIdFor(event);
			appendSetCookie(response, serializeCookie(encode(sessionId, now())));
		},

		verify(cookieHeader, opts) {
			const required = !!(opts && opts.required);
			const value = readCookie(cookieHeader, cookieName);
			if (value == null) {
				// Absent cookie: a hard fail only when the caller marked it required
				// (typically under elevated/siege posture). Optional otherwise, so a
				// quiet site never locks out a first-time visitor.
				return !required;
			}
			return decode(value) !== null;
		}
	};
}
