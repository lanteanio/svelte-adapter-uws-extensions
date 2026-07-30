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
 * @property {import('./prometheus/index.js').MetricsRegistry} [metrics] - Prometheus registry; registers `capability_cookie_misses_total{reason}`.
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
/**
 * Floor for an HMAC secret.
 *
 * The cookie's whole security property is that a client cannot forge the
 * signature, and a weak secret is brute-forceable OFFLINE from a single
 * observed cookie - the attacker holds both the message and the tag, so they
 * grind candidates at memory speed with no network involved.
 *
 * 16 characters, deliberately, and NOT 32: the floor has to admit every shape
 * a correctly-provisioned deployment produces, and those differ in width for
 * the same entropy. `randomBytes(16)` is 128 bits either way but 32 chars as
 * hex and only 24 as base64; `randomBytes(12).toString('hex')` is 24. A
 * 32-char rule refuses all of those, and since `capabilityCookie()` is called
 * at module scope in the documented setup, refusing one is a process that
 * will not boot. 16 is below every generated form and above every placeholder
 * (`dev`, `changeme`, `hunter2`), which is the line worth drawing.
 */
const MIN_SECRET_LENGTH = 16;

/** Distinct characters below which a value is padding, not key material. */
const MIN_SECRET_DISTINCT = 8;

/**
 * @param {unknown} value
 * @param {string} label
 */
function assertSecretStrength(value, label) {
	if (typeof value !== 'string' || value.length === 0) {
		throw new Error(`capability-cookie: ${label} must be a non-empty string`);
	}
	if (value.length < MIN_SECRET_LENGTH) {
		throw new Error(
			`capability-cookie: ${label} must be at least ${MIN_SECRET_LENGTH} characters ` +
			`(got ${value.length}); generate one with ` +
			`\`node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"\``
		);
	}
	// Length is not entropy: 'xxxx...' carries as much key material as 'x'
	// however long it runs, and that is the shape a padded placeholder takes.
	// A generated key of any common encoding clears this comfortably.
	if (new Set(value).size < MIN_SECRET_DISTINCT) {
		throw new Error(
			`capability-cookie: ${label} has too few distinct characters to be generated key ` +
			'material; it looks like a placeholder'
		);
	}
}

export function capabilityCookie(options) {
	if (!options || typeof options !== 'object') {
		throw new Error('capability-cookie: options object is required');
	}
	const { secret, previousSecret } = options;
	assertSecretStrength(secret, 'secret');
	// The floor deliberately does NOT apply to `previousSecret`. It exists to
	// let a deployment rotate, and the single most important rotation to allow
	// is the one away from a weak secret - refusing the old value there would
	// leave "keep the weak secret" and "sign every live session out" as the
	// only options, which is how a weak secret survives. It only ever VERIFIES,
	// never signs, and the window is meant to be closed shortly after.
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
	// cookieName, path and sameSite are serialized verbatim into the
	// Set-Cookie header, so they are validated at construction rather than
	// emitted. CR/LF would split the response and ';' would smuggle an
	// attribute, but the check is the full RFC 6265 token set for the same
	// reason: a name like 'my cookie' or 'a=b' produces a header the browser
	// silently drops, readCookie never matches it again, and the capability
	// check degrades to permanently-absent with nothing logged anywhere.
	// Failing at construction is the only place an operator can see it.
	if (!/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(cookieName)) {
		throw new Error(
			'capability-cookie: cookieName must be an RFC 6265 token (letters, digits, and !#$%&\'*+-.^_`|~); ' +
			`got ${JSON.stringify(cookieName)}`
		);
	}

	const secure = options.secure ?? true;
	// Cookie attribute values are case-insensitive, so 'lax' and 'none' are
	// as valid as 'Lax' and 'None'. Normalize rather than reject: rejecting
	// would turn a config that worked into a startup crash, and the
	// serialized header is identical either way.
	const sameSiteRaw = options.sameSite ?? 'Lax';
	// Null-prototype lookup. An object literal inherits from
	// Object.prototype, so 'constructor' resolves to the Object constructor
	// and '__proto__' to the prototype itself - both non-undefined, so both
	// slip past the guard below and get serialized into the Set-Cookie
	// header as a malformed SameSite attribute.
	const sameSite = typeof sameSiteRaw === 'string'
		? Object.assign(Object.create(null), { strict: 'Strict', lax: 'Lax', none: 'None' })[sameSiteRaw.toLowerCase()]
		: undefined;
	if (sameSite === undefined) {
		throw new Error(`capability-cookie: sameSite must be "Strict", "Lax", or "None"; got ${JSON.stringify(sameSiteRaw)}`);
	}
	const path = options.path ?? '/';
	// RFC 6265 path-value is any CHAR except CTLs and ';', and a leading '/'
	// is what makes it a path rather than a relative string the browser
	// resolves unpredictably. Stated as an ALLOWLIST rather than a list of
	// forbidden bytes: the forbidden-byte form missed NUL and the C0
	// controls, so `path: '/a\0b'` passed validation and reached the emitted
	// header, where undici's Headers.append throws `invalid header value` -
	// a per-request 500, which is precisely the failure this check exists to
	// move to startup. Non-ASCII is refused for the same reason: a header
	// value is bytes, so a path outside that range has to be percent-encoded
	// by the caller rather than silently transcoded here.
	//
	// Narrower than the grammar by three characters. Whitespace, ',' and '"'
	// are all legal path-value CHARs, but ',' is the separator a folded
	// Set-Cookie is split on and a bare '"' derails quoted-string parsers,
	// so each one produces a header some real client mishandles.
	if (typeof path !== 'string' || !/^\/[\x21\x23-\x2B\x2D-\x3A\x3C-\x7E]*$/.test(path)) {
		throw new Error(
			'capability-cookie: path must start with "/" and contain only printable ASCII except ' +
			`whitespace, ";", "," and '"'; got ${JSON.stringify(path)}`
		);
	}
	const ttlMs = ttlSeconds * 1000;

	const m = options.metrics;
	const rawMisses = m?.counter(
		'capability_cookie_misses_total',
		'Capability cookie verifications that failed (missing while required, or presented but invalid)',
		['reason']
	);
	// Contain emits: verify() runs on the upgrade hot path and must return a
	// boolean, never throw. A registry that fails at emit time (the contract
	// admits any registry-shaped object) logs once and goes silent;
	// registration above stays uncontained so a broken registry fails at
	// startup, loudly.
	let missesWarned = false;
	const mMisses = rawMisses == null ? undefined : {
		/** @param {Record<string, string>} labels */
		inc(labels) {
			try {
				rawMisses.inc(labels);
			} catch (err) {
				if (!missesWarned) {
					missesWarned = true;
					console.error('capability-cookie: metrics instrument threw; suppressing further errors from it:', err);
				}
			}
		}
	};

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
	// A malformed cookie exits before any real verification; burn one
	// signature + compare over fixed inputs first so "unparseable" is not
	// separable from "wrong signature" by the missing HMAC's timing.
	function burnAndFail() {
		safeEqual(sign(secret, '', 0, ''), 'x');
		return null;
	}

	function decode(value) {
		if (typeof value !== 'string') return null;
		const parts = value.split('.');
		if (parts.length !== 4) return burnAndFail();
		const [sidB64, issuedRaw, salt, presentedSig] = parts;

		let sessionId;
		try {
			sessionId = unb64u(sidB64);
		} catch {
			return burnAndFail();
		}
		const issuedAt = Number(issuedRaw);
		if (!Number.isInteger(issuedAt) || issuedAt <= 0) return burnAndFail();

		// Signature FIRST: authentication is the constant-work step, so every
		// parseable cookie costs the same HMAC work whether it turns out to be
		// expired or forged - the check order no longer separates the two by
		// timing. Expiry stays independent of the secret: an expired cookie is
		// invalid even when signed with the current key.
		const expectCurrent = sign(secret, sessionId, issuedAt, salt);
		let authentic = safeEqual(expectCurrent, presentedSig);
		if (!authentic && previousSecret) {
			const expectPrev = sign(previousSecret, sessionId, issuedAt, salt);
			authentic = safeEqual(expectPrev, presentedSig);
		}
		if (!authentic) return null;
		if (now() - issuedAt > ttlMs) return null;
		return { sessionId, issuedAt };
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
				// quiet site never locks out a first-time visitor - and no metric
				// either, or every first visit would count as a miss.
				if (required) mMisses?.inc({ reason: 'missing' });
				return !required;
			}
			// A presented cookie that fails to verify is a signal in every
			// posture, so it counts regardless of `required`. Expired cookies
			// from idle real users land here too (`refresh()` on page responses
			// keeps live users out of this bucket); expiry is deliberately not
			// its own reason - `issuedAt` is sender-supplied, so an "expired"
			// bucket would be forgeable, and decode's rejection is uniform
			// (signature-first, same HMAC work) across every failure cause.
			const ok = decode(value) !== null;
			if (!ok) mMisses?.inc({ reason: 'invalid' });
			return ok;
		}
	};
}
