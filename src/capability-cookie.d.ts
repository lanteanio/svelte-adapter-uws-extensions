import type { MetricsRegistry } from './prometheus/index.js';

export interface CapabilityCookieOptions {
	/** HMAC secret. Required, non-empty. */
	secret: string;
	/** Cookie lifetime in seconds. @default 300 */
	ttlSeconds?: number;
	/** Immediately-previous secret, accepted during a rotation window. */
	previousSecret?: string;
	/** Cookie name. @default 'sauws_cap' */
	cookieName?: string;
	/** Set the `Secure` attribute. @default true */
	secure?: boolean;
	/**
	 * SameSite policy, matched case-insensitively and emitted in canonical
	 * casing - `'lax'` works and is sent as `SameSite=Lax`. Only the three
	 * RFC 6265 values are accepted; anything else throws at construction.
	 * @default 'Lax'
	 */
	sameSite?: 'Strict' | 'Lax' | 'None' | (string & {});
	/** Cookie path. @default '/' */
	path?: string;
	/**
	 * Prometheus metrics registry. Registers
	 * `capability_cookie_misses_total{reason}` with reasons `missing` (cookie
	 * absent while `required`) and `invalid` (cookie presented but failed to
	 * verify - bad signature, malformed, or expired). Expired is deliberately
	 * not its own reason: expiry is checked before the signature, so the
	 * split would be forgeable by the sender.
	 */
	metrics?: MetricsRegistry;
}

export interface CapabilityCookieVerifyOptions {
	/** When true, an absent cookie fails verification. @default false */
	required?: boolean;
}

export interface CapabilityCookie {
	/** Set the capability cookie on a fresh page response. */
	issue(event: any, response: any): void;
	/** Re-issue from a still-valid presented cookie; falls back to a fresh issue. */
	refresh(event: any, response: any): void;
	/**
	 * Validate a presented `Cookie` header value. Accepts a cookie signed by the
	 * current OR previous secret (rotation window). Returns `true` to admit.
	 */
	verify(cookieHeader: string | null | undefined, opts?: CapabilityCookieVerifyOptions): boolean;
}

/**
 * Build a capability-cookie issuer/verifier. HMAC of `sessionId | issuedAt | salt`
 * via `node:crypto`; no Redis dependency.
 */
export function capabilityCookie(options: CapabilityCookieOptions): CapabilityCookie;
