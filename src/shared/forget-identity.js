/**
 * Tenant-qualified erasure identity shared by the stores whose records carry
 * no topic: Redis sessions, the Redis connection registry and Postgres tasks.
 *
 * An identity is `{ tenantId, userId }`. `tenantId` is server-trusted and uses
 * svelte-realtime's tenant id rule (1-64 of `[a-zA-Z0-9_-]`), or is exactly
 * `null` for the untenanted scope; null never means every tenant. `userId` is
 * a non-empty string kept exactly as given. The canonical encoding is
 * `JSON.stringify([tenantId, userId])`, which keeps null distinct from every
 * string and cannot collide on a delimiter; Redis keys embed its UTF-8 bytes
 * as unpadded base64url.
 *
 * @module svelte-adapter-uws-extensions/shared/forget-identity
 */

import { operationError } from './errors.js';

const TENANT_ID = /^[a-zA-Z0-9_-]{1,64}$/;

/**
 * @param {unknown} tenantId
 * @returns {tenantId is string | null}
 */
export function isTenantId(tenantId) {
	return tenantId === null || (typeof tenantId === 'string' && TENANT_ID.test(tenantId));
}

/**
 * Validate one identity object. `null` and `undefined` are the absent identity
 * and return null; anything else must be an object with an own `tenantId`
 * (a missing one is invalid, not the null scope) and a non-empty `userId`.
 * @param {unknown} value
 * @param {string} label
 * @returns {{ tenantId: string | null, userId: string } | null}
 */
export function readIdentity(value, label) {
	if (value === null || value === undefined) return null;
	if (typeof value !== 'object' || Array.isArray(value)) {
		throw new TypeError(`${label}: identity must be an object { tenantId, userId }, null or undefined`);
	}
	const identity = /** @type {Record<string, unknown>} */ (value);
	if (!Object.hasOwn(identity, 'tenantId') || !isTenantId(identity.tenantId)) {
		throw new TypeError(`${label}: tenantId must be null or 1-64 characters of [a-zA-Z0-9_-]`);
	}
	if (typeof identity.userId !== 'string' || identity.userId.length === 0) {
		throw new TypeError(`${label}: userId must be a non-empty string`);
	}
	return { tenantId: identity.tenantId, userId: identity.userId };
}

/**
 * The exact identity a `purgeUser(tenantId, userId)` call names. Validation is
 * the same whether the store is called directly or through `createForgetStore`.
 * @param {unknown} tenantId
 * @param {string} userId
 * @param {string} label
 */
export function purgeIdentity(tenantId, userId, label) {
	if (!isTenantId(tenantId)) {
		throw new TypeError(`${label}: tenantId must be null (the untenanted scope) or 1-64 characters of [a-zA-Z0-9_-]`);
	}
	return { tenantId, userId };
}

/** @param {{ tenantId: string | null, userId: string }} a @param {{ tenantId: string | null, userId: string }} b */
export function sameIdentity(a, b) {
	return a.tenantId === b.tenantId && a.userId === b.userId;
}

/** @param {{ tenantId: string | null, userId: string }} identity */
export function identityTuple(identity) {
	return JSON.stringify([identity.tenantId, identity.userId]);
}

/** Unpadded base64url of the canonical tuple, for Redis keys. */
export function identityKey(identity) {
	return Buffer.from(identityTuple(identity), 'utf8').toString('base64url');
}

/**
 * Parse a stored `[tenantId, userId]` tuple. Returns null for anything that is
 * not a valid identity.
 * @param {unknown} tuple
 */
export function tupleIdentity(tuple) {
	if (!Array.isArray(tuple) || tuple.length !== 2) return null;
	const [tenantId, userId] = tuple;
	if (!isTenantId(tenantId) || typeof userId !== 'string' || userId.length === 0) return null;
	return { tenantId, userId };
}

/**
 * Decode a key segment written by `identityKey`. Returns null unless the
 * segment is the canonical encoding of a valid identity, so a foreign or
 * hand-written key is never read as somebody's identity.
 * @param {string} key
 */
export function identityFromKey(key) {
	if (typeof key !== 'string' || key.length === 0 || !/^[A-Za-z0-9_-]+$/.test(key)) return null;
	let identity;
	try { identity = tupleIdentity(JSON.parse(Buffer.from(key, 'base64url').toString('utf8'))); } catch { return null; }
	if (identity === null || identityKey(identity) !== key) return null;
	return identity;
}

/**
 * The outcome of a qualified erasure that removed what it could reach but
 * cannot prove the identity absent everywhere: records written before
 * qualified mode carry no tenant, so they can be neither attributed nor
 * deleted. `removed` keeps the count this call committed.
 * @param {string} code
 * @param {string} message
 * @param {number} removed
 */
export function legacyResidueError(code, message, removed) {
	const error = operationError(code, message);
	/** @type {any} */ (error).erasureIncomplete = true;
	/** @type {any} */ (error).legacyResidue = true;
	/** @type {any} */ (error).removed = removed;
	return error;
}
