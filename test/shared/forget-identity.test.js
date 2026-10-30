import { describe, it, expect } from 'vitest';
import {
	readIdentity,
	purgeIdentity,
	identityKey,
	identityFromKey,
	identityTuple,
	tupleIdentity
} from '../../src/shared/forget-identity.js';

describe('tenant-qualified erasure identity', () => {
	it('accepts realtime tenant ids and the null tenant', () => {
		for (const tenantId of [null, 'a', 'A-z_09', 'x'.repeat(64)]) {
			expect(readIdentity({ tenantId, userId: 'u' }, 'test')).toEqual({ tenantId, userId: 'u' });
			expect(purgeIdentity(tenantId, 'u', 'test')).toEqual({ tenantId, userId: 'u' });
		}
	});

	it('refuses tenant ids outside the realtime grammar', () => {
		for (const tenantId of ['', 'x'.repeat(65), 'a.b', 'a:b', 'a/b', 'a b', '\u00e4', 1, false, {}]) {
			expect(() => readIdentity({ tenantId, userId: 'u' }, 'test'), String(tenantId)).toThrow(TypeError);
			expect(() => purgeIdentity(tenantId, 'u', 'test'), String(tenantId)).toThrow(TypeError);
		}
		// A missing tenant is invalid, never the untenanted scope.
		expect(() => readIdentity({ userId: 'u' }, 'test')).toThrow(/tenantId/);
		expect(() => readIdentity({ tenantId: undefined, userId: 'u' }, 'test')).toThrow(/tenantId/);
		expect(() => purgeIdentity(undefined, 'u', 'test')).toThrow(/tenantId/);
	});

	it('reads null and undefined as the absent identity and refuses other shapes', () => {
		expect(readIdentity(null, 'test')).toBeNull();
		expect(readIdentity(undefined, 'test')).toBeNull();
		for (const value of ['u', 7, ['t', 'u'], { tenantId: 't' }, { tenantId: 't', userId: '' }, { tenantId: 't', userId: 7 }]) {
			expect(() => readIdentity(value, 'test'), JSON.stringify(value)).toThrow(TypeError);
		}
	});

	it('keeps the user id exactly, without coercion, trimming or case folding', () => {
		for (const userId of [' u ', 'U', 'a:b', '["t","u"]', 'u\u0000v', 'Zo\u00eb \u{1F600}', '\u00e4\u00f6\u00fc']) {
			const identity = readIdentity({ tenantId: 't', userId }, 'test');
			expect(identity.userId).toBe(userId);
			expect(identityFromKey(identityKey(identity))).toEqual(identity);
		}
	});

	it('encodes the JSON tuple as unpadded base64url', () => {
		const identity = { tenantId: 't', userId: 'u' };
		expect(identityTuple(identity)).toBe('["t","u"]');
		expect(identityKey(identity)).toBe(Buffer.from('["t","u"]').toString('base64url'));
		expect(identityKey(identity)).not.toMatch(/[=+/]/);
	});

	it('keeps null, tenants and delimiter-like user ids apart', () => {
		const identities = [
			{ tenantId: null, userId: 'u' },
			{ tenantId: 'null', userId: 'u' },
			{ tenantId: 'a', userId: 'b:c' },
			{ tenantId: 'a-b', userId: 'c' },
			{ tenantId: 'a', userId: 'b-c' },
			{ tenantId: 'a', userId: '","b' }
		];
		const keys = identities.map(identityKey);
		expect(new Set(keys).size).toBe(identities.length);
		for (let i = 0; i < identities.length; i++) expect(identityFromKey(keys[i])).toEqual(identities[i]);
	});

	it('decodes only canonical keys of valid identities', () => {
		const b64 = (text) => Buffer.from(text).toString('base64url');
		for (const key of ['', 'conns', b64('["t"]'), b64('["t","u",1]'), b64('["a.b","u"]'), b64('[null,""]'), b64('{"tenantId":"t"}'), b64('[ "t","u"]'), b64('["t","u"]') + '=', 'not base64!']) {
			expect(identityFromKey(key), key).toBeNull();
		}
		expect(tupleIdentity(['t', 'u'])).toEqual({ tenantId: 't', userId: 'u' });
		expect(tupleIdentity([undefined, 'u'])).toBeNull();
	});
});
