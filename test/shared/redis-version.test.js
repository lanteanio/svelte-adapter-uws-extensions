// Version parsing and the per-field-hash-TTL (HEXPIRE/HPEXPIRE) capability
// probe. The probe must recognize Valkey: Valkey pins redis_version at 7.2.4
// forever and reports its real version in valkey_version, so a naive
// redis_version check false-negatives a Valkey 9.0 server that DOES have the
// commands.

import { describe, it, expect } from 'vitest';
import { parseRedisVersion, hashFieldTTLSupport } from '../../src/shared/redis-version.js';

const redisInfo = (v) => `# Server\nredis_version:${v}\nredis_mode:standalone\n`;
const valkeyInfo = (valkeyV, redisV = '7.2.4') =>
	`# Server\nredis_version:${redisV}\nserver_name:valkey\nvalkey_version:${valkeyV}\n`;

describe('parseRedisVersion', () => {
	it('reads the major version from an INFO payload', () => {
		expect(parseRedisVersion(redisInfo('7.4.0'))).toBe(7);
		expect(parseRedisVersion(redisInfo('6.2.7'))).toBe(6);
		expect(parseRedisVersion(redisInfo('8.0.1'))).toBe(8);
	});

	it('reads major 7 from a Valkey payload (redis_version pinned at 7.2.4)', () => {
		// The major-version gates (functions, sharded pubsub) only need major >= 7,
		// which Valkey's pinned 7.2.4 satisfies - so they need no Valkey awareness.
		expect(parseRedisVersion(valkeyInfo('9.0.0'))).toBe(7);
	});

	it('returns null for a non-string or unparseable payload', () => {
		expect(parseRedisVersion(undefined)).toBe(null);
		expect(parseRedisVersion('not real')).toBe(null);
	});
});

describe('hashFieldTTLSupport', () => {
	it('accepts Redis 7.4 and newer', () => {
		expect(hashFieldTTLSupport(redisInfo('7.4.0'))).toEqual({ supported: true, server: 'redis', version: '7.4' });
		expect(hashFieldTTLSupport(redisInfo('7.4.2')).supported).toBe(true);
		expect(hashFieldTTLSupport(redisInfo('8.0.0')).supported).toBe(true);
	});

	it('rejects Redis older than 7.4', () => {
		expect(hashFieldTTLSupport(redisInfo('7.2.4'))).toEqual({ supported: false, server: 'redis', version: '7.2' });
		expect(hashFieldTTLSupport(redisInfo('6.2.7')).supported).toBe(false);
	});

	it('accepts Valkey 9.0 and newer (the version it pins redis_version below)', () => {
		// The regression: Valkey 9.0 HAS HPEXPIRE but reports redis_version:7.2.4.
		expect(hashFieldTTLSupport(valkeyInfo('9.0.0'))).toEqual({ supported: true, server: 'valkey', version: '9.0' });
		expect(hashFieldTTLSupport(valkeyInfo('9.1.3')).supported).toBe(true);
		expect(hashFieldTTLSupport(valkeyInfo('10.0.0')).supported).toBe(true);
	});

	it('rejects Valkey older than 9.0 (no HPEXPIRE before 9.0)', () => {
		expect(hashFieldTTLSupport(valkeyInfo('8.1.0'))).toEqual({ supported: false, server: 'valkey', version: '8.1' });
		expect(hashFieldTTLSupport(valkeyInfo('7.2.0')).supported).toBe(false);
	});

	it('detects Valkey from valkey_version alone (no server_name line)', () => {
		const info = '# Server\nredis_version:7.2.4\nvalkey_version:9.0.0\n';
		expect(hashFieldTTLSupport(info)).toEqual({ supported: true, server: 'valkey', version: '9.0' });
	});

	it('assumes compatible (null) when the payload cannot be parsed', () => {
		expect(hashFieldTTLSupport('not real').supported).toBe(null);
		expect(hashFieldTTLSupport('').supported).toBe(null);
		expect(hashFieldTTLSupport(undefined).supported).toBe(null);
		// Valkey marker present but no parseable valkey_version -> still null, not a reject.
		expect(hashFieldTTLSupport('# Server\nserver_name:valkey\n').supported).toBe(null);
	});
});
