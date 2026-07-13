// Version parsing and the per-field-hash-TTL (HEXPIRE/HPEXPIRE) capability
// probe. The probe must recognize Valkey: Valkey pins redis_version at 7.2.4
// forever and reports its real version in valkey_version, so a naive
// redis_version check false-negatives a Valkey 9.0 server that DOES have the
// commands.

import { describe, it, expect } from 'vitest';
import { parseRedisVersion, hashFieldTTLSupport, createHashFieldTTLProbe } from '../../src/shared/redis-version.js';

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

describe('createHashFieldTTLProbe (soft gate)', () => {
	// A minimal ioredis double: only `info('server')` is exercised. `fail` forces
	// a transient rejection; `hang` returns a promise that never settles, modeling
	// an INFO parked on ioredis's offline queue during an outage (it neither
	// resolves nor rejects). `calls` counts probes so a re-probe is observable.
	function fakeRedis(info) {
		let fail = false;
		let hang = false;
		let calls = 0;
		const listeners = {};
		const hung = []; // resolvers of in-flight hung INFOs, so a test can release them
		return {
			setInfo(v) { info = v; },
			setFail(v) { fail = v; },
			setHang(v) { hang = v; },
			// Resolve every INFO currently parked on the "offline queue" with the
			// current payload, modeling ioredis flushing the queue on reconnect.
			releaseHang() { hang = false; hung.splice(0).forEach((r) => r(info)); },
			get calls() { return calls; },
			on(ev, fn) { (listeners[ev] ||= []).push(fn); return this; },
			emit(ev) { (listeners[ev] || []).forEach((fn) => fn()); },
			info() {
				calls++;
				if (hang) return new Promise((resolve) => { hung.push(resolve); }); // parked until released
				if (fail) return Promise.reject(new Error('CONNECTION_BROKEN'));
				return Promise.resolve(info);
			}
		};
	}

	it('supported() is false until the probe resolves, then reflects the server', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		const probe = createHashFieldTTLProbe(redis);
		// Synchronous first read: probe kicked but not yet resolved -> safe false.
		expect(probe.supported()).toBe(false);
		expect(await probe.ready()).toBe(true);
		// Now the cached answer is definitive.
		expect(probe.supported()).toBe(true);
	});

	it('ready() resolves true for Redis 7.4+ and false for older Redis', async () => {
		expect(await createHashFieldTTLProbe(fakeRedis(redisInfo('7.4.0'))).ready()).toBe(true);
		expect(await createHashFieldTTLProbe(fakeRedis(redisInfo('8.0.0'))).ready()).toBe(true);
		expect(await createHashFieldTTLProbe(fakeRedis(redisInfo('6.2.7'))).ready()).toBe(false);
		expect(await createHashFieldTTLProbe(fakeRedis(redisInfo('7.2.4'))).ready()).toBe(false);
	});

	it('is Valkey-aware: 9.0+ supports per-field TTL, older Valkey falls back', async () => {
		// The whole point of the soft gate on Valkey: HPEXPIRE landed in 9.0, but
		// Valkey pins redis_version at 7.2.4 forever. A naive check would either
		// lock Valkey 9 out of the optimization or (worse) HPEXPIRE a Valkey 8
		// that rejects it. The probe reads valkey_version, so it does neither.
		expect(await createHashFieldTTLProbe(fakeRedis(valkeyInfo('9.0.0'))).ready()).toBe(true);
		expect(await createHashFieldTTLProbe(fakeRedis(valkeyInfo('9.2.1'))).ready()).toBe(true);
		expect(await createHashFieldTTLProbe(fakeRedis(valkeyInfo('8.1.0'))).ready()).toBe(false);
		expect(await createHashFieldTTLProbe(fakeRedis(valkeyInfo('7.2.0'))).ready()).toBe(false);
	});

	it('falls back (false) on an unparseable INFO rather than risking HPEXPIRE', async () => {
		expect(await createHashFieldTTLProbe(fakeRedis('garbage')).ready()).toBe(false);
	});

	it('re-probes after a reconnect lands on a different server (ready event + invalidate)', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		const probe = createHashFieldTTLProbe(redis);
		expect(await probe.ready()).toBe(true); // modern primary supports HPEXPIRE
		// A failover promotes an older replica; the reconnect fires 'ready'.
		redis.setInfo(redisInfo('7.2.4'));
		redis.emit('ready');
		expect(await probe.ready()).toBe(false); // re-probed the older server -> fallback
		// invalidate() forces the same re-detection explicitly.
		redis.setInfo(redisInfo('7.4.0'));
		probe.invalidate();
		expect(await probe.ready()).toBe(true);
	});

	it('leaves the result unknown on a transient failure and re-probes next call', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		redis.setFail(true);
		const probe = createHashFieldTTLProbe(redis);
		expect(await probe.ready()).toBe(false); // transient failure -> safe fallback
		expect(redis.calls).toBe(1);
		redis.setFail(false);
		expect(await probe.ready()).toBe(true);  // recovered: re-probed and confirmed
		expect(redis.calls).toBe(2);
	});

	it('ready() does not stall when the probe INFO hangs on the offline queue', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		redis.setHang(true); // INFO never resolves or rejects (outage)
		const probe = createHashFieldTTLProbe(redis, { timeoutMs: 30 });
		// Without the bound, awaiting the in-flight INFO would hang the publish path
		// forever; the timer caps it and returns the safe fallback.
		expect(await probe.ready()).toBe(false);
		expect(redis.calls).toBe(1);
	});

	it('reuses a single hung probe across calls (no offline-queue pile-up)', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		redis.setHang(true);
		const probe = createHashFieldTTLProbe(redis, { timeoutMs: 20 });
		expect(await probe.ready()).toBe(false);
		expect(await probe.ready()).toBe(false);
		expect(await probe.ready()).toBe(false);
		// The one in-flight INFO is reused; a stalled probe is not re-issued per call.
		expect(redis.calls).toBe(1);
	});

	it('a reconnect during a hung probe forces a fresh re-detection (not blocked behind it)', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		redis.setHang(true);
		const probe = createHashFieldTTLProbe(redis, { timeoutMs: 20 });
		expect(await probe.ready()).toBe(false); // hung -> fallback, probe still pending
		// Reconnect to a reachable server; the stale INFO is dropped so the next call
		// re-probes instead of awaiting the one still stuck on the offline queue.
		redis.setHang(false);
		redis.setInfo(redisInfo('7.4.0'));
		redis.emit('ready');
		expect(await probe.ready()).toBe(true);
		expect(redis.calls).toBe(2);
	});

	it('overlapping ready() calls during a hang share one probe', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		redis.setHang(true);
		const probe = createHashFieldTTLProbe(redis, { timeoutMs: 20 });
		// Three publishes hit ready() concurrently while the server is unreachable;
		// all fall back within the bound and only ONE INFO is issued between them.
		const results = await Promise.all([probe.ready(), probe.ready(), probe.ready()]);
		expect(results).toEqual([false, false, false]);
		expect(redis.calls).toBe(1);
	});

	it('a hung probe that later resolves is picked up without a re-probe', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		redis.setHang(true);
		const probe = createHashFieldTTLProbe(redis, { timeoutMs: 20 });
		expect(await probe.ready()).toBe(false); // bounded fallback while hung
		expect(redis.calls).toBe(1);
		// The SAME in-flight INFO flushes on reconnect (no invalidate) and answers.
		redis.releaseHang();
		expect(await probe.ready()).toBe(true);
		expect(redis.calls).toBe(1); // reused: the resolved probe answered, no re-probe
	});

	it('supported() stays false during a hang without spinning up extra probes', async () => {
		const redis = fakeRedis(redisInfo('7.4.0'));
		redis.setHang(true);
		const probe = createHashFieldTTLProbe(redis, { timeoutMs: 20 });
		// supported() is a sync best-effort read: it kicks the probe (dispatched on a
		// microtask) and returns the safe fallback without ever awaiting.
		expect(probe.supported()).toBe(false);
		expect(probe.supported()).toBe(false); // reuses the same in-flight probe
		await new Promise((r) => setTimeout(r, 5)); // let the deferred INFO dispatch
		expect(probe.supported()).toBe(false); // still hung -> still the safe fallback
		expect(redis.calls).toBe(1); // one INFO across all reads, then it stays parked
	});
});
