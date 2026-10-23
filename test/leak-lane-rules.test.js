import { describe, it, expect } from 'vitest';
import {
	judge,
	judgeKeyspace,
	selfCheckVerdict,
	readFlag,
	quantile,
	retainedBytes,
	HANDLE_TOLERANCE,
	LISTENER_TOLERANCE,
	PERSISTENT_KEY_TOLERANCE,
	RSS_TOLERANCE_BYTES
} from '../scripts/leak-lane.js';

// The leak lane spends minutes per run; its verdict rules are pure and run
// here in milliseconds, so a rule that stops firing is caught by the unit suite.

const MiB = 1048576;
const flat = (n, value) => Array.from({ length: n }, () => value);
const ramp = (n, start, step) => Array.from({ length: n }, (_, i) => start + i * step);
/** Deterministic noise around a level, never a trend. */
const noisy = (n, level, amp) => Array.from({ length: n }, (_, i) => level + ((i * 7919) % 13 - 6) * (amp / 6));

function healthy(overrides = {}) {
	return {
		rss: noisy(60, 200 * MiB, 2 * MiB),
		handles: noisy(60, 40, 2),
		listeners: flat(60, 30),
		errors: 0,
		requests: 3600,
		baselineP95: 10,
		windowP95: 11,
		settled: true,
		retained: { baseline: 50 * MiB, final: 51 * MiB, settled: true },
		connectionsAfter: 0,
		handlesBefore: 40, handlesAfter: 38,
		listenersBefore: 30, listenersAfter: 30,
		...overrides
	};
}

describe('leak lane instance verdict', () => {
	it('passes a flat, healthy instance with no health notes', () => {
		const v = judge(healthy());
		expect(v.failures).toEqual([]);
		expect(v.health).toEqual([]);
		expect(v.failing).toBe(false);
	});

	it('fails a resident set that climbs on a line', () => {
		const v = judge(healthy({ rss: ramp(60, 200 * MiB, MiB) }));
		expect(v.growth.leaking).toBe(true);
		expect(v.failures.some((f) => f.startsWith('resident set grew'))).toBe(true);
	});

	it('does not read a noisy flat resident set as growth', () => {
		const v = judge(healthy({ rss: noisy(60, 200 * MiB, 6 * MiB) }));
		expect(v.growth.leaking).toBe(false);
	});

	it('does not read growth inside the tolerance as a leak', () => {
		const v = judge(healthy({ rss: ramp(60, 200 * MiB, (RSS_TOLERANCE_BYTES - MiB) / 59) }));
		expect(v.growth.leaking).toBe(false);
	});

	it('fails listeners that grow across the window', () => {
		const v = judge(healthy({ listeners: ramp(60, 30, 20), listenersAfter: 30 + 1200 }));
		expect(v.listeners.leaking).toBe(true);
		expect(v.failures.some((f) => f.startsWith('Redis connection listeners grew'))).toBe(true);
	});

	it('fails listeners kept after the cooldown even when the window looked flat', () => {
		const v = judge(healthy({ listenersAfter: 30 + LISTENER_TOLERANCE + 1 }));
		expect(v.listeners.trend.leaking).toBe(false);
		expect(v.listeners.leaking).toBe(true);
	});

	it('allows listeners and handles within their tolerances', () => {
		const v = judge(healthy({ listenersAfter: 30 + LISTENER_TOLERANCE, handlesAfter: 40 + HANDLE_TOLERANCE }));
		expect(v.failing).toBe(false);
	});

	it('fails handles that are kept after the cooldown', () => {
		const v = judge(healthy({ handlesAfter: 40 + HANDLE_TOLERANCE + 1 }));
		expect(v.handles.leaking).toBe(true);
		expect(v.failures.some((f) => f.startsWith('active handles grew'))).toBe(true);
	});

	it('fails connections still registered after the cooldown', () => {
		const v = judge(healthy({ connectionsAfter: 2 }));
		expect(v.failures.some((f) => f.includes('still registered after cooldown'))).toBe(true);
	});

	it('fails settled retention over both the ratio and the byte floor', () => {
		const v = judge(healthy({ retained: { baseline: 50 * MiB, final: 80 * MiB, settled: true } }));
		expect(v.retained.leaking).toBe(true);
	});

	it('fails an error rate and a latency creep over their ceilings', () => {
		const v = judge(healthy({ errors: 40, windowP95: 20 }));
		expect(v.failures.some((f) => f.includes('requests failed'))).toBe(true);
		expect(v.failures.some((f) => f.startsWith('p95 latency crept'))).toBe(true);
	});

	it('reports what it could not judge as health, not as a pass', () => {
		const v = judge(healthy({
			settled: false,
			rss: flat(3, 200 * MiB),
			baselineP95: 0,
			retained: { baseline: 50 * MiB, final: 50 * MiB, settled: false },
			connectionsAfter: undefined
		}));
		expect(v.health).toHaveLength(5);
	});
});

describe('leak lane keyspace verdict', () => {
	const base = { keys: flat(60, 177), persistentBefore: 176, persistentAfter: 48, membersAfter: 0, relayShare: 1 };

	it('passes a flat keyspace whose persistent keys shrank after the clients left', () => {
		const v = judgeKeyspace(base);
		expect(v.failing).toBe(false);
		expect(v.health).toEqual([]);
	});

	it('fails a key count that climbs across the window', () => {
		const v = judgeKeyspace({ ...base, keys: ramp(60, 177, 40) });
		expect(v.growth.leaking).toBe(true);
		expect(v.failures[0]).toMatch(/Redis keys grew/);
	});

	it('fails keys without an expiry kept past the baseline', () => {
		const v = judgeKeyspace({ ...base, persistentAfter: 176 + PERSISTENT_KEY_TOLERANCE + 1 });
		expect(v.failures.some((f) => f.includes('without an expiry'))).toBe(true);
	});

	it('fails membership left behind by closed clients', () => {
		const v = judgeKeyspace({ ...base, membersAfter: 1 });
		expect(v.failures.some((f) => f.includes('member entries left'))).toBe(true);
	});

	it('reports an unexercised relay as health', () => {
		expect(judgeKeyspace({ ...base, relayShare: null }).health[0]).toMatch(/relay was not exercised/);
		expect(judgeKeyspace({ ...base, relayShare: 0.2 }).health[0]).toMatch(/20% of the/);
	});
});

describe('leak lane self-check', () => {
	it('passes only when every planted leak was caught by its own gate', () => {
		expect(selfCheckVerdict({ growth: { leaking: true }, listeners: { leaking: true } }, { leaking: true }))
			.toEqual({ pass: true, reasons: [] });
	});

	it('names each gate that missed its planted leak', () => {
		const v = selfCheckVerdict({ growth: { leaking: false }, listeners: { leaking: true } }, { leaking: false });
		expect(v.pass).toBe(false);
		expect(v.reasons).toEqual([
			'the planted memory growth went undetected by the growth gate',
			'the planted Redis keys went undetected by the key gate'
		]);
	});

	it('does not accept a failure on some other gate as detection', () => {
		const v = selfCheckVerdict({ growth: { leaking: false }, listeners: { leaking: false }, failing: true }, { leaking: false });
		expect(v.pass).toBe(false);
		expect(v.reasons).toHaveLength(3);
	});
});

describe('leak lane helpers', () => {
	it('reads flags in both spellings and refuses a missing value', () => {
		expect(readFlag(['--minutes', '2'], 'minutes', '3')).toBe('2');
		expect(readFlag(['--minutes=5'], 'minutes', '3')).toBe('5');
		expect(readFlag([], 'minutes', '3')).toBe('3');
		expect(() => readFlag(['--minutes'], 'minutes', '3')).toThrow('requires a value');
		expect(() => readFlag(['--minutes', '--backend'], 'minutes', '3')).toThrow('requires a value');
	});

	it('takes quantiles and retained bytes the way the verdict reads them', () => {
		expect(quantile([], 0.95)).toBe(0);
		expect(quantile([5, 1, 3, 2, 4], 0.5)).toBe(3);
		expect(retainedBytes({ heapUsed: 10, external: 5 })).toBe(15);
		expect(retainedBytes({ heapUsed: 10 })).toBe(10);
	});
});
