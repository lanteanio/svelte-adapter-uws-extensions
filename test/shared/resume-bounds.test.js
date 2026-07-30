import { describe, it, expect } from 'vitest';
import { createResumeHook } from '../../src/shared/replay-helpers.js';
import { createLruMap } from '../../src/shared/lru-map.js';
import { MAX_RESUME_TOPICS } from '../../src/shared/caps.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

describe('resume topic bound', () => {
	function hookWithSpies(options = {}) {
		const epochQueries = [];
		const replayed = [];
		const hook = createResumeHook({
			currentEpochs: async (topics) => {
				epochQueries.push(topics);
				return new Map(topics.map((t) => [t, 0]));
			},
			replay: async (_ws, topic) => { replayed.push(topic); return 0; },
			...options
		});
		return { hook, epochQueries, replayed };
	}

	it('caps how many client-presented topics reach the store', async () => {
		const { hook, replayed } = hookWithSpies();
		const lastSeenSeqs = {};
		for (let i = 0; i < MAX_RESUME_TOPICS + 5000; i++) lastSeenSeqs['forged-' + i] = 0;

		await hook(mockWs({ id: 'm' }), { lastSeenSeqs, platform: mockPlatform() });

		// Every accepted topic costs pre-authorization state and a query, so
		// the bound has to be observable in what the store actually saw.
		expect(replayed.length).toBe(MAX_RESUME_TOPICS);
		expect(replayed).toContain('forged-0');
		expect(replayed).not.toContain('forged-' + (MAX_RESUME_TOPICS + 4999));
	});

	it('stops enumerating as soon as the first overflow topic is detected', async () => {
		const target = {};
		for (let i = 0; i < MAX_RESUME_TOPICS + 5000; i++) target['forged-' + i] = 0;
		const lastSeenSeqs = new Proxy(target, {
			getOwnPropertyDescriptor(obj, key) {
				const index = Number(String(key).slice('forged-'.length));
				// The first overflow entry (index == MAX_RESUME_TOPICS) is
				// inspected to detect truncation. Looking at any later entry means
				// the hostile tail is still being scanned after the cap.
				if (index > MAX_RESUME_TOPICS) throw new Error('enumerated beyond resume cap');
				return Reflect.getOwnPropertyDescriptor(obj, key);
			}
		});
		const { hook } = hookWithSpies({ authorize: async () => false });

		await expect(hook(mockWs({ id: 'm' }), {
			lastSeenSeqs,
			platform: mockPlatform()
		})).resolves.toEqual({});
	});

	it('still resumes a normal client in full', async () => {
		const { hook, replayed } = hookWithSpies();
		await hook(mockWs({ id: 'u' }), {
			lastSeenSeqs: { 'room:1': 5, 'room:2': 9 },
			platform: mockPlatform()
		});
		expect(replayed.sort()).toEqual(['room:1', 'room:2']);
	});

	describe('truncation report', () => {
		function overflowing() {
			const lastSeenSeqs = {};
			for (let i = 0; i < MAX_RESUME_TOPICS + 10; i++) lastSeenSeqs['t-' + i] = 0;
			return lastSeenSeqs;
		}

		async function warningsFrom(fn) {
			const seen = [];
			const real = console.warn;
			console.warn = (...args) => seen.push(args.join(' '));
			try { await fn(); } finally { console.warn = real; }
			return seen;
		}

		it('reports a truncated topic set', async () => {
			const { hook } = hookWithSpies();
			const warnings = await warningsFrom(() =>
				hook(mockWs({ id: 'm' }), { lastSeenSeqs: overflowing(), platform: mockPlatform() })
			);
			expect(warnings).toHaveLength(1);
			expect(warnings[0]).toContain('MAX_RESUME_TOPICS');
			expect(warnings[0]).toContain(`more than ${MAX_RESUME_TOPICS}`);
		});

		it('reports every overflow to metrics even while logs are throttled', async () => {
			let overflows = 0;
			const { hook } = hookWithSpies({
				authorize: async () => false,
				onTruncate: () => { overflows++; }
			});
			const warnings = await warningsFrom(async () => {
				await hook(mockWs({ id: 'm' }), { lastSeenSeqs: overflowing(), platform: mockPlatform() });
				await hook(mockWs({ id: 'm' }), { lastSeenSeqs: overflowing(), platform: mockPlatform() });
			});
			expect(overflows).toBe(2);
			expect(warnings).toHaveLength(1);
		});

		it('throttles a repeated frame rather than amplifying it', async () => {
			const { hook } = hookWithSpies();
			const ws = mockWs({ id: 'm' });
			const warnings = await warningsFrom(async () => {
				for (let i = 0; i < 5; i++) {
					await hook(ws, { lastSeenSeqs: overflowing(), platform: mockPlatform() });
				}
			});
			// Any socket can send this frame at will, so a line per frame is an
			// amplifier of exactly the kind the bound exists to prevent.
			expect(warnings).toHaveLength(1);
		});

		it('does not silence a DIFFERENT store, because the throttle is per hook', async () => {
			// A module-global latch let whichever store warned first mute every
			// other one for the life of the process - and made the behaviour
			// depend on test file order.
			const a = hookWithSpies();
			const b = hookWithSpies();
			const warnings = await warningsFrom(async () => {
				await a.hook(mockWs({ id: 'm' }), { lastSeenSeqs: overflowing(), platform: mockPlatform() });
				await b.hook(mockWs({ id: 'm' }), { lastSeenSeqs: overflowing(), platform: mockPlatform() });
			});
			expect(warnings).toHaveLength(2);
		});

		it('says nothing when the client is within the bound', async () => {
			const { hook } = hookWithSpies();
			const warnings = await warningsFrom(() =>
				hook(mockWs({ id: 'u' }), { lastSeenSeqs: { 'room:1': 1 }, platform: mockPlatform() })
			);
			expect(warnings).toEqual([]);
		});
	});
});

describe('epoch cache bound', () => {
	it('evicts least-recently-used, not first-inserted', () => {
		// Map.set on an existing key updates the value but does NOT move the
		// key, so evicting map.keys().next().value throws out the
		// first-INSERTED entry. Under a spray of fresh topics that is
		// precisely backwards: the hot topics subscribed at boot go first and
		// every forged entry survives.
		const lru = createLruMap(3);
		lru.set('hot', 1);
		lru.set('b', 2);
		lru.set('c', 3);

		lru.get('hot');       // touch: 'hot' is now the most recent
		lru.set('d', 4);      // evicts 'b', the genuinely least-recently-used

		expect(lru.has('hot')).toBe(true);
		expect(lru.has('b')).toBe(false);
		expect(lru.size).toBe(3);
	});

	it('re-setting an existing key refreshes its position', () => {
		const lru = createLruMap(2);
		lru.set('a', 1);
		lru.set('b', 2);
		lru.set('a', 11);
		lru.set('c', 3);
		expect(lru.has('a')).toBe(true);
		expect(lru.has('b')).toBe(false);
		expect(lru.get('a')).toBe(11);
	});

	it('never exceeds its bound', () => {
		const lru = createLruMap(50);
		for (let i = 0; i < 5000; i++) lru.set('forged-' + i, i);
		expect(lru.size).toBe(50);
	});

	it('rejects a nonsensical bound instead of silently not bounding', () => {
		expect(() => createLruMap(0)).toThrow();
		expect(() => createLruMap(1.5)).toThrow();
		expect(() => createLruMap(NaN)).toThrow();
	});
});
