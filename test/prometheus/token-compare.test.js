import { describe, it, expect } from 'vitest';
import { createMetrics, tokenEquals } from '../../src/prometheus/index.js';

describe('tokenEquals', () => {
	it('matches only exact equality and never throws on junk', () => {
		expect(tokenEquals('abc', 'abc')).toBe(true);
		expect(tokenEquals('abc', 'abd')).toBe(false);
		expect(tokenEquals('abc', 'abcd')).toBe(false);
		expect(tokenEquals('', 'a')).toBe(false);
		expect(tokenEquals(null, 'a')).toBe(false);
		expect(tokenEquals('a', undefined)).toBe(false);
		expect(tokenEquals(undefined, undefined)).toBe(false);
		expect(tokenEquals({}, {})).toBe(false);
	});

	it('never authorizes on an empty token, however it arrived', () => {
		// `process.env.X` is '' for `X=`, and uWS `req.getHeader()` returns ''
		// for an absent header. An empty-vs-empty comparison therefore lets
		// EVERY unauthenticated scrape through at exactly the moment an
		// operator believes they turned authentication on - and the shipped
		// example compares a header against an env var.
		expect(tokenEquals('', '')).toBe(false);
		expect(tokenEquals('', 'secret')).toBe(false);
		expect(tokenEquals('presented', '')).toBe(false);
	});

	it('does not leak the expected length through timing', () => {
		// Burning a same-length dummy on a mismatch sizes the work by the
		// PRESENTED length, so the cost varies with it and the minimum sits at
		// the secret's length. Comparing fixed-width digests removes the
		// dependence entirely: a 1-char and a 10000-char guess cost the same.
		const secret = 'a'.repeat(48);
		const samples = (presented) => {
			const t0 = process.hrtime.bigint();
			for (let i = 0; i < 20_000; i++) tokenEquals(presented, secret);
			return Number(process.hrtime.bigint() - t0);
		};
		for (const p of ['x', 'x'.repeat(47), 'x'.repeat(48), 'x'.repeat(49)]) samples(p);
		const short = samples('x');
		const atLength = samples('x'.repeat(48));
		const long = samples('x'.repeat(4096));
		// Generous bounds: this asserts the absence of a LENGTH-PROPORTIONAL
		// cost, not a precise constant, so it stays honest on a loaded box.
		expect(atLength).toBeLessThan(short * 4);
		expect(long).toBeLessThan(short * 8);
	});

	it('is reachable as metrics.tokenEquals, the way the shipped example calls it', () => {
		// The doc example people copy reads `metrics.tokenEquals(...)`. If it
		// only existed as a module export, that copy-paste would be a
		// TypeError on their metrics endpoint.
		const metrics = createMetrics();
		expect(typeof metrics.tokenEquals).toBe('function');
		expect(metrics.tokenEquals('t', 't')).toBe(true);
		expect(metrics.tokenEquals('t', 'u')).toBe(false);
	});

	it('gates authedHandler through the real predicate path', async () => {
		const metrics = createMetrics();
		metrics.counter('scrape_probe_total', 'Something to serialize').inc();
		const handler = metrics.authedHandler((_res, req) =>
			metrics.tokenEquals(req.getHeader('x-scrape-token'), 'sekret')
		);

		const call = async (token) => {
			let status = null;
			let body = '';
			const res = {
				cork: (fn) => { fn(); return res; },
				writeStatus: (s) => { status = s; return res; },
				writeHeader: () => res,
				end: (b) => { body = b || ''; },
				onAborted: () => res
			};
			handler(res, { getHeader: () => token });
			await new Promise((r) => setTimeout(r, 5));
			return { status, body };
		};

		const denied = await call('wrong');
		expect(String(denied.status)).toMatch(/401|403/);
		expect(denied.body).not.toContain('# HELP');

		const allowed = await call('sekret');
		expect(allowed.body).toContain('# HELP');
	});
});
