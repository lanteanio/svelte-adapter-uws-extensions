import { describe, it, expect } from 'vitest';
import { stripInternal, redactConnectionUrl, STRIP_MAX_DEPTH, STRIP_MAX_NODES } from '../../src/shared/sensitive.js';
import { deepEqual, DEEP_EQUAL_MAX_DEPTH } from '../../src/redis/presence/data.js';

describe('redactConnectionUrl', () => {
	it('redacts the query password for every userinfo password length', () => {
		// The query region used to be located with an offset captured from
		// the PRE-redaction string, so any userinfo password that was not
		// exactly 3 chars shifted the real '?' behind the offset and the
		// query credential survived verbatim into log-safe error text.
		// Lengths around the 3-char case the old arithmetic happened to get
		// right. Each password is distinctive so "not present" means the
		// userinfo was redacted, not that it collided with other URL text.
		for (const pw of ['Q', 'Qw', 'Qwe', 'Qwer', 'Qwertyuiop', 'Z'.repeat(200)]) {
			const out = redactConnectionUrl(`postgres://user:${pw}@host/db?password=hunter2&sslmode=require`);
			expect(out).not.toContain('hunter2');
			expect(out).not.toContain(pw);
			expect(out).toContain('password=***');
			expect(out).toContain('sslmode=require');
		}
		const redis = redactConnectionUrl('redis://:s3cretlongpassword@redis.internal:6379/0?password=hunter2');
		expect(redis).not.toContain('hunter2');
		expect(redis).toContain(':***@');
	});

	it('handles @-in-password, IPv6, no-userinfo and fragments', () => {
		expect(redactConnectionUrl('redis://user:p@ssword@host')).toBe('redis://user:***@host');
		expect(redactConnectionUrl('redis://:secret@[::1]:6379')).toBe('redis://:***@[::1]:6379');
		expect(redactConnectionUrl('postgres://host/db?pwd=x7')).toBe('postgres://host/db?pwd=***');
		expect(redactConnectionUrl('postgres://u:longpw@h/db?password=q#frag?ment')).toBe('postgres://u:***@h/db?password=***#frag?ment');
	});

	it('leaves a URL with no credentials untouched', () => {
		expect(redactConnectionUrl('postgres://host:5432/db')).toBe('postgres://host:5432/db');
		expect(redactConnectionUrl('not a url at all')).toBe('not a url at all');
	});
});

describe('stripInternal bounds', () => {
	it('never throws on a deep chain', () => {
		let deep = {};
		let cur = deep;
		for (let i = 0; i < 200000; i++) { cur.n = {}; cur = cur.n; }
		expect(() => stripInternal(deep)).not.toThrow();
	});

	it('substitutes a deterministic placeholder past the depth cap', () => {
		let deep = {};
		let cur = deep;
		for (let i = 0; i < STRIP_MAX_DEPTH + 5; i++) { cur.n = {}; cur = cur.n; }
		cur.leaf = 1;
		const out = stripInternal(deep);
		let node = out;
		for (let i = 0; i < STRIP_MAX_DEPTH; i++) node = node.n;
		expect(node).toBe('[deep]');
	});

	it('bounds total work, not just depth, on a shared-subtree fan-out', () => {
		// The ancestor WeakSet is a PATH set (entries are removed on the way
		// back up) so a repeated sibling is not mistaken for a cycle. That
		// makes a shared subtree cost one walk per reference: this shape is
		// only 22 deep - well inside STRIP_MAX_DEPTH - but expands to 2^22
		// visits, which without a node budget is a V8 heap OOM that kills the
		// process rather than an error any caller can catch.
		let n = { x: 1 };
		for (let i = 0; i < 22; i++) n = { a: n, b: n };
		const out = stripInternal(n);
		expect(JSON.stringify(out)).toContain('[truncated]');
	});

	it('leaves shallow input alone and still strips internal/sensitive keys', () => {
		expect(stripInternal({ a: 1, b: { c: [1, 2] }, __x: 1, token: 't' })).toEqual({ a: 1, b: { c: [1, 2] } });
	});

	it('exposes its bounds as constants so callers can reason about them', () => {
		expect(STRIP_MAX_DEPTH).toBeGreaterThan(0);
		expect(STRIP_MAX_NODES).toBeGreaterThan(STRIP_MAX_DEPTH);
	});
});

describe('presence deepEqual bounds', () => {
	it('does not overflow the stack on a deep client value', () => {
		// deepEqual runs on the RAW value off the presence-update wire frame.
		// V8's JSON parser is iterative, so a document this deep arrives
		// intact and unbounded recursion would overflow inside the message
		// handler.
		const build = (d) => { let o = {}; let c = o; for (let i = 0; i < d; i++) { c.n = {}; c = c.n; } return o; };
		const a = build(200000);
		const b = build(200000);
		expect(() => deepEqual(a, b)).not.toThrow();
	});

	it('compares as unequal past the depth cap rather than throwing', () => {
		const build = () => { let o = {}; let c = o; for (let i = 0; i < DEEP_EQUAL_MAX_DEPTH + 10; i++) { c.n = {}; c = c.n; } return o; };
		// Identical shapes, but deeper than the cap: reporting "not equal"
		// costs a redundant broadcast, which is the safe direction.
		expect(deepEqual(build(), build())).toBe(false);
	});

	it('is exact within the cap', () => {
		expect(deepEqual({ a: { b: [1, 2, { c: 3 }] } }, { a: { b: [1, 2, { c: 3 }] } })).toBe(true);
		expect(deepEqual({ a: 1 }, { a: 2 })).toBe(false);
		expect(deepEqual({ a: 1 }, { a: 1, b: 2 })).toBe(false);
	});

	it('bounds a shared-subtree fan-out, which the depth cap alone does not', () => {
		// The comparison walks the ancestor PATH, so a shared subtree is
		// re-walked once per reference: this is 2^26 visits at depth 26, a
		// tenth of the way to the depth cap, and took 6.0s unbudgeted. Not
		// reachable from a wire frame - JSON cannot express a shared reference
		// - but presence data is also compared against app-constructed objects.
		const build = (d) => { let n = { x: 1 }; for (let i = 0; i < d; i++) n = { a: n, b: n }; return n; };
		const a = build(26);
		const b = build(26);

		const t0 = process.hrtime.bigint();
		const eq = deepEqual(a, b);
		const ms = Number(process.hrtime.bigint() - t0) / 1e6;

		// Reporting "not equal" past the budget costs a redundant broadcast,
		// the same safe direction the depth cap takes.
		expect(eq).toBe(false);
		// A generous ceiling: this is ~3ms bounded and 6000ms without, so it
		// stays non-flaky on a loaded machine while still failing by three
		// orders of magnitude.
		expect(ms).toBeLessThan(1000);
	});

	it('stays exact for structures deeper than the budget threshold', () => {
		// The budget only starts being carried past a depth threshold, so the
		// charged path needs its own correctness check - a deep-but-small
		// structure must still compare exactly.
		const build = (d, leaf) => { let o = { v: leaf }; for (let i = 0; i < d; i++) o = { n: o }; return o; };
		expect(deepEqual(build(40, 1), build(40, 1))).toBe(true);
		expect(deepEqual(build(40, 1), build(40, 2))).toBe(false);
		expect(deepEqual(build(9, 'x'), build(9, 'x'))).toBe(true);
	});
});

describe('stripInternal fan-out bound', () => {
	it('charges each container its width, so a wide leaf is not re-expanded for free', () => {
		// Shared-subtree expansion re-walks the SAME leaf once per reference.
		// With only objects counted, the 1000 primitives in each leaf are free,
		// so the permitted object visits each multiply by 1000 and a tiny
		// input still allocates an enormous output.
		const leaf = {};
		for (let i = 0; i < 1000; i++) leaf['f' + i] = i;
		let n = leaf;
		for (let i = 0; i < 20; i++) n = { a: n, b: n };

		const out = stripInternal(n);
		expect(countValues(out)).toBeLessThanOrEqual(STRIP_MAX_NODES + 1000);
	});

	it('bounds the WORK, not only the output, once the budget is spent', () => {
		// Charging a container its width bounds what is EMITTED, but
		// `Object.keys` is O(width) and allocating, and a truncated child does
		// not stop the parent's sibling loop - so every remaining reference to
		// a wide object kept paying a full key materialization after the
		// budget was already gone. Output stayed small while the walk ran for
		// minutes.
		const huge = {};
		for (let i = 0; i < 60_000; i++) huge['k' + i] = i;
		const root = Array.from({ length: 4000 }, () => huge);

		const t0 = process.hrtime.bigint();
		const out = stripInternal(root);
		const ms = Number(process.hrtime.bigint() - t0) / 1e6;

		expect(countValues(out)).toBeLessThanOrEqual(STRIP_MAX_NODES + 60_000);
		// Pre-fix this shape took tens of seconds; the guard makes every
		// post-budget visit O(1). A generous ceiling keeps it non-flaky on a
		// loaded machine while still failing by orders of magnitude.
		expect(ms).toBeLessThan(2000);
	});

	it('stops invoking a container\'s remaining accessors once the budget is spent', () => {
		// Counting invocations, not wall time: this is the half of the bound
		// that is actually fixable, and a timing assertion would pin the
		// machine rather than the property. Reading a key past the budget is
		// pure waste - the value is discarded - but the read is not free,
		// because the property is allowed to be a getter that materializes
		// another wide object first. Every one of these 400 ran before the
		// per-key re-check, at ~5000 allocations each.
		const N = 400;
		let invoked = 0;
		const host = {};
		for (let i = 0; i < N; i++) {
			Object.defineProperty(host, 'g' + i, {
				enumerable: true,
				get() {
					invoked++;
					const wide = {};
					for (let j = 0; j < 5000; j++) wide['f' + j] = j;
					return wide;
				}
			});
		}

		const out = stripInternal(host);

		// The budget buys ~20 of these (400 for the host's own width, then
		// 5000 per accessed child against the 100k cap). Anything near N means
		// the walk kept reading after it had nothing left to spend.
		expect(invoked).toBeGreaterThan(0);
		expect(invoked).toBeLessThan(N / 4);
		// Shape is unchanged: a key whose accessor was never invoked reports
		// the same '[truncated]' the walk would have produced by reading it.
		expect(Object.keys(out)).toHaveLength(N);
		expect(out['g' + (N - 1)]).toBe('[truncated]');
	});

	it('leaves the ancestor path set balanced when a getter throws', () => {
		// The WeakSet is a PATH set. Unwinding without removing an object
		// leaves it there forever, and the next walk that reaches it drops the
		// subtree as a phantom cycle.
		//
		// The re-walk has to reach THE OBJECTS THAT WERE LEFT BEHIND. Asserting
		// on some sibling passes whether or not the unwind happened - only the
		// containers on the failing path are ever added to the set, so a
		// sibling was never in it to be corrupted. The getter therefore throws
		// exactly ONCE, so the second walk can succeed and report what the set
		// still holds.
		let thrown = false;
		const inner = { q: 1 };
		Object.defineProperty(inner, 'boom', {
			get() {
				if (!thrown) { thrown = true; throw new Error('boom'); }
				return 2;
			},
			enumerable: true
		});
		const outer = { inner };

		const ancestors = new WeakSet();
		expect(() => stripInternal(outer, ancestors)).toThrow('boom');
		// Both `outer` and `inner` were on the failing path. If either is still
		// recorded as an ancestor, the walk reads it as a cycle and yields
		// `undefined` in place of the whole subtree.
		expect(stripInternal({ outer }, ancestors)).toEqual({ outer: { inner: { q: 1, boom: 2 } } });
	});

	it('leaves the path set balanced when a subtree is TRUNCATED', () => {
		// A `return` out of a try does not run its `catch` the way it would run
		// a `finally`, so a truncation exit needs its own unwind. Without it
		// the truncated container stays in the caller's set and the next walk
		// that reaches it drops it entirely rather than truncating it.
		const wide = {};
		for (let i = 0; i < STRIP_MAX_NODES + 10; i++) wide['k' + i] = i;

		const ancestors = new WeakSet();
		expect(stripInternal({ big: wide }, ancestors, 0, { n: 0 })).toEqual({ big: '[truncated]' });
		// Fresh budget, same set. `wide` must truncate again - not vanish.
		expect(stripInternal({ big: wide }, ancestors, 0, { n: 0 })).toEqual({ big: '[truncated]' });
	});

	it('still passes an ordinary shape through untouched', () => {
		const out = stripInternal({ id: 'u1', name: 'Ada', tags: ['a', 'b'], nested: { ok: true } });
		expect(out).toEqual({ id: 'u1', name: 'Ada', tags: ['a', 'b'], nested: { ok: true } });
	});
});

function countValues(v) {
	if (!v || typeof v !== 'object') return 1;
	let n = 1;
	if (Array.isArray(v)) {
		for (const e of v) n += countValues(e);
	} else {
		for (const k of Object.keys(v)) n += countValues(v[k]);
	}
	return n;
}
