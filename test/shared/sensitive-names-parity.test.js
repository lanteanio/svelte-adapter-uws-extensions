// src/shared/sensitive-names.js is a copy of the adapter's field-name rules
// (src/plugins/_shared/sensitive.js), so the bundled presence and cursor
// plugins and the cluster variants here give one field name one verdict. A
// rule that changes on one side only shows up to an app as a roster that
// changes shape when it scales out: a field present on one node and dropped
// across the cluster, or the reverse.
//
// The first test compares the two files as text, so any change on the
// adapter's side fails here, including a list entry no generated name would
// exercise. When it fails after an adapter upgrade, copy the adapter's file
// over ours again, leaving out noteDroppedField: nothing here reports drops,
// so it would be a warning that never fires. The second block asks both
// copies the same questions over a generated corpus.

import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import * as ours from '../../src/shared/sensitive-names.js';
import * as adapter from '../../node_modules/svelte-adapter-uws/src/plugins/_shared/sensitive.js';

const adapterUrl = new URL('../../node_modules/svelte-adapter-uws/src/plugins/_shared/sensitive.js', import.meta.url);
const oursUrl = new URL('../../src/shared/sensitive-names.js', import.meta.url);

/** The adapter's source without noteDroppedField and the sentence that names it. */
function withoutDropReporter(source) {
	const sentence = ' * field. A caller that uses this matcher as a redactor can report such a drop\n' +
		' * once by name via noteDroppedField; the identity-only defaults do not\n' +
		' * enumerate non-identity fields and therefore do not emit drop warnings.\n';
	const start = source.indexOf('/**\n * Warn-once state for {@link noteDroppedField}');
	const end = source.indexOf('/**\n * Recursion budget for the default');
	expect(source.includes(sentence), 'the sentence naming noteDroppedField moved').toBe(true);
	expect(start, 'the noteDroppedField block moved').toBeGreaterThan(0);
	expect(end, 'the noteDroppedField block moved').toBeGreaterThan(start);
	return (source.slice(0, start) + source.slice(end)).replace(sentence, ' * field.\n');
}

describe('the field-name rules are the adapter\'s file', () => {
	it('matches it line for line apart from noteDroppedField', () => {
		const theirs = readFileSync(adapterUrl, 'utf8').replace(/\r\n/g, '\n');
		const mine = readFileSync(oursUrl, 'utf8').replace(/\r\n/g, '\n');
		expect(mine).toBe(withoutDropReporter(theirs));
	});
});

const WORDS = [
	'token', 'secret', 'password', 'passwd', 'pwd', 'pass', 'session', 'cookie',
	'jwt', 'credential', 'auth', 'oauth', 'author', 'authored', 'authors', 'api',
	'access', 'private', 'priv', 'refresh', 'master', 'root', 'admin', 'signing',
	'sign', 'signature', 'sig', 'hmac', 'nonce', 'key', 'keys', 'salt', 'seed',
	'bearer', 'csrf', 'otp', 'mfa', 'totp', 'pin', 'cc', 'cvv', 'ssn', 'dob',
	'iban', 'email', 'mail', 'phone', 'telephone', 'mobile', 'fax', 'number',
	'card', 'credit', 'debit', 'account', 'routing', 'sort', 'code', 'tax', 'id',
	'uid', 'national', 'passport', 'birth', 'date', 'of', 'home', 'billing',
	'shipping', 'address', 'addr', 'ip', 'ipv4', 'remote', 'client', 'peer',
	'server', 'host', 'proxy', 'forwarded', 'for', 'x', 'real', 'url', 'avatar',
	'image', 'profile', 'header', 'headers', 'raw', 'request', 'user', 'agent',
	'referer', 'name', 'display', 'color', 'role', 'typing', 'cursor',
	'microphone', 'headphones', 'spinner', 'zip', 'primary', 'hash', 'range',
	'partition', 'cache', 'translation', 'stream', 'device', 'webhook', 'service',
	'wallet', 'phrase', 'recovery', 'backup', 'invite', 'magic', 'link', 'last4',
	'hashed', 'masked', 'count', 'enabled', 'on', 'at', 'by', 'value', 'state',
	'group', 'team', 'org', 'tenant', 'live', 'test', 'keyboard', 'monkey',
	'process', 'business', 'syntax', 'halifax', 'pair', 'vault', 'material'
];

const cap = (w) => w[0].toUpperCase() + w.slice(1);

/** Six spellings of one word sequence: the forms names actually arrive in. */
function spellings(parts) {
	return [
		parts.join(''),
		parts[0] + parts.slice(1).map(cap).join(''),
		parts.map(cap).join(''),
		parts.join('_'),
		parts.join('-'),
		parts.join('_').toUpperCase()
	];
}

function buildCorpus() {
	const names = new Set(['', '_', '__proto__', 'constructor', 'prototype', '__x', 'a'.repeat(65), 'x'.repeat(64)]);
	for (const a of WORDS) {
		for (const s of spellings([a])) names.add(s);
		names.add(a + '1');
		names.add(a + '2');
		for (const b of WORDS) {
			for (const s of spellings([a, b])) names.add(s);
		}
	}
	// A deterministic sample of three-word names: every pair above, times a
	// sample of third words, would be several million names for little gain.
	let seed = 0x2545f491;
	const next = () => {
		seed = (Math.imul(seed, 1103515245) + 12345) >>> 0;
		return seed % WORDS.length;
	};
	for (let i = 0; i < 15000; i++) {
		const parts = [WORDS[next()], WORDS[next()], WORDS[next()]];
		for (const s of spellings(parts)) names.add(s);
	}
	return [...names];
}

describe('field-name verdicts match the adapter', () => {
	const corpus = buildCorpus();

	it('covers a corpus large enough to mean something', () => {
		expect(corpus.length).toBeGreaterThan(100000);
	});

	for (const fn of ['isUnsafeProjectionFieldName', 'isSensitiveFieldName', 'isStructurallyUnsafeFieldName']) {
		it(`${fn} agrees on every name`, () => {
			const differ = [];
			for (const name of corpus) {
				const mine = ours[fn](name);
				const theirs = adapter[fn](name);
				if (mine !== theirs) differ.push(`${name}: ours=${mine} adapter=${theirs}`);
			}
			expect(differ.slice(0, 20)).toEqual([]);
		});
	}

	it('bounds depth the same way', () => {
		expect(ours.MAX_PROJECTION_DEPTH).toBe(adapter.MAX_PROJECTION_DEPTH);
		const nest = (n) => {
			let v = {};
			for (let i = 0; i < n; i++) v = { a: v };
			return v;
		};
		for (const depth of [1, 2, 63, 64, 65, 200]) {
			for (const limit of [1, 64]) {
				expect(ours.exceedsDepth(nest(depth), limit), `${depth}/${limit}`).toBe(adapter.exceedsDepth(nest(depth), limit));
			}
		}
	});
});
