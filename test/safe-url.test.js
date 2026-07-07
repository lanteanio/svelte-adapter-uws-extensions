// Smoke test for the safe-url re-export. The public entry
// (svelte-adapter-uws-extensions/safe-url) single-sources the SSRF logic from
// the adapter via a NAMED runtime re-export in src/shared/safe-url.js; the .d.ts
// re-exports the whole surface via `export *`, so a symbol missing from the
// runtime named list type-checks but resolves to `undefined` at runtime. This
// proves every symbol - including the address-level classifiers - is actually
// callable through the extensions entry.

import { describe, it, expect } from 'vitest';
import { isSafeUrl, checkUrl, checkUrlResolved, classifyAddress, isAddressSafe } from '../src/shared/safe-url.js';

describe('svelte-adapter-uws-extensions/safe-url re-export', () => {
	it('re-exports every symbol as a callable function (not undefined)', () => {
		for (const fn of [isSafeUrl, checkUrl, checkUrlResolved, classifyAddress, isAddressSafe]) {
			expect(typeof fn).toBe('function');
		}
	});

	it('classifyAddress flags blocked ranges and passes a real public IP', () => {
		expect(classifyAddress('127.0.0.1')).toBe('loopback');
		expect(classifyAddress('::1')).toBe('loopback');
		expect(classifyAddress('10.0.0.1')).toBe('rfc1918');
		expect(classifyAddress('169.254.169.254')).toBe('metadata');
		expect(classifyAddress('8.8.8.8')).toBeNull();
	});

	it('a DNS name or empty input is not-an-ip, so it can never masquerade as a safe public address', () => {
		expect(classifyAddress('example.com')).toBe('not-an-ip');
		expect(classifyAddress('')).toBe('not-an-ip');
	});

	it('isAddressSafe is the boolean gate over classifyAddress (null === safe)', () => {
		expect(isAddressSafe('8.8.8.8')).toBe(true);
		expect(isAddressSafe('127.0.0.1')).toBe(false);
		expect(isAddressSafe('example.com')).toBe(false);
	});
});
