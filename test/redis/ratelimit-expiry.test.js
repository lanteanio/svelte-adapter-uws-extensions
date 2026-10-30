// The rate limiters accept a fractional interval or blockDuration, as the
// adapter's in-memory limiter does. Redis refuses a fractional PEXPIRE, so
// every expiry the limiter scripts set must be rounded up; one that is not
// fails every consume or ban that reaches it.
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const read = (path) => readFileSync(fileURLToPath(new URL(path, import.meta.url)), 'utf8');
const sources = {
	'token-bucket-script.js': read('../../src/redis/token-bucket-script.js'),
	'ratelimit.js': read('../../src/redis/ratelimit.js'),
	'composite-ratelimit.js': read('../../src/redis/composite-ratelimit.js')
};

describe('rate-limit expiries are whole milliseconds', () => {
	it.each(Object.entries(sources))('%s rounds every PEXPIRE argument up', (_name, source) => {
		const calls = source.split('\n').filter((line) => /pexpire/i.test(line) && !/^\s*(\/\/|--|\*)/.test(line));
		expect(calls.length).toBeGreaterThan(0);
		for (const line of calls) {
			expect(line).toMatch(/redis\.call\('pexpire', [^,]+, math\.ceil\(|redis\.pexpire\([^,]+, Math\.ceil\(/);
		}
	});
});
