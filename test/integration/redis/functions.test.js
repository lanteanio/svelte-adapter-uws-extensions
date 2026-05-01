/**
 * Integration tests for redis/functions against a real Redis 7 server.
 *
 * Exercises FUNCTION LOAD REPLACE / FUNCTION DELETE / FCALL against the
 * server's real function namespace. Function names are server-global, so
 * each test uses a unique library name and the suite cleans up after
 * itself in beforeEach + afterAll. The mock-based suite at
 * test/redis/functions.test.js stays as-is; this file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createFunctionLibrary } from '../../../redis/functions.js';

const LIB_NAME = 'inttest_fnlib';

const SAMPLE_LIB = `#!lua name=${LIB_NAME}
redis.register_function('inttest_echo', function(keys, args)
  return args[1]
end)
redis.register_function('inttest_count_keys', function(keys, args)
  return #keys
end)
redis.register_function('inttest_set_get', function(keys, args)
  redis.call('SET', keys[1], args[1])
  return redis.call('GET', keys[1])
end)
redis.register_function('inttest_sum', function(keys, args)
  local s = 0
  for i, v in ipairs(args) do
    s = s + tonumber(v)
  end
  return s
end)
redis.register_function('inttest_boom', function(keys, args)
  return redis.error_reply('intentional error')
end)
`;

async function deleteLib(client, name) {
	try {
		await client.redis.function('DELETE', name);
	} catch {
		// Library was not loaded; ignore.
	}
}

describe('redis function library (integration)', () => {
	let client;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-fn:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Wipe under our prefix so each test starts clean.
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');

		// Function namespace is global per server -- always start from a
		// clean slate so a previous failed test does not leak state.
		await deleteLib(client, LIB_NAME);
	});

	afterAll(async () => {
		await deleteLib(client, LIB_NAME);
		await client.quit();
	});

	describe('load + FUNCTION LIST', () => {
		it('parses the library name from the shebang and registers it on the server', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			expect(lib.name).toBe(LIB_NAME);

			await lib.load();

			const list = await client.redis.function('LIST');
			const names = list.map((entry) => {
				// Each entry is [ 'library_name', '<name>', 'engine_name', '<engine>', ... ]
				const idx = entry.indexOf('library_name');
				return idx === -1 ? null : entry[idx + 1];
			});
			expect(names).toContain(LIB_NAME);
		});

		it('REPLACE makes load() idempotent across code revisions', async () => {
			const lib1 = createFunctionLibrary(client, SAMPLE_LIB);
			await lib1.load();

			const updated = SAMPLE_LIB.replace('return args[1]', 'return tostring(args[1]) .. "-v2"');
			const lib2 = createFunctionLibrary(client, updated);
			await lib2.load();

			const r = await lib2.call('inttest_echo', { args: ['hi'] });
			expect(r).toBe('hi-v2');
		});

		it('runs INFO server only once across multiple loads on the same library', async () => {
			let calls = 0;
			const orig = client.redis.info.bind(client.redis);
			client.redis.info = async function (...args) {
				calls++;
				return orig(...args);
			};
			try {
				const lib = createFunctionLibrary(client, SAMPLE_LIB);
				await lib.load();
				await lib.load();
				expect(calls).toBe(1);
			} finally {
				client.redis.info = orig;
			}
		});
	});

	describe('call (FCALL dispatch)', () => {
		it('returns the value the function returns', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			const r = await lib.call('inttest_echo', { args: ['hello'] });
			expect(r).toBe('hello');
		});

		it('passes keys with the right numkeys count', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			const r = await lib.call('inttest_count_keys', {
				keys: [client.key('a'), client.key('b'), client.key('c')]
			});
			expect(r).toBe(3);
		});

		it('actually mutates Redis state when the function calls redis.call()', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			const k = client.key('set-get-test');
			const r = await lib.call('inttest_set_get', { keys: [k], args: ['xyz'] });
			expect(r).toBe('xyz');
			expect(await client.redis.get(k)).toBe('xyz');
		});

		it('handles empty keys + args', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			const r = await lib.call('inttest_sum');
			expect(r).toBe(0);
		});

		it('args are passed as Redis bulk strings; numeric coercion works in Lua', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			const r = await lib.call('inttest_sum', { args: [1, 2, 3, 4] });
			expect(r).toBe(10);
		});

		it('propagates server-side errors from redis.error_reply', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await expect(lib.call('inttest_boom')).rejects.toThrow('intentional error');
		});

		it('calling an unknown function name reports the failure to the client', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await expect(lib.call('does-not-exist')).rejects.toThrow();
		});
	});

	describe('delete (FUNCTION DELETE)', () => {
		it('removes the library so subsequent FCALLs fail', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			expect(await lib.call('inttest_echo', { args: ['x'] })).toBe('x');

			await lib.delete();
			await expect(lib.call('inttest_echo', { args: ['x'] })).rejects.toThrow();
		});

		it('throws when deleting a library that was never loaded', async () => {
			// Make sure the namespace is empty for this name.
			await deleteLib(client, LIB_NAME);
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await expect(lib.delete()).rejects.toThrow();
		});
	});
});
