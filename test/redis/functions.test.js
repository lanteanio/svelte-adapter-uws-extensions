import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createFunctionLibrary } from '../../redis/functions.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

const SAMPLE_LIB = `#!lua name=ws-presence
redis.register_function('cleanup', function(keys, args) return 0 end)
redis.register_function('count_live', function(keys, args) return 0 end)
`;

describe('redis function library', () => {
	let client;

	beforeEach(() => {
		client = mockRedisClient('test:');
	});

	describe('construction', () => {
		it('parses the library name from the shebang', () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			expect(lib.name).toBe('ws-presence');
		});

		it('rejects code without a shebang', () => {
			expect(() => createFunctionLibrary(client, 'redis.register_function("x", ...)'))
				.toThrow('shebang');
		});

		it('rejects empty code', () => {
			expect(() => createFunctionLibrary(client, '')).toThrow('non-empty string');
		});

		it('rejects non-string code', () => {
			expect(() => createFunctionLibrary(client, null)).toThrow('non-empty string');
		});
	});

	describe('load', () => {
		it('issues FUNCTION LOAD REPLACE on first call', async () => {
			const calls = [];
			const origFunction = client.redis.function;
			client.redis.function = async (...args) => {
				calls.push(args);
				return origFunction.call(client.redis, ...args);
			};

			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();

			expect(calls).toHaveLength(1);
			expect(calls[0][0]).toBe('LOAD');
			expect(calls[0][1]).toBe('REPLACE');
			expect(calls[0][2]).toBe(SAMPLE_LIB);
		});

		it('persists the library across loads (REPLACE semantics)', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			expect(client._functionLibraries.has('ws-presence')).toBe(true);

			// Load again with different code -- REPLACE makes this idempotent.
			const updated = SAMPLE_LIB.replace('return 0', 'return 1');
			const lib2 = createFunctionLibrary(client, updated);
			await lib2.load();
			expect(client._functionLibraries.get('ws-presence')).toBe(updated);
		});

		it('throws on Redis < 7', async () => {
			client.redis._info = '# Server\nredis_version:6.2.7\n';
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await expect(lib.load()).rejects.toThrow('requires Redis 7 or newer');
		});

		it('throws on unparseable INFO output', async () => {
			client.redis._info = 'not real';
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await expect(lib.load()).rejects.toThrow('could not parse Redis version');
		});

		it('runs INFO only once across multiple loads', async () => {
			let infoCalls = 0;
			const origInfo = client.redis.info;
			client.redis.info = async (...args) => {
				infoCalls++;
				return origInfo.call(client.redis, ...args);
			};

			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await lib.load();
			expect(infoCalls).toBe(1);
		});
	});

	describe('call', () => {
		it('issues FCALL with numkeys + keys + args in the right order', async () => {
			const fcallCalls = [];
			const origFcall = client.redis.fcall;
			client.redis.fcall = async (...args) => {
				fcallCalls.push(args);
				return origFcall.call(client.redis, ...args);
			};
			client._registerFunction('cleanup', () => 7);

			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await lib.call('cleanup', { keys: ['k1', 'k2'], args: ['a1', 'a2', 'a3'] });

			expect(fcallCalls).toHaveLength(1);
			expect(fcallCalls[0]).toEqual(['cleanup', 2, 'k1', 'k2', 'a1', 'a2', 'a3']);
		});

		it('returns the function result', async () => {
			client._registerFunction('cleanup', () => 42);

			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			const result = await lib.call('cleanup', { keys: ['k1'], args: [] });
			expect(result).toBe(42);
		});

		it('passes keys and args to the registered handler', async () => {
			let receivedKeys, receivedArgs;
			client._registerFunction('cleanup', (keys, args) => {
				receivedKeys = keys;
				receivedArgs = args;
				return 0;
			});

			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await lib.call('cleanup', { keys: ['presence:room1'], args: [Date.now(), 90000] });

			expect(receivedKeys).toEqual(['presence:room1']);
			expect(receivedArgs).toHaveLength(2);
		});

		it('handles a call with no keys and no args', async () => {
			client._registerFunction('count_live', () => 0);
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			const result = await lib.call('count_live');
			expect(result).toBe(0);
		});

		it('rejects non-string funcName', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await expect(lib.call(123)).rejects.toThrow('funcName must be a non-empty string');
		});

		it('rejects non-array keys/args', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await expect(lib.call('cleanup', { keys: 'k1' })).rejects.toThrow('keys and args must be arrays');
		});

		it('propagates errors thrown by the function', async () => {
			client._registerFunction('cleanup', () => { throw new Error('Lua error'); });
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			await expect(lib.call('cleanup')).rejects.toThrow('Lua error');
		});
	});

	describe('delete', () => {
		it('issues FUNCTION DELETE for the library', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await lib.load();
			expect(client._functionLibraries.has('ws-presence')).toBe(true);

			await lib.delete();
			expect(client._functionLibraries.has('ws-presence')).toBe(false);
		});

		it('throws if the library was not loaded', async () => {
			const lib = createFunctionLibrary(client, SAMPLE_LIB);
			await expect(lib.delete()).rejects.toThrow('Library not found');
		});
	});

	describe('breaker accounting', () => {
		it('records failure when load fails', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			client.redis.info = async () => { throw new Error('connection lost'); };
			const lib = createFunctionLibrary(client, SAMPLE_LIB, { breaker });

			await expect(lib.load()).rejects.toThrow('connection lost');
			expect(breaker.failures).toBe(1);

			breaker.destroy();
		});

		it('records failure when fcall throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			client._registerFunction('cleanup', () => { throw new Error('Lua error'); });

			const lib = createFunctionLibrary(client, SAMPLE_LIB, { breaker });
			await lib.load();
			expect(breaker.failures).toBe(0);

			await expect(lib.call('cleanup')).rejects.toThrow('Lua error');
			expect(breaker.failures).toBe(1);

			breaker.destroy();
		});
	});

	describe('metrics', () => {
		it('counts loads, calls, and errors', async () => {
			const { createMetrics } = await import('../../prometheus/index.js');
			const metrics = createMetrics();
			const lib = createFunctionLibrary(client, SAMPLE_LIB, { metrics });

			client._registerFunction('cleanup', () => 0);
			client._registerFunction('count_live', () => { throw new Error('boom'); });

			await lib.load();
			await lib.call('cleanup', { keys: ['k1'] });
			await expect(lib.call('count_live')).rejects.toThrow();

			const out = metrics.serialize();
			expect(out).toMatch(/redis_function_loads_total\{library="ws-presence"\} 1/);
			expect(out).toMatch(/redis_function_calls_total\{function="cleanup",library="ws-presence"\} 1/);
			expect(out).toMatch(/redis_function_errors_total\{function="count_live",library="ws-presence"\} 1/);
		});
	});
});
