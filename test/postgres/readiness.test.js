import { spawnSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

describe.each([
	{ name: 'tasks', factory: 'createTaskRunner', options: { recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0 } },
	{ name: 'idempotency', factory: 'createIdempotencyStore', options: { cleanupInterval: 0 } }
])('postgres $name initialization', ({ name, factory, options }) => {
	it('preserves a failed migration for a later ready() caller without terminating the process', () => {
		const moduleUrl = new URL('../../postgres/' + name + '.js', import.meta.url).href;
		const source = `
			import assert from 'node:assert/strict';
			import { ${factory} } from ${JSON.stringify(moduleUrl)};
			const failure = new Error('Database initialization unavailable');
			const client = { query: async () => { throw failure; } };
			const resource = ${factory}(client, ${JSON.stringify(options)});
			try {
				// Let Node process unhandled rejections before the caller observes ready().
				await new Promise((resolve) => setImmediate(resolve));
				const ready = resource.ready();
				assert.equal(resource.ready(), ready);
				await assert.rejects(ready, (error) => error === failure);
			} finally {
				resource.destroy();
			}
		`;
		// A child keeps an unhandled rejection from contaminating the test runner.
		const result = spawnSync(process.execPath, ['--unhandled-rejections=strict', '--input-type=module'], {
			input: source,
			encoding: 'utf8',
			timeout: 10000,
			windowsHide: true
		});
		expect(result.error).toBeUndefined();
		expect(result.signal, result.stderr).toBeNull();
		expect(result.status, result.stderr).toBe(0);
	});
});
