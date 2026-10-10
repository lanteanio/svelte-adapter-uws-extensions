import { spawnSync } from 'node:child_process';
import { expect, it } from 'vitest';

it('does not retain discarded pools through a shared metrics collector', () => {
	const clientUrl = new URL('../../src/postgres/index.js', import.meta.url).href;
	const metricsUrl = new URL('../../src/prometheus/index.js', import.meta.url).href;
	const source = `
		import { createPgClient } from ${JSON.stringify(clientUrl)};
		import { createMetrics } from ${JSON.stringify(metricsUrl)};
		import assert from 'node:assert/strict';
		const NativeWeakRef = WeakRef, handles = [];
		globalThis.WeakRef = class extends NativeWeakRef {
			constructor(target) { super(target); if (target.metricTestPool) handles.push(new NativeWeakRef(this)); }
		};
		const metrics = createMetrics();
		let weak;
		(function register() {
			const pool = { metricTestPool: true, query: () => Promise.resolve({ rows: [] }), waitingCount: 3 };
			weak = new NativeWeakRef(pool);
			createPgClient({ pool, metrics });
			createPgClient({ pool, metrics: { ...metrics } });
		})();
		for (let i = 0; i < 10; i++) {
			await new Promise(resolve => setImmediate(resolve));
			global.gc();
		}
		assert.equal(weak.deref(), undefined, 'metrics collector retained the pool');
		assert.ok(handles.length > 0);
		assert.ok(handles.every(ref => ref.deref() === undefined), 'unscraped metrics retained registration handles');
		assert.match(metrics.serialize(), /postgres_pool_waiters 0/);
	`;
	const result = spawnSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', source], { encoding: 'utf8', timeout: 10000 });
	expect(result.error).toBeUndefined();
	expect(result.status, result.stderr).toBe(0);
});
