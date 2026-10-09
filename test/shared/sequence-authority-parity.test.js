// src/shared/sequence-authority.js copies the adapter's hasMultipleWorkers
// (src/runtime/handler/cluster-sequence-policy.js), the predicate behind its
// rule that a multi-worker runtime refuses a publish drawing the per-worker
// counter. The modules here choose their publish options from the same
// reading, so a predicate that changes on one side only would have them
// declare too little (every publish refused) or too much (frames that lose
// their seq on a runtime that would have stamped them).
//
// The first test compares the two functions as text, so any change on the
// adapter's side fails here; copy it over ours again when it does. The second
// starts real worker threads with the workerData shapes the question turns on
// and asks both modules, so the copy is also checked where it is read.

import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

const adapterUrl = new URL('../../node_modules/svelte-adapter-uws/src/runtime/handler/cluster-sequence-policy.js', import.meta.url);
const oursUrl = new URL('../../src/shared/sequence-authority.js', import.meta.url);

/** The hasMultipleWorkers declaration, from its JSDoc line to its closing brace. */
function predicate(url) {
	const source = readFileSync(url, 'utf8').replace(/\r\n/g, '\n');
	const match = source.match(/\/\*\* @param \{any\} \[data\] \*\/\nexport function hasMultipleWorkers\(data = workerData\) \{\n[\s\S]*?\n\}\n/);
	expect(match, `hasMultipleWorkers not found in ${url.pathname}`).not.toBeNull();
	return match[0];
}

/** Run both modules in a worker thread started with this workerData. */
function readInWorker(workerData) {
	const code = `
		const { parentPort } = require('node:worker_threads');
		Promise.all([import(${JSON.stringify(oursUrl.href)}), import(${JSON.stringify(adapterUrl.href)})]).then(([ours, adapter]) => {
			parentPort.postMessage({
				ours: ours.MULTI_WORKER_RUNTIME,
				adapter: adapter.hasMultipleWorkers(),
				undeclaredAccepted: adapter.clusterSequenceAccepted(undefined)
			});
		}, (err) => parentPort.postMessage({ error: String(err && err.stack || err) }));
	`;
	return new Promise((resolve, reject) => {
		const worker = new Worker(code, { eval: true, workerData });
		worker.once('message', (msg) => { worker.terminate(); resolve(msg); });
		worker.once('error', reject);
	});
}

describe('the multi-worker predicate is the adapter\'s', () => {
	it('matches it line for line', () => {
		expect(predicate(oursUrl)).toBe(predicate(adapterUrl));
	});

	it.each([
		[{ totalWorkers: 2 }, true],
		[{ totalWorkers: 8, role: 'io' }, true],
		[{ totalWorkers: 1 }, false],
		[{ totalWorkers: '2' }, false],
		[{ totalWorkers: 2.5 }, false],
		[{}, false],
		[null, false]
	])('reads workerData %j the way the adapter does', async (workerData, multi) => {
		const result = await readInWorker(workerData);
		expect(result.error).toBeUndefined();
		expect(result).toEqual({ ours: multi, adapter: multi, undeclaredAccepted: !multi });
	});
});
