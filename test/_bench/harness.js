// Local A/B microbenchmark harness for /simplify perf changes.
// NOT shipped (excluded from package via the package.json files allowlist).
// Run with: node --expose-gc test/_bench/<file>.js

import { performance } from 'node:perf_hooks';

export function bench(name, fn, opts = {}) {
	const warmup = opts.warmup ?? 1000;
	const iters = opts.iters ?? 100000;
	for (let i = 0; i < warmup; i++) fn();
	if (typeof globalThis.gc === 'function') globalThis.gc();
	const start = performance.now();
	for (let i = 0; i < iters; i++) fn();
	const elapsed = performance.now() - start;
	const nsPerOp = (elapsed * 1e6) / iters;
	console.log(`${name.padEnd(40)} ${nsPerOp.toFixed(2)} ns/op  (${iters} iters, ${elapsed.toFixed(1)} ms total)`);
	return { nsPerOp, elapsed, iters };
}

export async function benchAsync(name, fn, opts = {}) {
	const warmup = opts.warmup ?? 100;
	const iters = opts.iters ?? 10000;
	for (let i = 0; i < warmup; i++) await fn();
	if (typeof globalThis.gc === 'function') globalThis.gc();
	const start = performance.now();
	for (let i = 0; i < iters; i++) await fn();
	const elapsed = performance.now() - start;
	const nsPerOp = (elapsed * 1e6) / iters;
	console.log(`${name.padEnd(40)} ${nsPerOp.toFixed(2)} ns/op  (${iters} iters, ${elapsed.toFixed(1)} ms total)`);
	return { nsPerOp, elapsed, iters };
}

export function compare(label, baseline, candidate) {
	const delta = ((candidate.nsPerOp - baseline.nsPerOp) / baseline.nsPerOp) * 100;
	const sign = delta > 0 ? '+' : '';
	const verdict = delta < -2 ? 'WIN' : delta > 2 ? 'LOSS' : 'NEUTRAL';
	console.log(`${label}: ${sign}${delta.toFixed(1)}% [${verdict}]`);
	return { delta, verdict };
}
