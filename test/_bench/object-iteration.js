// Bench: parsing entries from a hgetall result
//   A) for-of Object.entries(all)
//   B) for-of Object.keys(all) + indexed access

import { bench, compare } from './harness.js';

function buildHash(n) {
	const h = {};
	for (let i = 0; i < n; i++) {
		h['inst-' + i + '|user-' + i] = JSON.stringify({ data: { name: 'u' + i }, ts: Date.now() });
	}
	return h;
}

const small = buildHash(10);
const medium = buildHash(100);
const large = buildHash(1000);

function withEntries(all) {
	let count = 0;
	for (const [field, v] of Object.entries(all)) {
		try {
			const parsed = JSON.parse(v);
			if (parsed.ts && field.length > 0) count++;
		} catch { /* skip */ }
	}
	return count;
}

function withKeys(all) {
	let count = 0;
	for (const field of Object.keys(all)) {
		try {
			const parsed = JSON.parse(all[field]);
			if (parsed.ts && field.length > 0) count++;
		} catch { /* skip */ }
	}
	return count;
}

console.log('Small (10 entries)');
const a1 = bench('Object.entries', () => withEntries(small), { iters: 100000 });
const b1 = bench('Object.keys',    () => withKeys(small), { iters: 100000 });
compare('keys vs entries', a1, b1);

console.log('\nMedium (100 entries)');
const a2 = bench('Object.entries', () => withEntries(medium), { iters: 10000 });
const b2 = bench('Object.keys',    () => withKeys(medium), { iters: 10000 });
compare('keys vs entries', a2, b2);

console.log('\nLarge (1000 entries)');
const a3 = bench('Object.entries', () => withEntries(large), { iters: 1000 });
const b3 = bench('Object.keys',    () => withKeys(large), { iters: 1000 });
compare('keys vs entries', a3, b3);
