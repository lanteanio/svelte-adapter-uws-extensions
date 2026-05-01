import { bench, compare } from './harness.js';

const sample = { id: 'user-12345', name: 'Alice Example', avatar: 'https://cdn.example.com/avatar/abc.png', plan: 'pro' };

function joinCurrent(data) {
	try { JSON.stringify(data); } catch { throw new Error('not serializable'); }
	const now = 1700000000000;
	const value = JSON.stringify({ data, ts: now });
	const serialized = JSON.stringify(data);
	return { value, serialized };
}

function joinReuse(data) {
	let serialized;
	try { serialized = JSON.stringify(data); } catch { throw new Error('not serializable'); }
	const now = 1700000000000;
	const value = JSON.stringify({ data, ts: now });
	return { value, serialized };
}

const a = bench('current (validate + stringify(data) twice)', () => joinCurrent(sample));
const b = bench('reuse  (validate yields serialized once)', () => joinReuse(sample));
compare('reuse vs current', a, b);
