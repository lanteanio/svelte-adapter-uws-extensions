import { MAX_ALARM_GENERATION } from '../shared/alarm-generation.js';

// Synchronous evaluator for the ordered-alarm script. No await may separate its
// reads and writes: callers must observe the same atomicity as native EVAL.
export function evaluateAlarmGeneration(args, { sortedSets, hashes }) {
	const [dueKey, metaKey, reservedKey, committedKey, currentKey, operation, ...argv] = args;
	const get = (key, topic) => hashes.get(key)?.get(topic);
	const put = (key, topic, value) => {
		let hash = hashes.get(key);
		if (!hash) hashes.set(key, hash = new Map());
		hash.set(topic, value);
	};
	const less = (a, b) => a.length < b.length || (a.length === b.length && a < b);
	const valid = (value, zero = false) => (zero && value === '0')
		|| (typeof value === 'string' && /^[1-9][0-9]*$/.test(value) && !less(MAX_ALARM_GENERATION, value));
	const corrupt = () => { throw new Error('redis alarm store: inconsistent generation authority'); };
	function read(topic) {
		const entry = sortedSets.get(dueKey)?.find(row => row.member === topic);
		const payload = get(metaKey, topic);
		let reserved = get(reservedKey, topic), committed = get(committedKey, topic), current = get(currentKey, topic);
		let legacy = false;
		if (reserved === undefined) {
			if (committed !== undefined || current !== undefined) corrupt();
			reserved = committed = '0';
			if (entry) { reserved = committed = current = '1'; legacy = true; }
		} else if (!valid(reserved) || !valid(committed, true) || less(reserved, committed)) corrupt();
		if (entry) {
			if (!valid(current) || current !== committed) corrupt();
		} else if (current !== undefined) corrupt();
		return { topic, entry, payload, reserved, committed, current, legacy };
	}
	function adopt(row) {
		if (!row.legacy) return;
		for (const key of [reservedKey, committedKey, currentKey]) put(key, row.topic, '1');
	}
	const result = row => row.entry ? [row.topic, String(row.entry.score), row.payload ?? '', row.current] : [];
	if (operation === 'clear') {
		const topics = new Set((sortedSets.get(dueKey) ?? []).map(row => row.member));
		for (const key of [metaKey, reservedKey, committedKey, currentKey]) {
			for (const topic of hashes.get(key)?.keys() ?? []) topics.add(topic);
		}
		const rows = [...topics].map(read);
		for (const row of rows) {
			adopt(row);
			if (row.reserved !== '0') put(committedKey, row.topic, row.reserved);
		}
		sortedSets.delete(dueKey); hashes.delete(metaKey); hashes.delete(currentKey);
		return 1;
	}
	if (operation === 'due') {
		const rows = (sortedSets.get(dueKey) ?? []).filter(row => row.score <= Number(argv[0]))
			.slice(0, Number(argv[1])).map(row => read(row.member));
		return rows.map(row => { adopt(row); return result(row); });
	}
	const row = read(argv[0]);
	if (operation === 'next') {
		if (row.reserved === MAX_ALARM_GENERATION) throw new Error('redis alarm store: generation allocator exhausted');
		adopt(row);
		if (row.reserved === '0') put(committedKey, row.topic, '0');
		const next = (BigInt(row.reserved) + 1n).toString();
		put(reservedKey, row.topic, next);
		return next;
	}
	if (operation === 'get') { adopt(row); return result(row); }
	if (operation === 'set') {
		const generation = argv[1];
		if (!valid(generation)) throw new Error('redis alarm store: invalid generation');
		if (generation !== row.reserved || !less(row.committed, generation)) return 0;
		const at = Number(argv[2]);
		if (!Number.isFinite(at)) throw new Error('redis alarm store: at must be finite');
		put(metaKey, row.topic, argv[3]);
		const entries = (sortedSets.get(dueKey) ?? []).filter(entry => entry.member !== row.topic);
		entries.push({ member: row.topic, score: at });
		entries.sort((a, b) => a.score - b.score || (a.member < b.member ? -1 : a.member > b.member ? 1 : 0));
		sortedSets.set(dueKey, entries);
		put(committedKey, row.topic, generation); put(currentKey, row.topic, generation);
		return 1;
	}
	if (operation === 'delete') {
		if (row.current !== argv[1]) return 0;
		adopt(row);
		sortedSets.set(dueKey, (sortedSets.get(dueKey) ?? []).filter(entry => entry.member !== row.topic));
		hashes.get(metaKey)?.delete(row.topic); hashes.get(currentKey)?.delete(row.topic);
		return 1;
	}
	throw new Error('redis alarm store: unknown operation');
}
