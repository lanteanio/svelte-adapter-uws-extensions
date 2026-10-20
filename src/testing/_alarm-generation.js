import { MAX_ALARM_GENERATION } from '../shared/alarm-generation.js';

// Synchronous evaluator for the ordered-alarm script. No await may separate its
// reads and writes: callers must observe the same atomicity as native EVAL.
export function evaluateAlarmGeneration(args, { sortedSets, hashes }) {
	const [dueKey, metaKey, reservedKey, committedKey, currentKey, attemptKey, executionsKey, orderKey, operation, ...argv] = args;
	const get = (key, topic) => hashes.get(key)?.get(topic);
	const put = (key, topic, value) => {
		let hash = hashes.get(key);
		if (!hash) hashes.set(key, hash = new Map());
		hash.set(topic, value);
	};
	const remove = (key, field) => hashes.get(key)?.delete(field);
	const less = (a, b) => a.length < b.length || (a.length === b.length && a < b);
	const valid = (value, zero = false) => (zero && value === '0')
		|| (typeof value === 'string' && /^[1-9][0-9]*$/.test(value) && !less(MAX_ALARM_GENERATION, value));
	const corrupt = () => { throw new Error('redis alarm store: inconsistent generation authority'); };
	const inconsistent = () => { throw new Error('redis alarm store: inconsistent execution record'); };
	const finite = text => { const at = Number(text); if (!Number.isFinite(at)) throw new Error('redis alarm store: at must be finite'); return at; };
	// Redis orders ZRANGEBYLEX members by their bytes.
	const bytes = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
	const id = (topic, generation) => topic.replaceAll('\0', '\0\x01') + '\0\0' + generation;
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
	function execution(topic, generation) {
		const raw = get(executionsKey, id(topic, generation));
		if (raw === undefined) return null;
		let record;
		try { record = JSON.parse(raw); } catch { inconsistent(); }
		if (!record || typeof record !== 'object' || record.topic !== topic || record.generation !== generation || typeof record.attempt !== 'string') inconsistent();
		return record;
	}
	function attempt(row) {
		const value = get(attemptKey, row.topic);
		if (value === undefined) return '1';
		const match = /^([1-9][0-9]*):([1-9][0-9]*)$/.exec(value);
		if (!match) inconsistent();
		return match[1] === row.current ? match[2] : '1';
	}
	const unschedule = topic => sortedSets.set(dueKey, (sortedSets.get(dueKey) ?? []).filter(entry => entry.member !== topic));
	function schedule(topic, score) {
		const entries = (sortedSets.get(dueKey) ?? []).filter(entry => entry.member !== topic);
		entries.push({ member: topic, score });
		entries.sort((a, b) => a.score - b.score || (a.member < b.member ? -1 : a.member > b.member ? 1 : 0));
		sortedSets.set(dueKey, entries);
	}
	const order = () => [...(sortedSets.get(orderKey) ?? [])].sort((a, b) => bytes(a.member, b.member));
	function unretain(key) {
		remove(executionsKey, key);
		sortedSets.set(orderKey, (sortedSets.get(orderKey) ?? []).filter(entry => entry.member !== key));
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
		sortedSets.delete(dueKey); hashes.delete(metaKey); hashes.delete(currentKey); hashes.delete(attemptKey);
		return 1;
	}
	if (operation === 'due') {
		const rows = (sortedSets.get(dueKey) ?? []).filter(row => row.score <= Number(argv[0]))
			.slice(0, Number(argv[1])).map(row => read(row.member));
		return rows.map(row => { adopt(row); return result(row); });
	}
	if (operation === 'executions') {
		const [limit, topic, generation] = argv;
		const after = topic === undefined ? null : id(topic, generation);
		const ids = order().map(entry => entry.member).filter(member => after === null || bytes(member, after) > 0).slice(0, Number(limit) + 1);
		return ids.map(member => get(executionsKey, member) ?? inconsistent());
	}
	if (operation === 'execution') return get(executionsKey, id(argv[0], argv[1])) ?? null;
	if (operation === 'complete') {
		const record = execution(argv[0], argv[1]);
		if (!record || record.attempt !== argv[2]) return 0;
		unretain(id(argv[0], argv[1]));
		return 1;
	}
	if (operation === 'fail') {
		const record = execution(argv[0], argv[1]);
		if (!record || record.attempt !== argv[2]) return 0;
		if (record.status !== 'failed') {
			Object.assign(record, { status: 'failed', failedAt: argv[3], errorClass: 'handler' });
			put(executionsKey, id(argv[0], argv[1]), JSON.stringify(record));
		}
		return 1;
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
		const at = finite(argv[2]);
		put(metaKey, row.topic, argv[3]);
		schedule(row.topic, at);
		put(committedKey, row.topic, generation); put(currentKey, row.topic, generation);
		remove(attemptKey, row.topic);
		return 1;
	}
	if (operation === 'delete') {
		if (row.current !== argv[1]) return 0;
		adopt(row);
		unschedule(row.topic);
		remove(metaKey, row.topic); remove(currentKey, row.topic); remove(attemptKey, row.topic);
		return 1;
	}
	if (operation === 'claim') {
		const [, generation, claimedAt, capacity] = argv;
		if (!row.entry || row.current !== generation) return null;
		const key = id(row.topic, generation);
		if (get(executionsKey, key) !== undefined) inconsistent();
		if ((sortedSets.get(orderKey) ?? []).length >= Number(capacity)) return -1;
		const count = attempt(row);
		adopt(row);
		put(executionsKey, key, JSON.stringify({ topic: row.topic, generation, score: String(row.entry.score), payload: row.payload ?? '', attempt: count, status: 'claimed', claimedAt }));
		sortedSets.set(orderKey, [...(sortedSets.get(orderKey) ?? []), { member: key, score: 0 }]);
		unschedule(row.topic);
		remove(metaKey, row.topic); remove(currentKey, row.topic); remove(attemptKey, row.topic);
		return count;
	}
	if (operation === 'requeue') {
		const [, generation, expected, atText, next, allowAbandoned] = argv;
		const record = execution(row.topic, generation);
		if (!record || record.attempt !== expected) return 0;
		if (record.status !== 'failed' && allowAbandoned !== '1') return 0;
		if (row.entry || row.reserved !== generation || row.committed !== generation) return 0;
		const at = finite(atText);
		if (record.payload !== '') {
			let payload = record.payload;
			try {
				const decoded = JSON.parse(payload);
				if (decoded && typeof decoded === 'object') payload = '{"at":' + atText + ',"meta":' + JSON.stringify(decoded.meta ?? null) + '}';
			} catch { /* keep unreadable metadata bytes */ }
			put(metaKey, row.topic, payload);
		}
		schedule(row.topic, at);
		put(currentKey, row.topic, generation);
		put(attemptKey, row.topic, generation + ':' + next);
		unretain(id(row.topic, generation));
		return 1;
	}
	throw new Error('redis alarm store: unknown operation');
}
