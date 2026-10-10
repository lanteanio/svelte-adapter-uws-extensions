import { afterEach, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { WS_SUBSCRIPTIONS } from '../../src/shared/ws-subscriptions.js';

afterEach(() => vi.useRealTimers());

for (const scenario of [
	{ name: 'read-only duplicate preserves a later peer identity', local: true, selected: 'Old', peer: 'New', expected: 'New' },
	{ name: 'failed identity write preserves a later peer identity', local: true, selected: 'Failed', peer: 'New', expected: 'New' },
	{ name: 'failed identity write restores the prior identity', local: true, selected: 'Failed', expected: 'Old' },
	{ name: 'first local join restores the prior peer identity', local: false, selected: 'Failed', expected: 'Old' },
	{ name: 'identity rollback retains later durable fields', local: true, selected: 'Failed', fields: true, expected: 'Old' },
	{ name: 'rollback leaves a later legacy peer write intact', local: true, selected: 'Failed', legacy: true, expected: 'Legacy' }
]) it(scenario.name, async () => {
	vi.useFakeTimers();
	const client = mockRedisClient();
	const options = { binary: false, heartbeat: 100, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform();
	const anchor = mockWs({ id: 'alice', name: 'Old' }), peer = mockWs({ id: 'alice', name: 'Old' });
	const hgetall = client.redis.hgetall.bind(client.redis);
	try {
		await b.join(peer, 'room', pb);
		if (scenario.local) await a.join(anchor, 'room', pa);
		await b.update(peer, 'room', { color: 'blue' }, pb);
		let release, sawRead;
		const read = new Promise(resolve => { sawRead = resolve; });
		let hold = true;
		client.redis.hgetall = key => {
			if (hold && key === client.key('presence:topic:{room}')) {
				hold = false; sawRead();
				return new Promise((resolve, reject) => { release = () => reject(new Error('snapshot unavailable')); });
			}
			return hgetall(key);
		};
		const pending = a.join(mockWs({ id: 'alice', name: scenario.selected }), 'room', pa).catch(error => error);
		await read;
		if (scenario.peer) await b.join(mockWs({ id: 'alice', name: scenario.peer }), 'room', pb);
		if (scenario.fields) await b.update(peer, 'room', { color: 'red' }, pb);
		if (scenario.legacy) {
			const hash = client.key('presence:topic:{room}');
			const stored = JSON.parse(await client.redis.hget(hash, 'alice'));
			stored.data.name = 'Legacy';
			await client.redis.hset(hash, 'alice', JSON.stringify({ data: stored.data, ts: stored.ts, fields: stored.fields }));
		}

		release();
		expect(await pending).toMatchObject({ message: 'snapshot unavailable' });
		client.redis.hgetall = hgetall;
		const expected = { id: 'alice', name: scenario.expected, color: scenario.fields ? 'red' : 'blue' };
		expect(await a.list('room')).toEqual([expected]);
		expect(await b.list('room')).toEqual([expected]);
		expect(await b.count('room')).toBe(1);
		if (scenario.local) {
			pa.reset();
			await vi.advanceTimersByTimeAsync(100);
			expect(pa.published.find(frame => frame.event === 'heartbeat').data.alice).toEqual(expected);
			await a.leave(anchor, pa, 'room');
		}
		expect(await client.redis.hlen(client.key('presence:user:{room}:alice'))).toBe(1);
	} finally { client.redis.hgetall = hgetall; a.destroy(); b.destroy(); }
});

for (const [hasAnchor, replacementName] of [[false, 'Replacement'], [true, 'Replacement'], [false, 'Old'], [true, 'Old']]) it('retains a newer local membership while compensation is pending, anchor=' + hasAnchor + ', replacement=' + replacementName, async () => {
	const client = mockRedisClient();
	const options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform();
	const anchor = mockWs({ id: 'alice', name: 'Old' });
	const replacement = mockWs({ id: 'alice', name: replacementName });
	const hgetall = client.redis.hgetall.bind(client.redis), evaluate = client.redis.eval.bind(client.redis);
	try {
		await b.join(mockWs({ id: 'alice', name: 'Old' }), 'room', pb);
		if (hasAnchor) await a.join(anchor, 'room', pa);
		let fail = true, replace = true;
		client.redis.hgetall = key => {
			if (fail && key === client.key('presence:topic:{room}')) { fail = false; return Promise.reject(new Error('snapshot unavailable')); }
			return hgetall(key);
		};
		client.redis.eval = async (script, ...args) => {
			if (replace && script.includes('PRESENCE_ROLLBACK')) {
				replace = false;
				await a.join(replacement, 'room', pa);
			}
			return evaluate(script, ...args);
		};
		await expect(a.join(mockWs({ id: 'alice', name: 'Failed' }), 'room', pa)).rejects.toThrow('snapshot unavailable');
		expect(replace).toBe(false);
		expect(await a.list('room')).toEqual([{ id: 'alice', name: replacementName }]);
		if (hasAnchor) await a.leave(anchor, pa, 'room');
		expect(await client.redis.hlen(client.key('presence:user:{room}:alice'))).toBe(2);
		await a.leave(replacement, pa, 'room');
		expect(await client.redis.hlen(client.key('presence:user:{room}:alice'))).toBe(1);
	} finally { client.redis.hgetall = hgetall; client.redis.eval = evaluate; a.destroy(); b.destroy(); }
});

for (const peerWins of [false, true]) it('recovers a completed JOIN whose reply was lost, newer peer=' + peerWins, async () => {
	const client = mockRedisClient();
	const options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform();
	const evaluate = client.redis.eval.bind(client.redis);
	try {
		const peer = mockWs({ id: 'alice', name: 'Old' });
		await b.join(peer, 'room', pb);
		await b.update(peer, 'room', { color: 'blue' }, pb);
		let lose = true;
		client.redis.eval = async (script, ...args) => {
			const result = await evaluate(script, ...args);
			if (lose && script.includes('PRESENCE_JOIN')) {
				lose = false;
				expect(JSON.parse(await client.redis.hget(client.key('presence:topic:{room}'), 'alice')).data.name).toBe('Old');
				if (peerWins) await b.join(mockWs({ id: 'alice', name: 'New' }), 'room', pb);
				else await b.update(peer, 'room', { color: 'red' }, pb);
				throw new Error('JOIN reply lost');
			}
			return result;
		};
		await expect(a.join(mockWs({ id: 'alice', name: 'Failed' }), 'room', pa)).rejects.toThrow('JOIN reply lost');
		expect(await b.list('room')).toEqual([{ id: 'alice', name: peerWins ? 'New' : 'Old', color: peerWins ? 'blue' : 'red' }]);
		expect(await client.redis.hlen(client.key('presence:user:{room}:alice'))).toBe(1);

	} finally { client.redis.eval = evaluate; a.destroy(); b.destroy(); }
});

it('does not remove a replacement reservation when select reuses its object', async () => {
	const client = mockRedisClient(), selected = { id: 'alice', name: 'Old' };
	const presence = createPresence(client, { binary: false, select: () => selected });
	const platform = mockPlatform(), ws = mockWs({ id: 'alice' });
	const evaluate = client.redis.eval.bind(client.redis);
	try {
		let release, sawWrite;
		const write = new Promise(resolve => { sawWrite = resolve; });
		let hold = true;
		client.redis.eval = (script, ...args) => {
			if (hold && script.includes('PRESENCE_JOIN')) {
				hold = false; sawWrite();
				return new Promise((resolve, reject) => { release = () => reject(new Error('write unavailable')); });
			}
			return evaluate(script, ...args);
		};
		const pending = presence.join(ws, 'room', platform).catch(error => error);
		await write;
		await presence.leave(ws, platform, 'room');
		await presence.join(ws, 'room', platform);
		release();
		expect(await pending).toMatchObject({ message: 'write unavailable' });
		expect(await presence.list('room')).toEqual([selected]);
		expect(await client.redis.hlen(client.key('presence:user:{room}:alice'))).toBe(1);
		await presence.leave(ws, platform, 'room');
		expect(await client.redis.hlen(client.key('presence:user:{room}:alice'))).toBe(0);
	} finally { client.redis.eval = evaluate; presence.destroy(); }
});

for (const newerFirst of [false, true]) it('skips failed identity ancestors when overlapping joins both fail, newer first=' + newerFirst, async () => {
	const client = mockRedisClient();
	const presence = createPresence(client, { binary: false, select: u => ({ id: u.id, name: u.name }) });
	const platform = mockPlatform(), anchor = mockWs({ id: 'alice', name: 'Stable' });
	const hgetall = client.redis.hgetall.bind(client.redis), evaluate = client.redis.eval.bind(client.redis);
	try {
		await presence.join(anchor, 'room', platform);
		let release, sawRead, newerWritten, finishFirst;
		const read = new Promise(resolve => { sawRead = resolve; });
		const wroteNewer = new Promise(resolve => { newerWritten = resolve; });
		const firstDone = new Promise(resolve => { finishFirst = resolve; });
		const tokens = new Map();
		let hold = true;
		client.redis.hgetall = key => {
			if (hold && key === client.key('presence:topic:{room}')) {
				hold = false; sawRead();
				return new Promise((resolve, reject) => { release = () => reject(new Error('shared snapshot failed')); });
			}
			return hgetall(key);
		};
		client.redis.eval = async (script, ...args) => {
			if (script.includes('PRESENCE_ROLLBACK')) {
				const isNewer = tokens.get(args[6]) === 'FailedB';
				if (isNewer !== newerFirst) await firstDone;
				const result = await evaluate(script, ...args);
				if (isNewer === newerFirst) finishFirst();
				return result;
			}
			const result = await evaluate(script, ...args);
			if (script.includes('PRESENCE_JOIN')) {
				const name = JSON.parse(args[6]).data.name;
				tokens.set(args[9], name);
				if (name === 'FailedB') newerWritten();
			}
			return result;
		};
		const a = presence.join(mockWs({ id: 'alice', name: 'FailedA' }), 'room', platform).catch(error => error);
		await read;
		const b = presence.join(mockWs({ id: 'alice', name: 'FailedB' }), 'room', platform).catch(error => error);
		await wroteNewer;
		await Promise.resolve(); await Promise.resolve();
		release();
		for (const result of await Promise.all([a, b])) expect(result.message).toBe('shared snapshot failed');
		expect(await presence.list('room')).toEqual([{ id: 'alice', name: 'Stable' }]);
		await presence.leave(anchor, platform, 'room');
		expect(await client.redis.exists(client.key('presence:topic:{room}'))).toBe(0);
	} finally { client.redis.hgetall = hgetall; client.redis.eval = evaluate; presence.destroy(); }
});

for (const order of ['rollback-before-finalize', 'finalize-before-rollback', 'newer-commits-first', 'fresh-incarnation']) it('bounds first-user identity finalization: ' + order, async () => {
	const client = mockRedisClient();
	const options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform(), wsA = mockWs({ id: 'alice', name: 'A' });
	const evaluate = client.redis.eval.bind(client.redis), hgetall = client.redis.hgetall.bind(client.redis);
	let releaseFinalize;
	try {
		let sawFinalize;
		const finalizing = new Promise(resolve => { sawFinalize = resolve; });
		const release = new Promise(resolve => { releaseFinalize = resolve; });
		let hold = true;
		client.redis.eval = async (script, ...args) => {
			if (hold && script.includes('PRESENCE_FINALIZE')) { hold = false; sawFinalize(); await release; }
			return evaluate(script, ...args);
		};
		const joiningA = a.join(wsA, 'room', pa).catch(error => error);
		await finalizing;
		let joiningB;
		if (order === 'newer-commits-first' || order === 'fresh-incarnation') {
			if (order === 'fresh-incarnation') await a.leave(wsA, pa, 'room');
			await b.join(mockWs({ id: 'alice', name: 'B' }), 'room', pb);
			releaseFinalize();
		} else {
			let failRead, sawRead;
			const read = new Promise(resolve => { sawRead = resolve; });
			let fail = true;
			client.redis.hgetall = key => {
				if (fail && key === client.key('presence:topic:{room}')) {
					fail = false; sawRead();
					return new Promise((resolve, reject) => { failRead = () => reject(new Error('B snapshot failed')); });
				}
				return hgetall(key);
			};
			joiningB = b.join(mockWs({ id: 'alice', name: 'B' }), 'room', pb).catch(error => error);
			await read;
			if (order === 'rollback-before-finalize') {
				failRead(); await joiningB;
				expect(await b.list('room')).toEqual([]);
				expect(await b.count('room')).toBe(0);
				releaseFinalize();
			} else {
				releaseFinalize(); await joiningA;
				await a.update(wsA, 'room', { color: 'red' }, pa);
				failRead();
			}
		}
		const resultA = await joiningA;
		if (joiningB) expect((await joiningB).message).toBe('B snapshot failed');
		if (order === 'fresh-incarnation') expect(resultA).toBeInstanceOf(Error);
		else expect(resultA).toBeUndefined();
		const expected = { id: 'alice', name: order === 'newer-commits-first' || order === 'fresh-incarnation' ? 'B' : 'A' };
		if (order === 'finalize-before-rollback') expected.color = 'red';
		expect(await b.list('room')).toEqual([expected]);
		expect(await b.count('room')).toBe(1);
		const raw = JSON.parse(await client.redis.hget(client.key('presence:topic:{room}'), 'alice'));
		expect(raw.identityPending).toBeUndefined();
		expect(raw.identityBaseline).toBeUndefined();
	} finally { releaseFinalize?.(); client.redis.eval = evaluate; client.redis.hgetall = hgetall; a.destroy(); b.destroy(); }
});

for (const failures of [1, 2]) it('never rolls back a committed identity after lost finalize replies: ' + failures, async () => {
	const client = mockRedisClient();
	const options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options);
	const pa = mockPlatform(), pb = mockPlatform(), evaluate = client.redis.eval.bind(client.redis);
	try {
		await b.join(mockWs({ id: 'alice', name: 'Old' }), 'room', pb);
		let attempts = 0;
		client.redis.eval = async (script, ...args) => {
			const result = await evaluate(script, ...args);
			if (script.includes('PRESENCE_FINALIZE') && ++attempts <= failures) throw new Error('finalize reply lost');
			return result;
		};
		const result = await a.join(mockWs({ id: 'alice', name: 'New' }), 'room', pa).catch(error => error);
		if (failures === 1) expect(result).toBeUndefined();
		else expect(result.message).toBe('finalize reply lost');
		expect(attempts).toBe(2);
		expect(await b.list('room')).toEqual([{ id: 'alice', name: 'New' }]);
	} finally { client.redis.eval = evaluate; a.destroy(); b.destroy(); }
});

for (const survivingTab of [false, true]) it('does not recreate local state after closing during finalization, surviving tab=' + survivingTab, async () => {
	vi.useFakeTimers();
	const client = mockRedisClient(), presence = createPresence(client, { binary: false, heartbeat: 100, select: u => ({ id: u.id, name: u.name }) });
	const platform = mockPlatform(), ws = mockWs({ id: 'alice', name: 'New' });
	const evaluate = client.redis.eval.bind(client.redis);
	try {
		if (survivingTab) await presence.join(mockWs({ id: 'alice', name: 'Old' }), 'room', platform);
		platform.reset();
		let release, finalized;
		const done = new Promise(resolve => { finalized = resolve; });
		let hold = true;
		client.redis.eval = async (script, ...args) => {
			const result = await evaluate(script, ...args);
			if (hold && script.includes('PRESENCE_FINALIZE')) {
				hold = false; finalized();
				await new Promise(resolve => { release = resolve; });
			}
			return result;
		};
		const pending = presence.join(ws, 'room', platform).catch(error => error);
		await done;
		await presence.leave(ws, platform, 'room');
		release();
		expect(await pending).toBeInstanceOf(Error);
		expect(platform.sent.filter(frame => frame.event === 'state')).toHaveLength(0);
		await vi.advanceTimersByTimeAsync(100);
		if (survivingTab) expect(platform.published.find(frame => frame.event === 'heartbeat').data.alice.name).toBe('New');
		else {
			expect(platform.published.filter(frame => frame.event === 'heartbeat')).toHaveLength(0);
			expect(await presence.list('room')).toEqual([]);
		}
	} finally { client.redis.eval = evaluate; presence.destroy(); }
});

it('finalizes a first concurrent same-instance join before the older join fails', async () => {
	const client = mockRedisClient(), presence = createPresence(client, { binary: false, select: u => ({ id: u.id, name: u.name }) });
	const platform = mockPlatform(), evaluate = client.redis.eval.bind(client.redis);
	try {
		let release, finalizing;
		const done = new Promise(resolve => { finalizing = resolve; });
		let hold = true;
		client.redis.eval = async (script, ...args) => {
			if (hold && script.includes('PRESENCE_FINALIZE')) {
				hold = false; finalizing();
				await new Promise(resolve => { release = resolve; });
				throw new Error('old finalize failed');
			}
			return evaluate(script, ...args);
		};
		const pending = presence.join(mockWs({ id: 'alice', name: 'A' }), 'room', platform).catch(error => error);
		await done;
		await presence.join(mockWs({ id: 'alice', name: 'B' }), 'room', platform);
		release();
		await pending;
		expect(await presence.list('room')).toEqual([{ id: 'alice', name: 'B' }]);
	} finally { client.redis.eval = evaluate; presence.destroy(); }
});


it('publishes the first committed user when an earlier reservation is still pending', async () => {
	const client = mockRedisClient(), options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options), pa = mockPlatform(), pb = mockPlatform();
	const evaluate = client.redis.eval.bind(client.redis);
	let release;
	try {
		let sawFinalize;
		const ready = new Promise(resolve => { sawFinalize = resolve; });
		let hold = true;
		client.redis.eval = async (script, ...args) => {
			if (hold && script.includes('PRESENCE_FINALIZE')) { hold = false; sawFinalize(); await new Promise(resolve => { release = resolve; }); }
			return evaluate(script, ...args);
		};
		const pending = a.join(mockWs({ id: 'alice', name: 'A' }), 'room', pa);
		await ready;
		expect(await a.count('room')).toBe(0);
		expect(await a.list('room')).toEqual([]);
		const publish = vi.spyOn(client.redis, 'publish');
		await b.join(mockWs({ id: 'alice', name: 'B' }), 'room', pb);
		const events = publish.mock.calls.map(([, raw]) => JSON.parse(raw));
		expect(events.filter(event => event.event === 'join')).toHaveLength(1);
		expect(events.find(event => event.event === 'join').payload.data.name).toBe('B');
		release(); await pending;
	} finally { release?.(); client.redis.eval = evaluate; a.destroy(); b.destroy(); }
});


for (const leaveAll of [false, true]) it('does not promote a pending reservation during partial leave, all=' + leaveAll, async () => {
	const client = mockRedisClient(), options = { binary: false, select: u => ({ id: u.id, name: u.name }) };
	const a = createPresence(client, options), b = createPresence(client, options), pa = mockPlatform(), pb = mockPlatform();
	const anchor = mockWs({ id: 'alice', name: 'Stable' });
	const hgetall = client.redis.hgetall.bind(client.redis);
	let release;
	try {
		await b.join(mockWs({ id: 'alice', name: 'Stable' }), 'room', pb);
		await a.join(anchor, 'room', pa);
		let sawRead;
		const reading = new Promise(resolve => { sawRead = resolve; });
		let hold = true;
		client.redis.hgetall = key => {
			if (hold && key === client.key('presence:topic:{room}')) {
				hold = false; sawRead();
				return new Promise((resolve, reject) => { release = () => reject(new Error('snapshot failed')); });
			}
			return hgetall(key);
		};
		const pending = a.join(mockWs({ id: 'alice', name: 'Failed' }), 'room', pa).catch(error => error);
		await reading;
		if (leaveAll) await a.leave(anchor, pa);
		else await a.leave(anchor, pa, 'room');
		expect(await b.list('room')).toEqual([{ id: 'alice', name: 'Stable' }]);
		release();
		expect((await pending).message).toBe('snapshot failed');
		expect(await b.list('room')).toEqual([{ id: 'alice', name: 'Stable' }]);
	} finally { release?.(); client.redis.hgetall = hgetall; a.destroy(); b.destroy(); }
});


for (const observer of [false, true]) it('cleans failed finalization subscriptions while retaining observer role=' + observer, async () => {
	const client = mockRedisClient(), presence = createPresence(client, { binary: false });
	const platform = mockPlatform(), ws = mockWs({ id: 'alice' }), evaluate = client.redis.eval.bind(client.redis);
	try {
		if (observer) await presence.sync(ws, 'room', platform);
		client.redis.eval = (script, ...args) => script.includes('PRESENCE_FINALIZE') ? Promise.reject(new Error('finalize unavailable')) : evaluate(script, ...args);
		await expect(presence.join(ws, 'room', platform)).rejects.toThrow('finalize unavailable');
		expect(ws.isSubscribed('__presence:room')).toBe(observer);
		expect(ws.getUserData()[WS_SUBSCRIPTIONS].has('__presence:room')).toBe(observer);
	} finally { client.redis.eval = evaluate; presence.destroy(); }
});
