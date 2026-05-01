// Bench: how much JSON.parse work do we save by fetching only seq>sinceSeq
// instead of the full sorted set, when replay clients have only fallen behind
// by a small fraction of the buffer.
//
// Models the common case: client missed last 10 of 1000 buffered messages.

import { bench, compare } from './harness.js';

function buildBuffer(n) {
	const buf = [];
	for (let i = 1; i <= n; i++) {
		buf.push(JSON.stringify({ seq: i, topic: 't', event: 'msg', data: { v: i, payload: 'lorem ipsum dolor sit amet ' + i } }));
	}
	return buf;
}

const fullBuffer = buildBuffer(1000);
const sinceSeq = 990; // missed last 10
const tailBuffer = fullBuffer.slice(sinceSeq);

function replayCurrent(rawAll, sinceSeq) {
	let oldestSeq = null;
	for (let i = 0; i < rawAll.length; i++) {
		try {
			const parsed = JSON.parse(rawAll[i]);
			if (parsed.seq != null) {
				oldestSeq = parsed.seq;
				break;
			}
		} catch { /* skip */ }
	}
	const missed = [];
	for (let i = 0; i < rawAll.length; i++) {
		try {
			const parsed = JSON.parse(rawAll[i]);
			if (parsed.seq > sinceSeq) missed.push(parsed);
		} catch { /* skip */ }
	}
	return { oldestSeq, missed };
}

function replayPipelined(oldestRaw, missedRaw) {
	let oldestSeq = null;
	if (oldestRaw && oldestRaw.length > 0) {
		try {
			const parsed = JSON.parse(oldestRaw[0]);
			if (parsed.seq != null) oldestSeq = parsed.seq;
		} catch { /* skip */ }
	}
	const missed = [];
	for (let i = 0; i < missedRaw.length; i++) {
		try {
			missed.push(JSON.parse(missedRaw[i]));
		} catch { /* skip */ }
	}
	return { oldestSeq, missed };
}

const oldestRaw = [fullBuffer[0]];

console.log('Common case: 1000-msg buffer, client missed last 10');
const a = bench('current  (full scan + double parse)', () => replayCurrent(fullBuffer, sinceSeq), { iters: 5000 });
const b = bench('pipelined (oldest + missed only)', () => replayPipelined(oldestRaw, tailBuffer), { iters: 5000 });
compare('pipelined vs current', a, b);

console.log('\nWorst case: client missed entire 1000 messages');
const a2 = bench('current  (sinceSeq=0)', () => replayCurrent(fullBuffer, 0), { iters: 5000, warmup: 500 });
const b2 = bench('pipelined (sinceSeq=0)', () => replayPipelined(oldestRaw, fullBuffer), { iters: 5000, warmup: 500 });
compare('pipelined vs current (full miss)', a2, b2);

console.log('\nMid case: client missed last 100 of 1000');
const sinceSeqMid = 900;
const tailBufferMid = fullBuffer.slice(sinceSeqMid);
const a3 = bench('current  (mid miss)', () => replayCurrent(fullBuffer, sinceSeqMid), { iters: 5000, warmup: 500 });
const b3 = bench('pipelined (mid miss)', () => replayPipelined(oldestRaw, tailBufferMid), { iters: 5000, warmup: 500 });
compare('pipelined vs current (mid miss)', a3, b3);
