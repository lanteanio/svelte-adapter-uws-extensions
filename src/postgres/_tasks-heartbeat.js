import { monotonicNow, setTimer, clearTimer, setIntervalTimer, clearIntervalTimer } from '../shared/runtime.js';
import { coalescedWork } from '../shared/coalesced-work.js';

/** Bound renewal concurrency and confidence without replacing durable fence checks. */
export function startTaskHeartbeat({ interval, ttlMs, started = monotonicNow(), renew, onLost }) {
	let stopped = false;
	let finishing = false;
	let active = false;
	let finishWaiter;
	let intervalId;
	let deadlineTimer;
	const budget = ttlMs - Math.min(interval, (ttlMs - interval) / 2);
	let deadline = started + budget;
	function stop() {
		if (stopped) return;
		stopped = true;
		tick.stop();
		clearIntervalTimer(intervalId);
		clearTimer(deadlineTimer);
		finishWaiter?.();
		finishWaiter = undefined;
	}
	function lose() {
		if (stopped) return;
		stop();
		onLost();
	}
	function valid() {
		if (stopped) return false;
		if (monotonicNow() >= deadline) { lose(); return false; }
		return true;
	}
	function remaining() { return Math.max(1, Math.floor(Math.min(interval, deadline - monotonicNow()))); }
	function arm() {
		clearTimer(deadlineTimer);
		deadlineTimer = setTimer(() => { if (valid()) arm(); }, Math.min(2147483647, Math.max(0, deadline - monotonicNow())));
		deadlineTimer?.unref?.();
	}
	const tick = coalescedWork(async () => {
		if (!valid()) return;
		const renewalStarted = monotonicNow();
		active = true;
		try {
			const owned = await renew(valid, remaining);
			if (!valid()) return;
			if (!owned) { lose(); return; }
			// Include I/O latency in the confidence budget for both stores.
			deadline = renewalStarted + budget;
			if (valid()) arm();
		} catch {
			// While executing, a later tick may recover within the existing budget.
			// A finishing handler cannot ignore an uncertain external renewal.
			if (finishing) lose();
		} finally {
			active = false;
			finishWaiter?.();
			finishWaiter = undefined;
		}
	});
	intervalId = setIntervalTimer(tick, interval);
	intervalId?.unref?.();
	arm();
	if (monotonicNow() - started >= interval) tick();
	function finish() {
		finishing = true;
		clearIntervalTimer(intervalId);
		tick.stop();
		if (!valid() || !active) return;
		return new Promise(resolve => { finishWaiter = resolve; });
	}
	return { stop, valid, finish };
}
