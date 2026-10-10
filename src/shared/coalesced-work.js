/** Run at most one asynchronous tick and retain at most one rerun request. */
export function coalescedWork(run, onError = () => {}) {
	let running = false;
	let queued = false;
	let stopped = false;
	function tick() {
		if (stopped) return;
		if (running) { queued = true; return; }
		running = true;
		Promise.resolve().then(() => { if (!stopped) return run(); }).catch(onError).finally(() => {
			running = false;
			if (queued && !stopped) { queued = false; tick(); }
		});
	}
	tick.stop = () => { stopped = true; queued = false; };
	return tick;
}
