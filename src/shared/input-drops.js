import { emitRegisteredFailure } from './diagnostic.js';

// Internal callers supply only fixed reason/stage strings. Never pass payloads,
// identifiers or caught values here: even an error's name/code can echo input.
// Count every observation, but emit at most once per pair per component instance.
export function createInputDropReporter(metrics, prefix, errorId) {
	const counter = metrics?.counter(prefix + '_input_dropped_total', 'Rejected inputs and failed background operations', ['reason', 'stage']);
	const reported = new Set();
	return (reason, stage) => {
		try { counter?.inc({ reason, stage }); } catch { /* observers cannot break delivery */ }
		const key = reason + ':' + stage;
		if (reported.has(key)) return;
		reported.add(key);
		emitRegisteredFailure(errorId, { attributes: { reason, stage } });
	};
}
