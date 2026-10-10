import { monotonicNow } from '../shared/runtime.js';

const durationBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

/** Optional relay telemetry; all labels are bounded transport outcomes. */
export function createRelayMetrics(metrics, prefix) {
	if (!metrics) return null;
	const attempts = metrics.counter(prefix + '_relay_attempts_total', 'Relay envelopes completed or refused by the breaker', ['outcome']);
	const localOnly = metrics.counter(prefix + '_messages_local_only_total', 'Locally accepted messages without a confirmed Redis relay', ['reason']);
	const duration = metrics.histogram(prefix + '_relay_duration_seconds', 'Redis relay flush duration in seconds', [], durationBuckets);
	return {
		start: monotonicNow,
		settled(started, envelopes, failed, failedMessages) {
			if (envelopes > failed) attempts.inc({ outcome: 'success' }, envelopes - failed);
			if (failed) {
				attempts.inc({ outcome: 'error' }, failed);
				localOnly.inc({ reason: 'backend_error' }, failedMessages);
			}
			duration.observe(Math.max(0, monotonicNow() - started) / 1000);
		},
		dropped(reason, envelopes, messages) {
			if (reason === 'breaker_open' && envelopes) attempts.inc({ outcome: 'breaker_open' }, envelopes);
			if (messages) localOnly.inc({ reason }, messages);
		}
	};
}
