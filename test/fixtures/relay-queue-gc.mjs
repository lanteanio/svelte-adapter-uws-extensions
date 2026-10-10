import assert from 'node:assert/strict';
import { createRelayQueue } from '../../src/redis/_relay-queue.js';
import { createMetrics } from '../../src/prometheus/index.js';

const metrics = createMetrics();
function createReference() {
	const queue = createRelayQueue({ submit() {}, dropped() {}, overflow() {}, metrics, prefix: 'pubsub' });
	return new WeakRef(queue.state);
}
const ref = createReference();
let collected = false;
let registrationRemoved = false;
const deleteEntry = Set.prototype.delete;
Set.prototype.delete = function (entry) {
	if (entry instanceof WeakRef && !entry.deref()) registrationRemoved = true;
	return deleteEntry.call(this, entry);
};
try {
	for (let i = 0; i < 20; i++) {
		await new Promise(resolve => setImmediate(resolve));
		global.gc();
		if (!ref.deref()) collected = true;
		if (collected && registrationRemoved) break;
	}
} finally {
	Set.prototype.delete = deleteEntry;
}
assert.ok(collected, 'Metrics retained an abandoned relay queue');
assert.ok(registrationRemoved, 'Metrics retained a dead queue registration without a scrape');
const output = metrics.serialize();
assert.match(output, /pubsub_relay_queue_envelopes\{state="queued"\} 0/);
assert.match(output, /pubsub_relay_queue_bytes\{state="in_flight"\} 0/);
