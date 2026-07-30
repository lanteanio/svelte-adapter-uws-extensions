import { describe, it, expect } from 'vitest';
import { createDiffBuffer } from '../../src/redis/presence/diff-buffer.js';

// The diff buffer is the third ingress the reserved-name rule covers: both
// bufferUpdate callers pre-filter through the same predicate, so the buffer's
// own mergeFields is defense-in-depth over provably safe inputs. These tests
// drive the buffer DIRECTLY, so reverting its mergeFields calls back to
// Object.assign fails here even though the end-to-end presence suite would
// stay green behind the two pre-filtering callers.

function harness() {
	const emitted = [];
	const localData = new Map();
	const buffer = createDiffBuffer({
		emit: (fullTopic, event, data) => emitted.push({ fullTopic, event, data }),
		localData,
		// Production publicData also filters reserved names; the passthrough
		// keeps these tests pointed at the buffer's own merges only.
		publicData: (entry) => entry,
		mt: undefined,
		mDiffCoalesced: null,
		mDiffFrames: null
	});
	return { emitted, localData, buffer };
}

describe('presence diff buffer field policy', () => {
	it('stores a fresh update in a proto-free map and drops reserved names', () => {
		const { emitted, buffer } = harness();
		buffer.bufferUpdate('room', 'alice', { __admin: 1, constructor: 'c', color: 'red' }, {});
		buffer.flushPendingDiffs();

		expect(emitted).toHaveLength(1);
		const updates = emitted[0].data.updates;
		expect(updates.alice.color).toBe('red');
		expect(Object.getPrototypeOf(updates)).toBe(null);
		expect(Object.getPrototypeOf(updates.alice)).toBe(null);
		for (const k of ['__admin', 'constructor']) {
			expect(Object.prototype.hasOwnProperty.call(updates.alice, k), k).toBe(false);
		}
	});

	it('drops reserved names when ACCUMULATING onto a pending update', () => {
		const { emitted, buffer } = harness();
		// The second bufferUpdate merges into the first's stored `changed`
		// map via mergeFields(prev.changed, changed) - the accumulation sink.
		buffer.bufferUpdate('room', 'alice', { color: 'red' }, {});
		buffer.bufferUpdate('room', 'alice', { __internal: 'x', prototype: 'p', x: 1 }, {});
		buffer.flushPendingDiffs();

		expect(emitted).toHaveLength(1);
		const updates = emitted[0].data.updates;
		expect(updates.alice.color).toBe('red');
		expect(updates.alice.x).toBe(1);
		for (const k of ['__internal', 'prototype']) {
			expect(Object.prototype.hasOwnProperty.call(updates.alice, k), k).toBe(false);
		}
	});

	it('drops reserved names absorbed into a RELAYED join payload', () => {
		const { emitted, buffer } = harness();
		// A relayed join for a user with no local entry absorbs a same-tick
		// field update into its buffered payload via mergeFields(prev.data,
		// changed) - the sink a cross-instance FIELDS envelope reaches.
		// JSON.parse so '__proto__' arrives as an OWN data property; an object
		// literal would invoke the prototype setter and probe nothing.
		buffer.bufferDiff('room', 'join', 'alice', { id: 'alice' }, {});
		buffer.bufferUpdate('room', 'alice', JSON.parse('{"__proto__":{"isAdmin":true},"color":"blue"}'), {});
		buffer.flushPendingDiffs();

		expect(emitted).toHaveLength(1);
		const joins = emitted[0].data.joins;
		expect(joins.alice.id).toBe('alice');
		expect(joins.alice.color).toBe('blue');
		expect(joins.alice.isAdmin).toBeUndefined();
		expect(({}).isAdmin).toBeUndefined();
		expect(Object.getPrototypeOf(joins.alice)).toBe(Object.prototype);
		expect(JSON.stringify(joins)).not.toContain('__proto__');
	});
});
