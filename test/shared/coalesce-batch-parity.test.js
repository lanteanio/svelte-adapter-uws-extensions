import { it, expect } from 'vitest';
import { collapseByCoalesceKey as adapter } from '../../node_modules/svelte-adapter-uws/src/runtime/utils/backpressure.js';
import { collapseByCoalesceKey as relay } from '../../src/shared/_coalesce-batch.js';

it('retains the adapter batch survivor order across topics and relay choices', () => {
	for (let offset = 0; offset < 16; offset++) {
		const messages = Array.from({ length: 32 }, (_, index) => ({
			topic: 'topic:' + (index % 3), event: 'msg', data: index,
			...(index % 4 ? { coalesceKey: 'key:' + ((index + offset) % 5) } : {}),
			options: { relay: index % 7 !== 0 }
		}));
		const expected = adapter(messages);
		const actual = relay(messages);
		expect(actual).toEqual(expected);
		for (let index = 0; index < actual.length; index++) expect(actual[index]).toBe(expected[index]);
	}
});

it('preserves array identity when no entry requests coalescing', () => {
	const messages = [{ topic: 'a', event: 'msg', data: 1 }, { topic: 'b', event: 'msg', data: 2 }];
	expect(adapter(messages)).toBe(messages);
	expect(relay(messages)).toBe(messages);
});
