import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

const trackers = [];
afterEach(() => { for (const tracker of trackers.splice(0)) tracker.destroy(); });

describe('presence message hook', () => {
	for (const shape of ['parsed', 'msg', 'bytes', 'arraybuffer']) {
		it(`serves snapshots and updates from ${shape}`, async () => {
			const tracker = createPresence(mockRedisClient(), { binary: false, clientUpdateFields: ['color'] });
			trackers.push(tracker);
			const platform = mockPlatform();
			const ws = mockWs({ id: 'alice' });
			await tracker.join(ws, 'room', platform);
			tracker.flushDiffs();
			platform.reset();
			const frame = (env) => {
				const bytes = new TextEncoder().encode(JSON.stringify(env));
				if (shape === 'parsed') return { data: env, platform };
				if (shape === 'msg') return { data: new Uint8Array([0]), msg: env, platform };
				return { data: shape === 'arraybuffer' ? bytes.buffer : bytes, platform };
			};
			expect(tracker.hooks.message(ws, frame({ type: 'presence-update', topic: 'room', fields: { color: 'blue' } }))).toBe(true);
			await vi.waitFor(async () => expect(await tracker.list('room')).toEqual([{ id: 'alice', color: 'blue' }]));
			expect(tracker.hooks.message(ws, frame({ type: 'presence-snapshot', topic: 'room' }))).toBe(true);
			await vi.waitFor(() => expect(platform.sent.some((f) => f.event === 'state' && f.data.alice.color === 'blue')).toBe(true));
			expect(tracker.hooks.message(ws, frame({ type: 'other', topic: 'room' }))).toBeUndefined();
		});
	}
});
