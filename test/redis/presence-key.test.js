import { describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

describe('presence dedup keys', () => {
	for (const key of ['constructor', '__proto__', 'prototype']) {
		it(`keeps connections distinct with the default ${key} selector`, async () => {
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const tracker = createPresence(mockRedisClient(), { key });
			try {
				const platform = mockPlatform();
				await tracker.join(mockWs({}), 'room', platform);
				await tracker.join(mockWs({}), 'room', platform);
				expect(await tracker.count('room')).toBe(2);
			} finally { tracker.destroy(); warn.mockRestore(); }
		});
	}
	it('ignores an inherited key returned by an explicit selector', async () => {
		const tracker = createPresence(mockRedisClient(), { select: () => Object.create({ id: 'shared' }) });
		try {
			const platform = mockPlatform();
			await tracker.join(mockWs({}), 'room', platform);
			await tracker.join(mockWs({}), 'room', platform);
			expect(await tracker.count('room')).toBe(2);
		} finally { tracker.destroy(); }
	});
});
