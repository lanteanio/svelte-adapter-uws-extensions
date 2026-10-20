import { afterEach, describe, expect, it } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const HEARTBEAT = 12345;
const trackers = [];
afterEach(() => {
	for (const t of trackers.splice(0)) t.destroy();
	resetRuntimeEnv();
});

/** Record every interval armed at the heartbeat period. */
function watchIntervals() {
	const armed = [];
	setRuntimeEnv({
		timers: {
			setInterval(cb, ms, ...a) {
				const h = setInterval(cb, ms, ...a);
				if (ms === HEARTBEAT) armed.push(h);
				return h;
			}
		}
	});
	return armed;
}

describe('redis presence heartbeat starts on first use', () => {
	it('arms no heartbeat until the first join, then exactly one', async () => {
		const armed = watchIntervals();
		const presence = createPresence(mockRedisClient(), { key: 'id', heartbeat: HEARTBEAT, consistencyAuditIntervalMs: 0 });
		trackers.push(presence);
		expect(armed).toHaveLength(0);

		const platform = mockPlatform();
		await presence.join(mockWs({ id: 'a' }), 'room', platform);
		await presence.join(mockWs({ id: 'b' }), 'other', platform);
		expect(armed).toHaveLength(1);
	});

	it('a sync-only observer starts it too', async () => {
		const armed = watchIntervals();
		const presence = createPresence(mockRedisClient(), { key: 'id', heartbeat: HEARTBEAT, consistencyAuditIntervalMs: 0 });
		trackers.push(presence);
		await presence.sync(mockWs({ id: 'w' }), 'room', mockPlatform());
		expect(armed).toHaveLength(1);
	});

	it('destroy before any use leaves nothing armed, and a later join arms nothing', async () => {
		const armed = watchIntervals();
		const presence = createPresence(mockRedisClient(), { key: 'id', heartbeat: HEARTBEAT, consistencyAuditIntervalMs: 0 });
		presence.destroy();
		await presence.join(mockWs({ id: 'a' }), 'room', mockPlatform()).catch(() => {});
		expect(armed).toHaveLength(0);
	});
});
