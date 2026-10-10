import { afterEach, expect, it, vi } from 'vitest';
import { createReplay } from '../../src/redis/replay.js';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

const flush = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };
afterEach(resetRuntimeEnv);
const breaker = () => ({ state: 'healthy', isHealthy: true, guard() {}, success: vi.fn(), failure: vi.fn(), subscribe() { return () => {}; } });
function failingPipeline(redis, mode, error) {
	redis.pipeline = () => {
		let count = 0;
		const pipe = new Proxy({}, { get(_target, name) {
			if (name === 'exec') return async () => { if (mode === 'reject') throw error; return Array.from({ length: count }, () => [error, undefined]); };
			return () => { count++; return pipe; };
		} });
		return pipe;
	};
}
for (const storage of ['sortedset', 'stream']) it.each(['reject', 'tuple'])(`${storage} resume epoch failure reaches its breaker (%s)`, async mode => {
	const client = mockRedisClient(), b = breaker(), error = new Error('epoch unavailable');
	const replay = createReplay(client, { storage, breaker: b });
	try {
		failingPipeline(client.redis, mode, error);
		await expect(replay.resumeHook()({}, { lastSeenSeqs: { room: 1 }, lastSeenEpochs: { room: 0 }, platform: mockPlatform() })).rejects.toBe(error);
		expect(b.failure).toHaveBeenCalledExactlyOnceWith(error, undefined);
		expect(b.success).not.toHaveBeenCalled();
	} finally { replay.destroy?.(); }
});

for (const operation of ['leave', 'heartbeat']) it.each(['reject', 'tuple'])(`presence ${operation} failure reaches its breaker (%s)`, async mode => {
	const intervals = new Map();
	setRuntimeEnv({ timers: { setInterval(cb, ms) { intervals.set(ms, cb); return { unref() {} }; }, clearInterval() {} } });
	const client = mockRedisClient(), b = breaker(), error = new Error('presence unavailable');
	const presence = createPresence(client, { heartbeat: 60000, breaker: b });
	const ws = mockWs({ id: 'user' }), platform = mockPlatform();
	try {
		await presence.join(ws, 'room', platform);
		b.success.mockClear(); b.failure.mockClear();
		failingPipeline(client.redis, mode, error);
		if (operation === 'leave') await presence.leave(ws, platform);
		else { intervals.get(60000)(); await flush(); }
		expect(b.failure).toHaveBeenCalledExactlyOnceWith(error);
		expect(b.success).not.toHaveBeenCalled();
	} finally { presence.destroy(); }
});
