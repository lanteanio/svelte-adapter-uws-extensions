import { describe, it, expect } from 'vitest';
import { createReplay as createRedisReplay } from '../../src/redis/replay.js';
import { createReplay as createPgReplay } from '../../src/postgres/replay.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';

for (const backend of ['sorted-set', 'stream', 'postgres']) {
	describe(backend + ' replay correlation', () => {
		function setup() {
			const client = backend === 'postgres' ? mockPgClient() : mockRedisClient('correlation:');
			const replay = backend === 'postgres'
				? createPgReplay(client, { size: 2, cleanupInterval: 0 })
				: createRedisReplay(client, { size: 2, storage: backend === 'stream' ? 'stream' : 'sortedset' });
			return { client, replay, platform: mockPlatform() };
		}

		it.each(['first', 0, 17])('correlates trimmed history and its end marker for request %s', async (reqId) => {
			const { replay, platform } = setup();
			try {
				for (let n = 1; n <= 4; n++) await replay.publish(platform, 'room', 'change', { n });
				await replay.replay({}, 'room', 1, platform, reqId);
				// Observe the wire representation, including omission of undefined properties.
				const frames = JSON.parse(JSON.stringify(platform.sent));
				const requestFrames = frames.filter((frame) => frame.data?.reqId === reqId);
				expect(requestFrames.map((frame) => frame.event)).toEqual(['msg', 'msg', 'end']);
				expect(requestFrames.filter((frame) => frame.event === 'msg').map((frame) => frame.data.seq)).toEqual([3, 4]);
				expect(requestFrames.at(-1).data).toEqual({ reqId, truncated: true });
				expect(frames.filter((frame) => frame.event === 'truncated')).toHaveLength(1);
			} finally { replay.destroy?.(); }
		});

		it('reports an entirely expired buffer as truncated on the correlated end', async () => {
			const { client, replay, platform } = setup();
			try {
				for (let n = 1; n <= 3; n++) await replay.publish(platform, 'room', 'change', { n });
				if (backend === 'postgres') await client.query('DELETE FROM svti_replay WHERE topic = $1', ['room']);
				else await client.redis.del(client.key(backend === 'stream' ? 'replay:streambuf:{room}' : 'replay:buf:{room}'));
				await replay.replay({}, 'room', 1, platform, 'expired');
				expect(platform.sent.filter((frame) => frame.event === 'msg')).toEqual([]);
				expect(platform.sent.at(-1).data).toEqual({ reqId: 'expired', truncated: true });
			} finally { replay.destroy?.(); }
		});

		it('keeps concurrent requests isolated and complete requests unmarked', async () => {
			const { replay, platform } = setup();
			try {
				for (let n = 1; n <= 4; n++) await replay.publish(platform, 'room', 'change', { n });
				await Promise.all([
					replay.replay({}, 'room', 1, platform, 'old'),
					replay.replay({}, 'room', 3, platform, 'current')
				]);
				const current = platform.sent.filter((frame) => frame.data?.reqId === 'current');
				expect(current.map((frame) => frame.event)).toEqual(['msg', 'end']);
				expect(current[0].data.seq).toBe(4);
				expect(current.at(-1).data).toEqual({ reqId: 'current' });
				expect(platform.sent.find((frame) => frame.event === 'end' && frame.data.reqId === 'old').data.truncated).toBe(true);
			} finally { replay.destroy?.(); }
		});

		it('omits correlation when no request ID was supplied', async () => {
			const { replay, platform } = setup();
			try {
				await replay.publish(platform, 'room', 'change', 1);
				await replay.replay({}, 'room', 0, platform);
				const frames = JSON.parse(JSON.stringify(platform.sent));
				expect(frames.map((frame) => frame.event)).toEqual(['msg', 'end']);
				for (const frame of frames) expect(frame.data).not.toHaveProperty('reqId');
			} finally { replay.destroy?.(); }
		});
	});
}
