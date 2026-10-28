import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { WS_SUBSCRIPTIONS } from '../../src/shared/ws-subscriptions.js';

const ACCOUNTING_HOOK = Symbol.for('adapter-uws.subscription-accounting-hook');
let previousHook;
let total;
let summed;

beforeEach(() => {
	previousHook = globalThis[ACCOUNTING_HOOK];
	total = 0;
	summed = (ws) => ws.getUserData()[WS_SUBSCRIPTIONS].size;
	globalThis[ACCOUNTING_HOOK] = (delta) => { total += delta; };
});

afterEach(() => {
	if (previousHook === undefined) delete globalThis[ACCOUNTING_HOOK];
	else globalThis[ACCOUNTING_HOOK] = previousHook;
});

describe('tracked extension subscriptions', () => {
	it('keeps presence join and leave accounting equal to the registry', async () => {
		const client = mockRedisClient('accounting:');
		const platform = mockPlatform();
		const presence = createPresence(client, { key: 'id', heartbeat: 60000, ttl: 180 });
		const ws = mockWs({ id: 'alice' });
		try {
			await presence.join(ws, 'room', platform);
			expect(total).toBe(1);
			expect(total).toBe(summed(ws));

			await presence.leave(ws, platform, 'room');
			expect(total).toBe(0);
			expect(total).toBe(summed(ws));
		} finally {
			presence.destroy();
		}
	});

	it('keeps cursor attach and detach accounting equal to the registry', async () => {
		const client = mockRedisClient('accounting:');
		const platform = mockPlatform();
		const cursors = createCursor(client, { select: (userData) => ({ id: userData.id }) });
		const ws = mockWs({ id: 'alice' });
		try {
			await cursors.attach(ws, 'board', platform);
			expect(total).toBe(1);
			expect(total).toBe(summed(ws));

			cursors.detach(ws, 'board', platform);
			expect(total).toBe(0);
			expect(total).toBe(summed(ws));
		} finally {
			cursors.destroy();
		}
	});
});
