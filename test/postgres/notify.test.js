import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createNotifyBridge } from '../../postgres/notify.js';

/**
 * Mock PgClient with createClient() that returns a standalone Client mock
 * with LISTEN/NOTIFY simulation.
 */
function mockPgClient({ failConnect = false } = {}) {
	let connListeners = new Map();
	let queries = [];

	const mockClient = {
		on(event, fn) {
			if (!connListeners.has(event)) connListeners.set(event, []);
			connListeners.get(event).push(fn);
		},
		removeListener(event, fn) {
			const list = connListeners.get(event);
			if (!list) return;
			const idx = list.indexOf(fn);
			if (idx !== -1) list.splice(idx, 1);
		},
		async connect() {
			if (failConnect) throw new Error('connection refused');
		},
		async query(text) {
			queries.push(text);
			return { rows: [], rowCount: 0 };
		},
		async end() {
			// no-op in mock
		}
	};

	return {
		pool: {},
		createClient() {
			connListeners = new Map();
			queries = [];
			return mockClient;
		},
		async query() { return { rows: [], rowCount: 0 }; },
		async end() {},

		// Test helpers
		_mockClient: mockClient,
		_getQueries() { return queries; },
		_simulate(channel, payload) {
			const listeners = connListeners.get('notification') || [];
			for (const fn of listeners) {
				fn({ channel, payload });
			}
		}
	};
}

describe('postgres notify bridge', () => {
	let client;
	let platform;
	let bridge;

	beforeEach(() => {
		client = mockPgClient();
		platform = mockPlatform();
	});

	afterEach(async () => {
		if (bridge) await bridge.deactivate();
	});

	describe('createNotifyBridge', () => {
		it('throws if channel is missing', () => {
			expect(() => createNotifyBridge(client)).toThrow('non-empty string');
			expect(() => createNotifyBridge(client, {})).toThrow('non-empty string');
		});

		it('throws if channel is empty', () => {
			expect(() => createNotifyBridge(client, { channel: '' })).toThrow('non-empty string');
		});

		it('returns a bridge with expected API', () => {
			bridge = createNotifyBridge(client, { channel: 'test' });
			expect(typeof bridge.activate).toBe('function');
			expect(typeof bridge.deactivate).toBe('function');
		});
	});

	describe('activate', () => {
		it('runs LISTEN on the channel', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);

			const queries = client._getQueries();
			expect(queries.some((q) => q.includes('LISTEN') && q.includes('changes'))).toBe(true);
		});

		it('is idempotent', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);
			await bridge.activate(platform);
			// Should not throw or double-listen
		});
	});

	describe('activate - failure handling', () => {
		it('with autoReconnect:false, can retry activate after failure', async () => {
			const failClient = mockPgClient({ failConnect: true });
			bridge = createNotifyBridge(failClient, {
				channel: 'changes',
				autoReconnect: false
			});

			// First activate should throw
			await expect(bridge.activate(platform)).rejects.toThrow('connection refused');

			// Should be able to call activate again (not stuck)
			await expect(bridge.activate(platform)).rejects.toThrow('connection refused');
			// The point is it doesn't silently return -- it actually retries
		});
	});

	describe('notification forwarding', () => {
		it('forwards parsed notifications to platform.publish() with relay: false', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);

			client._simulate('changes', JSON.stringify({
				topic: 'messages',
				event: 'created',
				data: { id: 1, text: 'hello' }
			}));

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toEqual({
				topic: 'messages',
				event: 'created',
				data: { id: 1, text: 'hello' },
				options: { relay: false }
			});
		});

		it('skips notifications on wrong channel', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);

			client._simulate('other_channel', JSON.stringify({
				topic: 'x', event: 'y', data: null
			}));

			expect(platform.published).toHaveLength(0);
		});

		it('skips malformed JSON', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);

			client._simulate('changes', 'not-json');
			expect(platform.published).toHaveLength(0);
		});

		it('skips payloads missing topic or event', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);

			client._simulate('changes', JSON.stringify({ data: 'no topic or event' }));
			expect(platform.published).toHaveLength(0);
		});

		it('uses custom parse function', async () => {
			bridge = createNotifyBridge(client, {
				channel: 'changes',
				parse: (payload) => {
					const row = JSON.parse(payload);
					return { topic: row.table, event: row.op, data: row.record };
				}
			});
			await bridge.activate(platform);

			client._simulate('changes', JSON.stringify({
				table: 'users',
				op: 'insert',
				record: { id: 42 }
			}));

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toEqual({
				topic: 'users',
				event: 'insert',
				data: { id: 42 },
				options: { relay: false }
			});
		});

		it('skips when custom parse returns null', async () => {
			bridge = createNotifyBridge(client, {
				channel: 'changes',
				parse: () => null
			});
			await bridge.activate(platform);

			client._simulate('changes', 'anything');
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('deactivate', () => {
		it('runs UNLISTEN', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);
			await bridge.deactivate();

			const queries = client._getQueries();
			expect(queries.some((q) => q.includes('UNLISTEN'))).toBe(true);
		});

		it('is safe to call without activate', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.deactivate(); // should not throw
		});

		it('stops forwarding after deactivate', async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);
			await bridge.deactivate();

			client._simulate('changes', JSON.stringify({
				topic: 'x', event: 'y', data: null
			}));

			expect(platform.published).toHaveLength(0);
		});
	});
});
