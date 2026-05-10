import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createNotifyBridge } from '../../postgres/notify.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

/**
 * Cluster-scoped mock that simulates Postgres advisory-lock state across
 * multiple replicas. Each replica gets its own PgClient via
 * `createReplica()`; `pg_try_advisory_lock` and `pg_advisory_unlock`
 * share a single lockId -> ownerConnId map. Connection `end()` releases
 * any locks held by that connection (matches Postgres session-scoped
 * advisory-lock semantics).
 */
function mockCluster() {
	const advisoryLocks = new Map(); // lockId -> connId
	let nextConnId = 1;

	function createReplica({ failConnect = false } = {}) {
		const connId = nextConnId++;
		let connListeners = new Map();
		let queries = [];
		let isOpen = false;
		const heldLocks = new Set();

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
				isOpen = true;
			},
			async query(text, values) {
				queries.push({ text, values });
				if (text.includes('pg_try_advisory_lock')) {
					const lockId = values[0];
					const owner = advisoryLocks.get(lockId);
					if (owner === undefined || owner === connId) {
						advisoryLocks.set(lockId, connId);
						heldLocks.add(lockId);
						return { rows: [{ acquired: true }], rowCount: 1 };
					}
					return { rows: [{ acquired: false }], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			},
			async end() {
				if (isOpen) {
					for (const lockId of heldLocks) {
						if (advisoryLocks.get(lockId) === connId) {
							advisoryLocks.delete(lockId);
						}
					}
					heldLocks.clear();
					isOpen = false;
				}
			}
		};

		return {
			pool: {},
			createClient() {
				connListeners = new Map();
				queries = [];
				heldLocks.clear();
				return mockClient;
			},
			async query() { return { rows: [], rowCount: 0 }; },
			async end() {},
			_mockClient: mockClient,
			_getQueries() { return queries; },
			_connId: connId,
			_simulate(channel, payload) {
				const listeners = connListeners.get('notification') || [];
				for (const fn of listeners) fn({ channel, payload });
			},
			_emitError(err) {
				const listeners = connListeners.get('error') || [];
				for (const fn of listeners) fn(err);
			},
			_killConnection() {
				// Simulate the server side dropping the connection: release
				// the lock as if the session ended, and fire 'error' so the
				// bridge tears down its side.
				for (const lockId of heldLocks) {
					if (advisoryLocks.get(lockId) === connId) {
						advisoryLocks.delete(lockId);
					}
				}
				heldLocks.clear();
				const listeners = connListeners.get('error') || [];
				for (const fn of listeners) fn(new Error('connection dropped'));
			}
		};
	}

	return { createReplica, _advisoryLocks: advisoryLocks };
}

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

		it('throws on negative reconnectInterval', () => {
			expect(() => createNotifyBridge(client, { channel: 'x', reconnectInterval: -1 }))
				.toThrow('non-negative');
		});

		it('throws on non-function parse', () => {
			expect(() => createNotifyBridge(client, { channel: 'x', parse: 'bad' }))
				.toThrow('parse must be a function');
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
			// The point is it doesn't silently return - it actually retries
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

		it('logs warning when custom parse throws', async () => {
			const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			bridge = createNotifyBridge(client, {
				channel: 'changes',
				parse: () => { throw new Error('bad payload'); }
			});
			await bridge.activate(platform);

			client._simulate('changes', 'whatever');
			expect(platform.published).toHaveLength(0);
			expect(warnSpy).toHaveBeenCalledOnce();
			expect(warnSpy.mock.calls[0][0]).toContain('[postgres/notify]');
			expect(warnSpy.mock.calls[0][1]).toContain('bad payload');
			warnSpy.mockRestore();
		});

		it('does not log warning when default parser encounters bad JSON', async () => {
			const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);

			client._simulate('changes', 'not-json');
			expect(platform.published).toHaveLength(0);
			// Default parser handles errors silently via its own try/catch
			const notifyWarns = warnSpy.mock.calls.filter(
				(c) => c[0] && typeof c[0] === 'string' && c[0].includes('parse error')
			);
			expect(notifyWarns).toHaveLength(0);
			warnSpy.mockRestore();
		});
	});

	describe('activate - platform update', () => {
		it('second activate() with a different platform updates the forwarding target', async () => {
			const platform1 = mockPlatform();
			const platform2 = mockPlatform();

			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform1);

			// Second activate - idempotent for the listener, but updates platform
			await bridge.activate(platform2);

			client._simulate('changes', JSON.stringify({
				topic: 'messages',
				event: 'created',
				data: { id: 1 }
			}));

			// Should forward through platform2, not platform1
			expect(platform2.published).toHaveLength(1);
			expect(platform2.published[0].topic).toBe('messages');
			expect(platform1.published).toHaveLength(0);
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

	describe('breaker accounting', () => {
		it('records failure() when LISTEN connection error fires', async () => {
			const listeners = new Map();
			const testClient = {
				pool: {},
				createClient() {
					return {
						on(ev, fn) {
							if (!listeners.has(ev)) listeners.set(ev, []);
							listeners.get(ev).push(fn);
						},
						removeListener(ev, fn) {
							const arr = listeners.get(ev);
							if (arr) { const i = arr.indexOf(fn); if (i !== -1) arr.splice(i, 1); }
						},
						async connect() {},
						async query() { return { rows: [], rowCount: 0 }; },
						async end() {}
					};
				},
				async query() { return { rows: [], rowCount: 0 }; },
				async end() {}
			};

			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const b = createNotifyBridge(testClient, {
				channel: 'ch',
				breaker,
				autoReconnect: false
			});
			await b.activate(platform);

			expect(breaker.failures).toBe(0);
			const errorFns = listeners.get('error') || [];
			expect(errorFns.length).toBeGreaterThan(0);
			errorFns[0](new Error('connection lost'));
			expect(breaker.failures).toBe(1);

			breaker.destroy();
		});

		it('records success() when activate connect succeeds', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			bridge = createNotifyBridge(client, {
				channel: 'changes',
				breaker,
				autoReconnect: false
			});
			breaker.failure();
			expect(breaker.failures).toBe(1);

			await bridge.activate(platform);
			expect(breaker.failures).toBe(0);

			breaker.destroy();
		});

		it('records failure() when activate connect fails', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const failClient = mockPgClient({ failConnect: true });
			bridge = createNotifyBridge(failClient, {
				channel: 'changes',
				breaker,
				autoReconnect: false
			});

			await expect(bridge.activate(platform)).rejects.toThrow('connection refused');
			expect(breaker.failures).toBe(1);

			breaker.destroy();
		});
	});

	describe('advisory leader election', () => {
		it('rejects multiListener with an unknown value', () => {
			expect(() => createNotifyBridge(client, {
				channel: 'changes',
				multiListener: 'leader'
			})).toThrow("multiListener must be 'all' or 'advisory'");
		});

		it('requires lockId in advisory mode', () => {
			expect(() => createNotifyBridge(client, {
				channel: 'changes',
				multiListener: 'advisory'
			})).toThrow('lockId is required');
		});

		it('rejects non-positive pollInterval', () => {
			expect(() => createNotifyBridge(client, {
				channel: 'changes',
				multiListener: 'advisory',
				lockId: 1,
				pollInterval: 0
			})).toThrow('pollInterval must be a positive number');
		});

		it('first replica acquires the lock and runs LISTEN', async () => {
			const cluster = mockCluster();
			const a = cluster.createReplica();
			bridge = createNotifyBridge(a, {
				channel: 'changes',
				multiListener: 'advisory',
				lockId: 42
			});
			await bridge.activate(platform);

			const queries = a._getQueries();
			expect(queries.some((q) => q.text.includes('pg_try_advisory_lock'))).toBe(true);
			expect(queries.some((q) => q.text.includes('LISTEN'))).toBe(true);
			expect(cluster._advisoryLocks.get(42)).toBe(a._connId);
		});

		it('second replica polls but does not LISTEN while the first holds the lock', async () => {
			const cluster = mockCluster();
			const a = cluster.createReplica();
			const b = cluster.createReplica();

			const bridgeA = createNotifyBridge(a, {
				channel: 'changes', multiListener: 'advisory', lockId: 42
			});
			const bridgeB = createNotifyBridge(b, {
				channel: 'changes', multiListener: 'advisory', lockId: 42
			});

			await bridgeA.activate(platform);
			await bridgeB.activate(platform);

			expect(a._getQueries().some((q) => q.text.includes('LISTEN'))).toBe(true);
			expect(b._getQueries().some((q) => q.text.includes('LISTEN'))).toBe(false);
			expect(cluster._advisoryLocks.get(42)).toBe(a._connId);

			await bridgeA.deactivate();
			await bridgeB.deactivate();
		});

		it('leader publishes WITH relay (no relay: false) so the bus fans out', async () => {
			const cluster = mockCluster();
			const a = cluster.createReplica();
			bridge = createNotifyBridge(a, {
				channel: 'changes', multiListener: 'advisory', lockId: 42
			});
			await bridge.activate(platform);

			a._simulate('changes', JSON.stringify({
				topic: 'messages', event: 'created', data: { id: 1 }
			}));

			expect(platform.published).toHaveLength(1);
			// In advisory mode the leader must NOT pass relay: false --
			// the bus needs to fan the publish out to non-leader replicas.
			expect(platform.published[0].options).toBeUndefined();
		});

		it("'all' mode still publishes with relay: false (regression check)", async () => {
			bridge = createNotifyBridge(client, { channel: 'changes' });
			await bridge.activate(platform);
			client._simulate('changes', JSON.stringify({
				topic: 'messages', event: 'created', data: { id: 1 }
			}));
			expect(platform.published[0].options).toEqual({ relay: false });
		});

		it('promotes a follower when the leader deactivates', async () => {
			vi.useFakeTimers();
			try {
				const cluster = mockCluster();
				const a = cluster.createReplica();
				const b = cluster.createReplica();

				const bridgeA = createNotifyBridge(a, {
					channel: 'changes', multiListener: 'advisory', lockId: 42, pollInterval: 100
				});
				const bridgeB = createNotifyBridge(b, {
					channel: 'changes', multiListener: 'advisory', lockId: 42, pollInterval: 100
				});

				await bridgeA.activate(platform);
				await bridgeB.activate(platform);
				expect(cluster._advisoryLocks.get(42)).toBe(a._connId);

				await bridgeA.deactivate();
				expect(cluster._advisoryLocks.has(42)).toBe(false);

				// B's next poll picks up the lock.
				await vi.advanceTimersByTimeAsync(150);

				expect(cluster._advisoryLocks.get(42)).toBe(b._connId);
				expect(b._getQueries().some((q) => q.text.includes('LISTEN'))).toBe(true);

				await bridgeB.deactivate();
			} finally {
				vi.useRealTimers();
			}
		});

		it('recovers leadership when a leader connection drops', async () => {
			// On unexpected connection drop the session-scoped lock
			// auto-releases. Whether the dropped replica reconnects
			// faster than a follower polls is a race; the property
			// that matters is that some replica reacquires within
			// pollInterval ms.
			vi.useFakeTimers();
			try {
				const cluster = mockCluster();
				const a = cluster.createReplica();
				const b = cluster.createReplica();

				const bridgeA = createNotifyBridge(a, {
					channel: 'changes', multiListener: 'advisory', lockId: 42, pollInterval: 100
				});
				const bridgeB = createNotifyBridge(b, {
					channel: 'changes', multiListener: 'advisory', lockId: 42, pollInterval: 100
				});

				await bridgeA.activate(platform);
				await bridgeB.activate(platform);

				a._killConnection();
				expect(cluster._advisoryLocks.has(42)).toBe(false);

				await vi.advanceTimersByTimeAsync(200);

				expect(cluster._advisoryLocks.has(42)).toBe(true);

				await bridgeA.deactivate();
				await bridgeB.deactivate();
			} finally {
				vi.useRealTimers();
			}
		});

		it('deactivate releases the leader lock and stops polling', async () => {
			const cluster = mockCluster();
			const a = cluster.createReplica();
			bridge = createNotifyBridge(a, {
				channel: 'changes', multiListener: 'advisory', lockId: 42, pollInterval: 100
			});
			await bridge.activate(platform);
			expect(cluster._advisoryLocks.get(42)).toBe(a._connId);

			await bridge.deactivate();
			expect(cluster._advisoryLocks.has(42)).toBe(false);

			// Set bridge to undefined so afterEach doesn't double-deactivate.
			bridge = undefined;
		});
	});
});
