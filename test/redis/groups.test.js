import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createGroup } from '../../redis/groups.js';

describe('redis groups', () => {
	let client;
	let platform;
	let group;

	beforeEach(() => {
		client = mockRedisClient('test:');
		platform = mockPlatform();
		group = createGroup(client, 'lobby', { maxMembers: 5, memberTtl: 120 });
	});

	afterEach(() => {
		group.destroy();
	});

	describe('createGroup', () => {
		it('returns a group with the expected API', () => {
			expect(typeof group.join).toBe('function');
			expect(typeof group.leave).toBe('function');
			expect(typeof group.publish).toBe('function');
			expect(typeof group.send).toBe('function');
			expect(typeof group.localMembers).toBe('function');
			expect(typeof group.count).toBe('function');
			expect(typeof group.has).toBe('function');
			expect(typeof group.close).toBe('function');
			expect(typeof group.destroy).toBe('function');
			expect(group.name).toBe('lobby');
		});

		it('throws on empty/non-string name', () => {
			expect(() => createGroup(client, '')).toThrow('non-empty string');
			expect(() => createGroup(client, null)).toThrow('non-empty string');
			expect(() => createGroup(client, 42)).toThrow('non-empty string');
		});

		it('throws on invalid maxMembers', () => {
			expect(() => createGroup(client, 'x', { maxMembers: 0 })).toThrow('positive number');
			expect(() => createGroup(client, 'x', { maxMembers: -1 })).toThrow('positive number');
		});

		it('throws on non-function hooks', () => {
			expect(() => createGroup(client, 'x', { onJoin: 'bad' })).toThrow('function');
			expect(() => createGroup(client, 'x', { onLeave: 42 })).toThrow('function');
			expect(() => createGroup(client, 'x', { onFull: {} })).toThrow('function');
			expect(() => createGroup(client, 'x', { onClose: [] })).toThrow('function');
		});
	});

	describe('join', () => {
		it('adds member and subscribes to internal topic', async () => {
			const ws = mockWs();
			const result = await group.join(ws, platform);

			expect(result).toBe(true);
			expect(ws.isSubscribed('__group:lobby')).toBe(true);
			expect(await group.count()).toBe(1);
		});

		it('sends members list to joining ws', async () => {
			const ws = mockWs();
			await group.join(ws, platform);

			const membersSent = platform.sent.filter((s) => s.event === 'members');
			expect(membersSent).toHaveLength(1);
			expect(membersSent[0].topic).toBe('__group:lobby');
			expect(membersSent[0].data).toEqual([{ role: 'member' }]);
		});

		it('publishes join event without relay: false', async () => {
			const ws = mockWs();
			await group.join(ws, platform);

			const joins = platform.published.filter((p) => p.event === 'join');
			expect(joins).toHaveLength(1);
			expect(joins[0].data).toEqual({ role: 'member' });
			expect(joins[0].options).toBeUndefined();
		});

		it('default role is member', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			expect(group.localMembers()[0].role).toBe('member');
		});

		it('accepts admin and viewer roles', async () => {
			const ws1 = mockWs();
			const ws2 = mockWs();
			await group.join(ws1, platform, 'admin');
			await group.join(ws2, platform, 'viewer');

			const roles = group.localMembers().map((m) => m.role);
			expect(roles).toContain('admin');
			expect(roles).toContain('viewer');
		});

		it('throws on invalid role', async () => {
			const ws = mockWs();
			await expect(group.join(ws, platform, 'superuser')).rejects.toThrow('invalid role');
		});

		it('is idempotent', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			const pubCount = platform.published.length;

			expect(await group.join(ws, platform)).toBe(true);
			expect(platform.published.length).toBe(pubCount);
			expect(await group.count()).toBe(1);
		});

		it('returns false when group is full', async () => {
			const g = createGroup(client, 'small', { maxMembers: 2, memberTtl: 120 });
			await g.join(mockWs(), platform);
			await g.join(mockWs(), platform);

			expect(await g.join(mockWs(), platform)).toBe(false);
			expect(await g.count()).toBe(2);
			g.destroy();
		});

		it('calls onFull when full', async () => {
			const fullCalls = [];
			const g = createGroup(client, 'small2', {
				maxMembers: 1,
				memberTtl: 120,
				onFull: (ws, role) => fullCalls.push(role)
			});
			await g.join(mockWs(), platform);
			await g.join(mockWs(), platform);

			expect(fullCalls).toEqual(['member']);
			g.destroy();
		});

		it('calls onJoin hook', async () => {
			const joinCalls = [];
			const g = createGroup(client, 'hooks', {
				onJoin: (ws, role) => joinCalls.push(role)
			});
			await g.join(mockWs(), platform, 'admin');
			expect(joinCalls).toEqual(['admin']);
			g.destroy();
		});

		it('returns false when group is closed', async () => {
			await group.close(platform);
			expect(await group.join(mockWs(), platform)).toBe(false);
		});

		it('atomic maxMembers prevents overfill under concurrent joins', async () => {
			const g = createGroup(client, 'race', { maxMembers: 1, memberTtl: 120 });

			// Both should try to join, only one should succeed
			const ws1 = mockWs();
			const ws2 = mockWs();
			const [r1, r2] = await Promise.all([
				g.join(ws1, platform),
				g.join(ws2, platform)
			]);

			// Exactly one should succeed
			const successes = [r1, r2].filter(Boolean);
			expect(successes).toHaveLength(1);
			expect(await g.count()).toBe(1);
			g.destroy();
		});
	});

	describe('leave', () => {
		it('removes member and unsubscribes', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			await group.leave(ws, platform);

			expect(await group.count()).toBe(0);
			expect(ws.isSubscribed('__group:lobby')).toBe(false);
		});

		it('publishes leave event', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			platform.reset();

			await group.leave(ws, platform);

			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(1);
			expect(leaves[0].data).toEqual({ role: 'member' });
		});

		it('calls onLeave hook', async () => {
			const leaveCalls = [];
			const g = createGroup(client, 'hooks2', {
				onLeave: (ws, role) => leaveCalls.push(role)
			});
			const ws = mockWs();
			await g.join(ws, platform);
			await g.leave(ws, platform);
			expect(leaveCalls).toEqual(['member']);
			g.destroy();
		});

		it('is safe for non-member', async () => {
			await group.leave(mockWs(), platform);
			// Should not throw
		});
	});

	describe('publish', () => {
		it('broadcasts to all members via internal topic', async () => {
			await group.join(mockWs(), platform);
			platform.reset();

			await group.publish(platform, 'chat', { text: 'hello' });

			const pubs = platform.published.filter((p) => p.topic === '__group:lobby');
			expect(pubs).toHaveLength(1);
			expect(pubs[0].event).toBe('chat');
			expect(pubs[0].data).toEqual({ text: 'hello' });
		});

		it('filtered by role: only matching role members receive', async () => {
			const ws1 = mockWs();
			const ws2 = mockWs();
			const ws3 = mockWs();
			await group.join(ws1, platform, 'admin');
			await group.join(ws2, platform, 'member');
			await group.join(ws3, platform, 'admin');
			platform.reset();

			await group.publish(platform, 'admin-msg', { secret: true }, 'admin');

			// Role-filtered uses send() not publish()
			const adminSent = platform.sent.filter((s) => s.event === 'admin-msg');
			expect(adminSent).toHaveLength(2);
			expect(adminSent[0].ws).toBe(ws1);
			expect(adminSent[1].ws).toBe(ws3);
		});

		it('is no-op when group is closed', async () => {
			await group.join(mockWs(), platform);
			await group.close(platform);
			platform.reset();

			await group.publish(platform, 'chat', {});
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('publish - cross-instance role filtering', () => {
		it('remote instances filter role-filtered events locally', async () => {
			// Simulate: instance1 publishes role-filtered, instance2 receives
			const platform2 = mockPlatform();
			const instance1 = createGroup(client, 'cross-role', { memberTtl: 120 });
			const instance2 = createGroup(client, 'cross-role', { memberTtl: 120 });

			const wsAdmin = mockWs();
			const wsMember = mockWs();

			await instance1.join(mockWs(), platform, 'admin');
			await instance2.join(wsAdmin, platform2, 'admin');
			await instance2.join(wsMember, platform2, 'member');
			platform.reset();
			platform2.reset();

			// Publish role-filtered from instance1
			await instance1.publish(platform, 'secret', { data: 'admins-only' }, 'admin');

			// The cross-instance event was published to Redis.
			// Simulate instance2 receiving the __role_filtered event via pub/sub
			// by directly checking what publishEvent sent.
			// In real deployment, the subscriber on instance2 would receive and
			// filter locally. We verify the event structure is correct.
			const publishCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				publishCalls.push(JSON.parse(msg));
				return origPublish.call(client.redis, ch, msg);
			};

			await instance1.publish(platform, 'secret2', { data: 'test' }, 'admin');

			const filtered = publishCalls.find((p) => p.event === '__role_filtered');
			expect(filtered).toBeDefined();
			expect(filtered.data.event).toBe('secret2');
			expect(filtered.data.role).toBe('admin');

			instance1.destroy();
			instance2.destroy();
		});
	});

	describe('send', () => {
		it('sends to a single member', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			platform.reset();

			group.send(platform, ws, 'whisper', { text: 'hi' });

			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0].ws).toBe(ws);
			expect(platform.sent[0].event).toBe('whisper');
		});

		it('throws for non-member ws', () => {
			expect(() => group.send(platform, mockWs(), 'msg', {}))
				.toThrow('not a member');
		});
	});

	describe('localMembers / count / has', () => {
		it('localMembers() returns array with ws and role', async () => {
			const ws = mockWs();
			await group.join(ws, platform, 'admin');

			const m = group.localMembers();
			expect(m).toHaveLength(1);
			expect(m[0].ws).toBe(ws);
			expect(m[0].role).toBe('admin');
		});

		it('count() returns total member count', async () => {
			expect(await group.count()).toBe(0);
			await group.join(mockWs(), platform);
			expect(await group.count()).toBe(1);
			await group.join(mockWs(), platform);
			expect(await group.count()).toBe(2);
		});

		it('has() returns true for members, false otherwise', async () => {
			const ws = mockWs();
			expect(group.has(ws)).toBe(false);

			await group.join(ws, platform);
			expect(group.has(ws)).toBe(true);

			await group.leave(ws, platform);
			expect(group.has(ws)).toBe(false);
		});
	});

	describe('meta', () => {
		it('get/set metadata', async () => {
			await group.setMeta({ game: 'chess', round: 1 });
			const meta = await group.getMeta();
			expect(meta.game).toBe('chess');
			expect(meta.round).toBe('1'); // Redis stores as strings
		});

		it('initial meta from options is stored', async () => {
			const g = createGroup(client, 'with-meta', { meta: { game: 'chess' } });
			// Give Redis mock a tick to process
			const meta = await g.getMeta();
			expect(meta.game).toBe('chess');
			g.destroy();
		});
	});

	describe('close', () => {
		it('publishes close event', async () => {
			await group.join(mockWs(), platform);
			platform.reset();

			await group.close(platform);

			const closes = platform.published.filter((p) => p.event === 'close');
			expect(closes).toHaveLength(1);
		});

		it('unsubscribes all members', async () => {
			const ws1 = mockWs();
			const ws2 = mockWs();
			await group.join(ws1, platform);
			await group.join(ws2, platform);

			await group.close(platform);

			expect(ws1.isSubscribed('__group:lobby')).toBe(false);
			expect(ws2.isSubscribed('__group:lobby')).toBe(false);
		});

		it('clears member list', async () => {
			await group.join(mockWs(), platform);
			await group.close(platform);

			expect(await group.count()).toBe(0);
			expect(group.localMembers()).toEqual([]);
		});

		it('calls onClose hook', async () => {
			let called = false;
			const g = createGroup(client, 'close-hook', { onClose: () => { called = true; } });
			await g.close(platform);
			expect(called).toBe(true);
			g.destroy();
		});

		it('subsequent joins return false', async () => {
			await group.close(platform);
			expect(await group.join(mockWs(), platform)).toBe(false);
		});

		it('subsequent publish is no-op', async () => {
			await group.close(platform);
			platform.reset();
			await group.publish(platform, 'chat', {});
			expect(platform.published).toHaveLength(0);
		});

		it('closing twice is safe', async () => {
			await group.close(platform);
			await group.close(platform); // should not throw
		});
	});

	describe('platform update', () => {
		it('uses the latest platform for remote event forwarding', async () => {
			const platform1 = mockPlatform();
			const platform2 = mockPlatform();
			const g = createGroup(client, 'platform-update', { memberTtl: 120 });

			// Join with platform1 to set up subscriber
			const ws1 = mockWs();
			await g.join(ws1, platform1);
			platform1.reset();

			// Join with platform2 -- subscriber should update its platform ref
			const ws2 = mockWs();
			await g.join(ws2, platform2);
			platform2.reset();

			// Simulate a remote event via Redis pub/sub
			const channel = client.key('group:platform-update:events');
			const msg = JSON.stringify({
				instanceId: 'remote-instance',
				event: 'chat',
				data: { text: 'hello' }
			});
			await client.redis.publish(channel, msg);

			// The event should have been forwarded using platform2 (latest), not platform1
			const chatEvents = platform2.published.filter((p) => p.event === 'chat');
			expect(chatEvents).toHaveLength(1);
			expect(chatEvents[0].options).toEqual({ relay: false });
			expect(platform1.published.filter((p) => p.event === 'chat')).toHaveLength(0);

			g.destroy();
		});
	});

	describe('remote close', () => {
		it('remote close clears local members and unsubscribes ws', async () => {
			// Simulate two instances sharing the same Redis
			const platform2 = mockPlatform();
			const instance1 = createGroup(client, 'remote-close', { memberTtl: 120 });
			const instance2 = createGroup(client, 'remote-close', { memberTtl: 120 });

			const ws1 = mockWs();
			const ws2 = mockWs();

			await instance1.join(ws1, platform);
			await instance2.join(ws2, platform2);

			// Instance1 closes the group
			await instance1.close(platform);

			// Instance2 should have received the close event via pub/sub
			// and cleaned up its local members
			expect(instance2.localMembers()).toEqual([]);
			expect(instance2.has(ws2)).toBe(false);
			expect(ws2.isSubscribed('__group:remote-close')).toBe(false);

			// Remote close should use relay: false
			const closeEvents = platform2.published.filter((p) => p.event === 'close');
			expect(closeEvents).toHaveLength(1);
			expect(closeEvents[0].options).toEqual({ relay: false });

			instance1.destroy();
			instance2.destroy();
		});

		it('remote close fires onClose callback', async () => {
			let closeCalled = false;
			const instance1 = createGroup(client, 'remote-close-hook', { memberTtl: 120 });
			const instance2 = createGroup(client, 'remote-close-hook', {
				memberTtl: 120,
				onClose: () => { closeCalled = true; }
			});

			await instance1.join(mockWs(), platform);
			await instance2.join(mockWs(), mockPlatform());

			await instance1.close(platform);

			expect(closeCalled).toBe(true);

			instance1.destroy();
			instance2.destroy();
		});

		it('remote close prevents further joins', async () => {
			const instance1 = createGroup(client, 'remote-close-join', { memberTtl: 120 });
			const instance2 = createGroup(client, 'remote-close-join', { memberTtl: 120 });

			await instance1.join(mockWs(), platform);
			await instance2.join(mockWs(), mockPlatform());

			await instance1.close(platform);

			// The closed key is set in Redis, so instance2 should reject new joins
			const ws = mockWs();
			expect(await instance2.join(ws, mockPlatform())).toBe(false);

			instance1.destroy();
			instance2.destroy();
		});
	});

	describe('stale member cleanup', () => {
		it('heartbeat removes stale entries from crashed instances', async () => {
			// Use a short memberTtl so the heartbeat interval is 5s (the minimum)
			const g = createGroup(client, 'cleanup-test', { memberTtl: 10 });
			const membersKey = client.key('group:cleanup-test:members');

			// Insert a stale member entry directly into Redis
			const staleData = JSON.stringify({
				role: 'member',
				instanceId: 'dead-instance',
				ts: Date.now() - 20_000 // 20 seconds ago, well past 10s TTL
			});
			await client.redis.hset(membersKey, 'dead-instance:1', staleData);

			// Add a live member so the heartbeat runs
			const ws = mockWs();
			await g.join(ws, platform);

			// Verify stale entry exists before heartbeat
			let all = await client.redis.hgetall(membersKey);
			expect(Object.keys(all)).toContain('dead-instance:1');

			// Wait for the heartbeat to fire (interval is max(memberTtlMs/3, 5000) = 5s)
			// We use real timers, so wait just past the interval.
			await new Promise((r) => setTimeout(r, 5100));

			// Stale entry should be cleaned up
			all = await client.redis.hgetall(membersKey);
			expect(Object.keys(all)).not.toContain('dead-instance:1');

			g.destroy();
		}, 10000);
	});

	describe('member expiry', () => {
		it('stale members are excluded from count', async () => {
			// Manually insert a stale member entry into Redis
			const membersKey = client.key('group:lobby:members');
			const staleData = JSON.stringify({
				role: 'member',
				instanceId: 'dead-instance',
				ts: Date.now() - 200_000 // 200 seconds ago, well past 120s TTL
			});
			await client.redis.hset(membersKey, 'dead-instance:1', staleData);

			// Add a live member
			const ws = mockWs();
			await group.join(ws, platform);

			// Count should only include the live member
			expect(await group.count()).toBe(1);
		});

		it('stale members do not block joins with maxMembers', async () => {
			const g = createGroup(client, 'expiry-test', { maxMembers: 1, memberTtl: 120 });
			const membersKey = client.key('group:expiry-test:members');

			// Insert a stale member
			const staleData = JSON.stringify({
				role: 'member',
				instanceId: 'dead-instance',
				ts: Date.now() - 200_000
			});
			await client.redis.hset(membersKey, 'dead-instance:1', staleData);

			// Should still be able to join because the stale member doesn't count
			const ws = mockWs();
			expect(await g.join(ws, platform)).toBe(true);
			g.destroy();
		});
	});
});
