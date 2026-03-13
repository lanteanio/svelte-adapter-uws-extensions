import { describe, it, expect, beforeEach, afterEach } from 'vitest';
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
		group = createGroup(client, 'lobby', { maxMembers: 5 });
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

		it('publishes join event', async () => {
			const ws = mockWs();
			await group.join(ws, platform);

			const joins = platform.published.filter((p) => p.event === 'join');
			expect(joins).toHaveLength(1);
			expect(joins[0].data).toEqual({ role: 'member' });
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
			const g = createGroup(client, 'small', { maxMembers: 2 });
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
});
