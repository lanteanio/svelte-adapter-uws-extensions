import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createGroup } from '../../src/redis/groups.js';
import { createGroup as createLocalGroup } from 'svelte-adapter-uws/plugins/groups';
import { createCircuitBreaker, CircuitBrokenError } from '../../src/shared/breaker.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { installSteppedClock } from '../helpers/stepped-clock.js';

function interceptNextGroupLeave(redis, handler) {
	const originalDefine = redis.defineCommand.bind(redis);
	let commandName;
	let originalCommand;
	const defineSpy = vi.spyOn(redis, 'defineCommand').mockImplementation((name, options) => {
		originalDefine(name, options);
		if (!options.lua.includes('GROUP_LEAVE')) return;
		commandName = name;
		originalCommand = redis[name];
		redis[name] = async (...args) => {
			redis[name] = originalCommand;
			defineSpy.mockRestore();
			return handler(originalCommand.bind(redis), args);
		};
	});
	return () => {
		defineSpy.mockRestore();
		if (commandName) redis[commandName] = originalCommand;
	};
}

function failNextGroupLeave(redis) {
	return interceptNextGroupLeave(redis, async () => { throw new Error('leave script failed'); });
}

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

		it('throws on invalid memberTtl', () => {
			expect(() => createGroup(client, 'x', { memberTtl: 0 })).toThrow('positive number');
			expect(() => createGroup(client, 'x', { memberTtl: -1 })).toThrow('positive number');
			expect(() => createGroup(client, 'x', { memberTtl: NaN })).toThrow('positive number');
			expect(() => createGroup(client, 'x', { memberTtl: Infinity })).toThrow('positive number');
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

		it('stamps a key-level TTL on the members hash so an abandoned group expires', async () => {
			// The members hash has no engine TTL; a member joining then crashing
			// before the first heartbeat used to leak the roster forever. Join now
			// sets a PEXPIRE with a margin over memberTtl (heartbeat refreshes it
			// while the group stays open, so it only fires once every instance dies).
			const spy = vi.spyOn(client.redis, 'pexpire');
			await group.join(mockWs(), platform);
			const membersCall = spy.mock.calls.find(([key]) => String(key).endsWith(':members'));
			expect(membersCall, 'join should PEXPIRE the members key').toBeTruthy();
			// 2 * memberTtl(120s) in ms.
			expect(membersCall[1]).toBe(240000);
			spy.mockRestore();
		});

		it('sends members list to joining ws', async () => {
			const ws = mockWs();
			await group.join(ws, platform);

			const membersSent = platform.sent.filter((s) => s.event === 'members');
			expect(membersSent).toHaveLength(1);
			expect(membersSent[0].topic).toBe('__group:lobby');
			expect(membersSent[0].data).toEqual([{ role: 'member' }]);
		});

		it('publishes the join event worker-local and unsequenced', async () => {
			const ws = mockWs();
			await group.join(ws, platform);

			const joins = platform.published.filter((p) => p.event === 'join');
			expect(joins).toHaveLength(1);
			expect(joins[0].data).toEqual({ role: 'member', count: 1 });
			// The group's own channel reaches the other workers, so the adapter's
			// in-process relay would deliver it to them twice.
			expect(joins[0].options).toEqual({ relay: false, seq: false });
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

		it('matches the adapter onJoin admission contract', async () => {
			const decisions = [false, 'viewer', 'superuser', Promise.resolve('admin')];
			for (const [index, decision] of decisions.entries()) {
				const local = createLocalGroup(`admission-${index}`, { onJoin: () => decision });
				const localPlatform = mockPlatform();
				let localResult;
				let localError;
				try { localResult = local.join(mockWs(), localPlatform, 'member'); }
				catch (err) { localError = err; }

				const g = createGroup(client, `admission-${index}`, { onJoin: () => decision });
				const distributedPlatform = mockPlatform();
				let result;
				let error;
				try { result = await g.join(mockWs(), distributedPlatform, 'member'); }
				catch (err) { error = err; }

				expect(result).toBe(localResult);
				expect(error?.message).toBe(localError?.message);
				expect(await g.count()).toBe(local.count());
				expect(distributedPlatform.published.filter((p) => p.event === 'join')).toHaveLength(localPlatform.published.filter((p) => p.event === 'join').length);
				if (result === true) expect(g.localMembers()[0].role).toBe(local.members()[0].role);
				g.destroy();
			}
		});

		it('refuses onJoin before subscriber, roster and publish side effects', async () => {
			const duplicate = vi.spyOn(client, 'duplicate');
			const g = createGroup(client, 'refused', { onJoin: () => false });

			expect(await g.join(mockWs(), platform)).toBe(false);

			expect(duplicate).not.toHaveBeenCalled();
			expect(await client.redis.hgetall(client.key('group:{refused}:members'))).toEqual({});
			expect(platform.published).toEqual([]);
			expect(platform.sent).toEqual([]);
			g.destroy();
		});

		it('does not write or publish a refused cross-instance join', async () => {
			const allowed = createGroup(client, 'shared-admission');
			const refused = createGroup(client, 'shared-admission', { onJoin: () => false });
			const refusedPlatform = mockPlatform();
			try {
				await allowed.join(mockWs(), platform);
				expect(await refused.join(mockWs(), refusedPlatform)).toBe(false);

				expect(await allowed.count()).toBe(1);
				expect(refused.localMembers()).toEqual([]);
				expect(refusedPlatform.published).toEqual([]);
				expect(platform.published.filter((p) => p.event === 'join')).toHaveLength(1);
			} finally {
				allowed.destroy();
				refused.destroy();
			}
		});

		it('does not install membership when onJoin returns a Promise', async () => {
			const g = createGroup(client, 'async-admission', { onJoin: () => Promise.resolve('admin') });

			await expect(g.join(mockWs(), platform)).rejects.toThrow('group async-admission: onJoin must be synchronous');
			expect(await g.count()).toBe(0);
			expect(g.localMembers()).toEqual([]);
			expect(platform.published).toEqual([]);
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
			expect(leaves[0].data).toEqual({ role: 'member', count: 0 });
		});

		it('excludes stale and corrupt roster fields from the leave count', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			const membersKey = client.key('group:{lobby}:members');
			await client.redis.hset(membersKey, 'expired', JSON.stringify({ role: 'member', instanceId: 'old', ts: 0 }));
			await client.redis.hset(membersKey, 'corrupt', 'not-json');
			platform.reset();

			await group.leave(ws, platform);

			expect(platform.published.find((p) => p.event === 'leave').data).toEqual({ role: 'member', count: 0 });
			expect(await client.redis.hgetall(membersKey)).toEqual({});
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

	it('matches the adapter join and leave event payload shape', async () => {
		const local = createLocalGroup('parity');
		const localPlatform = mockPlatform();
		const localWs = mockWs();
		local.join(localWs, localPlatform, 'admin');
		local.join(mockWs(), localPlatform, 'member');

		const distributed = createGroup(client, 'parity');
		const peer = createGroup(client, 'parity');
		const peerPlatform = mockPlatform();
		const distributedWs = mockWs();
		try {
			await distributed.join(distributedWs, platform, 'admin');
			await peer.join(mockWs(), peerPlatform, 'member');
			const localJoin = localPlatform.published.filter((p) => p.event === 'join').at(-1);
			const distributedJoin = peerPlatform.published.filter((p) => p.event === 'join').at(-1);
			expect(distributedJoin.data).toEqual(localJoin.data);
			expect(distributedJoin.data.count).toBe(2);

			local.leave(localWs, localPlatform);
			await distributed.leave(distributedWs, platform);
			const localLeave = localPlatform.published.filter((p) => p.event === 'leave').at(-1);
			const distributedLeave = platform.published.filter((p) => p.event === 'leave').at(-1);
			expect(distributedLeave.data).toEqual(localLeave.data);
			expect(distributedLeave.data.count).toBe(1);
		} finally {
			distributed.destroy();
			peer.destroy();
		}
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
			const meta = await g.getMeta();
			expect(meta.game).toBe('chess');
			g.destroy();
		});

		it('retries failed initial meta write on getMeta', async () => {
			const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const hostile = 'hmset failed ' + String.fromCodePoint(0x85, 0x202e, 0x5e9);
			const origHmset = client.redis.hmset;
			let failOnce = true;
			client.redis.hmset = async (...args) => {
				if (failOnce) {
					failOnce = false;
					throw new Error(hostile);
				}
				return origHmset.apply(client.redis, args);
			};

			const g = createGroup(client, 'meta-retry', { meta: { game: 'go' } });
			await new Promise((r) => setTimeout(r, 10));

			client.redis.hmset = origHmset;
			const meta = await g.getMeta();
			expect(meta.game).toBe('go');
			expect(warnSpy).toHaveBeenCalledOnce();
			const line = warnSpy.mock.calls[0][0];
			expect(line).toMatch(/^\[lantean\/diagnostic source=svelte-adapter-uws-extensions component=runtime\.redis\.groups event=redis\.groups\.meta-initialize\.failed severity=warn\]/);
			expect(line).not.toMatch(/[\u0000-\u001f\u007f-\uffff]/);
			expect(JSON.parse(line.slice(line.indexOf('{'))).attributes).toEqual({
				group: 'meta-retry',
				error: { name: 'Error', code: null, message: hostile }
			});
			warnSpy.mockRestore();
			g.destroy();
		});

		it('setMeta clears metaInitError so getMeta does not overwrite newer data', async () => {
			const origHmset = client.redis.hmset;
			let failOnce = true;
			client.redis.hmset = async (...args) => {
				if (failOnce) {
					failOnce = false;
					throw new Error('hmset failed');
				}
				return origHmset.apply(client.redis, args);
			};

			const g = createGroup(client, 'meta-overwrite', { meta: { game: 'go' } });
			await new Promise((r) => setTimeout(r, 10));

			client.redis.hmset = origHmset;
			await g.setMeta({ game: 'chess' });

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

			// Join with platform2 - subscriber should update its platform ref
			const ws2 = mockWs();
			await g.join(ws2, platform2);
			platform2.reset();

			// Simulate a remote event via Redis pub/sub
			const channel = client.key('group:{platform-update}:events');
			const msg = JSON.stringify({
				instanceId: 'remote-instance',
				event: 'chat',
				data: { text: 'hello' }
			});
			await client.redis.publish(channel, msg);

			// The event should have been forwarded using platform2 (latest), not platform1
			const chatEvents = platform2.published.filter((p) => p.event === 'chat');
			expect(chatEvents).toHaveLength(1);
			expect(chatEvents[0].options).toEqual({ relay: false, seq: false });
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
			expect(closeEvents[0].options).toEqual({ relay: false, seq: false });

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

	describe('hooks', () => {
		it('API shape includes hooks with subscribe, unsubscribe, and close', () => {
			expect(typeof group.hooks.subscribe).toBe('function');
			expect(typeof group.hooks.unsubscribe).toBe('function');
			expect(typeof group.hooks.close).toBe('function');
		});

		it('hooks.subscribe auto-joins when subscribing to the group topic', async () => {
			const ws = mockWs();
			await group.hooks.subscribe(ws, '__group:lobby', { platform });

			expect(group.has(ws)).toBe(true);
			expect(await group.count()).toBe(1);
		});

		it('hooks.subscribe ignores unrelated topics', async () => {
			const ws = mockWs();
			await group.hooks.subscribe(ws, 'other-topic', { platform });

			expect(group.has(ws)).toBe(false);
		});

		it('hooks.unsubscribe auto-leaves the group', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			platform.reset();

			await group.hooks.unsubscribe(ws, '__group:lobby', { platform });

			expect(group.has(ws)).toBe(false);
			expect(await group.count()).toBe(0);
		});

		it('hooks.close leaves the group', async () => {
			const ws = mockWs();
			await group.join(ws, platform);
			platform.reset();

			await group.hooks.close(ws, { platform });

			expect(group.has(ws)).toBe(false);
		});
	});

	describe('stale member cleanup', () => {
		it('heartbeat removes stale entries from crashed instances', async () => {
			// Use a short memberTtl so the heartbeat interval is 5s (the minimum)
			const g = createGroup(client, 'cleanup-test', { memberTtl: 10 });
			const membersKey = client.key('group:{cleanup-test}:members');

			// Add a live member so the heartbeat runs
			const ws = mockWs();
			await g.join(ws, platform);

			// Insert a stale member entry directly into Redis after join
			// (join's Lua script cleans stale entries, so we add it afterwards)
			const staleData = JSON.stringify({
				role: 'member',
				instanceId: 'dead-instance',
				ts: Date.now() - 20_000 // 20 seconds ago, well past 10s TTL
			});
			await client.redis.hset(membersKey, 'dead-instance:1', staleData);

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

	describe('close under broken breaker', () => {
		it('throws when breaker is broken so peers learn about the close', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const g = createGroup(client, 'breaker-close', { breaker, memberTtl: 120 });

			const ws = mockWs();
			await g.join(ws, platform);

			// Break the circuit
			breaker.failure();
			expect(breaker.state).toBe('broken');

			await expect(g.close(platform)).rejects.toThrow(CircuitBrokenError);

			// isClosed should NOT be set - the group is still open
			// so new joins on a peer instance should still succeed
			breaker.reset();
			const ws2 = mockWs();
			expect(await g.join(ws2, platform)).toBe(true);

			g.destroy();
		});

		it('close succeeds after breaker recovers', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const g = createGroup(client, 'breaker-recover', { breaker, memberTtl: 120 });

			const ws = mockWs();
			await g.join(ws, platform);

			breaker.failure();
			await expect(g.close(platform)).rejects.toThrow(CircuitBrokenError);

			breaker.reset();
			await g.close(platform);

			// Now the group is actually closed
			expect(await g.join(mockWs(), platform)).toBe(false);

			g.destroy();
		});

		it('peer instance rejects joins after close persists to Redis', async () => {
			const g1 = createGroup(client, 'peer-close', { memberTtl: 120 });
			const g2 = createGroup(client, 'peer-close', { memberTtl: 120 });

			await g1.join(mockWs(), platform);
			await g2.join(mockWs(), mockPlatform());

			// g1 closes successfully - closedKey is set in Redis
			await g1.close(platform);

			// g2 should reject new joins because it reads closedKey from Redis
			expect(await g2.join(mockWs(), mockPlatform())).toBe(false);

			g1.destroy();
			g2.destroy();
		});
	});

	describe('leave when the roster removal fails', () => {
		it('still ends the local membership, publishes leave and fires onLeave', async () => {
			const leaveCalls = [];
			const g = createGroup(client, 'hdel-fail', {
				onLeave: (ws, role) => leaveCalls.push(role)
			});
			const ws = mockWs();
			await g.join(ws, platform);
			platform.reset();

			const restoreLeave = failNextGroupLeave(client.redis);
			try {
				await g.leave(ws, platform);
			} finally {
				restoreLeave();
			}

			expect(g.has(ws)).toBe(false);
			expect(platform.published.filter((p) => p.event === 'leave')).toHaveLength(1);
			expect(leaveCalls).toEqual(['member']);
			g.destroy();
		});

		it('the next heartbeat removes a roster field whose HDEL failed', async () => {
			const g = createGroup(client, 'hdel-retry', { memberTtl: 1 });
			const membersKey = client.key('group:{hdel-retry}:members');
			const ws = mockWs();
			await g.join(ws, platform);

			const restoreLeave = failNextGroupLeave(client.redis);
			try {
				await g.leave(ws, platform);
				expect(Object.keys(await client.redis.hgetall(membersKey))).toHaveLength(1);
				await new Promise((r) => setTimeout(r, 800));
			} finally {
				restoreLeave();
			}
			expect(g.has(ws)).toBe(false);
			expect(await client.redis.hgetall(membersKey)).toEqual({});
			g.destroy();
		});

		it('a leave under a broken breaker is removed from the roster once it recovers', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const leaveCalls = [];
			const g = createGroup(client, 'leave-breaker', { breaker, memberTtl: 1, onLeave: (ws, role) => leaveCalls.push(role) });
			const membersKey = client.key('group:{leave-breaker}:members');
			const ws = mockWs();
			await g.join(ws, platform);
			platform.reset();

			breaker.failure();
			await g.leave(ws, platform);
			expect(g.has(ws)).toBe(false);
			expect(platform.published.filter((p) => p.event === 'leave')).toHaveLength(1);
			expect(leaveCalls).toEqual(['member']);

			breaker.reset();
			await new Promise((r) => setTimeout(r, 800));
			expect(await client.redis.hgetall(membersKey)).toEqual({});
			expect(g.has(ws)).toBe(false);
			g.destroy();
			breaker.destroy();
		});
	});

	describe('leave keeps the roster consistent on hdel failure', () => {
		it('count() counts the member until its removal is retried', async () => {
			const ws = mockWs();
			await group.join(ws, platform);

			const restoreLeave = failNextGroupLeave(client.redis);
			try {
				await group.leave(ws, platform);
				expect(group.has(ws)).toBe(false);
				expect(await group.count()).toBe(1);
			} finally {
				restoreLeave();
			}
		});
	});

	describe('subscriber poisoning on startup failure', () => {
		it('does not poison subscriber when subscribe() fails', async () => {
			const origDuplicate = client.duplicate.bind(client);
			let failOnce = true;
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				const origSub = dup.subscribe.bind(dup);
				dup.subscribe = async (ch) => {
					if (failOnce) {
						failOnce = false;
						throw new Error('subscribe failed');
					}
					return origSub(ch);
				};
				return dup;
			};

			const g = createGroup(client, 'poison-test', { memberTtl: 120 });
			const ws1 = mockWs();

			// First join should fail because subscriber setup fails
			await expect(g.join(ws1, platform)).rejects.toThrow('subscribe failed');

			// Subscriber should not be poisoned - retry should work
			const ws2 = mockWs();
			const result = await g.join(ws2, platform);
			expect(result).toBe(true);
			expect(await g.count()).toBe(1);

			g.destroy();
		});

		it('cleans up Redis membership when subscriber setup fails', async () => {
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				dup.subscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};

			const g = createGroup(client, 'cleanup-fail', { memberTtl: 120 });
			const ws = mockWs();

			await expect(g.join(ws, platform)).rejects.toThrow('subscribe failed');

			// Redis membership should have been cleaned up
			expect(await g.count()).toBe(0);

			g.destroy();
		});
	});

	describe('close relay failure', () => {
		it('throws when close pub/sub relay fails', async () => {
			const g = createGroup(client, 'relay-fail', { memberTtl: 120 });
			const ws = mockWs();
			await g.join(ws, platform);

			// Make redis.publish fail
			const origPublish = client.redis.publish;
			client.redis.publish = async () => { throw new Error('publish failed'); };

			await expect(g.close(platform)).rejects.toThrow('publish failed');

			client.redis.publish = origPublish;
			g.destroy();
		});

		it('closedKey is still set in Redis even when relay fails', async () => {
			const g = createGroup(client, 'relay-key', { memberTtl: 120 });
			const ws = mockWs();
			await g.join(ws, platform);

			const origPublish = client.redis.publish;
			client.redis.publish = async () => { throw new Error('publish failed'); };

			await expect(g.close(platform)).rejects.toThrow('publish failed');

			client.redis.publish = origPublish;

			// closedKey should still be set, blocking new joins
			const g2 = createGroup(client, 'relay-key', { memberTtl: 120 });
			expect(await g2.join(mockWs(), platform)).toBe(false);

			g.destroy();
			g2.destroy();
		});
	});

	describe('member expiry', () => {
		it('stale members are excluded from count', async () => {
			// Manually insert a stale member entry into Redis
			const membersKey = client.key('group:{lobby}:members');
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
			const membersKey = client.key('group:{expiry-test}:members');

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

	describe('close partial-close state', () => {
		it('isClosed blocks publish even when close relay fails', async () => {
			const g = createGroup(client, 'partial-close', { memberTtl: 120 });
			const ws = mockWs();
			await g.join(ws, platform);

			const origPublish = client.redis.publish;
			client.redis.publish = async () => { throw new Error('publish failed'); };

			await expect(g.close(platform)).rejects.toThrow('publish failed');

			// isClosed is true, so publish() is a no-op even though
			// the relay failed
			platform.reset();
			await g.publish(platform, 'msg', { data: 'hi' });
			expect(platform.published).toHaveLength(0);

			client.redis.publish = origPublish;
			g.destroy();
		});

		it('local members are NOT cleaned up when close relay fails', async () => {
			const g = createGroup(client, 'partial-members', { memberTtl: 120 });
			const ws = mockWs();
			await g.join(ws, platform);

			const origPublish = client.redis.publish;
			client.redis.publish = async () => { throw new Error('publish failed'); };

			await expect(g.close(platform)).rejects.toThrow('publish failed');

			// Local members remain because cleanup happens after the relay.
			// The close() threw before reaching the cleanup code.
			expect(g.localMembers()).toHaveLength(1);
			expect(g.has(ws)).toBe(true);

			client.redis.publish = origPublish;
			g.destroy();
		});

		it('second close attempt succeeds and cleans up after relay recovery', async () => {
			const g = createGroup(client, 'close-retry', { memberTtl: 120 });
			const ws = mockWs();
			await g.join(ws, platform);

			let failCount = 0;
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				failCount++;
				if (failCount <= 1) throw new Error('publish failed');
				return origPublish.call(client.redis, ch, msg);
			};

			await expect(g.close(platform)).rejects.toThrow('publish failed');

			// Retry - closedKey is already set, so this takes the "already closed" path
			await g.close(platform);

			expect(g.localMembers()).toHaveLength(0);
			expect(g.has(ws)).toBe(false);

			client.redis.publish = origPublish;
			g.destroy();
		});
	});

	describe('join rollback with hdel failure', () => {
		it('subscribe failure before JOIN_SCRIPT leaves no Redis entry', async () => {
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				dup.subscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};

			const g = createGroup(client, 'sub-before-join', { memberTtl: 120 });
			const ws = mockWs();

			await expect(g.join(ws, platform)).rejects.toThrow('subscribe failed');

			expect(g.has(ws)).toBe(false);
			expect(g.localMembers()).toHaveLength(0);

			const count = await g.count();
			expect(count).toBe(0);

			g.destroy();
		});
	});

	describe('breaker accounting in groups', () => {
		it('join records success() after JOIN_SCRIPT eval succeeds', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const g = createGroup(client, 'breaker-join', { breaker, memberTtl: 120 });

			breaker.failure();
			expect(breaker.failures).toBe(1);

			const ws = mockWs();
			await g.join(ws, platform);
			expect(breaker.failures).toBe(0);

			g.destroy();
			breaker.destroy();
		});

		it('join records failure() when JOIN_SCRIPT eval throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const g = createGroup(client, 'breaker-join-fail', { breaker, memberTtl: 120 });

			const origEval = client.redis.eval;
			client.redis.eval = async () => { throw new Error('Redis down'); };

			const ws = mockWs();
			await expect(g.join(ws, platform)).rejects.toThrow('Redis down');
			expect(breaker.failures).toBe(1);

			client.redis.eval = origEval;
			g.destroy();
			breaker.destroy();
		});

		it('leave records success() when hdel succeeds', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const g = createGroup(client, 'breaker-leave', { breaker, memberTtl: 120 });

			const ws = mockWs();
			await g.join(ws, platform);
			breaker.failure();
			expect(breaker.failures).toBe(1);

			await g.leave(ws, platform);
			expect(breaker.failures).toBe(0);

			g.destroy();
			breaker.destroy();
		});

		it('leave records failure() when hdel throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const g = createGroup(client, 'breaker-leave-fail', { breaker, memberTtl: 120 });

			const ws = mockWs();
			await g.join(ws, platform);

			const restoreLeave = failNextGroupLeave(client.redis);
			try {
				await g.leave(ws, platform);
				expect(breaker.failures).toBe(1);
			} finally {
				restoreLeave();
			}
			g.destroy();
			breaker.destroy();
		});

		it('close records success() on Redis set', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const g = createGroup(client, 'breaker-close', { breaker, memberTtl: 120 });

			breaker.failure();
			expect(breaker.failures).toBe(1);

			await g.close(platform);
			expect(breaker.failures).toBe(0);

			g.destroy();
			breaker.destroy();
		});

		it('ensureSubscriber failure records breaker failure', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				dup.subscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};

			const g = createGroup(client, 'breaker-sub-fail', { breaker, memberTtl: 120 });
			const ws = mockWs();

			await expect(g.join(ws, platform)).rejects.toThrow('subscribe failed');
			expect(breaker.failures).toBe(1);

			g.destroy();
			breaker.destroy();
		});
	});

	describe('close retry relays to peers', () => {
		it('retried close re-publishes close event and clears members', async () => {
			const platform2 = mockPlatform();
			const instance1 = createGroup(client, 'retry-relay', { memberTtl: 120 });
			const instance2 = createGroup(client, 'retry-relay', { memberTtl: 120 });

			const ws1 = mockWs();
			const ws2 = mockWs();
			await instance1.join(ws1, platform);
			await instance2.join(ws2, platform2);

			let failCount = 0;
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				failCount++;
				if (failCount <= 1) throw new Error('publish failed');
				return origPublish.call(client.redis, ch, msg);
			};

			await expect(instance1.close(platform)).rejects.toThrow('publish failed');

			expect(instance1.localMembers()).toHaveLength(1);

			await instance1.close(platform);

			expect(instance1.localMembers()).toHaveLength(0);

			expect(instance2.localMembers()).toHaveLength(0);
			expect(instance2.has(ws2)).toBe(false);

			client.redis.publish = origPublish;
			instance1.destroy();
			instance2.destroy();
		});

		it('retried close blocks new joins on peer instances', async () => {
			const g1 = createGroup(client, 'retry-join-block', { memberTtl: 120 });
			const g2 = createGroup(client, 'retry-join-block', { memberTtl: 120 });

			await g1.join(mockWs(), platform);
			await g2.join(mockWs(), mockPlatform());

			let failCount = 0;
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				failCount++;
				if (failCount <= 1) throw new Error('publish failed');
				return origPublish.call(client.redis, ch, msg);
			};

			await expect(g1.close(platform)).rejects.toThrow('publish failed');

			client.redis.publish = origPublish;
			await g1.close(platform);

			expect(await g2.join(mockWs(), mockPlatform())).toBe(false);

			g1.destroy();
			g2.destroy();
		});

		it('close relay failure still blocks local publish', async () => {
			const g = createGroup(client, 'relay-block', { memberTtl: 120 });
			const ws = mockWs();
			await g.join(ws, platform);

			const origPublish = client.redis.publish;
			client.redis.publish = async () => { throw new Error('publish failed'); };

			await expect(g.close(platform)).rejects.toThrow('publish failed');

			platform.reset();
			await g.publish(platform, 'msg', { data: 'hello' });
			expect(platform.published).toHaveLength(0);

			client.redis.publish = origPublish;
			g.destroy();
		});
	});

	describe('join/close TOCTOU race', () => {
		it('join atomically checks closedKey inside the Lua script', async () => {
			const g1 = createGroup(client, 'toctou', { memberTtl: 120 });
			const g2 = createGroup(client, 'toctou', { memberTtl: 120 });

			await g1.join(mockWs(), platform);
			await g1.close(platform);

			const ws = mockWs();
			const result = await g2.join(ws, platform);
			expect(result).toBe(false);
			expect(g2.has(ws)).toBe(false);

			g1.destroy();
			g2.destroy();
		});

		it('join returns false and sets local isClosed when Lua detects closed', async () => {
			const g = createGroup(client, 'toctou-local', { memberTtl: 120 });

			await client.redis.set(client.key('group:{toctou-local}:closed'), '1');

			const ws1 = mockWs();
			expect(await g.join(ws1, platform)).toBe(false);

			const ws2 = mockWs();
			expect(await g.join(ws2, platform)).toBe(false);

			g.destroy();
		});
	});

	describe('leave racing a heartbeat', () => {
		it('a heartbeat during the leave does not write the departing member back', async () => {
			// memberTtl 1 beats every 333ms. The removal reaches Redis at once but
			// its reply is held past a beat, so a beat that still counted the
			// member would re-add it after the removal and leave a member nobody
			// holds until memberTtl reaps it.
			const g = createGroup(client, 'leave-race', { memberTtl: 1 });
			const membersKey = client.key('group:{leave-race}:members');
			const ws = mockWs();
			await g.join(ws, platform);
			const restoreLeave = interceptNextGroupLeave(client.redis, async (leave, args) => {
				const result = await leave(...args);
				await new Promise((r) => setTimeout(r, 500));
				return result;
			});
			try {
				await g.leave(ws, platform);
			} finally {
				restoreLeave();
			}
			expect(g.has(ws)).toBe(false);
			expect(await client.redis.hgetall(membersKey)).toEqual({});
			g.destroy();
		});
	});

	describe('concurrent leave and join', () => {
		it('a second leave waits for the first, so a failed first removal leaves no phantom', async () => {
			const leaveCalls = [];
			const g = createGroup(client, 'double-leave', { memberTtl: 3, onLeave: (ws, role) => leaveCalls.push(role) });
			const membersKey = client.key('group:{double-leave}:members');
			const ws = mockWs();
			await g.join(ws, platform);
			platform.reset();

			const hdel = client.redis.hdel;
			client.redis.hdel = async (...args) => {
				const result = await hdel.apply(client.redis, args);
				await new Promise((r) => setTimeout(r, 1500));
				return result;
			};
			const restoreLeave = interceptNextGroupLeave(client.redis, async () => {
				await new Promise((r) => setTimeout(r, 100));
				throw new Error('leave script failed');
			});
			try {
				await Promise.all([g.leave(ws, platform), g.leave(ws, platform)]);
				// Past the next beat (memberTtl 3 beats every second), well short of
				// the 3 s a rewritten member would take to be reaped.
				await new Promise((r) => setTimeout(r, 1300));
			} finally {
				restoreLeave();
				client.redis.hdel = hdel;
			}
			expect(g.has(ws)).toBe(false);
			expect(await client.redis.hgetall(membersKey)).toEqual({});
			expect(leaveCalls).toEqual(['member']);
			expect(platform.published.filter((p) => p.event === 'leave')).toHaveLength(1);
			g.destroy();
		});

		it('a join during an in-flight leave waits for it and joins again', async () => {
			const g = createGroup(client, 'rejoin', { memberTtl: 120 });
			const membersKey = client.key('group:{rejoin}:members');
			const ws = mockWs();
			await g.join(ws, platform);
			const firstId = Object.keys(await client.redis.hgetall(membersKey))[0];

			const restoreLeave = interceptNextGroupLeave(client.redis, async (leave, args) => {
				const result = await leave(...args);
				await new Promise((r) => setTimeout(r, 500));
				return result;
			});
			let rejoined;
			try {
				const leaving = g.leave(ws, platform);
				await new Promise((r) => setTimeout(r, 50));
				rejoined = await g.join(ws, platform);
				await leaving;
			} finally {
				restoreLeave();
			}
			expect(rejoined).toBe(true);
			expect(g.has(ws)).toBe(true);
			const fields = Object.keys(await client.redis.hgetall(membersKey));
			expect(fields).toHaveLength(1);
			expect(fields[0]).not.toBe(firstId);
			g.destroy();
		});

		it('a heartbeat that never replies holds neither the leave nor a rejoin, and the removal lands after recovery', async () => {
			const g = createGroup(client, 'silent-beat', { memberTtl: 1 });
			const membersKey = client.key('group:{silent-beat}:members');
			const ws = mockWs();
			await g.join(ws, platform);
			const firstId = Object.keys(await client.redis.hgetall(membersKey))[0];

			const pipeline = client.redis.pipeline;
			let silent = true;
			let beatSent;
			const beatSeen = new Promise((r) => { beatSent = r; });
			client.redis.pipeline = () => {
				const batch = pipeline.call(client.redis);
				return new Proxy(batch, {
					get(target, prop) {
						if (prop !== 'exec') return target[prop];
						if (!silent) return () => target.exec();
						return () => {
							beatSent();
							return new Promise(() => {});
						};
					}
				});
			};
			const timeout = (ms) => new Promise((r) => setTimeout(() => r('timed out'), ms));
			try {
				await beatSeen;
				silent = false;
				const leaving = g.leave(ws, platform);
				expect(await Promise.race([g.join(ws, platform), timeout(2000)])).toBe(true);
				expect(await Promise.race([leaving.then(() => 'left'), timeout(2000)])).toBe('left');
				// memberTtl 1 beats every 333 ms: the queued removal rides the next one.
				await new Promise((r) => setTimeout(r, 800));
			} finally {
				client.redis.pipeline = pipeline;
			}
			const fields = Object.keys(await client.redis.hgetall(membersKey));
			expect(fields).not.toContain(firstId);
			expect(fields).toHaveLength(1);
			expect(g.has(ws)).toBe(true);
			g.destroy();
		});

		it('a leave waits for a heartbeat already sent, so its HDEL lands after the beat', async () => {
			// A connection that queues commands while it is not ready can
			// deliver a pipeline built before the leave after the leave's own
			// HDEL. The mock models that with a beat whose exec is delayed.
			const g = createGroup(client, 'late-beat', { memberTtl: 1 });
			const membersKey = client.key('group:{late-beat}:members');
			const ws = mockWs();
			await g.join(ws, platform);

			const pipeline = client.redis.pipeline;
			let beatSent;
			const beatSeen = new Promise((r) => { beatSent = r; });
			client.redis.pipeline = () => {
				const batch = pipeline.call(client.redis);
				return new Proxy(batch, {
					get(target, prop) {
						if (prop !== 'exec') return target[prop];
						return async () => {
							beatSent();
							await new Promise((r) => setTimeout(r, 200));
							return target.exec();
						};
					}
				});
			};
			try {
				await beatSeen;
				await g.leave(ws, platform);
				// Long enough for the delayed beat to land in either order.
				await new Promise((r) => setTimeout(r, 300));
			} finally {
				client.redis.pipeline = pipeline;
			}
			expect(await client.redis.hgetall(membersKey)).toEqual({});
			g.destroy();
		});
	});

	describe('small memberTtl heartbeat interval', () => {
		// A virtual clock and captured heartbeat intervals, installed through the
		// runtime seam, so real event-loop delays cannot move a member past its
		// TTL. The cached now() lags the exact clock and refreshes every 1010 ms,
		// as it does in production.
		let clock;
		beforeEach(() => { clock = installSteppedClock(); });
		afterEach(() => resetRuntimeEnv());

		const flush = async () => {
			for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
		};
		const beat = async () => {
			clock.tick();
			await flush();
		};

		it('memberTtl: 1 does not expire live members before first heartbeat', async () => {
			const g = createGroup(client, 'short-ttl', { memberTtl: 1 });
			try {
				expect(clock.intervals.map((h) => h.ms)).toEqual([333]);
				const ws = mockWs();
				await g.join(ws, platform);
				expect(await g.count()).toBe(1);

				// The first beat is due one interval after the group was created.
				clock.advance(333);
				expect(await g.count()).toBe(1);
				await beat();

				// 1332 ms after join: only the beat's refresh keeps it live. The
				// cached clock refreshed at 1010 ms, so a stamp read from it at
				// the beat would read as 1010 ms old here.
				clock.advance(999);
				expect(await g.count()).toBe(1);

				for (let i = 0; i < 6; i++) {
					await beat();
					clock.advance(333);
					expect(g.has(ws)).toBe(true);
					expect(await g.count()).toBe(1);
				}
			} finally {
				g.destroy();
			}
		});

		it('memberTtl: 1 expires a member one TTL after its last beat on the exact clock', async () => {
			const g = createGroup(client, 'short-ttl-expiry', { memberTtl: 1 });
			try {
				const ws = mockWs();
				await g.join(ws, platform);
				clock.advance(333);
				await beat();

				// No further beat runs.
				clock.advance(1000);
				expect(await g.count()).toBe(1);
				clock.advance(1);
				expect(await g.count()).toBe(0);
			} finally {
				g.destroy();
			}
		});

		it('memberTtl: 1 keeps the live members of another instance through cleanup', async () => {
			const a = createGroup(client, 'short-ttl-shared', { memberTtl: 1 });
			const b = createGroup(client, 'short-ttl-shared', { memberTtl: 1 });
			const [beatA, beatB] = clock.intervals;
			const membersKey = client.key('group:{short-ttl-shared}:members');
			try {
				await a.join(mockWs(), platform);
				await b.join(mockWs(), mockPlatform());
				clock.advance(333);
				await beat();

				// B refreshes its member 20 ms before A's beat, with the cached
				// clock refreshing in between, so the two beats would stamp and
				// judge on cached readings 1010 ms apart.
				clock.advance(660);
				beatB.cb();
				await flush();
				clock.advance(20);
				beatA.cb();
				await flush();

				expect(Object.keys(await client.redis.hgetall(membersKey))).toHaveLength(2);
				expect(await a.count()).toBe(2);
			} finally {
				a.destroy();
				b.destroy();
			}
		});
	});

	describe('local durations under a wall-clock step', () => {
		// The exact wall clock jumps (an NTP step) while monotonic time runs on.
		let clock;
		beforeEach(() => {
			clock = { t: 1_000_000, wallOffset: 0, intervals: [] };
			setRuntimeEnv({
				clock: { now: () => clock.t + clock.wallOffset, monotonic: () => clock.t, wallEpoch: () => clock.t + clock.wallOffset },
				timers: {
					setInterval: (cb, ms) => { const h = { cb, ms, cleared: false, unref() {} }; clock.intervals.push(h); return h; },
					clearInterval: (h) => { if (h) h.cleared = true; }
				}
			});
		});
		afterEach(() => resetRuntimeEnv());
		const flush = async () => {
			for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
		};

		it('a queued removal is retried for one memberTtl of monotonic time when the wall clock steps back', async () => {
			const g = createGroup(client, 'step-back', { memberTtl: 1 });
			const [heartbeat] = clock.intervals;
			const ws = mockWs();
			await g.join(ws, platform);

			const pipeline = client.redis.pipeline;
			const restoreLeave = failNextGroupLeave(client.redis);
			let retries = 0;
			client.redis.pipeline = () => {
				const batch = pipeline.call(client.redis);
				return new Proxy(batch, {
					get(target, prop) {
						if (prop === 'hdel') return (...args) => { retries++; return target.hdel(...args); };
						if (prop === 'exec') return async () => { throw new Error('beat failed'); };
						return target[prop];
					}
				});
			};
			try {
				await g.leave(ws, platform);
				clock.wallOffset = -3_600_000;
				for (let i = 0; i < 5; i++) {
					clock.t += 333;
					heartbeat.cb();
					await flush();
				}
			} finally {
				restoreLeave();
				client.redis.pipeline = pipeline;
				g.destroy();
			}
			// Beats at 333, 666 and 999 ms retry it; at 1332 ms it is past memberTtl.
			expect(retries).toBe(3);
		});

		it('a leave still waits for a beat in flight when the wall clock steps forward', async () => {
			const g = createGroup(client, 'step-forward', { memberTtl: 1 });
			const membersKey = client.key('group:{step-forward}:members');
			const [heartbeat] = clock.intervals;
			const ws = mockWs();
			await g.join(ws, platform);

			const pipeline = client.redis.pipeline;
			let release;
			const held = new Promise((r) => { release = r; });
			client.redis.pipeline = () => {
				const batch = pipeline.call(client.redis);
				return new Proxy(batch, {
					get(target, prop) {
						if (prop !== 'exec') return target[prop];
						return async () => { await held; return target.exec(); };
					}
				});
			};
			let direct = 0;
			const restoreLeave = interceptNextGroupLeave(client.redis, (leave, args) => { direct++; return leave(...args); });
			try {
				clock.t += 333;
				heartbeat.cb();
				clock.wallOffset = 3_600_000;
				const leaving = g.leave(ws, platform);
				await flush();
				release();
				await leaving;
			} finally {
				client.redis.pipeline = pipeline;
				restoreLeave();
				g.destroy();
			}
			// The leave script ran once the beat settled instead of being queued.
			expect(direct).toBe(1);
			expect(await client.redis.hgetall(membersKey)).toEqual({});
		});
	});

	describe('close() propagates membersKey deletion failure', () => {
		it('close() throws when redis.del(membersKey) fails', async () => {
			const g = createGroup(client, 'del-fail', { memberTtl: 120 });
			const ws = mockWs();
			await g.join(ws, platform);

			const origDel = client.redis.del;
			client.redis.del = async () => { throw new Error('del failed'); };

			await expect(g.close(platform)).rejects.toThrow('del failed');

			client.redis.del = origDel;
			g.destroy();
		});

		it('count() returns 0 after successful close()', async () => {
			const g = createGroup(client, 'del-ok', { memberTtl: 120 });
			await g.join(mockWs(), platform);
			await g.close(platform);

			expect(await g.count()).toBe(0);
			g.destroy();
		});
	});

	describe('join ws.subscribe failure with hdel failure', () => {
		it('throws when ws.subscribe fails and rollback hdel also fails', async () => {
			const g = createGroup(client, 'double-fail', { memberTtl: 120 });
			const ws = mockWs();
			ws.close();

			const origHdel = client.redis.hdel;
			client.redis.hdel = async () => { throw new Error('hdel failed'); };

			await expect(g.join(ws, platform)).rejects.toThrow('join rollback failed');

			client.redis.hdel = origHdel;
			g.destroy();
		});

		it('returns false when ws.subscribe fails but rollback hdel succeeds', async () => {
			const g = createGroup(client, 'sub-fail-clean', { memberTtl: 120 });
			const ws = mockWs();
			ws.close();

			const result = await g.join(ws, platform);
			expect(result).toBe(false);
			expect(g.has(ws)).toBe(false);
			expect(await g.count()).toBe(0);

			g.destroy();
		});
	});

	describe('join snapshot failure rolls back', () => {
		it('throws and rolls back when post-join hgetall fails', async () => {
			const g = createGroup(client, 'snapshot-fail', { memberTtl: 120 });
			const ws = mockWs();

			const origHgetall = client.redis.hgetall;
			client.redis.hgetall = async () => { throw new Error('hgetall failed'); };

			await expect(g.join(ws, platform)).rejects.toThrow('hgetall failed');

			expect(g.has(ws)).toBe(false);
			expect(g.localMembers()).toHaveLength(0);

			client.redis.hgetall = origHgetall;
			expect(await g.count()).toBe(0);

			g.destroy();
		});

		it('does not publish join or leave events on snapshot failure', async () => {
			const g = createGroup(client, 'snapshot-leave', { memberTtl: 120 });
			const ws = mockWs();

			const origHgetall = client.redis.hgetall;
			client.redis.hgetall = async () => { throw new Error('hgetall failed'); };

			platform.reset();
			await expect(g.join(ws, platform)).rejects.toThrow('hgetall failed');

			const joins = platform.published.filter((p) => p.event === 'join');
			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(joins).toHaveLength(0);
			expect(leaves).toHaveLength(0);

			client.redis.hgetall = origHgetall;
			g.destroy();
		});

		it('throws rollback error when snapshot fails and hdel also fails', async () => {
			const g = createGroup(client, 'snapshot-hdel-fail', { memberTtl: 120 });
			const ws = mockWs();

			const origHgetall = client.redis.hgetall;
			const origHdel = client.redis.hdel;
			client.redis.hgetall = async () => { throw new Error('hgetall failed'); };
			client.redis.hdel = async () => { throw new Error('hdel failed'); };

			await expect(g.join(ws, platform)).rejects.toThrow('join rollback failed');

			client.redis.hgetall = origHgetall;
			client.redis.hdel = origHdel;
			g.destroy();
		});

		it('does not publish join or leave when rollback hdel fails', async () => {
			const g = createGroup(client, 'snapshot-no-leave', { memberTtl: 120 });
			const ws = mockWs();

			const origHgetall = client.redis.hgetall;
			const origHdel = client.redis.hdel;
			client.redis.hgetall = async () => { throw new Error('hgetall failed'); };
			client.redis.hdel = async () => { throw new Error('hdel failed'); };

			platform.reset();
			await expect(g.join(ws, platform)).rejects.toThrow('join rollback failed');

			const joins = platform.published.filter((p) => p.event === 'join');
			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(joins).toHaveLength(0);
			expect(leaves).toHaveLength(0);

			client.redis.hgetall = origHgetall;
			client.redis.hdel = origHdel;
			g.destroy();
		});
	});

	describe('onLeave on leave', () => {
		it('calls onLeave once when breaker is broken', async () => {
			const leaveCalls = [];
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const g = createGroup(client, 'leave-no-cb', {
				breaker,
				memberTtl: 120,
				onLeave: (ws, role) => leaveCalls.push(role)
			});

			const ws = mockWs();
			await g.join(ws, platform);

			breaker.failure();
			await g.leave(ws, platform);

			expect(leaveCalls).toEqual(['member']);
			expect(g.has(ws)).toBe(false);

			g.destroy();
			breaker.destroy();
		});

		it('calls onLeave only after successful hdel and local removal', async () => {
			const leaveCalls = [];
			const g = createGroup(client, 'leave-success-cb', {
				onLeave: (ws, role) => leaveCalls.push(role)
			});

			const ws = mockWs();
			await g.join(ws, platform);

			await g.leave(ws, platform);
			expect(leaveCalls).toEqual(['member']);
			expect(g.has(ws)).toBe(false);

			g.destroy();
		});
	});

	describe('destroy under subscriber quit() failure', () => {
		it('falls back to disconnect() without throwing TypeError when quit rejects', async () => {
			// Regression: destroy() previously called subscriber.quit().catch(() => subscriber.disconnect())
			// then immediately set subscriber = null. If the catch fired AFTER the
			// null-assignment, accessing subscriber.disconnect threw TypeError.
			// The fix captures into a local before nulling the outer reference.
			let disconnectCalled = false;
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				const sub = origDuplicate(overrides);
				sub.quit = () => Promise.reject(new Error('connection lost'));
				sub.disconnect = () => { disconnectCalled = true; };
				return sub;
			};

			const g = createGroup(client, 'destroy-fallback', { maxMembers: 5, memberTtl: 120 });
			await g.join(mockWs(), platform);

			// Capture any unhandled rejection that escapes destroy().
			let unhandled = null;
			const onRejection = (err) => { unhandled = err; };
			process.once('unhandledRejection', onRejection);

			g.destroy();

			// Allow the rejected quit() promise + .catch chain to settle.
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();

			process.removeListener('unhandledRejection', onRejection);

			expect(disconnectCalled).toBe(true);
			expect(unhandled).toBeNull();
		});
	});
});
