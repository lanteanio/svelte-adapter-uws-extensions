import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createGroup } from '../../src/redis/groups.js';
import { createPublishRateAggregator } from '../../src/redis/publish-rate.js';
import { createTopicBroadcast } from '../../src/redis/topic-broadcast.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { MAX_AGGREGATOR_REMOTE_INSTANCES } from '../../src/shared/caps.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const RATE_CHANNEL = 'uws:pressure:rates';

/** One envelope per call: the aggregator admits per envelope, so a batch would skip the admission path. */
function publishSlice(client, instanceId, ts, topic, messagesPerSec) {
	return client.redis.publish(RATE_CHANNEL, JSON.stringify({
		instanceId, ts, slice: [{ topic, messagesPerSec }]
	}));
}

/**
 * Fill the aggregator's instance table to its tracking bound. The admission
 * rule under test only runs once the table is FULL - short of the bound every
 * id is simply inserted, and an assertion there says nothing about eviction.
 */
async function fillTrackingBound(client, ts) {
	for (let i = 0; i < MAX_AGGREGATOR_REMOTE_INSTANCES; i++) {
		await publishSlice(client, 'filler-' + i, ts, 'noise', 1);
	}
}

// Every one of these channels is reachable by anything with write access to
// the shared Redis - a co-tenant app, a broad ACL, a compromised sibling.
// The envelopes are untrusted input.

describe('connection registry inbound bus', () => {
	async function registryWithVictim(prefix, extraOptions) {
		const client = mockRedisClient(prefix);
		const platform = mockPlatform();
		const registry = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId,
			heartbeat: 60000,
			ttl: 90,
			...extraOptions
		});
		const victim = mockWs({ userId: 'v1' });
		victim.getUserData()[WS_SESSION_ID] = 'sess-1';
		await registry.hooks.open(victim, { platform });
		return { client, platform, registry, push: `${prefix}__push:${registry.instanceId}` };
	}

	it('drops an oversized envelope before JSON.parse', async () => {
		const { client, platform, registry, push } = await registryWithVictim('app1:');
		// Valid JSON, over the cap: the drop has to come from the pre-parse
		// byte guard, not from a parse failure.
		await client.redis.publish(push, JSON.stringify({ type: 'send', sessionId: 'sess-1', topic: 't', event: 'e', data: 'x'.repeat(2 * 1024 * 1024) }));
		await sleep(10);
		expect(platform.sent.length).toBe(0);
		await registry.destroy();
	});

	it('refuses a forged request whose replyTo is not an instance id', async () => {
		const { client, platform, registry, push } = await registryWithVictim('app2:');
		// This is the exfiltration shape: the request runs against a real
		// local socket with attacker-chosen event and data, and the RESULT is
		// published to a channel the attacker names.
		await client.redis.publish(push, JSON.stringify({
			type: 'request', ref: 'r1', sessionId: 'sess-1', event: 'export-keys', data: {}, replyTo: 'attacker-channel'
		}));
		await sleep(10);
		expect(platform.requested.length).toBe(0);
		await registry.destroy();
	});

	it('serves a request whose replyTo is a well-formed instance id', async () => {
		const { client, platform, registry, push } = await registryWithVictim('app3:');
		await client.redis.publish(push, JSON.stringify({
			type: 'request', ref: 'r1', sessionId: 'sess-1', event: 'ping', data: {}, replyTo: 'a1b2c3d4e5f60718'
		}));
		await sleep(10);
		expect(platform.requested.length).toBe(1);
		await registry.destroy();
	});

	it('clamps a bus-supplied request timeout to its own bound', async () => {
		const { client, platform, registry, push } = await registryWithVictim('app4:');
		await client.redis.publish(push, JSON.stringify({
			type: 'request', ref: 'r1', sessionId: 'sess-1', event: 'ping', data: {},
			replyTo: 'a1b2c3d4e5f60718', timeoutMs: 1e12
		}));
		await sleep(10);
		expect(platform.requested).toHaveLength(1);
		expect(platform.requested[0].options.timeoutMs).toBeLessThanOrEqual(60000);
		await registry.destroy();
	});

	it('does not clamp a legitimate per-call timeout down to the default', async () => {
		// The ceiling bounds a hostile peer; it must not silently rewrite an
		// app's deliberate `request(..., { timeoutMs })`. Clamping to
		// `requestTimeoutMs` made the SAME call succeed when the target socket
		// was local and fail at 5s when a reconnect moved it to a peer.
		const { client, platform, registry, push } = await registryWithVictim('app4b:');
		await client.redis.publish(push, JSON.stringify({
			type: 'request', ref: 'r1', sessionId: 'sess-1', event: 'export', data: {},
			replyTo: 'a1b2c3d4e5f60718', timeoutMs: 30000
		}));
		await sleep(10);
		expect(platform.requested).toHaveLength(1);
		expect(platform.requested[0].options.timeoutMs).toBe(30000);
		await registry.destroy();
	});

	it('lets an instance configured for longer RPCs keep its own bound as the ceiling', async () => {
		// The 60s constant is a FLOOR under the bound, not a cap on the
		// operator. An instance whose own default is 10 minutes has already
		// accepted a pending slot held that long, and clamping inbound to 60s
		// would resurrect the exact asymmetry the test above pins: the same
		// call succeeding locally and failing cross-instance.
		const { client, platform, registry, push } = await registryWithVictim('app4c:', { requestTimeoutMs: 600000 });
		await client.redis.publish(push, JSON.stringify({
			type: 'request', ref: 'r1', sessionId: 'sess-1', event: 'export', data: {},
			replyTo: 'a1b2c3d4e5f60718', timeoutMs: 300000
		}));
		await sleep(10);
		expect(platform.requested).toHaveLength(1);
		expect(platform.requested[0].options.timeoutMs).toBe(300000);
		await registry.destroy();
	});

	it('still bounds a hostile timeout at the instance default when that default is the larger term', async () => {
		const { client, platform, registry, push } = await registryWithVictim('app4d:', { requestTimeoutMs: 600000 });
		await client.redis.publish(push, JSON.stringify({
			type: 'request', ref: 'r1', sessionId: 'sess-1', event: 'ping', data: {},
			replyTo: 'a1b2c3d4e5f60718', timeoutMs: 1e12
		}));
		await sleep(10);
		expect(platform.requested).toHaveLength(1);
		expect(platform.requested[0].options.timeoutMs).toBe(600000);
		await registry.destroy();
	});

	it('refuses a present-but-invalid maxEnvelopeBytes at construction on every bus module', async () => {
		// Silently falling back to the 1 MB default gave a deployment that
		// believed it had a 64 KB bound a 1 MB one, with nothing anywhere
		// reporting the difference.
		const mk = {
			registry: (v) => createConnectionRegistry(mockRedisClient('bad1:'), { identify: () => 'u', maxEnvelopeBytes: v }),
			group: (v) => createGroup(mockRedisClient('bad2:'), 'g', { maxEnvelopeBytes: v }),
			topicBroadcast: (v) => createTopicBroadcast(mockRedisClient('bad3:'), { maxEnvelopeBytes: v })
		};
		for (const [name, build] of Object.entries(mk)) {
			for (const bad of [0, -1, NaN, '65536']) {
				expect(() => build(bad), `${name} with ${String(bad)}`)
					.toThrow(/maxEnvelopeBytes must be a positive integer/);
			}
		}
	});

	it('tells the caller when an outbound envelope is too large to publish', async () => {
		// Encoding inside the Redis try-block means the refusal is caught by
		// the failure handler: `send` resolves as if delivered, the message
		// reaches nobody, and the breaker is told Redis failed while Redis is
		// perfectly healthy. The sender is the only side that can report this.
		const client = mockRedisClient('app5:');
		const platform = mockPlatform();
		const mk = () => createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId, heartbeat: 60000, ttl: 90
		});
		const origin = mk();
		const owner = mk();
		const victim = mockWs({ userId: 'v1' });
		victim.getUserData()[WS_SESSION_ID] = 'sess-1';
		await owner.hooks.open(victim, { platform });
		await sleep(10);

		await expect(origin.send('v1', 't', 'e', { blob: 'x'.repeat(2 * 1024 * 1024) }))
			.rejects.toThrow(/maxEnvelopeBytes/);
		await expect(origin.send('v1', 't', 'e', { ok: 1 })).resolves.toBeUndefined();

		await origin.destroy();
		await owner.destroy();
	});

	it('does not leak a pending slot when an outbound request is too large', async () => {
		const client = mockRedisClient('app6:');
		const platform = mockPlatform();
		const metrics = createMetrics();
		const origin = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId, heartbeat: 60000, ttl: 90, metrics
		});
		const owner = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId, heartbeat: 60000, ttl: 90
		});
		const victim = mockWs({ userId: 'v1' });
		victim.getUserData()[WS_SESSION_ID] = 'sess-1';
		await owner.hooks.open(victim, { platform });
		await sleep(10);

		// Encoding inside the Promise executor still rejects the caller, but
		// only after registering a slot and a timer nothing clears - so the
		// request occupies the pending bound until it fires and reports a
		// `timeout` outcome for a request that was never sent.
		await expect(origin.request('v1', 'e', { blob: 'x'.repeat(2 * 1024 * 1024) }, { timeoutMs: 50 }))
			.rejects.toThrow(/maxEnvelopeBytes/);

		// The registry publishes no pending-slot count, so the leak is read
		// through the artifact it produces: waiting out the timeout window
		// and finding the spurious `timeout` outcome the leaked timer records.
		await sleep(120);
		expect(await metrics.serialize())
			.not.toMatch(/push_requests_total\{result="timeout"\}\s+[1-9]/);

		// Positive control for that negative: a request that DOES time out has
		// to produce the very sample asserted absent above. Without this, a
		// counter rename or an extra label turns the assertion green forever.
		// The owner answers normally, so it has to be made to hang first.
		platform.request = () => new Promise(() => {});
		await expect(origin.request('v1', 'e', { ok: 1 }, { timeoutMs: 30 }))
			.rejects.toThrow(/timed out/);
		expect(await metrics.serialize())
			.toMatch(/push_requests_total\{result="timeout"\}\s+1/);

		await origin.destroy();
		await owner.destroy();
	});

	it('refuses a forged owner id that would blackhole a local user', async () => {
		const client = mockRedisClient('app7:');
		const platform = mockPlatform();
		const registry = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId,
			attributes: (ws) => ({ role: ws.getUserData()?.role }),
			heartbeat: 60000,
			ttl: 90
		});
		const ws = mockWs({ userId: 'local1', role: 'admin' });
		ws.getUserData()[WS_SESSION_ID] = 'sess-1';
		await registry.hooks.open(ws, { platform });
		await sleep(10);

		platform.reset();
		await registry.sendTo({ role: 'admin' }, 'topic', 'ev', { a: 1 });
		expect(platform.sent).toHaveLength(1);

		// An owner id is the routing target for that user's traffic. One forged
		// `open` redirects a LOCALLY connected user's frames to a push channel
		// nobody listens on, and it stays dead until the user reconnects.
		//
		// Both shapes matter. A malformed id is refused by the shape pin - but
		// testing ONLY that leaves the attack live, because a forger picks a
		// WELL-FORMED id (real ones are randomBytes(8).toString('hex')) and
		// sails through the pin. What actually closes it is the sender treating
		// a locally connected user as locally owned.
		for (const forged of ['nope', 'deadbeefdeadbeef', 'ffffffffffffffff']) {
			await client.redis.publish('app7:__registry-events', JSON.stringify({
				type: 'open', userId: 'local1', instanceId: forged, attrs: { role: 'admin' }
			}));
			await sleep(20);

			platform.reset();
			await registry.sendTo({ role: 'admin' }, 'topic', 'ev', { a: 1 });
			expect(platform.sent, `forged id ${forged}`).toHaveLength(1);
		}

		await registry.destroy();
	});

	it('survives a flood of unknown registry event types', async () => {
		const client = mockRedisClient('app6:');
		const platform = mockPlatform();
		const registry = createConnectionRegistry(client, { identify: (ws) => ws.getUserData()?.userId, heartbeat: 60000, ttl: 90 });
		await registry.hooks.open(mockWs({ userId: 'v1' }), { platform });
		// An assert here would log once per envelope in production and THROW
		// in test mode, inside a subscriber callback where nothing catches it.
		for (let i = 0; i < 50; i++) {
			// A real instance id is `randomBytes(8).toString('hex')`; using a
			// well-formed one keeps this exercising the unknown-TYPE drop
			// rather than being refused one guard earlier on its shape.
			await client.redis.publish('app6:__registry-events', JSON.stringify({ type: 'bogus-' + i, userId: 'v1', instanceId: 'ffffffffffffffff' }));
		}
		await sleep(20);
		await registry.destroy();
	});
});

describe('group event channel', () => {
	it('drops a bus-only forged CLOSE and latches a real one', async () => {
		const client = mockRedisClient('g1:');
		const platform = mockPlatform();
		const group = createGroup(client, 'war-room', {});
		await group.join(mockWs({ id: 'm1' }), platform);

		// Forged: the authoritative Redis flag is unset, so this must not
		// brick the group on this instance.
		await client.redis.publish('g1:group:{war-room}:events', JSON.stringify({ instanceId: 'foreign', event: 'close', data: {} }));
		await sleep(20);
		expect(await group.join(mockWs({ id: 'm2' }), platform)).toBe(true);

		// Real close: the closing instance sets the flag BEFORE publishing.
		await client.redis.set('g1:group:{war-room}:closed', '1');
		await client.redis.publish('g1:group:{war-room}:events', JSON.stringify({ instanceId: 'foreign', event: 'close', data: {} }));
		await sleep(20);
		expect(await group.join(mockWs({ id: 'm3' }), platform)).toBe(false);
		await group.destroy?.();
	});

	it('retries a close whose authoritative flag could not be read', async () => {
		const client = mockRedisClient('g2:');
		const platform = mockPlatform();
		const group = createGroup(client, 'room', { memberTtl: 1 }); // fast heartbeat
		await group.join(mockWs({ id: 'm1' }), platform);

		await client.redis.set('g2:group:{room}:closed', '1');
		const realGet = client.redis.get.bind(client.redis);
		let failed = false;
		client.redis.get = async (k) => {
			if (!failed && String(k).endsWith(':closed')) { failed = true; throw new Error('connection reset'); }
			return realGet(k);
		};

		await client.redis.publish('g2:group:{room}:events', JSON.stringify({ instanceId: 'foreign', event: 'close', data: {} }));
		await sleep(20);
		// Dropping an unverifiable close loses a REAL close forever while the
		// heartbeat keeps rebuilding the roster the closer just deleted, so
		// it is held and retried instead.
		await sleep(700);
		client.redis.get = realGet;

		// Asserting on join() alone proves nothing: the join script reads the
		// SAME authoritative flag and refuses on its own, so the assertion
		// passes whether or not the retry ever latched. Clearing the flag
		// removes that second source, leaving only the local latch the retry
		// was supposed to set.
		await client.redis.del('g2:group:{room}:closed');
		expect(await group.join(mockWs({ id: 'm2' }), platform)).toBe(false);
		await group.destroy?.();
	});

	// Publishing AFTER destroy() proves nothing here: the subscriber reads
	// `subscribedPlatform`, which destroy() nulls, so that path is already
	// closed. The open window is an event that was dispatched BEFORE destroy()
	// and is parked behind an await when it lands - it captured the platform
	// and never re-reads it. Both tests below hold the authoritative flag read
	// open to sit inside that window.
	function gateFlagRead(client) {
		let release;
		const gate = new Promise((r) => { release = r; });
		const realGet = client.redis.get.bind(client.redis);
		client.redis.get = async (k) => {
			if (String(k).endsWith(':closed')) await gate;
			return realGet(k);
		};
		return { release: () => release(), restore: () => { client.redis.get = realGet; } };
	}

	it('does not fire onClose when destroy() lands while the flag read is in flight', async () => {
		const client = mockRedisClient('g7:');
		const platform = mockPlatform();
		let closed = 0;
		const group = createGroup(client, 'room', { onClose: () => { closed++; } });
		await group.join(mockWs({ id: 'm1' }), platform);

		// A REAL close: the flag is set, so the verification WILL latch unless
		// something stops it.
		await client.redis.set('g7:group:{room}:closed', '1');
		const gate = gateFlagRead(client);
		const before = platform.published.length;

		await client.redis.publish('g7:group:{room}:events', JSON.stringify({ instanceId: 'foreign', event: 'close', data: {} }));
		await sleep(10); // the verification has started and is parked
		group.destroy();
		gate.release();
		await sleep(30);
		gate.restore();

		expect(closed).toBe(0);
		expect(platform.published.length).toBe(before);
	});

	it('does not relay an event queued behind a parked close when destroy() lands', async () => {
		// The chain is serialized, so an ordinary event published behind a
		// parked CLOSE waits with it and reaches applyEvent after destroy().
		const client = mockRedisClient('g8:');
		const platform = mockPlatform();
		const group = createGroup(client, 'room', {});
		await group.join(mockWs({ id: 'm1' }), platform);

		const gate = gateFlagRead(client);
		await client.redis.publish('g8:group:{room}:events', JSON.stringify({ instanceId: 'foreign', event: 'close', data: {} }));
		await sleep(10);
		await client.redis.publish('g8:group:{room}:events', JSON.stringify({ instanceId: 'foreign', event: 'ping', data: { n: 1 } }));

		const before = platform.published.length;
		group.destroy();
		gate.release();
		await sleep(30);
		gate.restore();

		expect(platform.published.filter((p) => p.event === 'ping')).toHaveLength(0);
		expect(platform.published.length).toBe(before);
	});

	it('coalesces a burst of forged CLOSEs into a bounded number of flag reads', async () => {
		// Each CLOSE used to cost its own GET on the SERIALIZED event chain, so
		// a forged burst held the chain for a round trip apiece and delayed
		// legitimate events behind it.
		const client = mockRedisClient('g9:');
		const platform = mockPlatform();
		const group = createGroup(client, 'room', {});
		await group.join(mockWs({ id: 'm1' }), platform);

		let flagReads = 0;
		const realGet = client.redis.get.bind(client.redis);
		client.redis.get = async (k) => {
			if (String(k).endsWith(':closed')) flagReads++;
			return realGet(k);
		};

		// Published without awaiting between them, which is how a flood
		// actually arrives - awaiting each one lets the chain drain in between
		// and measures scheduling rather than coalescing.
		const sends = [];
		for (let i = 0; i < 50; i++) {
			sends.push(client.redis.publish('g9:group:{room}:events', JSON.stringify({ instanceId: 'foreign', event: 'close', data: { i } })));
		}
		await Promise.all(sends);
		await sleep(60);
		client.redis.get = realGet;

		// One read in flight plus a single follow-up standing in for the other
		// 49, rather than one round trip apiece.
		expect(flagReads).toBeGreaterThan(0);
		expect(flagReads).toBeLessThanOrEqual(4);
		// Still not closed - none of them was authoritative.
		expect(await group.join(mockWs({ id: 'm2' }), platform)).toBe(true);
		group.destroy();
	});

	it('still latches a REAL close that arrives during a forged burst', async () => {
		// The reason a burst cannot simply be dropped: the coalesced follow-up
		// reads the flag strictly after the arrival of every CLOSE it stands in
		// for, so a close that becomes authoritative mid-burst is still seen.
		const client = mockRedisClient('ga:');
		const platform = mockPlatform();
		let closed = 0;
		const group = createGroup(client, 'room', { onClose: () => { closed++; } });
		await group.join(mockWs({ id: 'm1' }), platform);

		const publishClose = () => client.redis.publish(
			'ga:group:{room}:events',
			JSON.stringify({ instanceId: 'foreign', event: 'close', data: {} })
		);
		for (let i = 0; i < 20; i++) await publishClose();
		// The real closer sets the flag BEFORE publishing, mid-burst.
		await client.redis.set('ga:group:{room}:closed', '1');
		for (let i = 0; i < 20; i++) await publishClose();

		await sleep(80);
		expect(closed).toBe(1);
		// Clearing the flag removes the join script's own read as a second
		// source, so this asserts the LOCAL latch and nothing else.
		await client.redis.del('ga:group:{room}:closed');
		expect(await group.join(mockWs({ id: 'm2' }), platform)).toBe(false);
		group.destroy();
	});

	it('drops an oversized or malformed group envelope', async () => {
		const client = mockRedisClient('g3:');
		const platform = mockPlatform();
		const group = createGroup(client, 'room', {});
		await group.join(mockWs({ id: 'm1' }), platform);
		const before = platform.published.length;
		await client.redis.publish('g3:group:{room}:events', JSON.stringify({ instanceId: 'foreign', event: 'msg', data: 'x'.repeat(2 * 1024 * 1024) }));
		await client.redis.publish('g3:group:{room}:events', JSON.stringify({ instanceId: 'foreign', data: 'no event field' }));
		await sleep(20);
		expect(platform.published.length).toBe(before);
		await group.destroy?.();
	});

	it('refuses to publish an outbound event every peer would drop', async () => {
		const client = mockRedisClient('g4:');
		const platform = mockPlatform();
		const group = createGroup(client, 'room', {});
		await group.join(mockWs({ id: 'm1' }), platform);
		// Bounding only the inbound side is a silent split-brain: local
		// members receive it, no remote instance does, and nothing says so.
		await expect(group.publish(platform, 'ev', { blob: 'x'.repeat(2 * 1024 * 1024) })).rejects.toThrow(/maxEnvelopeBytes/);
		await group.destroy?.();
	});
});

describe('publish-rate aggregation bus', () => {
	it('clamps a far-future publisher ts so a forged entry cannot pin itself', async () => {
		const client = mockRedisClient('pr1:');
		const platform = mockPlatform();
		platform.pressure = { topPublishers: [] };
		const agg = createPublishRateAggregator(client, { topN: 5, publishInterval: 100000, staleAfter: 50 });
		await agg.activate(platform);
		await client.redis.publish('uws:pressure:rates', JSON.stringify({
			instanceId: 'evil', ts: Date.now() + 1e12, slice: [{ topic: 'forged', messagesPerSec: 1e9 }]
		}));
		await sleep(10);
		expect(agg.rateOf('forged')).toBe(1e9);
		await sleep(80);
		expect(agg.rateOf('forged')).toBe(0);
		await agg.deactivate();
	});

	it('drops a valid but oversized envelope pre-parse', async () => {
		const client = mockRedisClient('pr2:');
		const platform = mockPlatform();
		platform.pressure = { topPublishers: [] };
		const agg = createPublishRateAggregator(client, { publishInterval: 100000 });
		await agg.activate(platform);
		// Valid JSON over the cap, so only the pre-parse guard can drop it.
		await client.redis.publish('uws:pressure:rates', JSON.stringify({
			instanceId: 'evil', ts: Date.now(), slice: [{ topic: 'x'.repeat(2 * 1024 * 1024), messagesPerSec: 1 }]
		}));
		await sleep(10);
		expect(agg.topPublishers.length).toBe(0);
		await agg.deactivate();
	});

	it('bounds the slice entries accepted from one instance', async () => {
		const client = mockRedisClient('pr3:');
		const platform = mockPlatform();
		platform.pressure = { topPublishers: [] };
		const agg = createPublishRateAggregator(client, { topN: 5, publishInterval: 100000 });
		await agg.activate(platform);
		const slice = [];
		for (let i = 0; i < 5000; i++) slice.push({ topic: 'forged-' + i, messagesPerSec: 10 });
		await client.redis.publish('uws:pressure:rates', JSON.stringify({ instanceId: 'evil', ts: Date.now(), slice }));
		await sleep(10);
		expect(agg.rateOf('forged-4999')).toBe(0);
		expect(agg.rateOf('forged-0')).toBe(10);
		await agg.deactivate();
	});

	it('evicts a stale entry to admit a real sibling once the table is full', async () => {
		const client = mockRedisClient('pr4:');
		const platform = mockPlatform();
		platform.pressure = { topPublishers: [] };
		const metrics = createMetrics();
		const agg = createPublishRateAggregator(client, { topN: 5, publishInterval: 100000, staleAfter: 60_000, metrics });
		await agg.activate(platform);

		const now = Date.now();
		// A FULL table whose entries have all aged past staleAfter. Refusing
		// every newcomer here would let a cold table filled by forged ids lock
		// the real fleet out permanently. Nothing reads the aggregate during
		// the fill, so the stale entries are not pruned out from under it.
		await fillTrackingBound(client, now - 70_000);
		await publishSlice(client, 'real', now, 'real-hot', 500);
		await sleep(20);

		expect(await metrics.serialize())
			.toMatch(/cluster_publish_rate_remote_evicted_total\s+[1-9]/);
		expect(agg.rateOf('real-hot')).toBe(500);
		await agg.deactivate();
	});
});

describe('topic-broadcast request envelopes', () => {
	it('serves a system topic, which is what requestTopic exists to carry', async () => {
		const client = mockRedisClient('tb1:');
		const served = [];
		const bc = createTopicBroadcast(client, {});
		bc.onRequest((topic) => { served.push(topic); return []; });
		await bc.broadcast('seed', 'ev', {}).catch(() => {});
		// The framework's own coordination topics are '__'-prefixed. Denying
		// them at the boundary does not stop an attacker, it breaks the
		// feature - and silently, because the origin just waits out its
		// timeout while every peer drops a request it should have served.
		await client.redis.publish('tb1:topic-broadcast:events', JSON.stringify({
			i: 'foreign', k: 'treq', ref: 'r1', t: '__signal:user-9', e: 'ping', d: {}, ms: 500
		}));
		await sleep(20);
		expect(served).toContain('__signal:user-9');
		bc.destroy?.();
	});

	it('drops a request whose topic the adapter publish path could not handle', async () => {
		const client = mockRedisClient('tb2:');
		const served = [];
		const bc = createTopicBroadcast(client, {});
		bc.onRequest((topic) => { served.push(topic); return []; });
		await bc.broadcast('seed', 'ev', {}).catch(() => {});
		for (const t of ['bad\0topic', 'quote"topic', 'back\\slash', 'x'.repeat(300)]) {
			await client.redis.publish('tb2:topic-broadcast:events', JSON.stringify({
				i: 'foreign', k: 'treq', ref: 'r1', t, e: 'ping', d: {}, ms: 500
			}));
		}
		await sleep(20);
		expect(served).toEqual(['seed']);
		bc.destroy?.();
	});

	it('rejects an invalid topic at the sender instead of timing out', async () => {
		const client = mockRedisClient('tb3:');
		const bc = createTopicBroadcast(client, {});
		bc.onRequest(() => []);
		// Validating one end only is what turns a rejected topic into a
		// mystery five-second stall.
		await expect(bc.broadcast('bad\0topic', 'ev', {})).rejects.toThrow(/invalid topic/);
		bc.destroy?.();
	});
});

describe('outbound envelope bounds', () => {
	it('refuses a role-filtered publish by the envelope it will actually send', async () => {
		const client = mockRedisClient('g5:');
		const platform = mockPlatform();
		// The role-filtered wrapper ({event, data, role}) is ~50 bytes larger
		// than the plain envelope. Pre-checking the plain shape lets a payload
		// sized between the two pass, deliver to local members, and be dropped
		// by every peer with the caller told nothing.
		const group = createGroup(client, 'room', { maxEnvelopeBytes: 4060 });
		const member = mockWs({ id: 'm1' });
		await group.join(member, platform, 'admin');
		const payload = { blob: 'x'.repeat(3980) };
		await expect(group.publish(platform, 'ev', payload, 'admin')).rejects.toThrow(/maxEnvelopeBytes/);
		await group.destroy?.();
	});
});

describe('publish-rate tracking bound', () => {
	it('never evicts a live sibling to admit a forged newcomer', async () => {
		const client = mockRedisClient('pr5:');
		const platform = mockPlatform();
		platform.pressure = { topPublishers: [] };
		// staleAfter is long, so every tracked entry stays "live" for the run.
		const metrics = createMetrics();
		const agg = createPublishRateAggregator(client, { topN: 5, publishInterval: 100000, staleAfter: 60_000, metrics });
		await agg.activate(platform);

		const now = Date.now();
		await publishSlice(client, 'real', now, 'real-hot', 500);

		// A flood of forged ids refreshing faster than publishInterval, filling
		// the table to its bound. With an always-evict rule these displace the
		// live sibling and the cluster's shedding signal goes blind; with a
		// stale-only rule the newcomers are dropped instead. `real` is the
		// oldest key, so it is the entry the eviction rule considers first.
		await fillTrackingBound(client, now);
		await publishSlice(client, 'forged-newcomer', now, 'noise', 1);
		await sleep(20);

		const out = await metrics.serialize();
		expect(out).toMatch(/cluster_publish_rate_remote_dropped_total\s+[1-9]/);
		expect(out).not.toMatch(/cluster_publish_rate_remote_evicted_total\s+[1-9]/);
		expect(agg.rateOf('real-hot')).toBe(500);
		await agg.deactivate();
	});
});
