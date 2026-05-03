/**
 * Integration tests for postgres/notify against a real Postgres 16 server.
 *
 * Exercises the actual LISTEN/NOTIFY cross-connection path: the bridge
 * opens its own dedicated Client (not from the pool) and listens on a
 * channel; we fire pg_notify() from a pool connection and verify the
 * notification reaches the listener. The mock-based suite at
 * test/postgres/notify.test.js shallowly simulates this via direct
 * listener invocation; this file uses real Postgres delivery semantics.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createPgClient } from '../../../postgres/index.js';
import { createNotifyBridge } from '../../../postgres/notify.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 2000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(20);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe('postgres notify bridge (integration)', () => {
	let client;
	let platform;
	let bridge;

	beforeAll(() => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) {
			throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		}
		client = createPgClient({
			connectionString: url,
			autoShutdown: false
		});
	});

	beforeEach(() => {
		platform = mockPlatform();
	});

	afterEach(async () => {
		if (bridge) {
			await bridge.deactivate();
			bridge = undefined;
		}
	});

	afterAll(async () => {
		await client.end();
	});

	it('forwards a NOTIFY from a separate connection to platform.publish()', async () => {
		bridge = createNotifyBridge(client, { channel: 'inttest_changes' });
		await bridge.activate(platform);

		await client.query('SELECT pg_notify($1, $2)', [
			'inttest_changes',
			JSON.stringify({ topic: 'messages', event: 'created', data: { id: 1, text: 'hi' } })
		]);

		await waitFor(() => platform.published.length > 0);
		expect(platform.published[0]).toEqual({
			topic: 'messages',
			event: 'created',
			data: { id: 1, text: 'hi' },
			options: { relay: false }
		});
	});

	it('default parser silently drops payloads without topic/event', async () => {
		bridge = createNotifyBridge(client, { channel: 'inttest_partial' });
		await bridge.activate(platform);

		await client.query('SELECT pg_notify($1, $2)', [
			'inttest_partial',
			JSON.stringify({ data: 'no topic or event' })
		]);
		await wait(200);
		expect(platform.published).toHaveLength(0);
	});

	it('default parser silently drops malformed JSON', async () => {
		bridge = createNotifyBridge(client, { channel: 'inttest_badjson' });
		await bridge.activate(platform);

		await client.query('SELECT pg_notify($1, $2)', ['inttest_badjson', 'not-json']);
		await wait(200);
		expect(platform.published).toHaveLength(0);
	});

	it('skips notifications on a channel the bridge is not listening to', async () => {
		bridge = createNotifyBridge(client, { channel: 'inttest_a' });
		await bridge.activate(platform);

		await client.query('SELECT pg_notify($1, $2)', [
			'inttest_b',
			JSON.stringify({ topic: 'x', event: 'y', data: null })
		]);
		await wait(200);
		expect(platform.published).toHaveLength(0);
	});

	it('honours a custom parse function and remaps fields', async () => {
		bridge = createNotifyBridge(client, {
			channel: 'inttest_custom',
			parse: (payload) => {
				const row = JSON.parse(payload);
				return { topic: row.table, event: row.op, data: row.record };
			}
		});
		await bridge.activate(platform);

		await client.query('SELECT pg_notify($1, $2)', [
			'inttest_custom',
			JSON.stringify({ table: 'users', op: 'insert', record: { id: 42 } })
		]);

		await waitFor(() => platform.published.length > 0);
		expect(platform.published[0]).toMatchObject({
			topic: 'users',
			event: 'insert',
			data: { id: 42 }
		});
	});

	it('deactivate stops forwarding subsequent notifications', async () => {
		bridge = createNotifyBridge(client, { channel: 'inttest_stop' });
		await bridge.activate(platform);
		await bridge.deactivate();
		bridge = undefined;

		await client.query('SELECT pg_notify($1, $2)', [
			'inttest_stop',
			JSON.stringify({ topic: 'x', event: 'y', data: null })
		]);
		await wait(200);
		expect(platform.published).toHaveLength(0);
	});

	describe('multiListener: advisory leader election', () => {
		// pg_advisory_lock is session-level: when a session ends (the
		// bridge's dedicated client disconnects), the lock is released and
		// the next polling follower wins it. The bridge owns its own
		// connection (separate from the pool), so deactivate() ends that
		// session and triggers failover. Pinning the failover behavior
		// against real Postgres advisory locks is the point of this group;
		// the mock test exercises only the polling logic with a fake
		// pg_try_advisory_lock helper.
		const bridges = [];
		// Unique lockId per test run so a previous test's lingering session
		// (rare but possible during teardown) cannot accidentally hold the
		// lock against this run.
		let lockId;

		beforeEach(() => {
			bridges.length = 0;
			// Postgres advisory lock ids are bigint; stay well below 2^31 so
			// signed-int columns / clients don't truncate.
			lockId = Math.floor(Date.now() % 1_000_000_000) + Math.floor(Math.random() * 1000);
		});

		afterEach(async () => {
			while (bridges.length) {
				const b = bridges.pop();
				await b.deactivate().catch(() => {});
			}
		});

		function makeAdvisoryBridge(channel) {
			const platform = mockPlatform();
			const b = createNotifyBridge(client, {
				channel,
				multiListener: 'advisory',
				lockId,
				pollInterval: 200
			});
			bridges.push(b);
			return { bridge: b, platform };
		}

		it('exactly one bridge wins the lock and receives notifications', async () => {
			const channel = 'inttest_advisory_one';
			const a = makeAdvisoryBridge(channel);
			const b = makeAdvisoryBridge(channel);

			await a.bridge.activate(a.platform);
			await b.bridge.activate(b.platform);
			// Allow the initial advisoryTick on the first activate to acquire,
			// and the second activate's tick to observe the lock-held outcome.
			await wait(100);

			await client.query('SELECT pg_notify($1, $2)', [
				channel,
				JSON.stringify({ topic: 't', event: 'e', data: { v: 1 } })
			]);
			await waitFor(() => a.platform.published.length + b.platform.published.length > 0);

			// Exactly one bridge is the leader; it received the publish.
			const total = a.platform.published.length + b.platform.published.length;
			expect(total).toBe(1);
			expect(a.platform.published.length === 1 || b.platform.published.length === 1).toBe(true);
		});

		it('a follower takes over after the leader deactivates and releases the lock', async () => {
			const channel = 'inttest_advisory_failover';
			const a = makeAdvisoryBridge(channel);
			const b = makeAdvisoryBridge(channel);

			await a.bridge.activate(a.platform);
			await b.bridge.activate(b.platform);
			await wait(100);

			// Identify the leader by who received the first notification.
			await client.query('SELECT pg_notify($1, $2)', [
				channel,
				JSON.stringify({ topic: 't', event: 'first', data: { n: 1 } })
			]);
			await waitFor(() => a.platform.published.length + b.platform.published.length > 0);

			const aIsLeader = a.platform.published.length === 1;
			const leader = aIsLeader ? a : b;
			const follower = aIsLeader ? b : a;
			expect(follower.platform.published).toHaveLength(0);

			// Leader gives up: deactivate ends its session and releases the
			// advisory lock immediately.
			await leader.bridge.deactivate();
			// Drop the deactivated leader from the cleanup list.
			const idx = bridges.indexOf(leader.bridge);
			if (idx >= 0) bridges.splice(idx, 1);

			// Wait for the follower's next poll tick (pollInterval=200ms).
			// First tick after the lock is free will succeed.
			await wait(400);

			// New notification: only the surviving follower (now leader)
			// receives it. Reset its platform first so we cleanly distinguish
			// the failover delivery from the pre-failover one.
			follower.platform.reset();
			await client.query('SELECT pg_notify($1, $2)', [
				channel,
				JSON.stringify({ topic: 't', event: 'after-failover', data: { n: 2 } })
			]);
			await waitFor(() => follower.platform.published.length > 0);

			expect(follower.platform.published).toHaveLength(1);
			expect(follower.platform.published[0]).toMatchObject({
				topic: 't', event: 'after-failover', data: { n: 2 }
			});
		});

		it('a single-bridge cluster keeps working: no follower needed', async () => {
			const channel = 'inttest_advisory_solo';
			const a = makeAdvisoryBridge(channel);
			await a.bridge.activate(a.platform);
			await wait(100);

			await client.query('SELECT pg_notify($1, $2)', [
				channel,
				JSON.stringify({ topic: 't', event: 'solo', data: { v: 9 } })
			]);
			await waitFor(() => a.platform.published.length > 0);
			expect(a.platform.published[0]).toMatchObject({
				topic: 't', event: 'solo', data: { v: 9 }
			});
		});
	});

	it('forwards notifications fired from a real trigger via pg_notify()', async () => {
		// End-to-end shape: trigger calls pg_notify() inside a transaction;
		// the notification is delivered after COMMIT to a separate listening
		// connection. This exercises the canonical Postgres path the docs
		// describe, not just a hand-rolled NOTIFY.
		await client.query(`
			CREATE TABLE IF NOT EXISTS inttest_messages (
				id serial PRIMARY KEY,
				body text NOT NULL
			)
		`);
		await client.query('TRUNCATE inttest_messages RESTART IDENTITY');
		await client.query(`
			CREATE OR REPLACE FUNCTION inttest_messages_notify() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('inttest_trigger', json_build_object(
					'topic', 'messages',
					'event', lower(TG_OP),
					'data', row_to_json(NEW)
				)::text);
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql
		`);
		await client.query('DROP TRIGGER IF EXISTS inttest_messages_trg ON inttest_messages');
		await client.query(`
			CREATE TRIGGER inttest_messages_trg
				AFTER INSERT ON inttest_messages
				FOR EACH ROW EXECUTE FUNCTION inttest_messages_notify()
		`);

		bridge = createNotifyBridge(client, { channel: 'inttest_trigger' });
		await bridge.activate(platform);

		await client.query('INSERT INTO inttest_messages (body) VALUES ($1)', ['hello']);

		await waitFor(() => platform.published.length > 0);
		expect(platform.published[0]).toMatchObject({
			topic: 'messages',
			event: 'insert',
			data: { id: 1, body: 'hello' },
			options: { relay: false }
		});

		await client.query('DROP TRIGGER inttest_messages_trg ON inttest_messages');
		await client.query('DROP FUNCTION inttest_messages_notify()');
		await client.query('DROP TABLE inttest_messages');
	});
});
