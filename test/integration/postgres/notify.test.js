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
