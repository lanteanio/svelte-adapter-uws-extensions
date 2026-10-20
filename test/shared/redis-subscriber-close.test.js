// closeConnection() and the buses that use it, driven against a real ioredis
// whose connection waits between two reconnect attempts: nothing listens on
// the port it connects to, or a minimal fake server that answered it went
// down, so no Redis server is needed.
import { createServer } from 'node:net';
import Redis from 'ioredis';
import { describe, it, expect, afterEach } from 'vitest';
import { closeConnection } from '../../src/shared/redis-subscriber.js';
import { createRedisClient } from '../../src/redis/index.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

const HANG = Symbol('hang');
const within = (promise, ms = 2000) => Promise.race([promise, new Promise((resolve) => setTimeout(resolve, ms, HANG))]);
const settled = (promise) => promise.then(() => 'resolved', (err) => 'rejected: ' + err.message);

async function until(check, ms = 3000) {
	const deadline = Date.now() + ms;
	while (!check()) {
		if (Date.now() > deadline) throw new Error('condition not reached');
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}

/** A localhost port nothing listens on. */
async function closedPort() {
	const server = createServer();
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	const { port } = /** @type {import('node:net').AddressInfo} */ (server.address());
	await new Promise((resolve) => server.close(resolve));
	return port;
}

/**
 * A server that answers as a Redis 7 would, as far as these tests go: INFO
 * with its version, +OK to anything else. down() closes every connection and
 * stops listening, so each client waits between reconnect attempts.
 */
async function fakeRedis({ replyInfo = true } = {}) {
	const sockets = new Set();
	let listening = true;
	const server = createServer((socket) => {
		sockets.add(socket);
		socket.on('close', () => sockets.delete(socket));
		socket.on('error', () => {});
		let buffered = '';
		socket.on('data', (chunk) => {
			buffered += chunk.toString('latin1');
			for (;;) {
				const parsed = parseCommand(buffered);
				if (!parsed) break;
				buffered = buffered.slice(parsed.length);
				const name = parsed.args[0].toLowerCase();
				if (name === 'info') {
					if (!replyInfo) continue;
					const body = '# Server\r\nredis_version:7.2.4\r\n';
					socket.write('$' + body.length + '\r\n' + body + '\r\n');
				} else {
					socket.write('+OK\r\n');
					if (name === 'quit') socket.end();
				}
			}
		});
	});
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	const { port } = /** @type {import('node:net').AddressInfo} */ (server.address());
	async function down() {
		if (!listening) return;
		listening = false;
		const closed = new Promise((resolve) => server.close(resolve));
		for (const socket of sockets) socket.destroy();
		await closed;
	}
	return { port, down };
}

/** One RESP array of bulk strings at the start of `text`, or null when incomplete. */
function parseCommand(text) {
	let at = text.indexOf('\r\n');
	if (at < 0) return null;
	const count = Number(text.slice(1, at));
	let pos = at + 2;
	const args = [];
	for (let i = 0; i < count; i++) {
		at = text.indexOf('\r\n', pos);
		if (at < 0) return null;
		const len = Number(text.slice(pos + 1, at));
		if (text.length < at + 2 + len + 2) return null;
		args.push(text.slice(at + 2, at + 2 + len));
		pos = at + 2 + len + 2;
	}
	return { args, length: pos };
}

/** createRedisClient whose duplicates the test can see. */
function trackedClient(port, options) {
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + port, autoShutdown: false, options });
	const dups = [];
	const duplicate = client.duplicate;
	client.duplicate = (overrides) => {
		const dup = duplicate(overrides);
		dups.push(dup);
		return dup;
	};
	return { client, dups };
}

const cleanup = [];
afterEach(async () => {
	while (cleanup.length) await cleanup.pop()();
});

describe('closeConnection between two reconnect attempts', () => {
	it('ends the connection, rejects what it queued, and lets quit() settle', async () => {
		const conn = new Redis({ host: '127.0.0.1', port: await closedPort(), retryStrategy: () => 10000, maxRetriesPerRequest: null });
		conn.on('error', () => {});
		cleanup.push(() => conn.disconnect());
		const queued = settled(conn.subscribe('ch'));
		await until(() => conn.status === 'reconnecting');
		let ended = false;
		conn.on('end', () => { ended = true; });

		closeConnection(conn);

		expect(conn.status).toBe('end');
		expect(await within(queued)).toBe('rejected: Connection is closed.');
		await until(() => ended);
		// An ended connection answers quit() at once; one still waiting
		// queues it behind the SUBSCRIBE for good.
		expect(await within(settled(conn.quit()))).not.toBe(HANG);
	});

	it('leaves a live connection to its own close path', async () => {
		const server = await fakeRedis();
		cleanup.push(() => server.down());
		const conn = new Redis({ host: '127.0.0.1', port: server.port });
		conn.on('error', () => {});
		await until(() => conn.status === 'ready');
		const ended = new Promise((resolve) => conn.once('end', resolve));
		closeConnection(conn);
		expect(await within(ended)).toBeUndefined();
		expect(conn.status).toBe('end');
	});
});

describe('bus deactivate() while the subscriber waits to reconnect', () => {
	it('pub/sub bus: the activation rejects and redis.quit() settles', async () => {
		const { client, dups } = trackedClient(await closedPort());
		cleanup.push(() => client.redis.disconnect());
		const bus = createPubSubBus(client);
		const activation = settled(bus.activate({ publish: () => true }));
		await until(() => dups.length === 1 && dups[0].status === 'reconnecting');

		await bus.deactivate();

		expect(await within(activation)).toBe('rejected: pubsub bus: deactivated during activation');
		expect(dups[0].status).toBe('end');
		expect(await within(settled(client.quit()))).toBe('resolved');
	});

	it('sharded bus: a follow waiting for the subscriber rejects and redis.quit() settles', async () => {
		const server = await fakeRedis();
		cleanup.push(() => server.down());
		const { client, dups } = trackedClient(server.port);
		cleanup.push(() => client.redis.disconnect());
		const bus = createShardedBus(client);
		await bus.activate({ publish: () => true });
		await until(() => dups.length === 1 && dups[0].status === 'ready');
		await server.down();
		await until(() => dups[0].status === 'reconnecting');
		const follow = settled(bus.follow('room'));
		await until(() => dups[0].offlineQueue.length > 0);

		await bus.deactivate();

		expect(await within(follow)).toMatch(/^rejected: /);
		expect(dups[0].status).toBe('end');
		expect(await within(settled(client.quit()))).toBe('resolved');
	});
});

describe('client quit with queued reconnecting connections', () => {
	const options = {
		maxRetriesPerRequest: null,
		retryStrategy: () => 10000,
		commandTimeout: undefined,
		socketTimeout: undefined
	};
	function closeTracked(client, dups) {
		for (const connection of [client.redis, ...dups]) closeConnection(connection);
	}

	it('still drains a ready connection before gracefully closing it', async () => {
		const server = await fakeRedis();
		cleanup.push(() => server.down());
		const { client, dups } = trackedClient(server.port, options);
		cleanup.push(() => closeTracked(client, dups));
		await until(() => client.redis.status === 'ready');
		const write = settled(client.redis.set('accepted', 'value'));

		expect(await within(settled(client.quit()))).toBe('resolved');

		expect(await within(write)).toBe('resolved');
		await until(() => client.redis.status === 'end');
		expect(client.redis.listenerCount('reconnecting')).toBe(0);
	});

	it('ends an initial connection attempt with a queued write before the first failure', async () => {
		const { client, dups } = trackedClient(await closedPort(), options);
		cleanup.push(() => closeTracked(client, dups));
		const write = settled(client.redis.set('never-written', 'value'));
		expect(client.redis.status).toBe('connecting');

		expect(await within(settled(client.quit()))).toBe('resolved');

		expect(await within(write)).toBe('rejected: Connection is closed.');
		await until(() => client.redis.status === 'end');
		expect(client.redis.offlineQueue.length).toBe(0);
	});

	it('ends an unanswered handshake without waiting behind its queued write', async () => {
		const server = await fakeRedis({ replyInfo: false });
		cleanup.push(() => server.down());
		const { client, dups } = trackedClient(server.port, options);
		cleanup.push(() => closeTracked(client, dups));
		const write = settled(client.redis.set('never-written', 'value'));
		await until(() => client.redis.status === 'connect');

		expect(await within(settled(client.quit()))).toBe('resolved');

		expect(await within(write)).toBe('rejected: Connection is closed.');
		await until(() => client.redis.status === 'end');
	});

	it('ends a ready connection whose socket closes just before QUIT is queued', async () => {
		const server = await fakeRedis();
		cleanup.push(() => server.down());
		const { client, dups } = trackedClient(server.port, { ...options, enableOfflineQueue: true });
		cleanup.push(() => closeTracked(client, dups));
		await until(() => client.redis.status === 'ready');
		client.redis.stream.destroy();
		const write = settled(client.redis.set('never-written', 'value'));
		expect(client.redis.status).toBe('ready');
		expect(client.redis.offlineQueue.length).toBe(1);

		expect(await within(settled(client.quit()))).toBe('resolved');

		expect(await within(write)).toBe('rejected: Connection is closed.');
		expect(client.redis.status).toBe('end');
		expect(client.redis.reconnectTimeout).toBeNull();
		expect(client.redis.listenerCount('reconnecting')).toBe(0);
	});

	it('rejects queued work on the main connection and duplicate without a reconnect', async () => {
		const { client, dups } = trackedClient(await closedPort(), options);
		cleanup.push(() => closeTracked(client, dups));
		const duplicate = client.duplicate();
		duplicate.on('error', () => {});
		const mainWrite = settled(client.redis.set('never-written', 'value'));
		const subscribe = settled(duplicate.subscribe('waiting'));
		await until(() => client.redis.status === 'reconnecting' && duplicate.status === 'reconnecting');
		expect(client.redis.offlineQueue.length).toBeGreaterThan(0);
		expect(duplicate.offlineQueue.length).toBeGreaterThan(0);

		expect(await within(settled(client.quit()))).toBe('resolved');

		expect(await within(mainWrite)).toBe('rejected: Connection is closed.');
		expect(await within(subscribe)).toBe('rejected: Connection is closed.');
		for (const connection of [client.redis, duplicate]) {
			expect(connection.status).toBe('end');
			expect(connection.offlineQueue.length).toBe(0);
			expect(connection.reconnectTimeout).toBeNull();
		}
	});

	it('ends a pending pubsub activation without requiring bus deactivation first', async () => {
		const { client, dups } = trackedClient(await closedPort(), options);
		const bus = createPubSubBus(client);
		cleanup.push(async () => { closeTracked(client, dups); await bus.deactivate(); });
		const activation = settled(bus.activate({ publish: () => true }));
		await until(() => dups.length === 1 && dups[0].status === 'reconnecting');

		expect(await within(settled(client.quit()))).toBe('resolved');

		expect(await within(activation)).toMatch(/^rejected: /);
		expect(dups[0].status).toBe('end');
		// A subscriber retry after terminal client shutdown must not open a
		// replacement connection that the completed quit no longer owns.
		expect(() => client.duplicate()).toThrow(/closed|shut down/i);
		expect(dups).toHaveLength(1);
	});

	it('rejects a sharded follow queued after a connected subscriber loses its socket', async () => {
		const server = await fakeRedis();
		cleanup.push(() => server.down());
		const { client, dups } = trackedClient(server.port, options);
		const bus = createShardedBus(client);
		cleanup.push(async () => { closeTracked(client, dups); await bus.deactivate(); });
		await bus.activate({ publish: () => true });
		await until(() => dups.length === 1 && dups[0].status === 'ready');
		await server.down();
		await until(() => dups[0].status === 'reconnecting');
		const follow = settled(bus.follow('room'));
		await until(() => dups[0].offlineQueue.length > 0);

		expect(await within(settled(client.quit()))).toBe('resolved');

		expect(await within(follow)).toMatch(/^rejected: /);
		expect(dups[0].status).toBe('end');
	});
});
