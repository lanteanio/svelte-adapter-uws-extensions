import { createServer } from 'node:net';
import Redis from 'ioredis';
import { describe, it, expect, vi } from 'vitest';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

function encode(value) {
	if (Array.isArray(value)) return '*' + value.length + '\r\n' + value.map(encode).join('');
	if (typeof value === 'number') return ':' + value + '\r\n';
	const text = String(value);
	return '$' + Buffer.byteLength(text) + '\r\n' + text + '\r\n';
}

async function redisFixture(cluster) {
	const sockets = new Set();
	const subscribed = [];
	const server = createServer((socket) => {
		sockets.add(socket);
		socket.on('close', () => sockets.delete(socket));
		let buffered = '';
		socket.on('data', (chunk) => {
			buffered += chunk.toString();
			while (buffered) {
				const firstEnd = buffered.indexOf('\r\n');
				if (firstEnd < 0) return;
				const count = Number(buffered.slice(1, firstEnd));
				let offset = firstEnd + 2;
				const args = [];
				for (let i = 0; i < count; i++) {
					const lineEnd = buffered.indexOf('\r\n', offset);
					if (lineEnd < 0) return;
					const length = Number(buffered.slice(offset + 1, lineEnd));
					const start = lineEnd + 2;
					if (buffered.length < start + length + 2) return;
					args.push(buffered.slice(start, start + length));
					offset = start + length + 2;
				}
				buffered = buffered.slice(offset);
				const command = args[0].toLowerCase();
				if (command === 'info') socket.write(encode('redis_version:7.4.0\r\n'));
				else if (command === 'cluster' && args[1].toLowerCase() === 'myid') socket.write(encode('master'));
				else if (command === 'cluster' && args[1].toLowerCase() === 'slots') {
					socket.write(encode([[0, 16383, ['127.0.0.1', server.address().port, 'master']]]));
				} else if (command === 'ssubscribe') {
					subscribed.push(socket);
					socket.write(encode(['ssubscribe', args[1], 1]));
				} else socket.write('+OK\r\n');
			}
		});
	});
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	const redis = new Redis({
		host: '127.0.0.1',
		port: server.address().port,
		enableReadyCheck: false,
		lazyConnect: true,
		maxRetriesPerRequest: 1
	});
	redis.on('error', () => {});
	if (cluster) redis.nodes = () => [redis];
	const duplicates = [];
	const client = {
		redis,
		key: (key) => key,
		duplicate(options) {
			const duplicate = redis.duplicate(options);
			duplicate.on('error', () => {});
			duplicates.push(duplicate);
			return duplicate;
		}
	};
	return {
		client,
		subscribed,
		async close() {
			redis.disconnect();
			for (const duplicate of duplicates) duplicate.disconnect();
			for (const socket of sockets) socket.destroy();
			await new Promise((resolve) => server.close(resolve));
		}
	};
}

const frame = (data) => encode(['smessage', 'uws:sharded:room', JSON.stringify({
	instanceId: 'peer', topic: 'room', event: 'msg', data
})]);

describe.each([
	['standalone', false],
	['cluster', true]
])('sharded %s subscriber replacement', (_name, cluster) => {
	it('drops buffered messages from the replaced subscriber and delivers fresh messages once', async () => {
		const fixture = await redisFixture(cluster);
		const bus = createShardedBus(fixture.client);
		const seen = [];
		let nextActivation;
		let delivered;
		const received = new Promise((resolve) => { delivered = resolve; });
		const current = {
			publish(_topic, _event, data) { seen.push({ platform: 'new', data }); }
		};
		const replaced = {
			publish(_topic, _event, data) {
				seen.push({ platform: 'old', data });
				void bus.deactivate();
				nextActivation = bus.activate(current);
				delivered();
			}
		};
		try {
			await bus.activate(replaced);
			await bus.follow('room');
			fixture.subscribed[0].write(frame('first') + frame('stale-after-deactivate'));
			await received;
			await nextActivation;
			await bus.follow('room');
			fixture.subscribed.at(-1).write(frame('fresh'));
			await vi.waitFor(() => expect(seen).toEqual([
				{ platform: 'old', data: 'first' },
				{ platform: 'new', data: 'fresh' }
			]));
		} finally {
			await bus.deactivate();
			await fixture.close();
		}
	});
});
