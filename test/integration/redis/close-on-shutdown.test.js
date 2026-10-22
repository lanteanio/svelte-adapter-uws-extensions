/**
 * A graceful shutdown does not wait for asynchronous close hooks: the process
 * exits once the hooks have started. Only what a hook wrote to Redis before
 * its first await survives. The child process in
 * test/integration/fixtures/close-on-shutdown.mjs starts the presence close hooks for
 * more connections than the client's in-flight pipeline budget and the
 * registry close hook and exits on the next turn of the event loop; this
 * suite checks what reached the real server.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createPresence } from '../../../src/redis/presence.js';

const PREFIX = 'inttest-shutdown:';
const FIXTURE = fileURLToPath(new URL('../fixtures/close-on-shutdown.mjs', import.meta.url));

function runChild() {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [FIXTURE, PREFIX], { env: process.env, stdio: ['pipe', 'pipe', 'inherit'] });
		let out = '';
		child.stdout.on('data', (chunk) => {
			out += chunk;
			if (out.includes('ready\n')) { out = ''; child.stdin.write('go\n'); }
		});
		child.once('error', reject);
		child.once('exit', (code) => (code === 0 ? resolve() : reject(new Error('child exited with ' + code))));
	});
}

describe('close hooks during a graceful shutdown (integration)', () => {
	let client;
	let probe;
	const events = [];
	const leaves = [];

	beforeAll(async () => {
		client = createBackendClient({ keyPrefix: PREFIX });
		await resetBackendKeys(client);
		probe = client.duplicate();
		probe.on('message', (ch, raw) => {
			if (ch === client.key('__registry-events')) events.push(JSON.parse(raw));
			if (ch === client.key('presence:events:room')) {
				const event = JSON.parse(raw);
				if (event.event === 'leave') leaves.push(event.payload.key);
			}
		});
		await probe.subscribe(client.key('__registry-events'), client.key('presence:events:room'));
	});

	afterAll(async () => {
		await probe?.quit().catch(() => probe.disconnect());
		await resetBackendKeys(client);
		await client.quit();
	});

	it('the presence entries, their leave events, the registry rows and the close event reach Redis', async () => {
		await runChild();

		const reader = createPresence(client, { key: 'id', consistencyAuditIntervalMs: 0 });
		try {
			await expect.poll(() => reader.list('room')).toEqual([]);
		} finally {
			reader.destroy();
		}
		await expect.poll(() => leaves.length).toBe(20);
		expect(new Set(leaves).size).toBe(20);
		await expect.poll(() => client.redis.exists(client.key('conns:u1'))).toBe(0);
		await expect.poll(() => client.redis.exists(client.key('regsess:app-1'))).toBe(0);
		await expect.poll(() => events.filter((e) => e.type === 'close' && e.userId === 'u1').length).toBe(1);
	});
});
