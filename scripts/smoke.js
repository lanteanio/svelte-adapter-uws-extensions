import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { createRedisClient } from '../src/redis/index.js';
import { createCursor } from '../src/redis/cursor.js';
import { mockRedisClient } from '../src/testing/mock-redis.js';
import { mockPlatform } from '../src/testing/mock-platform.js';
import { mockWs } from '../src/testing/mock-ws.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const require = createRequire(import.meta.url);
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
let container = null;

function docker(args) {
	const result = spawnSync('docker', args, { encoding: 'utf8', timeout: 55_000, windowsHide: true });
	if (result.error || result.status !== 0) throw new Error(`docker ${args[0]} failed: ${result.error?.message || result.stderr.trim()}`);
	return result.stdout.trim();
}
function cleanupContainer() {
	if (!container) return;
	const name = container;
	container = null;
	docker(['rm', '--force', name]);
}

async function until(check, message) {
	for (let attempt = 0; attempt < 300; attempt++) {
		if (await check()) return;
		await wait(10);
	}
	throw new Error(message);
}

export async function exerciseSharedCursors(first, second) {
	const options = { binary: false, throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 };
	const writer = createCursor(first, options);
	const reader = createCursor(second, options);
	const output = mockPlatform();
	const writerOutput = mockPlatform();
	const mover = mockWs({ id: 'mover' });
	const watcher = mockWs({ id: 'watcher' });
	try {
		await reader.attach(watcher, 'canvas', output);
		writer.update(mover, 'canvas', { x: 12, y: 34 }, writerOutput);
		await until(() => output.published.some((frame) => frame.event === 'update'), 'The reader did not receive the relayed move');
		const join = output.published.find((frame) => frame.event === 'join');
		const move = output.published.find((frame) => frame.event === 'update');
		assert.deepEqual(join.data.user, { id: 'mover' });
		assert.deepEqual(move.data.data, { x: 12, y: 34 });
		const stored = await reader.list('canvas');
		assert.equal(stored.length, 1);
		assert.equal(stored[0].key, move.data.key);
		assert.deepEqual(stored[0].data, { x: 12, y: 34 });
		await writer.remove(mover, writerOutput);
		await until(() => output.published.some((frame) => frame.event === 'remove'), 'The reader did not receive removal');
		assert.deepEqual(await reader.list('canvas'), []);
	} finally {
		writer.destroy();
		reader.destroy();
		await Promise.allSettled([first.quit(), ...(second === first ? [] : [second.quit()])]);
	}
}

function packageVersion(entry, expectedName) {
	let at = dirname(require.resolve(entry));
	for (;;) {
		try {
			const pkg = JSON.parse(readFileSync(resolve(at, 'package.json'), 'utf8'));
			if (pkg.name === expectedName) return pkg.version;
		} catch { /* walk to the package root */ }
		const parent = dirname(at);
		if (parent === at) throw new Error(`Cannot locate ${expectedName} version`);
		at = parent;
	}
}

async function main() {
	for (const signal of ['SIGINT', 'SIGTERM']) {
		process.once(signal, () => {
			try { cleanupContainer(); } finally { process.exit(signal === 'SIGINT' ? 130 : 143); }
		});
	}
	const args = process.argv.slice(2);
	if (args.some((arg) => arg !== '--mock-only')) throw new Error('Usage: npm run smoke [-- --mock-only]');
	const pkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
	console.log(`smoke: Node ${process.version}, extensions ${pkg.version}, adapter ${packageVersion('svelte-adapter-uws/plugins/cursor', 'svelte-adapter-uws')}, ioredis ${packageVersion('ioredis', 'ioredis')}`);
	const mock = mockRedisClient('smoke:');
	await exerciseSharedCursors(mock, mock);
	console.log('PASS mock Redis: two cursor instances shared join, move, snapshot and remove');
	if (args.includes('--mock-only')) {
		console.log('smoke: mock-only complete; real Redis was not exercised');
		return;
	}
	let first, second;
	try {
		docker(['info', '--format', '{{.ServerVersion}}']);
		container = 'extensions-smoke-' + randomUUID();
		docker(['run', '--detach', '--rm', '--name', container, '--publish', '127.0.0.1::6379', 'redis:7.4-alpine', 'redis-server', '--save', '', '--appendonly', 'no']);
		const port = docker(['port', container, '6379/tcp']).split(':').at(-1);
		const url = 'redis://127.0.0.1:' + port;
		const options = { url, keyPrefix: 'smoke:', autoShutdown: false, options: { connectTimeout: 3000, maxRetriesPerRequest: 1, retryStrategy: (attempt) => attempt < 4 ? 50 : null } };
		first = createRedisClient(options);
		second = createRedisClient(options);
		const info = await first.redis.info('server');
		console.log('Redis ' + /^redis_version:(.+)$/m.exec(info)?.[1].trim());
		await exerciseSharedCursors(first, second);
		console.log('PASS real Redis: independent connections shared join, move, snapshot and remove');
	} finally {
		await Promise.allSettled([first?.quit(), second?.quit()]);
		cleanupContainer();
	}
	console.log('smoke: all checks passed; clients and container closed');
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	main().catch((error) => { console.error('smoke failed: ' + error.message); process.exitCode = 1; });
}
