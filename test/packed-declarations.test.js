import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

function run(command, args, cwd) {
	const result = spawnSync(command, args, { cwd, encoding: 'utf8', timeout: 60_000 });
	expect(result.error, result.stderr).toBeUndefined();
	expect(result.status, result.stdout + result.stderr).toBe(0);
	return result.stdout;
}

it('resolves its boot-banner version and typechecks consumers from the packed package', () => {
	const workRoot = join(root, '.cross-repo-gate');
	mkdirSync(workRoot, { recursive: true });
	const work = mkdtempSync(join(workRoot, 'declarations-'));
	try {
		const npm = process.env.npm_execpath?.endsWith('npm-cli.js')
			? process.env.npm_execpath
			: join(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
		const packed = JSON.parse(run(process.execPath, [npm, 'pack', '--json', '--ignore-scripts', '--pack-destination', work], root));
		const installed = join(work, 'node_modules/svelte-adapter-uws-extensions');
		mkdirSync(installed, { recursive: true });
		run('tar', ['-xf', packed[0].filename, '--strip-components=1', '-C', installed], work);
		const manifest = JSON.parse(readFileSync(join(work, 'node_modules/svelte-adapter-uws-extensions/package.json'), 'utf8'));
		expect(manifest.dependencies['@types/pg']).toEqual(expect.any(String));
		writeFileSync(join(work, 'version-consumer.mjs'), `
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
const entry = import.meta.resolve('svelte-adapter-uws-extensions/testing');
if (!entry.startsWith('file:') || !existsSync(fileURLToPath(entry))) throw new Error('Missing testing entry');
let directory = dirname(fileURLToPath(entry));
let version;
for (let depth = 0; depth < 12; depth++) {
  const candidate = join(directory, 'package.json');
  if (existsSync(candidate)) {
    const pkg = JSON.parse(readFileSync(candidate, 'utf8'));
    if (pkg.name === 'svelte-adapter-uws-extensions' && typeof pkg.version === 'string') {
      version = pkg.version;
      break;
    }
  }
  const parent = dirname(directory);
  if (parent === directory) break;
  directory = parent;
}
if (!version) throw new Error('Testing entry has no readable owning package version');
process.stdout.write(version);
`);
		expect(run(process.execPath, ['version-consumer.mjs'], work)).toBe(JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')).version);
		writeFileSync(join(work, 'consumer.ts'), `
import { createDistributedSession, type DistributedSessionOptions } from 'svelte-adapter-uws-extensions/redis/session';
import type { RedisClient } from 'svelte-adapter-uws-extensions/redis';
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';
import { mockPlatform } from 'svelte-adapter-uws-extensions/testing';
import { createMetrics, wireBreakerMetrics } from 'svelte-adapter-uws-extensions/prometheus';
import { createCircuitBreaker } from 'svelte-adapter-uws-extensions/breaker';
const stopBreakerMetrics: () => void = wireBreakerMetrics(createCircuitBreaker(), createMetrics());
stopBreakerMetrics();
// @ts-expect-error A breaker cannot be replaced by an arbitrary object.
wireBreakerMetrics({}, createMetrics());
const platform = mockPlatform();
platform.publishBatched([{ topic: 'room', event: 'changed', coalesceKey: 'latest' }], { compress: true });
const compressed: boolean | undefined = platform.publishedBatches[0].options?.compress;
const coalesceKey: string | undefined = platform.publishedBatches[0].messages[0].coalesceKey;
// @ts-expect-error Compression must be boolean.
platform.publishBatched([], { compress: 'true' });
// @ts-expect-error Coalescing keys must be strings.
platform.publishBatched([{ topic: 'room', event: 'changed', coalesceKey: 1 }]);
declare const redis: RedisClient;
interface SessionData { userId: string; count: number }
const options: DistributedSessionOptions<SessionData> = { forgetUserId: data => data.userId };
const explicit = createDistributedSession<SessionData>(redis, options);
const inferred = createDistributedSession(redis, { forgetUserId: (data: SessionData) => data.userId });
const defaults: DistributedSessionOptions = { forgetUserId: data => typeof data === 'string' ? data : null };
createDistributedSession(redis, defaults);
explicit.set('token', { userId: 'user', count: 1 });
inferred.set('token', { userId: 'user', count: 1 });
// @ts-expect-error Inferred writes require the complete data shape.
inferred.set('token', { userId: 5 });
// @ts-expect-error Explicit writes require the complete data shape.
explicit.set('token', { userId: 'user' });
createPgClient({ connectionString: 'postgres://localhost/example' }).query('SELECT 1').then(result => result.rows);
`);
		run(process.execPath, [join(root, 'node_modules/typescript/bin/tsc'), '--strict', '--noEmit', '--target', 'es2022', '--module', 'esnext', '--moduleResolution', 'bundler', '--skipLibCheck', 'false', '--types', 'node', 'consumer.ts'], work);
	} finally {
		rmSync(work, { recursive: true, force: true });
	}
}, 60_000);
