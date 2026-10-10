import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { scanKeys } from '../../../src/shared/redis-scan.js';
import { createReplay } from '../../../src/redis/replay.js';
import { createAlarmStore } from '../../../src/redis/alarm-store.js';
import { createDeadLetter } from '../../../src/redis/dead-letter.js';
import { runVerification } from '../../../docs/tools/recovery-verify.mjs';
let source, restored;
beforeAll(() => {
	source = createBackendClient({ keyPrefix: 'recovery-source:' });
	restored = createBackendClient({ keyPrefix: 'recovery-restored:' });
});
afterAll(async () => {
	if (source) { await resetBackendKeys(source); await source.quit(); }
	if (restored) { await resetBackendKeys(restored); await restored.quit(); }
});
it('verifies actual Redis dump restoration and rejects an incomplete authority group', async () => {
	await resetBackendKeys(source); await resetBackendKeys(restored);
	const replay = createReplay(source, { size: 10, ttl: 600 });
	await replay.publish({ publish() { return true; } }, 'private-room', 'message', { hidden: 'private-payload' });
	const stream = createReplay(source, { storage: 'stream', size: 10, ttl: 600 });
	await stream.publish({ publish() { return true; } }, 'stream-room', 'message', { value: 1 });
	await createAlarmStore(source).set('private-room', Date.now() + 60000, { alarmId: 'notify' });
	await createDeadLetter(source).add({ webhookId: 'delivery', topic: 'private-room', event: 'message', data: { value: 4 }, attempts: 1, error: 'failure' });
	const records = [];
	for (const key of await scanKeys(source.redis, source.key('*'))) {
		records.push({ key: key.slice(source.key('').length), expiresAt: await source.redis.pexpiretime(key), dump: (await source.redis.dumpBuffer(key)).toString('base64') });
	}
	for (const record of records) {
		await restored.redis.restore(restored.key(record.key), record.expiresAt < 0 ? 0 : record.expiresAt, Buffer.from(record.dump, 'base64'), ...(record.expiresAt < 0 ? [] : ['ABSTTL']));
	}
	const config = { schemaVersion: 1, redis: { keyPrefix: restored.key(''), families: ['replay','alarms','dead-letter'], minimumKeys: { replay: 4, alarms: 2, 'dead-letter': 3 } } };
	let report;
	if (typeof restored.redis.nodes === 'function') report = await runVerification(config, { redis: restored.redis });
	else {
		mkdirSync('source/recovery-drill', { recursive: true });
		writeFileSync('source/recovery-drill/redis-scope.json', JSON.stringify(config));
		report = JSON.parse(execFileSync(process.execPath, ['docs/tools/recovery-verify.mjs', 'source/recovery-drill/redis-scope.json'], {
			encoding: 'utf8', windowsHide: true, timeout: 30000,
			env: { ...process.env, RECOVERY_REDIS_URL: process.env.INTEGRATION_REDIS_URL }
		}));
	}
	expect(report.ok, JSON.stringify(report)).toBe(true);
	expect(report.redis.scannedKeys).toBe(records.length);
	expect(JSON.stringify(report)).not.toMatch(/private-room|private-payload|recovery-restored:/);
	await restored.redis.del(restored.key('replay:seq:{private-room}'));
	const partial = await runVerification(config, { redis: restored.redis });
	expect(partial.ok).toBe(false);
	expect(partial.redis.checks).toContainEqual({ scope: 'replay', name: 'sequence_authority', ok: false });
	await restored.redis.hdel(restored.key('alarm:{alarms}:meta'), 'private-room');
	const unpaired = await runVerification(config, { redis: restored.redis });
	expect(unpaired.redis.checks).toContainEqual({ scope: 'redis', name: 'paired_membership', ok: false });
	const artifact = JSON.stringify({ schemaVersion: 1, records });
	mkdirSync('source/recovery-drill', { recursive: true });
	writeFileSync('source/recovery-drill/redis-dump.json', artifact + '\n');
	writeFileSync('source/recovery-drill/redis-report.json', JSON.stringify({ archiveSha256: createHash('sha256').update(artifact).digest('hex'), restored: report, partial, unpaired }, null, 2) + '\n');
	await resetBackendKeys(restored);
	await restored.redis.set(restored.key('dlq:seq:{dlq}'), 'not-an-integer');
	const empty = await runVerification({ schemaVersion: 1, redis: { keyPrefix: restored.key(''), families: ['dead-letter'] } }, { redis: restored.redis });
	expect(empty.complete).toBe(true);
	expect(empty.ok).toBe(false);
	expect(empty.redis.checks).toContainEqual({ scope: 'redis', name: 'paired_membership', ok: false });
});
