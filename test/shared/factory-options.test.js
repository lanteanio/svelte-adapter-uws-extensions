// Every public factory runs its options through the shared validator: an
// unknown key warns (once, with a did-you-mean), a known key with a value the
// factory cannot honor throws naming what it accepts, and null stays absent.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { MAX_TIMER_INTERVAL_MS } from '../../src/shared/options.js';
import { createRedisClient } from '../../src/redis/index.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createReplay, migrateReplayToStream } from '../../src/redis/replay.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { createCompositeRateLimit } from '../../src/redis/composite-ratelimit.js';
import { createGroup } from '../../src/redis/groups.js';
import { createDistributedSession } from '../../src/redis/session.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createLeader } from '../../src/redis/leader.js';
import { createDistributedLock } from '../../src/redis/lock.js';
import { createClockSkewSampler } from '../../src/redis/clock-skew.js';
import { createClusterClock } from '../../src/redis/clock.js';
import { createNtpSource } from '../../src/redis/ntp-source.js';
import { createPublishRateAggregator } from '../../src/redis/publish-rate.js';
import { createAlarmStore as createRedisAlarmStore } from '../../src/redis/alarm-store.js';
import { createCrdtCluster } from '../../src/redis/crdt.js';
import { createSmoothCluster } from '../../src/redis/smooth-cluster.js';
import { createTopicBroadcast } from '../../src/redis/topic-broadcast.js';
import { createRedisFence } from '../../src/redis/fence.js';
import { createFunctionLibrary } from '../../src/redis/functions.js';
import { createIdempotencyStore as createRedisIdempotency } from '../../src/redis/idempotency.js';
import { createUpgradeBucket, createLocalUpgradeBucket } from '../../src/redis/upgrade-bucket.js';
import { createRetryBudget, createWebhookBreaker } from '../../src/redis/webhook-controls.js';
import { createDeadLetter as createRedisDeadLetter } from '../../src/redis/dead-letter.js';
import { createPgClient } from '../../src/postgres/index.js';
import { createAlarmStore as createPgAlarmStore } from '../../src/postgres/alarm-store.js';
import { createIdempotencyStore as createPgIdempotency } from '../../src/postgres/idempotency.js';
import { createJobQueue } from '../../src/postgres/jobs.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { createNotifyBridge } from '../../src/postgres/notify.js';
import { createReplay as createPgReplay } from '../../src/postgres/replay.js';
import { createDeadLetter as createPgDeadLetter } from '../../src/postgres/dead-letter.js';
import { postgresMigrations, migrate, validateSchema } from '../../src/postgres/migrations.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { createDegradationPolicy } from '../../src/shared/degradation.js';
import { createAdmissionControl } from '../../src/shared/admission.js';
import { createBusValidator } from '../../src/shared/bus-validate.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { capabilityCookie } from '../../src/capability-cookie.js';
import { createMetrics, wirePublishRateMetrics, wireClusterPublishRateMetrics } from '../../src/prometheus/index.js';

const MAX = MAX_TIMER_INTERVAL_MS;
const dimensions = {
	ip: { points: 2, interval: 1000, keyBy: 'ip' },
	connection: { points: 2, interval: 1000, keyBy: 'connection' }
};
const pool = () => ({ query: async () => ({ rows: [] }), on() {}, end: async () => {} });
const overflow = (surface) => new RegExp(`^${surface} must be no greater than \\d+ (milliseconds|seconds), because Node stores a timer delay`);
const whole = (surface) => new RegExp(`^${surface} must be a whole number of `);

let warn;
let created;
let redis;
let pg;
let metrics;
beforeEach(() => {
	warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
	created = [];
	redis = mockRedisClient();
	pg = mockPgClient();
	metrics = createMetrics();
});
afterEach(async () => {
	for (const value of created.reverse()) {
		try {
			if (typeof value?.destroy === 'function') await value.destroy();
			else if (typeof value?.stop === 'function') await value.stop();
			else if (typeof value?.deactivate === 'function') await value.deactivate();
			else if (typeof value?.quit === 'function') await value.quit();
		} catch { /* construction cleanup is best-effort */ }
	}
	warn.mockRestore();
});

function keep(value) {
	if (value && typeof value.then !== 'function') created.push(value);
	return value;
}
function unknownWarnings() {
	return warn.mock.calls.map(([message]) => String(message)).filter((m) => m.startsWith('[extensions] unknown '));
}

/**
 * name, build(options), a near-miss key and the key it should suggest,
 * values that must throw (with the message they must carry), and values
 * that must keep working - null for every option that accepted it before.
 */
const cases = [
	['capabilityCookie', (o) => capabilityCookie({ secret: '0123456789abcdef', ...o }), ['cookiename', 'cookieName'],
		[[{ secure: 'false' }, 'capability-cookie: secure must be true or false - got "false" (string).']],
		[{ secure: false }, { secure: true }, { metrics: null }]],
	['createRedisClient', (o) => createRedisClient({ autoShutdown: false, options: { lazyConnect: true }, ...o }), ['keyprefix', 'keyPrefix'],
		[[{ autoShutdown: 'false' }, 'redis client: autoShutdown must be true or false'], [{ keyPrefix: 5 }, 'redis client: keyPrefix must be a string'], [{ url: 6379 }, 'redis client: url must be a string']],
		[{ url: null }, { keyPrefix: null }, { autoShutdown: null }, { options: null }]],
	['createPubSubBus', (o) => createPubSubBus(redis, o), ['chanel', 'channel'],
		[[{ channel: 5 }, 'pubsub bus: channel must be a string'], [{ allowSystemTopics: 'yes' }, 'pubsub bus: allowSystemTopics must be true or false'], [{ breaker: 'on' }, 'pubsub bus: breaker must be an object (or omitted)']],
		[{ breaker: null }, { metrics: null }, { breaker: false }, { channel: null }, { allowSystemTopics: false }]],
	['createShardedBus', (o) => createShardedBus(redis, o), ['channelPrefx', 'channelPrefix'],
		[[{ allowSystemTopics: 1 }, 'sharded bus: allowSystemTopics must be true or false']],
		[{ breaker: null }, { metrics: null }]],
	['createReplay', (o) => createReplay(redis, o), ['sise', 'size'],
		[[{ metrics: 'on' }, 'redis replay: metrics must be an object (or omitted)'], [{ storage: 'list' }, "redis replay: storage must be 'sortedset' or 'stream'"]],
		[{ breaker: null }, { metrics: null }, { storage: 'stream' }, { storage: 'sortedset' }]],
	['createPresence', (o) => createPresence(redis, o), ['hearbeat', 'heartbeat'],
		[
			[{ ttl: 1.0001 }, whole('redis presence: ttl')],
			[{ keyspaceNotifications: 'yes' }, 'redis presence: keyspaceNotifications must be true or false'],
			[{ consistencyAuditIntervalMs: '1000' }, 'redis presence: consistencyAuditIntervalMs must be a number >= 0 (milliseconds)'],
			[{ consistencyAuditIntervalMs: -1 }, 'redis presence: consistencyAuditIntervalMs must be a number >= 0 (milliseconds)'],
			// Refused by the adapter's presence wire codec, as by its in-memory plugin.
			[{ binary: 'false' }, 'presence: binary must be true or false'],
			// Refused with the in-memory plugin's messages, which ours contain.
			[{ select: false }, 'presence: select must be a function'],
			[{ select: 'identity' }, 'presence: select must be a function'],
			[{ transient: 'typing' }, 'presence: transient must be an array of field names - got "typing" (string).'],
			[{ transient: ['typing', 5] }, 'presence: transient entries must be field-name strings - got 5 (number).'],
			[{ clientUpdateFields: 'role' }, 'presence: clientUpdateFields must be an array of field names - got "role" (string).'],
			[{ clientUpdateFields: ['role', 1] }, 'presence: clientUpdateFields entries must be field-name strings - got 1 (number).'],
			[{ heartbeat: MAX + 1 }, overflow('redis presence: heartbeat')],
			[{ topicThrottle: MAX + 1 }, overflow('redis presence: topicThrottle')]
		],
		[
			{ select: null }, { breaker: null }, { metrics: null }, { ttl: 1.5 }, { transient: [] }, { binary: false }, { consistencyAuditIntervalMs: 0 }, { heartbeat: MAX },
			{ topicThrottle: MAX }, { transient: null }, { clientUpdateFields: null }, { clientUpdateFields: [] },
			// Values the adapter's in-memory presence plugin accepts.
			{ key: 0 }
		]],
	['createCursor', (o) => createCursor(redis, o), ['throtle', 'throttle'],
		[
			[{ snapshotIntervalMs: Math.floor(MAX / 2) + 1 }, overflow('redis cursor: snapshotIntervalMs')],
			[{ ttl: 1.5 }, whole('redis cursor: ttl')],
			[{ ttl: Math.floor(MAX / 1000) + 1 }, overflow('redis cursor: ttl')],
			// Refused by the adapter's cursor wire codec, as by its in-memory plugin.
			[{ binary: 0 }, 'cursor: binary must be true or false'],
			[{ dictionary: 'no' }, 'cursor: dictionary must be true or false'],
			// Refused with the in-memory plugin's messages, which ours contain.
			[{ throttle: MAX + 1 }, overflow('redis cursor: throttle')],
			[{ topicThrottle: MAX + 1 }, overflow('redis cursor: topicThrottle')],
			[{ viewport: { enabled: 'true' } }, 'cursor: viewport.enabled must be true or false - got "true" (string). Only false turns it off'],
			[{ backpressure: { enabled: 1, maxBufferedBytes: 5 } }, 'cursor: backpressure.enabled must be true or false - got 1 (number).'],
			[{ backpressure: { enabled: null, maxBufferedBytes: 5 } }, 'cursor: backpressure.maxBufferedBytes is set but backpressure.enabled is not']
		],
		[
			{ select: null }, { breaker: null }, { metrics: null }, { binary: false }, { dictionary: false }, { ttl: 30 },
			{ throttle: MAX }, { topicThrottle: MAX },
			// A nested null is not set, as at the top level.
			{ viewport: { enabled: true, padding: null, cell: null } }, { backpressure: { enabled: true, maxBufferedBytes: null } },
			{ viewport: { enabled: null } }, { backpressure: { enabled: false } }
		]],
	// Fractional intervals and blocks are accepted, as the in-memory limiter
	// accepts them; their Redis expiries are rounded up.
	['createRateLimit', (o) => createRateLimit(redis, { points: 2, interval: 1000, ...o }), ['blockduration', 'blockDuration'],
		[],
		[
			{ breaker: null }, { metrics: null }, { blockDuration: 0 }, { refill: null },
			{ interval: 1000.5 }, { interval: 1000.25, refill: 'sliding' }, { interval: 1000.7, refill: 'gcra' }, { blockDuration: 0.5 }, { blockDuration: 1.5, refill: 'gcra' }
		]],
	['createCompositeRateLimit', (o) => createCompositeRateLimit(redis, { dimensions, ...o }), ['tennant', 'tenant'],
		[],
		[
			{ breaker: null }, { metrics: null },
			{ dimensions: { ...dimensions, ip: { ...dimensions.ip, interval: 999.5, blockDuration: 0.5 } } }
		]],
	['createGroup', (o) => createGroup(redis, 'room', o), ['maxMember', 'maxMembers'],
		[
			[{ memberTtl: 1.0001 }, whole('redis group: memberTtl')],
			[{ memberTtl: Math.floor(MAX / 1000) + 1 }, overflow('redis group: memberTtl')],
			// Refused with the in-memory plugin's message, which ours contains.
			[{ meta: 'vip' }, 'group: meta must be a plain object - got "vip" (string)'],
			[{ meta: ['a'] }, 'group: meta must be a plain object'],
			[{ meta: new Map() }, 'group: meta must be a plain object'],
			[{ meta: false }, 'group: meta must be a plain object']
		],
		[{ meta: null }, { onJoin: null }, { breaker: null }, { metrics: null }, { memberTtl: 1.5 }, { meta: {} }, { meta: Object.create(null) }]],
	['createDistributedSession', (o) => createDistributedSession(redis, o), ['ttlms', 'ttlMs'],
		[[{ ttlMs: 1500.5 }, whole('session: ttlMs')], [{ refreshOnGet: 'no' }, 'session: refreshOnGet must be true or false']],
		[{ breaker: null }, { metrics: null }, { refreshOnGet: false }, { onLoadError: null }]],
	['createConnectionRegistry', (o) => createConnectionRegistry(redis, { identify: () => null, ...o }), ['heartbaet', 'heartbeat'],
		[
			[{ ttl: 1.5 }, whole('registry: ttl')],
			[{ heartbeat: MAX + 1 }, overflow('registry: heartbeat')],
			[{ requestTimeoutMs: MAX + 1 }, overflow('registry: requestTimeoutMs')]
		],
		[{ breaker: null }, { metrics: null }, { keyPrefix: null }]],
	['createLeader', (o) => createLeader(redis, o), ['leasems', 'leaseMs'],
		[
			[{ leaseMs: 30000.5 }, whole('leader: leaseMs')],
			[{ leaseMs: 3 * MAX + 3 }, overflow('leader: leaseMs')],
			[{ leaseMs: MAX + 10, renewMs: MAX + 1 }, overflow('leader: renewMs')]
		],
		[{ breaker: null }, { metrics: null }, { leaseMs: 3 * MAX + 3, renewMs: 1000 }]],
	['createDistributedLock', (o) => createDistributedLock(redis, o), ['maxWait', 'maxWaitMs'],
		[
			[{ defaultTtlMs: 1500.5 }, whole('lock: defaultTtlMs')],
			[{ retryDelayMs: Math.floor(MAX / 1.25) + 1 }, overflow('lock: retryDelayMs')],
			[{ heartbeatMs: MAX + 1 }, overflow('lock: heartbeatMs')],
			[{ defaultTtlMs: 3 * MAX + 3 }, overflow('lock: defaultTtlMs')]
		],
		[{ breaker: null }, { metrics: null }, { defaultTtlMs: 3 * MAX + 3, heartbeatMs: 1000 }]],
	['createClockSkewSampler', (o) => createClockSkewSampler(redis, { immediate: false, ...o }), ['interval', 'intervalMs'],
		[[{ intervalMs: MAX + 1 }, overflow('clock-skew: intervalMs')], [{ immediate: 'no' }, 'clock-skew: immediate must be true or false']],
		[{ breaker: null }, { metrics: null }, { intervalMs: MAX }]],
	['createClusterClock', (o) => createClusterClock(redis, { immediate: false, ...o }), ['samples_', 'samples'],
		[[{ intervalMs: MAX + 1 }, overflow('cluster clock: intervalMs')], [{ immediate: 0 }, 'cluster clock: immediate must be true or false']],
		[{ breaker: null }, { metrics: null }]],
	['createNtpSource', (o) => createNtpSource({ host: 'localhost', ...o }), ['timeoutms', 'timeoutMs'],
		[[{ timeoutMs: MAX + 1 }, overflow('ntp: timeoutMs')]],
		[{ transport: null }, { port: null }]],
	['createPublishRateAggregator', (o) => createPublishRateAggregator(redis, o), ['publishIntervall', 'publishInterval'],
		[[{ channel: 5 }, 'publish-rate: channel must be a string'], [{ publishInterval: MAX + 1 }, overflow('publish-rate: publishInterval')]],
		[{ breaker: null }, { metrics: null }, { topicPublishRatePerSec: null }]],
	['createAlarmStore', (o) => createRedisAlarmStore(redis, o), ['duebatch', 'dueBatch'],
		[[{ breaker: 'on' }, 'redis alarm store: breaker must be an object (or omitted)']],
		[{ breaker: null }, { metrics: null }]],
	['createCrdtCluster', (o) => createCrdtCluster(redis, o), ['persistLease', 'persistLeaseMs'],
		[
			[{ persistLeaseMs: 0 }, 'crdt cluster: persistLeaseMs must be an integer >= 1 - got 0 (number).'],
			[{ persistLeaseMs: '6000' }, 'crdt cluster: persistLeaseMs must be an integer >= 1'],
			[{ persistLeaseMs: 1500.5 }, 'crdt cluster: persistLeaseMs must be an integer >= 1'],
			[{ persistLeaseMs: 3 * MAX + 1 }, overflow('crdt cluster: persistLeaseMs')],
			[{ onError: 'log' }, 'crdt cluster: onError must be a function']
		],
		[{ persistLeaseMs: 6000 }, { persistLeaseMs: null }, { breaker: null }, { onError: null }]],
	['createSmoothCluster', (o) => createSmoothCluster(redis, o), ['leasems', 'leaseMs'],
		[
			[{ leaseMs: 1500.5 }, 'smooth cluster: leaseMs must be an integer >= 1'],
			[{ snapshotTtlMs: -1 }, 'smooth cluster: snapshotTtlMs must be an integer >= 1'],
			[{ onError: true }, 'smooth cluster: onError must be a function']
		],
		[{ leaseMs: 10000 }, { leaseMs: null }, { breaker: null }]],
	['createTopicBroadcast', (o) => createTopicBroadcast(redis, o), ['requestTimeout', 'requestTimeoutMs'],
		[
			[{ heartbeat: 0 }, 'topic-broadcast: heartbeat must be a number > 0 (milliseconds) - got 0 (number).'],
			[{ heartbeat: '10000' }, 'topic-broadcast: heartbeat must be a number > 0 (milliseconds)'],
			[{ heartbeat: MAX + 1 }, overflow('topic-broadcast: heartbeat')],
			[{ requestTimeoutMs: MAX + 1 }, overflow('topic-broadcast: requestTimeoutMs')],
			[{ presenceTtlMs: -1 }, 'topic-broadcast: presenceTtlMs must be a number > 0'],
			[{ onError: 'log' }, 'topic-broadcast: onError must be a function']
		],
		[{ heartbeat: 2500.5 }, { heartbeat: null }, { breaker: null }, { metrics: null }, { keyPrefix: null }]],
	['createRedisFence', (o) => createRedisFence(redis, o), ['keyprefix', 'keyPrefix'], [], [{}]],
	['createFunctionLibrary', (o) => createFunctionLibrary(redis, '#!lua name=test', o), ['breakr', 'breaker'],
		[[{ metrics: 1 }, 'redis functions: metrics must be an object (or omitted)']],
		[{ breaker: null }, { metrics: null }]],
	['createIdempotencyStore', (o) => createRedisIdempotency(redis, o), ['acquireTTL', 'acquireTtl'],
		[[{ breaker: 'on' }, 'redis idempotency: breaker must be an object (or omitted)']],
		[{ breaker: null }, { metrics: null }, { maxResultBytes: null }]],
	['createUpgradeBucket', (o) => createUpgradeBucket(redis, { perMinute: 1, ...o }), ['perminute', 'perMinute'],
		[],
		// Fractional blocks are accepted, as the local bucket accepts them.
		[{ breaker: null }, { metrics: null }, { blockDuration: 1000 }, { blockDuration: 0.5 }, { siege: { perMinute: 1, blockDuration: 1.5 } }]],
	['createLocalUpgradeBucket', (o) => createLocalUpgradeBucket({ perMinute: 1, ...o }), ['maxEntrie', 'maxEntries'],
		[[{ metrics: 'on' }, 'local upgrade-bucket: metrics must be an object (or omitted)']],
		// The shared options type declares emergency and breaker.
		[{ blockDuration: 0.5 }, { emergency: {}, breaker: null }]],
	['createRetryBudget', (o) => createRetryBudget(redis, o), ['capacty', 'capacity'],
		[[{ breaker: 7 }, 'redis retry budget: breaker must be an object (or omitted)']],
		[{ breaker: null }]],
	['createWebhookBreaker', (o) => createWebhookBreaker(redis, o), ['resetms', 'resetMs'],
		[[{ breaker: 'x' }, 'redis webhook breaker: breaker must be an object (or omitted)']],
		[{ breaker: null }]],
	['createDeadLetter', (o) => createRedisDeadLetter(redis, o), ['forgetUserID', 'forgetUserId'],
		[[{ metrics: 'on' }, 'redis dead-letter: metrics must be an object (or omitted)']],
		[{ breaker: null }, { metrics: null }]],
	['createPgClient', (o) => createPgClient({ pool: pool(), ...o }), ['maxInflight', 'maxInFlight'],
		[[{ autoShutdown: 'false' }, 'postgres: autoShutdown must be true or false'], [{ connectionString: 5 }, 'postgres: connectionString must be a string'], [{ options: 'x' }, 'postgres: options must be an object']],
		[{ autoShutdown: false }, { metrics: null }, { maxInFlight: null }]],
	['createAlarmStore', (o) => createPgAlarmStore(pg, { autoMigrate: false, ...o }), ['duebatch', 'dueBatch'],
		[[{ autoMigrate: 'false' }, 'postgres alarm store: autoMigrate must be true or false']],
		[{ breaker: null }, { metrics: null }]],
	['createIdempotencyStore', (o) => createPgIdempotency(pg, { autoMigrate: false, cleanupInterval: 0, ...o }), ['acquireTTL', 'acquireTtl'],
		[[{ autoMigrate: 0 }, 'postgres idempotency: autoMigrate must be true or false']],
		[{ breaker: null }, { metrics: null }]],
	['createJobQueue', (o) => createJobQueue(pg, { autoMigrate: false, ...o }), ['visibilitytimeout', 'visibilityTimeout'],
		[[{ autoMigrate: 'no' }, 'postgres jobs: autoMigrate must be true or false']],
		[{ breaker: null }, { metrics: null }]],
	['createTaskRunner', (o) => createTaskRunner(pg, { autoMigrate: false, recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0, ...o }), ['dispatchIntervall', 'dispatchInterval'],
		[
			[{ recoveryInterval: MAX + 1 }, overflow('postgres tasks: recoveryInterval')],
			[{ dispatchInterval: MAX + 1 }, overflow('postgres tasks: dispatchInterval')],
			[{ awaitPollInterval: MAX + 1 }, overflow('postgres tasks: awaitPollInterval')],
			[{ serializeErrorCause: 'yes' }, 'postgres tasks: serializeErrorCause must be true or false']
		],
		[{ breaker: null }, { metrics: null }, { idempotency: null }, { platform: null }, { heartbeatInterval: null }]],
	['createNotifyBridge', (o) => createNotifyBridge(pg, { channel: 'test', ...o }), ['pollinterval', 'pollInterval'],
		[[{ autoReconnect: 'false' }, 'notify bridge: autoReconnect must be true or false'], [{ allowSystemTopics: 1 }, 'notify bridge: allowSystemTopics must be true or false']],
		[{ breaker: null }, { metrics: null }, { parse: null }]],
	['createReplay', (o) => createPgReplay(pg, { autoMigrate: false, cleanupInterval: 0, ...o }), ['cleanupInterva', 'cleanupInterval'],
		[[{ autoMigrate: 'false' }, 'postgres replay: autoMigrate must be true or false']],
		[{ breaker: null }, { metrics: null }]],
	['createDeadLetter', (o) => createPgDeadLetter(pg, { autoMigrate: false, cleanupInterval: 0, ...o }), ['TTLms', 'ttlMs'],
		[[{ autoMigrate: 1 }, 'postgres dead-letter: autoMigrate must be true or false']],
		[{ breaker: null }, { metrics: null }]],
	['postgresMigrations', (o) => postgresMigrations({ store: 'jobs', ...o }), ['tabel', 'table'], [], [{}]],
	['createCircuitBreaker', (o) => createCircuitBreaker(o), ['resettimeout', 'resetTimeout'],
		[[{ resetTimeout: MAX + 1 }, overflow('circuit breaker: resetTimeout')], [{ onStateChange: 'log' }, 'circuit breaker: onStateChange must be a function']],
		[{ onStateChange: null }, { resetTimeout: 0 }, { resetTimeout: MAX }]],
	['createDegradationPolicy', (o) => createDegradationPolicy(o), ['jitterms', 'jitterMs'], [], [{ mitigation: null }]],
	['createAdmissionControl', (o) => createAdmissionControl({ classes: { background: ['MEMORY'] }, ...o }), ['aggregater', 'aggregator'],
		[[{ metrics: 'on' }, 'admission control: metrics must be an object (or omitted)']],
		[{ metrics: null }, { aggregator: null }]],
	['createBusValidator', (o) => createBusValidator(o), ['maxbytes', 'maxBytes'],
		[[{ allowedSystemTopics: '__x' }, 'bus validator: allowedSystemTopics must be an array of strings'], [{ label: 5 }, 'bus validator: label must be a string']],
		[{ allowedSystemTopics: [] }, { label: 'my bus' }]],
	['createForgetStore', (o) => createForgetStore({}, o), ['Redis', 'redis'], [], [{ redis: null }]],
	['createMetrics', (o) => createMetrics(o), ['maxseries', 'maxSeries'],
		[[{ prefix: 5 }, 'createMetrics: prefix must be a string'], [{ defaultBuckets: ['fast'] }, 'createMetrics: defaultBuckets must be an array of finite numbers']],
		[{ prefix: '' }, { defaultBuckets: [0.1, 1] }, { mapTopic: null }]],
	['wirePublishRateMetrics', (o) => wirePublishRateMetrics(mockPlatform(), metrics, o), ['topn', 'topN'],
		[[{ mapTopic: 'all' }, 'wirePublishRateMetrics: mapTopic must be a function']],
		[{ mapTopic: null }, { topN: 3 }]],
	['wireClusterPublishRateMetrics', (o) => wireClusterPublishRateMetrics({ topPublishers: [] }, metrics, o), ['topn', 'topN'],
		[[{ mapTopic: 1 }, 'wireClusterPublishRateMetrics: mapTopic must be a function']],
		[{ mapTopic: null }]]
];

describe.each(cases)('%s', (factory, build, [typo, suggestion], invalid, valid) => {
	it('warns once, with a did-you-mean, for a near-miss key and still constructs', () => {
		const before = unknownWarnings().length;
		keep(build({ [typo]: 1 }));
		keep(build({ [typo]: 2 }));
		const added = unknownWarnings().slice(before);
		expect(added).toHaveLength(1);
		expect(added[0]).toMatch(new RegExp(
			`^\\[extensions\\] unknown ${factory.replace(/[()]/g, '\\$&')} option\\(s\\): ${typo} \\(did you mean '${suggestion}'\\?\\) - ` +
			`not recognized by ${factory.replace(/[()]/g, '\\$&')} and ignored\\. Check the spelling against the documented options \\(\\w+ in [\\w/.-]+\\.d\\.ts\\)\\.$`
		));
	});

	it.each(invalid.map(([options, message]) => [JSON.stringify(options), options, message]))(
		'refuses %s with the accepted values', (_shown, options, message) => {
			expect(() => keep(build(options))).toThrow(message);
		}
	);

	it.each(valid.map((options) => [JSON.stringify(options), options]))('accepts %s', (_shown, options) => {
		expect(() => keep(build(options))).not.toThrow();
	});
});

/**
 * Optional options whose own check is written `options.x !== undefined` (or
 * a destructuring default), which a null used to reach and fail. Each must
 * now take its default, as an omitted key does.
 */
const nullDefaults = [
	['createPubSubBus', (o) => createPubSubBus(redis, o), ['onDegraded', 'onRecovered', 'degradationPolicy', 'maxEnvelopeBytes']],
	['createShardedBus', (o) => createShardedBus(redis, o), ['shardKey', 'subscribersAggregator', 'maxEnvelopeBytes']],
	['redis createReplay', (o) => createReplay(redis, o), ['storage', 'size', 'ttl', 'durability', 'localFanoutOnStorageFailure', 'maxDataBytes', 'forgetUserId']],
	['redis createReplay (stream)', (o) => createReplay(redis, { storage: 'stream', ...o }), ['size', 'ttl', 'idempotencyTtl', 'maxDataBytes']],
	['createPresence', (o) => createPresence(redis, o), ['clientUpdateFields', 'selfPreservation', 'maxEnvelopeBytes']],
	['createCursor', (o) => createCursor(redis, o), ['position', 'maxEnvelopeBytes']],
	['createRateLimit', (o) => createRateLimit(redis, { points: 2, interval: 1000, ...o }), ['blockDuration', 'keyBy', 'tenant', 'budget', 'localFloorOnStorageFailure']],
	['createCompositeRateLimit', (o) => createCompositeRateLimit(redis, { dimensions, ...o }), ['tenant', 'localFloorOnStorageFailure', 'emergency']],
	['createGroup', (o) => createGroup(redis, 'room', o), ['maxEnvelopeBytes']],
	['createDistributedSession', (o) => createDistributedSession(redis, o), ['identify', 'maxAgeMs', 'forgetUserId']],
	['createConnectionRegistry', (o) => createConnectionRegistry(redis, { identify: () => null, ...o }), ['attributes', 'sessionIdentify', 'maxEnvelopeBytes']],
	['createLeader', (o) => createLeader(redis, o), ['key', 'instanceId', 'onError', 'mapKey']],
	['createDistributedLock', (o) => createDistributedLock(redis, o), ['keyPrefix', 'fence', 'mapKey']],
	['createClockSkewSampler', (o) => createClockSkewSampler(redis, { immediate: false, ...o }), ['onSkew', 'onWarn', 'onTrip', 'onError', 'fence', 'onFence']],
	['createClusterClock', (o) => createClusterClock(redis, { immediate: false, ...o }), ['ntp', 'leader', 'leaderKey', 'legacyLeaderKeyFallback', 'onSample', 'onWarn', 'onTrip', 'onDrift', 'onError']],
	['createPublishRateAggregator', (o) => createPublishRateAggregator(redis, o), ['subjects']],
	['redis createAlarmStore', (o) => createRedisAlarmStore(redis, o), ['keyPrefix']],
	['createCrdtCluster', (o) => createCrdtCluster(redis, o), ['maxEnvelopeBytes']],
	['createSmoothCluster', (o) => createSmoothCluster(redis, o), ['maxEnvelopeBytes']],
	['createTopicBroadcast', (o) => createTopicBroadcast(redis, o), ['maxEnvelopeBytes']],
	['createRedisFence', (o) => createRedisFence(redis, o), ['keyPrefix']],
	['redis createIdempotencyStore', (o) => createRedisIdempotency(redis, o), ['keyPrefix', 'ttl', 'acquireTtl']],
	['createUpgradeBucket', (o) => createUpgradeBucket(redis, { perMinute: 1, ...o }), ['blockDuration', 'elevated', 'siege']],
	['redis createDeadLetter', (o) => createRedisDeadLetter(redis, o), ['max', 'ttlMs', 'forgetUserId', 'forgetTombstoneMs']],
	['postgres createIdempotencyStore', (o) => createPgIdempotency(pg, { autoMigrate: false, cleanupInterval: 0, ...o }), ['ttl', 'acquireTtl', 'cleanupInterval']],
	['createTaskRunner', (o) => createTaskRunner(pg, { autoMigrate: false, recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0, ...o }),
		['fenceTtl', 'recoveryInterval', 'recoveryBatchSize', 'dispatchInterval', 'dispatchBatchSize', 'awaitPollInterval', 'awaitTimeout', 'cleanupInterval', 'rowTtl', 'maxPayloadBytes', 'forgetUserId', 'onStateChange']],
	['createNotifyBridge', (o) => createNotifyBridge(pg, { channel: 'test', ...o }), ['maxEnvelopeBytes']],
	['postgres createReplay', (o) => createPgReplay(pg, { autoMigrate: false, cleanupInterval: 0, ...o }), ['size', 'ttl', 'cleanupInterval', 'localFanoutOnStorageFailure', 'maxDataBytes', 'forgetUserId']],
	['postgres createDeadLetter', (o) => createPgDeadLetter(pg, { autoMigrate: false, cleanupInterval: 0, ...o }), ['max', 'ttlMs', 'cleanupInterval', 'forgetUserId']],
	['createDegradationPolicy', (o) => createDegradationPolicy(o), ['jitterMs', 'recoveryJitterMs']],
	['createBusValidator', (o) => createBusValidator(o), ['maxBytes']],
	['capabilityCookie', (o) => capabilityCookie({ secret: '0123456789abcdef', ...o }), ['previousSecret']]
];

describe.each(nullDefaults)('%s: null means not set', (_factory, build, keys) => {
	it.each(keys)('%s: null takes the default', (key) => {
		expect(() => keep(build({ [key]: null }))).not.toThrow();
	});
});

describe('null takes the default, observed', () => {
	it('a null fence keyPrefix writes the default fence: key', async () => {
		const fence = createRedisFence(redis, { keyPrefix: null });
		await fence.acquire('t1', 'f1', 30);
		expect(await redis.redis.get(redis.key('fence:t1'))).toBe('f1');
	});

	it('the caller options object is not modified', () => {
		const options = { size: null, ttl: null };
		keep(createReplay(redis, options));
		expect(options).toEqual({ size: null, ttl: null });
	});

	it('a null pub/sub systemChannel still turns the health events off', async () => {
		// open() subscribes the connection to the system channel; the default
		// channel shows the comparison is live.
		const breaker = createCircuitBreaker();
		const platform = mockPlatform();
		const announcing = keep(createPubSubBus(redis, { breaker }));
		const heard = mockWs();
		await announcing.hooks.open(heard, { platform });
		expect(heard.isSubscribed('__realtime')).toBe(true);
		const silent = keep(createPubSubBus(redis, { breaker, systemChannel: null }));
		const quiet = mockWs();
		await silent.hooks.open(quiet, { platform });
		expect(quiet.isSubscribed('__realtime')).toBe(false);
		expect(platform.subscribed).toEqual([]);
	});
});

describe('values shared with the in-memory plugins read the same way', () => {
	it('group meta is shallow-copied as the in-memory groups plugin copies it', async () => {
		const meta = { tier: 'vip' };
		const vip = keep(createGroup(redis, 'vip-room', { meta }));
		meta.tier = 'changed';
		expect(await vip.getMeta()).toEqual({ tier: 'vip' });
		const empty = keep(createGroup(redis, 'empty-room', { meta: {} }));
		expect(await empty.getMeta()).toEqual({});
	});

	it('cursor viewport and backpressure keys are checked as the in-memory plugin checks them', () => {
		const before = unknownWarnings().length;
		keep(createCursor(redis, { viewport: { enabled: true, paddng: 8 }, backpressure: { enabled: true, maxBuffered: 1 } }));
		keep(createCursor(redis, { viewport: { enabled: true, paddng: 8 }, backpressure: { enabled: true, maxBuffered: 1 } }));
		expect(unknownWarnings().slice(before)).toEqual([
			"[extensions] unknown createCursor option(s): viewport.paddng (did you mean 'viewport.padding'?), backpressure.maxBuffered - " +
			'not recognized by createCursor and ignored. Check the spelling against the documented options (RedisCursorOptions in redis/cursor.d.ts).'
		]);
		// The shorthand `true` has no keys to check.
		keep(createCursor(redis, { viewport: true, backpressure: true }));
		expect(unknownWarnings().slice(before)).toHaveLength(1);
	});

	it('replay maxTopics is a documented key on every backend', () => {
		const before = unknownWarnings().length;
		keep(createReplay(redis, { maxTopics: 50 }));
		keep(createReplay(redis, { maxTopics: 50, storage: 'stream' }));
		keep(createPgReplay(pg, { maxTopics: 50 }));
		expect(unknownWarnings().slice(before)).toEqual([]);
	});

	// As the in-memory lock, session, smooth and webhook-control factories
	// read `options ?? {}`.
	it.each([
		['createDistributedLock', () => createDistributedLock(redis, null)],
		['createDistributedSession', () => createDistributedSession(redis, null)],
		['createSmoothCluster', () => createSmoothCluster(redis, null)],
		['createRetryBudget', () => createRetryBudget(redis, null)],
		['createWebhookBreaker', () => createWebhookBreaker(redis, null)],
		['createCrdtCluster', () => createCrdtCluster(redis, null)]
	])('%s takes a null options object as no options', (_name, build) => {
		expect(() => keep(build())).not.toThrow();
	});
});

describe('construction-rate behaviour', () => {
	it('a per-room factory warns once for a typo shared by every room', () => {
		const before = unknownWarnings().length;
		for (let i = 0; i < 5; i++) keep(createGroup(redis, `room-${i}`, { memberTTL: 60 }));
		expect(unknownWarnings().slice(before)).toEqual([
			"[extensions] unknown createGroup option(s): memberTTL (did you mean 'memberTtl'?) - not recognized by createGroup " +
			'and ignored. Check the spelling against the documented options (RedisGroupOptions in redis/groups.d.ts).'
		]);
	});

	it('createReplay with storage stream reports a typo once across both entry points', () => {
		const before = unknownWarnings().length;
		keep(createReplay(redis, { storage: 'stream', idempotencyTTL: 60 }));
		expect(unknownWarnings().slice(before)).toHaveLength(1);
	});

	it('the metric options form warns for a key the instrument ignores and judges allowNegative', () => {
		const before = unknownWarnings().length;
		metrics.counter('typed_total', 'Typed', { labelNames: ['a'], buckets: [1] });
		expect(unknownWarnings().slice(before)).toEqual([
			"[extensions] unknown counter() / gauge() option(s): buckets - not recognized by counter() / gauge() and ignored. " +
			'Check the spelling against the documented options (MetricOptions in prometheus/index.d.ts).'
		]);
		expect(() => metrics.histogram('typed_seconds', 'Typed', { allowNegative: 'yes' }))
			.toThrow('metric: allowNegative must be true or false - got "yes" (string).');
		expect(() => metrics.histogram('typed2_seconds', 'Typed', { allowNegative: true, buckets: [-1, 1] })).not.toThrow();
	});

	it('migrate and validateSchema judge their options before any I/O', async () => {
		const before = unknownWarnings().length;
		await expect(migrate(null, { store: 'nope', lockTimeout: 1 })).rejects.toThrow('unknown store');
		await expect(validateSchema(null, { store: 'nope', tabel: 'x' })).rejects.toThrow('unknown store');
		expect(unknownWarnings().slice(before)).toEqual([
			"[extensions] unknown migrate option(s): lockTimeout (did you mean 'lockTimeoutMs'?) - not recognized by migrate and ignored. " +
			'Check the spelling against the documented options (PostgresMigrationOptions in postgres/index.d.ts).',
			"[extensions] unknown validateSchema option(s): tabel (did you mean 'table'?) - not recognized by validateSchema and ignored. " +
			'Check the spelling against the documented options (PostgresSchemaOptions in postgres/index.d.ts).'
		]);
	});

	it('migrateReplayToStream reports an unknown key', async () => {
		const before = unknownWarnings().length;
		await migrateReplayToStream(redis, { topics: [], dryrun: true });
		expect(unknownWarnings().slice(before)).toHaveLength(1);
		expect(unknownWarnings().at(-1)).toContain("dryrun (did you mean 'dryRun'?)");
	});
});
