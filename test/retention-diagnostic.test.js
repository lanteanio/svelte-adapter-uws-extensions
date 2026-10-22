import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../src/testing/mock-redis.js';
import { mockPgClient } from './helpers/mock-pg.js';
import { setOperationalEventSink } from '../src/shared/diagnostic.js';
import { EXTENSIONS_ERROR_IDS, extensionsErrorDefinition } from '../src/error-registry.js';
import { createReplay as createRedisReplay } from '../src/redis/replay.js';
import { createDeadLetter as createRedisDeadLetter } from '../src/redis/dead-letter.js';
import { createReplay as createPgReplay } from '../src/postgres/replay.js';
import { createDeadLetter as createPgDeadLetter } from '../src/postgres/dead-letter.js';

const FIRST_REPORTS = Symbol.for('svelte-adapter-uws-extensions.first-reports.v1');
const pgOptions = { autoMigrate: false, cleanupInterval: 0 };

// Each kind: how to build it with retention options, and the entry it reports.
const kinds = [
	['redis replay', EXTENSIONS_ERROR_IDS.REDIS_REPLAY_RETENTION_UNBOUNDED,
		(o) => createRedisReplay(mockRedisClient(), o), { ttl: 60 }],
	['redis stream replay', EXTENSIONS_ERROR_IDS.REDIS_REPLAY_STREAM_RETENTION_UNBOUNDED,
		(o) => createRedisReplay(mockRedisClient(), { storage: 'stream', ...o }), { ttl: 60 }],
	['postgres replay', EXTENSIONS_ERROR_IDS.POSTGRES_REPLAY_RETENTION_UNBOUNDED,
		(o) => createPgReplay(mockPgClient(), { ...pgOptions, ...o }), { ttl: 60 }],
	['redis dead-letter', EXTENSIONS_ERROR_IDS.REDIS_DEAD_LETTER_RETENTION_UNBOUNDED,
		(o) => createRedisDeadLetter(mockRedisClient(), o), { ttlMs: 60000 }],
	['postgres dead-letter', EXTENSIONS_ERROR_IDS.POSTGRES_DEAD_LETTER_RETENTION_UNBOUNDED,
		(o) => createPgDeadLetter(mockPgClient(), { ...pgOptions, ...o }), { ttlMs: 60000 }]
];

describe('unbounded retention diagnostic', () => {
	let records;
	let stores;
	let dispose;
	beforeEach(() => {
		delete globalThis[FIRST_REPORTS];
		records = [];
		stores = [];
		dispose = setOperationalEventSink((record) => { records.push(record); });
	});
	afterEach(() => {
		dispose();
		for (const store of stores) store.destroy?.();
		delete globalThis[FIRST_REPORTS];
	});
	const build = (make, options) => { stores.push(make(options)); };
	const reported = (id) => {
		const { event } = extensionsErrorDefinition(id);
		return records.filter((record) => record.event === event);
	};

	for (const [name, id, make, ttlOptions] of kinds) {
		it(`${name}: reports once per process when neither a ttl nor forgetUserId is set`, () => {
			build(make, {});
			build(make, {});
			const found = reported(id);
			expect(found).toHaveLength(1);
			expect(found[0].severity).toBe('warn');
			expect(found[0].message).toBe(extensionsErrorDefinition(id).problemPrefix);
		});

		it(`${name}: stays silent with a ttl or with forgetUserId`, () => {
			build(make, ttlOptions);
			build(make, { forgetUserId: () => 'u1' });
			expect(reported(id)).toHaveLength(0);
		});
	}

	it('reports each store kind on its own', () => {
		for (const [, , make] of kinds) build(make, {});
		for (const [, id] of kinds) expect(reported(id)).toHaveLength(1);
	});
});
