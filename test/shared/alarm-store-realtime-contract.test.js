import { describe, expect, it } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPgClient } from '../../src/testing/mock-pg.js';
import { createAlarmStore as createRedisAlarmStore } from '../../src/redis/alarm-store.js';
import { createAlarmStore as createPgAlarmStore } from '../../src/postgres/alarm-store.js';
import { realtimeAlarmContract } from '../helpers/realtime-alarm-contract.js';

// The members svelte-realtime's configureAlarm requires of a durable store
// (svelte-realtime src/server/alarm.js, configureAlarm store validation).
// `due` is optional there; both stores provide it for restart recovery.
const REQUIRED = ['nextGeneration', 'get', 'set', 'delete', 'claim', 'complete', 'fail', 'getExecution', 'executions', 'requeue'];

const stores = {
	redis: () => createRedisAlarmStore(mockRedisClient('app:')),
	postgres: () => createPgAlarmStore(mockPgClient(), { autoMigrate: false })
};

describe('durable alarm stores satisfy the realtime store contract', () => {
	it.each(Object.keys(stores))('%s implements every required member and due()', name => {
		const store = stores[name]();
		for (const member of [...REQUIRED, 'due']) expect(typeof store[member], member).toBe('function');
	});

	realtimeAlarmContract('redis double', stores.redis);
	// The SQL double does not execute alarm statements, so only the member
	// check runs here; the native PostgreSQL suite runs the whole contract.
	realtimeAlarmContract('postgres double', stores.postgres, { acceptOnly: true });
});
