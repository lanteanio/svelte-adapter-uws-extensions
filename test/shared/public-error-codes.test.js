import { describe, it, expect } from 'vitest';
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { ConnectionError, TimeoutError } from '../../src/shared/errors.js';
import { CircuitBrokenError } from '../../src/shared/breaker.js';
import { ReplicationTimeoutError, ReplayStorageError, ReplaySerializationError } from '../../src/redis/replay.js';
import { ReplayStorageError as PgReplayStorageError, ReplaySerializationError as PgReplaySerializationError } from '../../src/postgres/replay.js';
import { LockAcquireTimeoutError, LockLostError } from '../../src/redis/lock.js';
import { TaskInFlightError, UnknownTaskError } from '../../src/postgres/tasks.js';
import { serialiseErrorSafe, deserialiseError } from '../../src/postgres/_tasks-errors.js';

describe('stable operational error codes', () => {
	it('exposes literal error codes to a TypeScript consumer', () => {
		const compiler = createRequire(import.meta.url).resolve('typescript/bin/tsc');
		const fixture = fileURLToPath(new URL('../helpers/public-error-code-types.ts', import.meta.url));
		const result = spawnSync(process.execPath, [compiler, '--noEmit', '--strict', '--skipLibCheck',
			'--target', 'ES2022', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', fixture],
		{ encoding: 'utf8', timeout: 20_000 });
		expect(result.error).toBeUndefined();
		expect(result.status, result.stdout + result.stderr).toBe(0);
	}, 30_000);

	it.each([
		[new ConnectionError('redis', 'unavailable'), 'CONNECTION_FAILED'],
		[new TimeoutError('postgres', 'query', 20), 'OPERATION_TIMEOUT'],
		[new CircuitBrokenError(), 'CIRCUIT_BROKEN'],
		[new ReplicationTimeoutError(0, 1, 20), 'REPLICATION_TIMEOUT'],
		[new ReplayStorageError('publish', new Error('unavailable')), 'REPLAY_STORAGE_FAILED'],
		[new ReplaySerializationError('publish', new TypeError('invalid')), 'REPLAY_SERIALIZATION_FAILED'],
		[new LockAcquireTimeoutError('resource', 20), 'LOCK_ACQUIRE_TIMEOUT'],
		[new LockLostError('resource'), 'LOCK_LOST'],
		[new TaskInFlightError('key'), 'TASK_IN_FLIGHT'],
		[new UnknownTaskError('operation'), 'UNKNOWN_TASK']
	])('preserves %s code %s through persisted error reconstruction', (error, code) => {
		expect(error.code).toBe(code);
		const restored = deserialiseError(JSON.parse(JSON.stringify(serialiseErrorSafe(error))));
		expect(restored).toBeInstanceOf(Error);
		expect(restored).toMatchObject({ name: error.name, message: error.message, code });
	});

	it('shares replay error constructors and codes between Redis and PostgreSQL', () => {
		expect(PgReplayStorageError).toBe(ReplayStorageError);
		expect(PgReplaySerializationError).toBe(ReplaySerializationError);
	});
});
