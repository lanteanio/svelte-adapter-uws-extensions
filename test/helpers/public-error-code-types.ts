import { CircuitBrokenError } from '../../src/shared/breaker.js';
import { ReplicationTimeoutError, ReplayStorageError, ReplaySerializationError } from '../../src/redis/replay.js';
import { ReplayStorageError as PgStorageError, ReplaySerializationError as PgSerializationError } from '../../src/postgres/replay.js';
import { LockAcquireTimeoutError, LockLostError } from '../../src/redis/lock.js';
import { TaskInFlightError, UnknownTaskError } from '../../src/postgres/tasks.js';

const circuit: 'CIRCUIT_BROKEN' = new CircuitBrokenError().code;
const replication: 'REPLICATION_TIMEOUT' = new ReplicationTimeoutError(0, 1, 20).code;
const storage: 'REPLAY_STORAGE_FAILED' = new ReplayStorageError('publish', null).code;
const serialization: 'REPLAY_SERIALIZATION_FAILED' = new ReplaySerializationError('publish', null).code;
const pgStorage: 'REPLAY_STORAGE_FAILED' = new PgStorageError('publish', null).code;
const pgSerialization: 'REPLAY_SERIALIZATION_FAILED' = new PgSerializationError('publish', null).code;
const acquire: 'LOCK_ACQUIRE_TIMEOUT' = new LockAcquireTimeoutError('resource', 20).code;
const lost: 'LOCK_LOST' = new LockLostError('resource').code;
const inFlight: 'TASK_IN_FLIGHT' = new TaskInFlightError('key').code;
const unknown: 'UNKNOWN_TASK' = new UnknownTaskError('operation').code;
void [circuit, replication, storage, serialization, pgStorage, pgSerialization, acquire, lost, inFlight, unknown];
