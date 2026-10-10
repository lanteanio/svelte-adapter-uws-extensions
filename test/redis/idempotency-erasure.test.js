import { beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { idempotencyErasureContract } from '../helpers/idempotency-erasure-contract.js';
let client;
beforeEach(() => { client = mockRedisClient('erasure-contract:'); });
idempotencyErasureContract(() => client);
