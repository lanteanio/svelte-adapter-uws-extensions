import { beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { sessionErasureContract } from '../helpers/session-erasure-contract.js';
let client;
beforeEach(() => { client = mockRedisClient('session-erasure:'); });
sessionErasureContract(() => client);
