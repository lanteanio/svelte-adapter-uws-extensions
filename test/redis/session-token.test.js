import { beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { sessionTokenContract } from '../helpers/session-token-contract.js';
let client;
beforeEach(() => { client = mockRedisClient('token-contract:'); });
sessionTokenContract(() => client);
