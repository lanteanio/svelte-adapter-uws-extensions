import { beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { sessionIdentityContract } from '../helpers/session-identity-contract.js';
let client;
beforeEach(() => { client = mockRedisClient('session-identity:'); });
sessionIdentityContract(() => client);
