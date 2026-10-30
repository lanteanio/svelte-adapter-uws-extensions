import { beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { registryIdentityContract } from '../helpers/registry-identity-contract.js';
let client;
beforeEach(() => { client = mockRedisClient('registry-identity:'); });
registryIdentityContract(() => client);
