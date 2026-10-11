import { mockRedisClient } from '../helpers/mock-redis.js';
import { redisIOContract } from '../helpers/redis-io-contract.js';

redisIOContract(() => mockRedisClient('io-budget:'));
