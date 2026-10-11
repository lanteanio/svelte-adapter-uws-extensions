import { createBackendClient } from '../helpers/backend.js';
import { redisIOContract } from '../../helpers/redis-io-contract.js';

redisIOContract(() => createBackendClient({ keyPrefix: 'inttest-io-budget:' }));
