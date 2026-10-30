import { beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { registryIdentityContract } from '../../helpers/registry-identity-contract.js';
let client;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-registry-identity:' }); });
beforeEach(async () => { await resetBackendKeys(client); });
afterAll(async () => { await client.quit(); });
registryIdentityContract(() => client);
