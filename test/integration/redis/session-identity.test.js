import { beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { sessionIdentityContract } from '../../helpers/session-identity-contract.js';
let client;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-session-identity:' }); });
beforeEach(async () => { await resetBackendKeys(client); });
afterAll(async () => { await client.quit(); });
sessionIdentityContract(() => client);
