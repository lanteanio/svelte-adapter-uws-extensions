import { afterAll, beforeAll, beforeEach } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { sessionTokenContract } from '../../helpers/session-token-contract.js';
let client;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-session-token:' }); });
beforeEach(async () => { await resetBackendKeys(client); });
afterAll(async () => { await client.quit(); });
sessionTokenContract(() => client);
