import { beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { sessionErasureContract } from '../../helpers/session-erasure-contract.js';
let client;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-session-erasure:' }); });
beforeEach(async () => { await resetBackendKeys(client); });
afterAll(async () => { await client.quit(); });
sessionErasureContract(() => client);
