import { describe } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { taskErrorCodeContract } from '../helpers/task-error-code-contract.js';

describe('task operational error codes', () => {
	taskErrorCodeContract(() => mockPgClient());
});
