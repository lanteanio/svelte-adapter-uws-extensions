import { describe } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { taskErrorCodeContract } from '../../helpers/task-error-code-contract.js';

describe('task error codes across PostgreSQL and worker boundaries', { retry: 0 }, () => {
	taskErrorCodeContract(() => {
		if (!process.env.INTEGRATION_POSTGRES_URL) throw new Error('PostgreSQL integration setup did not run');
		return createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
	});
});
