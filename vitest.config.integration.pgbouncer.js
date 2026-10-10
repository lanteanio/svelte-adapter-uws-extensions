import { defineConfig } from 'vitest/config';
export default defineConfig({
	test: {
		include: ['test/integration/pgbouncer/**/*.test.js'],
		globalSetup: ['./test/integration/global-setup-pgbouncer.js'],
		fileParallelism: false,
		pool: 'forks',
		testTimeout: 30000,
		hookTimeout: 60000
	}
});
