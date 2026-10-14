import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		include: ['test/integration/redis/**/*.test.js', 'test/integration/postgres/**/*.test.js'],
		globalSetup: ['./test/integration/global-setup.js'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks',
		testTimeout: 30000,
		hookTimeout: 60000
	}
});
