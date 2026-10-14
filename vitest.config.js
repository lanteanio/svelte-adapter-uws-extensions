import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		include: ['test/**/*.test.js'],
		exclude: ['test/integration/**', 'test/fixtures/**', '**/node_modules/**'],
		fileParallelism: false,
		isolate: true,
		pool: 'forks'
	}
});
