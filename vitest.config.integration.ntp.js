import { defineConfig } from 'vitest/config';

// Real UDP responders bind ephemeral loopback ports; no external service is used.
export default defineConfig({
	test: {
		include: ['test/integration/ntp-source.test.js'],
		fileParallelism: false,
		pool: 'forks',
		testTimeout: 5000,
		hookTimeout: 5000
	}
});
