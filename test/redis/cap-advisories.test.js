import { describe, it, expect, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { registeredRecord } from '../helpers/diagnostic-line.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';

// The production index bounds are sized for real clusters (up to
// ten million values), far past what a unit test can drive. Lowering them
// here runs each module's real path up to its bound, so the advisory it prints
// there is the one an operator would see.
vi.mock('../../src/shared/caps.js', async (importOriginal) => ({
	...(await importOriginal()),
	MAX_REGISTRY_INDEX_VALUES_PER_KEY: 3,
	MAX_REGISTRY_USER_INDEX: 3,
	MAX_AGGREGATOR_REMOTE_INSTANCES: 3
}));

const { createConnectionRegistry } = await import('../../src/redis/registry.js');
const { createPublishRateAggregator } = await import('../../src/redis/publish-rate.js');
const { createMetrics } = await import('../../src/prometheus/index.js');

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

/** Every console.warn line printed while `fn` runs. */
async function warnings(fn) {
	const lines = [];
	const spy = vi.spyOn(console, 'warn').mockImplementation((line) => { lines.push(line); });
	try { await fn(); } finally { spy.mockRestore(); }
	return lines;
}

afterEach(() => {
	vi.restoreAllMocks();
});

describe('cap advisories are registered diagnostics', () => {
	it('registry: the attribute bucket and the user index report once each at their bounds', async () => {
		const platform = mockPlatform();
		const registry = createConnectionRegistry(mockRedisClient('adv-registry:'), {
			identify: (ws) => ws.getUserData().userId,
			attributes: () => ({ plan: 'pro' }),
			heartbeat: 60000
		});
		try {
			const lines = await warnings(async () => {
				for (let i = 0; i < 5; i++) {
					const ws = mockWs({ userId: 'member-' + i });
					ws.getUserData()[WS_SESSION_ID] = 'session-' + i;
					await registry.hooks.open(ws, { platform });
				}
			});
			expect(lines).toHaveLength(2);
			const bucket = lines.find((line) => line.includes(' event=redis.registry.index-bucket.large '));
			const users = lines.find((line) => line.includes(' event=redis.registry.user-index.large '));
			expect(registeredRecord(bucket, EXTENSIONS_ERROR_IDS.REGISTRY_INDEX_BUCKET).attributes)
				.toEqual({ attribute: 'plan', userIds: 3 });
			expect(registeredRecord(users, EXTENSIONS_ERROR_IDS.REGISTRY_USER_INDEX).attributes)
				.toEqual({ entries: 3 });
			// Neither line names a user or an attribute VALUE.
			for (const line of lines) {
				expect(line).not.toContain('member-');
				expect(line).not.toContain('"pro"');
			}
		} finally {
			await registry.destroy();
		}
	});

	describe('publish-rate at its remote-instance bound', () => {
		/** Deliver one sibling slice on the aggregation channel. */
		function slice(client, instanceId, ts) {
			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId,
				ts,
				slice: [{ topic: 'room:' + instanceId, messagesPerSec: 1, bytesPerSec: 10 }]
			}));
		}

		// A table full of LIVE siblings drops every newcomer, which is when the
		// cluster-wide sum stops seeing them - the case that has to be visible.
		it('reports once when a live table drops newcomers', async () => {
			const client = mockRedisClient('adv-rate-live:');
			const metrics = createMetrics();
			const aggregator = createPublishRateAggregator(client, { metrics });
			await aggregator.activate(mockPlatform());
			try {
				const lines = await warnings(async () => {
					for (let i = 0; i < 6; i++) slice(client, 'live-' + i, Date.now());
					await settle();
				});
				expect(lines).toHaveLength(1);
				const record = registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PUBLISH_RATE_REMOTE_CAP);
				expect(record.attributes).toEqual({ maxRemoteInstances: 3 });
				expect(metrics.serialize()).toMatch(/cluster_publish_rate_remote_dropped_total 3/);
			} finally {
				await aggregator.deactivate?.();
			}
		});

		it('reports once when stale entries are evicted to admit newcomers', async () => {
			const client = mockRedisClient('adv-rate-stale:');
			const metrics = createMetrics();
			const aggregator = createPublishRateAggregator(client, { metrics });
			await aggregator.activate(mockPlatform());
			try {
				const lines = await warnings(async () => {
					const hourAgo = Date.now() - 3_600_000;
					for (let i = 0; i < 3; i++) slice(client, 'stale-' + i, hourAgo);
					for (let i = 0; i < 3; i++) slice(client, 'fresh-' + i, Date.now());
					await settle();
				});
				expect(lines).toHaveLength(1);
				registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PUBLISH_RATE_REMOTE_CAP);
				expect(metrics.serialize()).toMatch(/cluster_publish_rate_remote_evicted_total 3/);
			} finally {
				await aggregator.deactivate?.();
			}
		});
	});
});
