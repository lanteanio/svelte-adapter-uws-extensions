import { describe, expect, it, vi } from 'vitest';
import { migrate, postgresMigrations, postgresReadiness, validateSchema } from '../../src/postgres/index.js';
import { createReplay } from '../../src/postgres/replay.js';
import { createJobQueue } from '../../src/postgres/jobs.js';
import { mockPgClient } from '../../src/testing/mock-pg.js';

describe('PostgreSQL migration contracts', () => {
	it('keeps published migration checksums immutable', () => {
		const expected = {
			replay: ['95cad9e6180f1ac2e0c2ec852f7999cbe93f6f8a93e1db3b7da36aab65dfd9ce', 'c2767b2b839b55691f7ed6f1765593557f6dac4df93fe0c135d33cfc0cbcc608'],
			jobs: ['11f28312fe2e244e9ae6d46b047f4a2a7f9a55a41376be0feacd4004229c13c9', '5ef2532f61b39c4c9624b34b8cc9bc16688da0f6bc9a909db0bb98d7cc097faa'],
			tasks: ['974a263bf65bdcaef75e66700a146bceb117b925eef1cad9435abafa2c5702ae', '0704682d4a9506e64c399c2a5df98b306c9200c89ab8d4ae7e0939a61eb678ce'],
			idempotency: ['85bbe9cab20fccf9d78a3e0e95d16cb8ee0fd2958f8a48a5beef3ee2b576ba48', 'f7d87331da8e2891e9f9eea36489a3c0af90ac70b9a72acce89da068427ee499'],
			'dead-letter': ['d71e2cd0374adc70f47e952165a315c1f1b0a0b9301cd71954a4d2d93c785a9d', '2391d3029698b6b26609c93cb1db6b8f976a2f47f71d4135d03e406a2ed75d7b'],
			'alarm-store': ['95b27e2254ed8aeff9736649f151c5f4f9953ae985c597657ab62bcabb996ab2', '641d90e61f980753b9f05b92aed76a0d9ca05abfb542b0e944b0e0555fe74499']
		};
		for (const [store, checksums] of Object.entries(expected)) expect(postgresMigrations({ store }).slice(0, 2).map(entry => entry.checksum)).toEqual(checksums);
	});
	it.each(['replay', 'jobs', 'tasks', 'idempotency', 'dead-letter', 'alarm-store'])('migrates and validates %s repeatedly', async store => {
		const client = mockPgClient();
		await migrate(client, { store });
		await migrate(client, { store });
		await validateSchema(client, { store });
	});
	it('does not change the migration corpus when a caller edits the returned manifest', () => {
		const original = postgresMigrations({ store: 'jobs' });
		const copy = postgresMigrations({ store: 'jobs' });
		copy[0].statements.length = 0;
		expect(postgresMigrations({ store: 'jobs' })).toEqual(original);
		expect(original.map(entry => entry.version)).toEqual([1, 2]);
		expect(original.every(entry => /^[0-9a-f]{64}$/.test(entry.checksum))).toBe(true);
	});
	it('uses one initialization flight and exposes a retryable failure', async () => {
		const client = mockPgClient();
		const connect = client.connect;
		client.connect = vi.fn().mockRejectedValueOnce(new Error('unavailable')).mockImplementation(connect);
		const jobs = createJobQueue(client);
		expect(jobs.status()).toEqual({ state: 'idle', error: null });
		const first = jobs.ready();
		expect(jobs.ready()).toBe(first);
		await expect(first).rejects.toThrow('unavailable');
		expect(jobs.status()).toMatchObject({ state: 'failed' });
		await jobs.ready();
		expect(jobs.status()).toEqual({ state: 'ready', error: null });
		expect(client.connect).toHaveBeenCalledTimes(2);
	});
	it('latches failed automatic migrations until an explicit readiness retry', async () => {
		const client = mockPgClient();
		const connect = client.connect;
		const failure = new Error('duplicate legacy identity');
		client.connect = vi.fn().mockRejectedValueOnce(failure).mockImplementation(connect);
		const jobs = createJobQueue(client);
		await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
		await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
		await expect(jobs.claim('test')).rejects.toBe(failure);
		expect(client.connect).toHaveBeenCalledTimes(1);
		await jobs.ready();
		await jobs.enqueue('test', { repaired: true });
		expect(client.connect).toHaveBeenCalledTimes(2);
		expect(jobs.status()).toEqual({ state: 'ready', error: null });
	});
	it('keeps disabled automatic migration read-only and reports an absent schema', async () => {
		const client = mockPgClient();
		client.connect = vi.fn(client.connect);
		client.query = vi.fn(client.query);
		const replay = createReplay(client, { autoMigrate: false, cleanupInterval: 0 });
		await expect(replay.ready()).rejects.toThrow('missing expected column');
		expect(client.connect).not.toHaveBeenCalled();
		expect(client.query.mock.calls.every(([statement]) => statement.text.startsWith('SELECT '))).toBe(true);
		replay.destroy();
	});
	it('returns every failed component from the aggregate gate, including synchronous errors', async () => {
		const first = new Error('first'), second = new Error('second');
		const client = { check: vi.fn().mockResolvedValue({}) };
		const components = [{ ready() { throw first; } }, { ready: async () => { throw second; } }];
		await expect(postgresReadiness(client, components)).rejects.toMatchObject({ errors: [first, second] });
	});
	it('destroys the migration checkout after a failed statement', async () => {
		const release = vi.fn(), error = new Error('DDL failed');
		const client = { connect: async () => ({ query: async () => { throw error; }, release }) };
		await expect(migrate(client, { store: 'jobs' })).rejects.toBe(error);
		expect(release).toHaveBeenCalledExactlyOnceWith(true);
	});
});
