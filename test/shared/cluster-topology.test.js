import { describe, it, expect } from 'vitest';
import { clusterTopology } from '../../src/shared/cluster.js';

const lines = [
	'a1 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460',
	'b2 10.0.0.2:6379@16379 master - 0 1 2 connected 5461-10922',
	'c3 10.0.0.3:6379@16379 master - 0 1 3 connected 10923-16383',
	'd4 10.0.0.4:6379@16379 slave a1 0 1 1 connected'
];
const cluster = (text) => ({ nodes: () => [], cluster: async () => text });

describe('cluster topology fingerprint', () => {
	it('is empty on a standalone server', async () => {
		expect(await clusterTopology({ get: async () => null })).toBe('');
	});

	it('names each master and its slots, whatever the line order and replica state', async () => {
		const first = await clusterTopology(cluster(lines.join('\n')));
		expect(first).toContain('a1 0-5460');
		expect(first).not.toContain('d4');
		const reordered = [lines[3], lines[2], lines[0], lines[1].replace(' 0 1 2 ', ' 0 9 2 ')].join('\n');
		expect(await clusterTopology(cluster(reordered + '\n'))).toBe(first);
	});

	it('changes when slots move or a master is replaced', async () => {
		const first = await clusterTopology(cluster(lines.join('\n')));
		const moved = lines.map((line) => line.replace('5461-10922', '5462-10922').replace('0-5460', '0-5461')).join('\n');
		expect(await clusterTopology(cluster(moved))).not.toBe(first);
		const failedOver = [lines[0].replace('myself,master', 'myself,slave'), lines[1], lines[2], lines[3].replace('slave a1', 'master -') + ' 0-5460'].join('\n');
		expect(await clusterTopology(cluster(failedOver))).not.toBe(first);
	});

	it('is null while a slot is migrating or importing', async () => {
		expect(await clusterTopology(cluster([lines[0] + ' [5461->-b2]', ...lines.slice(1)].join('\n')))).toBeNull();
		expect(await clusterTopology(cluster([lines[0], lines[1] + ' [100-<-a1]', ...lines.slice(2)].join('\n')))).toBeNull();
	});
});
