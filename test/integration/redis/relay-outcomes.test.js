import { expect, it, vi } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { createMetrics } from '../../../src/prometheus/index.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

for (const [name, factory, prefix, command] of [
	['standard', createPubSubBus, 'pubsub', 'publish'],
	['sharded', createShardedBus, 'sharded_pubsub', 'spublish']
]) it(`${name} counts real Redis refusals and subsequent successful relays`, async () => {
	const admin = createBackendClient();
	await admin.redis.ping();
	const nodes = isClusterBackend() ? admin.redis.nodes('all') : [admin.redis];
	const user = 'inttest-outcome-' + name + '-' + Date.now();
	const password = 'test-relay-password';
	const changeUser = (...args) => Promise.all(nodes.map(node => node.call('ACL', 'SETUSER', user, ...args)));
	await changeUser('reset', 'on', '>' + password, '~*', '&*', '+@all', '-' + command);
	const auth = { username: user, password };
	const client = createBackendClient({ options: isClusterBackend() ? { redisOptions: auth } : auth });
	const metrics = createMetrics();
	const channel = 'inttest-outcome:' + user;
	const options = { channel, channelPrefix: channel + ':' };
	const bus = factory(client, { ...options, metrics }), peer = factory(admin, options);
	const source = mockPlatform(), remote = mockPlatform();
	try {
		await bus.activate(source); await peer.activate(remote);
		if (name === 'sharded') await peer.follow('room');
		const wrapped = bus.wrap(source);
		wrapped.publishBatched([{ topic: 'room', event: 'event', data: 1 }, { topic: 'room', event: 'event', data: 2 }]);
		await vi.waitFor(() => expect(metrics.serialize()).toContain(prefix + '_relay_attempts_total{outcome="error"} 1'));
		expect(source.published).toHaveLength(2);
		expect(remote.published).toHaveLength(0);
		expect(metrics.serialize()).toContain(prefix + '_messages_local_only_total{reason="backend_error"} 2');
		await changeUser('+' + command);
		wrapped.publish('room', 'event', 3);
		await vi.waitFor(() => expect(remote.published.map(message => message.data)).toEqual([3]));
		expect(metrics.serialize()).toContain(prefix + '_relay_attempts_total{outcome="success"} 1');
		expect(metrics.serialize()).toContain(prefix + '_relay_duration_seconds_count 2');
	} finally {
		await bus.deactivate(); await peer.deactivate(); await client.quit();
		await Promise.all(nodes.map(node => node.call('ACL', 'DELUSER', user)));
		await admin.quit();
	}
});
