/**
 * The complete set of public Platform members that `bus.wrap()` and the
 * mock are expected to expose. Single source of truth: parity tests
 * iterate this list to assert the wrap surface matches the adapter's
 * Platform interface. When the adapter adds a new member, append it
 * here AND mirror it in `mockPlatform()` below AND in the wrap
 * implementations in `redis/pubsub.js` / `redis/sharded-pubsub.js`.
 *
 * Drift on any of those three sites fails the parity test at CI time.
 */
export const PLATFORM_KEYS = Object.freeze([
	'publish', 'publishBatched', 'batch',
	'send', 'sendTo', 'sendCoalesced',
	'request',
	'connections', 'requestId',
	'pressure', 'onPressure', 'onPublishRate',
	'subscribers', 'subscribe', 'unsubscribe', 'checkSubscribe',
	'topic',
	'maxPayloadLength', 'bufferedAmount'
]);

/**
 * Create a mock platform that records publish/send calls.
 * Matches the core svelte-adapter-uws Platform interface.
 */
export function mockPlatform() {
	const pressureSubscribers = new Set();
	const publishRateSubscribers = new Set();
	const p = {
		published: [],
		publishedBatches: [],
		sent: [],
		sentCoalesced: [],
		requested: [],
		subscribed: [],
		unsubscribed: [],
		checkedSubscribe: [],
		connections: 0,
		requestId: '',
		// Mirror the adapter's default. `1024 * 1024` (1 MB) is the
		// post-next.19 default; tests that need a different cap can
		// reassign p.maxPayloadLength directly.
		maxPayloadLength: 1024 * 1024,
		// Framework-convention slot: svelte-realtime's auto-replay routing
		// reads `platform.replay`. Default `undefined`; tests that need to
		// drive replay paths reassign it directly.
		replay: undefined,
		// platform.pressure stub. Default snapshot mirrors a healthy worker.
		// Tests drive transitions via _setPressure(snapshot).
		pressure: {
			active: false,
			subscriberRatio: 0,
			publishRate: 0,
			memoryMB: 0,
			reason: 'NONE'
		},
		onPressure(cb) {
			pressureSubscribers.add(cb);
			return () => pressureSubscribers.delete(cb);
		},
		onPublishRate(cb) {
			publishRateSubscribers.add(cb);
			return () => publishRateSubscribers.delete(cb);
		},
		_setPressure(snapshot) {
			p.pressure = snapshot;
			for (const cb of pressureSubscribers) {
				try { cb(snapshot); } catch { /* swallow */ }
			}
		},
		_emitPublishRate(events) {
			for (const cb of publishRateSubscribers) {
				try { cb(events); } catch { /* swallow */ }
			}
		},
		bufferedAmount(_ws) {
			return 0;
		},
		publish(topic, event, data, options) {
			p.published.push({ topic, event, data, options });
			return true;
		},
		publishBatched(messages) {
			p.publishedBatches.push({ messages });
			for (let i = 0; i < messages.length; i++) {
				const m = messages[i];
				p.published.push({
					topic: m.topic,
					event: m.event,
					data: m.data,
					options: m.options,
					batched: true
				});
			}
		},
		send(ws, topic, event, data) {
			p.sent.push({ ws, topic, event, data });
			return 1;
		},
		sendCoalesced(ws, payload) {
			p.sentCoalesced.push({ ws, ...payload });
			return 1;
		},
		request(ws, event, data, options) {
			p.requested.push({ ws, event, data, options });
			return Promise.resolve(undefined);
		},
		batch(messages) {
			return messages.map((m) => p.publish(m.topic, m.event, m.data));
		},
		sendTo(filter, topic, event, data) {
			return 0;
		},
		subscribers(topic) {
			return 0;
		},
		subscribe(ws, topic) {
			p.subscribed.push({ ws, topic });
			return null;
		},
		unsubscribe(ws, topic) {
			p.unsubscribed.push({ ws, topic });
			return false;
		},
		checkSubscribe(ws, topic) {
			p.checkedSubscribe.push({ ws, topic });
			return null;
		},
		topic(t) {
			return {
				publish(event, data) { p.publish(t, event, data); },
				created(data) { p.publish(t, 'created', data); },
				updated(data) { p.publish(t, 'updated', data); },
				deleted(data) { p.publish(t, 'deleted', data); },
				set(value) { p.publish(t, 'set', value); },
				increment(amount) { p.publish(t, 'increment', amount); },
				decrement(amount) { p.publish(t, 'decrement', amount); }
			};
		},
		reset() {
			p.published.length = 0;
			p.publishedBatches.length = 0;
			p.sent.length = 0;
			p.sentCoalesced.length = 0;
			p.requested.length = 0;
			p.subscribed.length = 0;
			p.unsubscribed.length = 0;
			p.checkedSubscribe.length = 0;
		}
	};
	return p;
}
