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
	'pressure', 'protection', 'onPressure', 'onPublishRate',
	'subscribers', 'forEachSubscriber', 'subscribe', 'unsubscribe', 'checkSubscribe',
	'authorizeWireSubscribe',
	'topic', 'topicEpoch',
	'maxPayloadLength', 'bufferedAmount',
	'closedWsAborts',
	'now', 'monotonic', 'random', 'hlc'
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
		wireSubscribeAuthorized: false,
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
		// Framework-convention slots also forwarded by the bus wraps: an app
		// stashes its ioredis client on `platform.redis` and the presence
		// registry on `platform.presence`. Default `undefined`; the wrap-parity
		// test drives the forward-when-set behavior.
		redis: undefined,
		presence: undefined,
		// Framework-convention slot: svelte-realtime's clustered `live.smooth`
		// reads `platform.smooth` (the cross-instance smooth coordinator). Default
		// `undefined`; the wrap-parity test drives the forward-when-set behavior.
		smooth: undefined,
		// Framework-convention slot: layers that stamp or order by this
		// instance's clock read `platform.clockFence` (attachClockFence) to
		// stand down while the clock is fenced. Default undefined = no fence.
		clockFence: undefined,
		// platform.pressure stub. Default snapshot mirrors a healthy worker.
		// Tests drive transitions via _setPressure(snapshot).
		pressure: {
			active: false,
			subscriberRatio: 0,
			publishRate: 0,
			memoryMB: 0,
			reason: 'NONE'
		},
		// Mirrors the adapter's resolved protection posture default. A plain
		// reassignable field (not a getter) - tests set p.protection directly
		// to drive 'normal' -> 'elevated' -> 'siege' transitions and assert the
		// wrap forwards the live value, the same pattern closedWsAborts uses.
		protection: 'normal',
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
		// Mirrors adapter 0.5.5's `platform.closedWsAborts` counter.
		// Tests that want to simulate a non-zero value can reassign
		// `p.closedWsAborts` directly; the default zero matches a healthy
		// worker and keeps the parity-test surface symmetric with the
		// adapter's real platform shape.
		closedWsAborts: 0,
		// Clock and RNG the adapter projects onto its Platform from the
		// injectable runtime module, forwarded by the bus wraps so a wrapped
		// platform exposes the same seedable source. `now` / `monotonic` are
		// functions; `random` is the `{float, u32, uuid, bytes}` object. Plain
		// reassignable fields - a harness swaps these to drive a seeded clock /
		// RNG through the wrap and assert the forward stays live.
		now: () => Date.now(),
		monotonic: () => Date.now(),
		random: {
			float: () => Math.random(),
			u32: () => (Math.random() * 0x100000000) >>> 0,
			uuid: () => '00000000-0000-0000-0000-000000000000',
			bytes: (n) => Buffer.alloc(n)
		},
		// Mirrors the adapter's projected hybrid logical clock. A plain
		// reassignable function (not a getter) so a harness can swap in a
		// seeded stamp and assert the bus wrap forwards the live reference,
		// the same pattern now / monotonic / random use. Default returns a
		// fixed stamp shape so the parity surface stays populated.
		hlc: () => ({ wall: 0, logical: 0, nodeId: 'mock' }),
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
		send(ws, topic, event, data, options) {
			p.sent.push({ ws, topic, event, data, options });
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
		// No local subscriber set in the mock, so the default walk yields
		// nothing. Tests that exercise a per-subscriber walk reassign this
		// with their own iteration over recorded subscribers.
		forEachSubscriber(topic, fn) {},
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
		authorizeWireSubscribe() {
			p.wireSubscribeAuthorized = true;
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
		// Mirrors the adapter's platform.topicEpoch. The mock returns the
		// baseline 0 for every topic; a test that drives the per-topic epoch
		// reassigns p.topicEpoch (e.g. to read a replay tracker's cache).
		topicEpoch(_topic) {
			return 0;
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
			p.wireSubscribeAuthorized = false;
		}
	};
	return p;
}
