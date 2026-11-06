import { PLATFORM_MEMBERS } from 'svelte-adapter-uws/platform-contract';
import { trackedSubscribe, trackedUnsubscribe } from 'svelte-adapter-uws/subscriptions';

/** Re-exported from the adapter. A member added there fails the parity test until the mock and both bus wraps provide it. */
export const PLATFORM_KEYS = PLATFORM_MEMBERS;

let mockProcesses = 0;

/** The options `authorizeDelivery` takes, as the adapter's test platform reads them. */
const DELIVERY_OPTION_KEYS = new Set(['maxPendingBytes', 'timeoutMs']);
const MAX_TIMER_MS = 2147483647;

/**
 * Create a mock platform that records publish/send calls.
 * Matches the core svelte-adapter-uws Platform interface.
 */
export function mockPlatform() {
	const pressureSubscribers = new Set();
	const publishRateSubscribers = new Set();
	const grants = new WeakMap();
	const epochs = new Map();
	const gameSequences = new Map();
	const p = {
		published: [],
		publishedBatches: [],
		sent: [],
		sentCoalesced: [],
		relayedCoalesced: [],
		deliveryAuthorization: null,
		requested: [],
		subscribed: [],
		unsubscribed: [],
		checkedSubscribe: [],
		// Verdict `checkSubscribe` returns. `null` allows, matching an adapter
		// with no app subscribe hook to consult; set a string to deny, which is
		// how a test drives the fail-closed paths.
		checkSubscribeDenial: null,
		wireSubscribeAuthorized: false,
		assertions: new Map(),
		metrics: null,
		connections: 0,
		requestId: '',
		traceContext: null,
		// Structural trace surface. `enabled: false` mirrors an adapter with
		// no provider configured. `traceOperations` records every run() and
		// `traceRestored` every context handed to withContext(), so a test can
		// assert what a consumer operation observed without a real provider.
		traceOperations: [],
		traceRestored: [],
		trace: {
			enabled: false,
			current: () => p.traceContext,
			extract: (carrier) => carrier ?? null,
			inject: (carrier, context = p.traceContext) => {
				if (context?.traceparent) carrier.traceparent = context.traceparent;
				if (context?.tracestate) carrier.tracestate = context.tracestate;
				return carrier;
			},
			run: (name, options, fn) => {
				p.traceOperations.push({ name, options });
				return fn(null);
			},
			withContext: (context, fn) => {
				p.traceRestored.push(context);
				return fn();
			}
		},
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
		// Framework-convention slot: cluster-consistent / leader-stamped time
		// (attachClusterClock). Default undefined = no cluster clock.
		clusterClock: undefined,
		// platform.pressure stub. Default snapshot mirrors a healthy worker.
		// Tests drive transitions via _setPressure(snapshot).
		pressure: {
			sampledAt: null,
			active: false,
			value: 0,
			subscriberRatio: 0,
			publishRate: 0,
			memoryMB: 0,
			reason: 'NONE',
			psi: null,
			cpuThrottle: null,
			maxBufferedBytes: 0,
			backpressuredConnections: 0,
			droppedFrames: 0,
			droppedBytes: 0,
			egress: { deliveries: 0, bytes: 0, refusedTopic: 0, refusedTenant: 0 },
			topPublishers: []
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
		adviseReconnect() { return 0; },
		diagnostic() { return null; },
		isWarmupRequest() { return false; },
		metricsSnapshot() { return Promise.resolve(null); },
		introspect() {
			const pressure = p.pressure;
			return {
				connections: p.connections, closedWsAborts: p.closedWsAborts,
				protection: p.protection, maxPayloadLength: p.maxPayloadLength,
				versions: { adapter: null, protocolRevision: null, realtime: null, extensions: null },
				pressure: {
					sampledAt: pressure.sampledAt ?? null, active: pressure.active,
					reason: pressure.reason, value: pressure.value ?? 0,
					subscriberRatio: pressure.subscriberRatio, publishRate: pressure.publishRate,
					memoryMB: pressure.memoryMB, maxBufferedBytes: pressure.maxBufferedBytes ?? 0,
					backpressuredConnections: pressure.backpressuredConnections ?? 0,
					droppedFrames: pressure.droppedFrames ?? 0, droppedBytes: pressure.droppedBytes ?? 0,
					egress: {
						deliveries: pressure.egress?.deliveries ?? 0, bytes: pressure.egress?.bytes ?? 0,
						refusedTopic: pressure.egress?.refusedTopic ?? 0, refusedTenant: pressure.egress?.refusedTenant ?? 0
					}
				},
				assertions: Object.fromEntries(p.assertions), diagnostics: { retained: 0, recent: [] }
			};
		},
		grantPublish(ws, topic) {
			try { ws.getUserData(); } catch { p.closedWsAborts++; return false; }
			grants.set(ws, topic);
			return true;
		},
		revokePublish(ws) {
			try { ws.getUserData(); } catch { return false; }
			return grants.delete(ws);
		},
		publishGrant(ws) {
			try { ws.getUserData(); } catch { return null; }
			return grants.get(ws) ?? null;
		},
		publishGame(senderWs, topic, event, data, id) {
			const seq = (gameSequences.get(topic) ?? 0) + 1;
			gameSequences.set(topic, seq);
			let delivered = 0;
			p.forEachSubscriber(topic, ws => {
				if (ws !== senderWs) {
					p.sent.push({ ws, topic, event, data, options: { seq }, id });
					delivered++;
				}
			});
			return { seq, delivered };
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
		publishBatched(messages, options) {
			p.publishedBatches.push({ messages, options });
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
		// Binary methods record their logical JSON fallback; no native encoder runs here.
		publishWire(topic, event, data, wire, options) {
			const { compress, ...otherOptions } = options ?? {};
			return p.publish(topic, event, data, { ...otherOptions, compress: compress === true });
		},
		publishWireBatch(topic, event, entries, wire, options) {
			let delivered = false;
			for (const entry of entries) {
				const local = { ...options };
				if (entry.seq !== undefined) local.seq = entry.seq;
				if (entry.excludeWs != null) local.excludeWs = entry.excludeWs;
				delivered = p.publishWire(topic, event, entry.data, wire, local) || delivered;
			}
			return delivered;
		},
		sendWire(ws, topic, event, data, wire, options) {
			const { compress, ...otherOptions } = options ?? {};
			return p.send(ws, topic, event, data, { ...otherOptions, compress: compress === true });
		},
		sendWireBatch(ws, topic, event, entries, wire, options) {
			let result = 1;
			for (const entry of entries) result = p.sendWire(ws, topic, event, entry.data, wire, options);
			return result;
		},
		registerWireCodec() {},
		requestTopic() { return Promise.resolve([]); },
		send(ws, topic, event, data, options) {
			p.sent.push({ ws, topic, event, data, options });
			return 1;
		},
		sendCoalesced(ws, payload) {
			p.sentCoalesced.push({ ws, ...payload });
			return 1;
		},
		// As the adapter's test platform: the key is recorded as the string the
		// relay carries, and with no simulated cluster nothing reaches a
		// sibling, so the call reports false.
		relayCoalesced(topic, event, data, coalesceKey) {
			p.relayedCoalesced.push({ topic, event, data, coalesceKey: coalesceKey == null ? '' : '' + coalesceKey });
			return false;
		},
		// Refused as the adapter refuses it, in the same order: one policy per
		// platform, a function, and only the options it reads. The mock
		// delivers nothing, so the policy is recorded and never called.
		authorizeDelivery(callback, options) {
			if (p.deliveryAuthorization !== null) throw new Error('Delivery authorization is already configured');
			if (typeof callback !== 'function') throw new TypeError('authorizeDelivery requires a function');
			if (options !== undefined && (options === null || typeof options !== 'object' || Array.isArray(options))) {
				throw new TypeError('authorizeDelivery options must be an object');
			}
			const values = options ?? {};
			for (const key of Object.keys(values)) {
				if (!DELIVERY_OPTION_KEYS.has(key)) throw new TypeError('Unknown authorizeDelivery option ' + key);
			}
			const bytes = values.maxPendingBytes;
			const timeout = values.timeoutMs;
			if (bytes != null && (!Number.isSafeInteger(bytes) || bytes <= 0)) throw new TypeError('Invalid authorizeDelivery maxPendingBytes');
			if (timeout != null && (!Number.isSafeInteger(timeout) || timeout <= 0 || timeout > MAX_TIMER_MS)) {
				throw new TypeError('Invalid authorizeDelivery timeoutMs');
			}
			p.deliveryAuthorization = { callback, options };
		},
		// A UUID, distinct per mock, as each adapter process has its own. A
		// plain field, so a test can reassign it to stand for another process,
		// or delete it to stand for an adapter without one.
		processId: '00000000-0000-4000-8000-' + (++mockProcesses).toString(16).padStart(12, '0'),
		request(ws, event, data, options) {
			p.requested.push({ ws, event, data, options });
			return Promise.resolve(undefined);
		},
		batch(messages) {
			return messages.map((m) => p.publish(m.topic, m.event, m.data, m.options));
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
		// subscribe/unsubscribe mirror the connection's SUBSCRIPTION REGISTRY,
		// as the adapter's own platform does. Recording the call without
		// touching the registry makes a revoked socket still read as
		// subscribed, so a membership gate that consults the registry looks
		// correct here and fails open - or closed - in production.
		subscribe(ws, topic) {
			p.subscribed.push({ ws, topic });
			return trackedSubscribe(ws, topic) ? null : 'INTERNAL_ERROR';
		},
		unsubscribe(ws, topic) {
			p.unsubscribed.push({ ws, topic });
			trackedUnsubscribe(ws, topic);
			return false;
		},
		/**
		 * Records `options` as well as `(ws, topic)`. Dropping the third
		 * argument made the observer-lane flag unobservable, so no test in
		 * either repo could tell a lane that asks the observer question from
		 * one that does not - which is the whole distinction. The peer floor
		 * reads the flag, so a call site passing it on the wrong lane (or
		 * dropping it from the right one) changes production authorization.
		 * @param {any} ws
		 * @param {string} topic
		 * @param {{ requireGrant?: boolean }} [options]
		 */
		checkSubscribe(ws, topic, options) {
			p.checkedSubscribe.push({ ws, topic, options });
			return p.checkSubscribeDenial;
		},
		authorizeWireSubscribe() {
			p.wireSubscribeAuthorized = true;
		},
		// The adapter's scoped helpers: options forwarded, counters default to 1,
		// each returns the publish result.
		topic(t) {
			return {
				publish: (event, data, options) => p.publish(t, event, data, options),
				created: (data, options) => p.publish(t, 'created', data, options),
				updated: (data, options) => p.publish(t, 'updated', data, options),
				deleted: (data, options) => p.publish(t, 'deleted', data, options),
				set: (value, options) => p.publish(t, 'set', value, options),
				increment: (amount = 1, options) => p.publish(t, 'increment', amount, options),
				decrement: (amount = 1, options) => p.publish(t, 'decrement', amount, options)
			};
		},
		// Mirrors the adapter's platform.topicEpoch. The mock returns the
		// baseline 0 for every topic; a test that drives the per-topic epoch
		// reassigns p.topicEpoch (e.g. to read a replay tracker's cache).
		topicEpoch(topic) {
			return epochs.get(topic) ?? 0;
		},
		bumpTopicEpoch(topic) {
			const previous = p.topicEpoch(topic);
			const drawn = p.random.u32();
			const next = drawn === previous ? (drawn + 1) >>> 0 : drawn;
			epochs.set(topic, next);
			return next;
		},
		reset() {
			p.published.length = 0;
			p.publishedBatches.length = 0;
			p.sent.length = 0;
			p.sentCoalesced.length = 0;
			p.relayedCoalesced.length = 0;
			p.deliveryAuthorization = null;
			p.requested.length = 0;
			p.subscribed.length = 0;
			p.unsubscribed.length = 0;
			p.checkedSubscribe.length = 0;
			p.wireSubscribeAuthorized = false;
		}
	};
	return p;
}
