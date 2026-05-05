/**
 * Create a mock platform that records publish/send calls.
 * Matches the core svelte-adapter-uws Platform interface.
 */
export function mockPlatform() {
	const pressureSubscribers = new Set();
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
		_setPressure(snapshot) {
			p.pressure = snapshot;
			for (const cb of pressureSubscribers) {
				try { cb(snapshot); } catch { /* swallow */ }
			}
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
