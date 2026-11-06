import { assertEnvelopeName } from './frame-topic.js';

/**
 * Prepare a relay-only coalesced value with the platform's name and data rules.
 * @param {string} instanceId
 * @param {string} topic
 * @param {string} event
 * @param {unknown} data
 * @param {string | null | undefined} coalesceKey
 */
export function prepareCoalescedEnvelope(instanceId, topic, event, data, coalesceKey) {
	// Both names are checked for type before either for content, as the
	// production platform's relayCoalesced does.
	if (typeof topic !== 'string' || typeof event !== 'string') throw new TypeError('Topic/event names must be strings.');
	assertEnvelopeName(topic);
	assertEnvelopeName(event);
	const key = coalesceKey == null ? '' : '' + coalesceKey;
	const json = JSON.stringify(data ?? null);
	if (typeof json !== 'string') throw new TypeError('message data has no JSON form (a function or a symbol cannot be sent)');
	return { env: { instanceId, coalesced: true, topic, event, coalesceKey: key }, json };
}

/**
 * Keep the serialized value intact while adding optional trace metadata.
 * @param {Record<string, unknown>} env
 * @param {string | undefined} [dataJson]
 */
export function stringifyRelayEnvelope(env, dataJson) {
	const json = JSON.stringify(env);
	return dataJson === undefined ? json : json.slice(0, -1) + ',"data":' + dataJson + '}';
}
