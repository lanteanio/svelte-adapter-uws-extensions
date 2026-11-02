/**
 * Prepare a relay-only coalesced value with the platform's name and data rules.
 * @param {string} instanceId
 * @param {string} topic
 * @param {string} event
 * @param {unknown} data
 * @param {string | null | undefined} coalesceKey
 */
export function prepareCoalescedEnvelope(instanceId, topic, event, data, coalesceKey) {
	if (typeof topic !== 'string' || typeof event !== 'string') throw new TypeError('Topic/event names must be strings.');
	for (const name of [topic, event]) {
		for (let i = 0; i < name.length; i++) {
			const c = name.charCodeAt(i);
			if (c < 32 || c === 34 || c === 92) {
				throw new Error(
					`Topic/event name contains invalid character at index ${i}: '${name}'. ` +
					'Names must not contain quotes, backslashes, or control characters.'
				);
			}
		}
	}
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
