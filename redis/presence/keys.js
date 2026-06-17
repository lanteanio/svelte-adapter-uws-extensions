/**
 * Redis key + channel builders for the Redis-backed presence tracker.
 *
 * The `{topic}` brace in the two hash keys is a Redis Cluster hash tag: it
 * colocates a topic's per-topic hash and every per-user hash for that topic
 * onto a single slot, so the two-key JOIN/LEAVE Lua evals stay on one node.
 * The `:userKey` suffix sits OUTSIDE the brace on purpose - it must not widen
 * the tag. `eventChannel` is deliberately NOT tagged: cross-instance pub/sub
 * fans out across the cluster rather than colocating with a topic's slot. Keep
 * these formats byte-identical and in lockstep with the KEYS arity documented
 * in lua.js.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/keys
 */

/**
 * Build the per-tracker key + channel helpers bound to a Redis client.
 *
 * @param {import('../index.js').RedisClient} client
 * @returns {{
 *   topicHashKey: (topic: string) => string,
 *   userHashKey: (topic: string, userKey: string) => string,
 *   eventChannel: (topic: string) => string
 * }}
 */
export function makeKeys(client) {
	// Per-topic hash: one field per unique user on the topic. Backs list() / count().
	function topicHashKey(topic) {
		return client.key('presence:topic:{' + topic + '}');
	}

	// Per-user hash for a topic: one field per instance currently presenting this
	// user. HLEN drives the JOIN/LEAVE broadcast decision.
	function userHashKey(topic, userKey) {
		return client.key('presence:user:{' + topic + '}:' + userKey);
	}

	function eventChannel(topic) {
		return client.key('presence:events:' + topic);
	}

	return { topicHashKey, userHashKey, eventChannel };
}
