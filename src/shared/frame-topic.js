/**
 * Whether a topic name can be put into a frame at all.
 *
 * The adapter quotes the topic into every envelope without escaping it, and
 * throws for a control character, a double quote or a backslash rather than
 * silently escaping (svelte-adapter-uws src/runtime/utils/topic.js `esc`). No
 * frame can be built for such a topic, so a tracker that accepted one would
 * hold state it can never deliver, and every timer that publishes for it would
 * throw. The wire subscribe path refuses these topics before any hook runs;
 * this is the same rule for topics an application passes in directly.
 *
 * @param {string} topic
 * @returns {boolean}
 */
export function isFramableTopic(topic) {
	for (let i = 0; i < topic.length; i++) {
		const c = topic.charCodeAt(i);
		if (c < 32 || c === 34 || c === 92) return false;
	}
	return true;
}
