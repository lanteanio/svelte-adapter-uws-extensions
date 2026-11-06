/**
 * Whether a topic name can be put into a frame at all.
 *
 * The adapter quotes the topic into every envelope without escaping it, and
 * throws for a control character, a double quote, a backslash or an unpaired
 * surrogate rather than silently escaping (svelte-adapter-uws
 * src/runtime/utils/topic.js `esc`): spliced raw into a text frame, an
 * unpaired surrogate is not UTF-8 and a peer fails the connection on it. No
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
		if (c >= 0xd800 && c <= 0xdfff) {
			if (c <= 0xdbff && i + 1 < topic.length) {
				const next = topic.charCodeAt(i + 1);
				if (next >= 0xdc00 && next <= 0xdfff) { i++; continue; }
			}
			return false;
		}
	}
	return true;
}

/**
 * Refuse a topic or event name the platform cannot put into a frame, with the
 * platform's errors (`esc`): a non-string, a control character, a double
 * quote, a backslash or an unpaired surrogate.
 *
 * @param {unknown} name
 */
export function assertEnvelopeName(name) {
	if (typeof name !== 'string') throw new TypeError('Topic/event names must be strings.');
	for (let i = 0; i < name.length; i++) {
		const c = name.charCodeAt(i);
		if (c < 32 || c === 34 || c === 92) {
			throw new Error(
				`Topic/event name contains invalid character at index ${i}: '${name}'. ` +
				'Names must not contain quotes, backslashes, or control characters.'
			);
		}
		if (c >= 0xd800 && c <= 0xdfff) {
			if (c <= 0xdbff && i + 1 < name.length) {
				const next = name.charCodeAt(i + 1);
				if (next >= 0xdc00 && next <= 0xdfff) { i++; continue; }
			}
			throw new Error(
				`Topic/event name contains an unpaired surrogate at index ${i}. ` +
				'Names must be well-formed Unicode.'
			);
		}
	}
}
