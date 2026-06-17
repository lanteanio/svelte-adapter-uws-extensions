/**
 * One-time dev-warn dedup for `cursor.hooks.message` shape misuse. The most
 * common cause is wiring the hook against `createMessage({ onUnhandled })`
 * which passes raw bytes, not a parsed envelope. The fix is to switch to
 * `createMessage({ onJsonMessage(ws, msg, platform) { ... } })` (svelte-
 * realtime >= 0.5.9 + svelte-adapter-uws >= 0.5.3), which forwards the
 * parsed object directly.
 */
let _cursorHooksMessageBadShapeWarned = false;

/**
 * @param {any} data
 */
export function _warnCursorHooksMessageShape(data) {
	if (_cursorHooksMessageBadShapeWarned) return;
	_cursorHooksMessageBadShapeWarned = true;
	const got = data instanceof ArrayBuffer
		? 'ArrayBuffer (raw bytes -- did you wire this from createMessage({onUnhandled}) ?)'
		: Array.isArray(data)
			? 'Array'
			: data === null
				? 'null'
				: typeof data === 'object'
					? 'object with data.type=' + String(data.type)
					: typeof data;
	console.warn(
		'[redis/cursor] hooks.message called with unexpected shape (' + got + '). ' +
		'Expected a parsed object {type:"cursor", topic, data} or ' +
		'{type:"cursor-snapshot", topic}. ' +
		'If you wired this from `createMessage({ onUnhandled })` and got raw bytes, ' +
		'switch to `createMessage({ onJsonMessage(ws, msg, platform) { ... } })` ' +
		'which forwards the parsed JSON envelope. ' +
		'This warning fires once per process.\n' +
		'  See: https://svti.me/cursor-hooks-message'
	);
}
