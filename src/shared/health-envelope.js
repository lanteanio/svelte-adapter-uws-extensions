/** Keep a status notice bounded independently of the enclosing bus frame. */
const MAX_NOTICE_BYTES = 2048;
const MAX_MESSAGE_KEY_BYTES = 128;
const MAX_MESSAGE_PARAMS = 64;
const MAX_MESSAGE_PARAM_STRING_BYTES = 1024;
const MAX_MESSAGE_PARAMS_BYTES = 4096;

function isMessageKey(value) {
	return typeof value === 'string' && /^[A-Za-z0-9_.-]+$/.test(value) && Buffer.byteLength(value, 'utf8') <= MAX_MESSAGE_KEY_BYTES;
}

function validateMessageParams(params, label) {
	const error = () => new Error('degradation policy: ' + label + '.messageParams must be a plain object with at most 64 entries, safe keys, scalar values, strings of at most 1024 UTF-8 bytes, and at most 4096 serialized UTF-8 bytes');
	if (!params || typeof params !== 'object' || Array.isArray(params)) throw error();
	const prototype = Object.getPrototypeOf(params);
	if (prototype !== Object.prototype && prototype !== null) throw error();
	const entries = Object.entries(params);
	if (entries.length > MAX_MESSAGE_PARAMS) throw error();
	for (const [key, value] of entries) {
		if (!isMessageKey(key)) throw error();
		if (value !== null && typeof value !== 'string' && typeof value !== 'boolean' && !(typeof value === 'number' && Number.isFinite(value))) throw error();
		if (typeof value === 'string' && Buffer.byteLength(value, 'utf8') > MAX_MESSAGE_PARAM_STRING_BYTES) throw error();
	}
	if (Buffer.byteLength(JSON.stringify(params), 'utf8') > MAX_MESSAGE_PARAMS_BYTES) throw error();
}

/**
 * Validate a mitigation / recovery envelope. All fields optional; unknown fields are
 * passed through untouched (the client interprets them).
 * @param {any} env
 * @param {string} label
 * @returns {any}
 */
export function validatePolicyEnvelope(env, label) {
	if (env == null) return null;
	if (typeof env !== 'object') throw new Error('degradation policy: ' + label + ' must be an object or null');
	if (env.streams !== undefined && (!Array.isArray(env.streams) || !env.streams.every((s) => typeof s === 'string'))) throw new Error('degradation policy: ' + label + '.streams must be an array of topic strings');
	if (env.rpcs !== undefined && (!Array.isArray(env.rpcs) || !env.rpcs.every((s) => typeof s === 'string'))) throw new Error('degradation policy: ' + label + '.rpcs must be an array of path strings');
	if (env.retryAfterMs !== undefined && (typeof env.retryAfterMs !== 'number' || !Number.isFinite(env.retryAfterMs) || env.retryAfterMs < 0)) {
		throw new Error('degradation policy: ' + label + '.retryAfterMs must be a non-negative number');
	}
	const notice = env.bannerCopy;
	// The same code point must be a letter/number and non-ignorable: a blank
	// symbol beside a default-ignorable letter filler is still not notice text.
	if (notice !== undefined && (typeof notice !== 'string' || Buffer.byteLength(notice, 'utf8') > MAX_NOTICE_BYTES || !notice.isWellFormed() || !/(?!\p{Default_Ignorable_Code_Point})[\p{L}\p{N}]/u.test(notice) || /(?![\u200c\u200d])[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u.test(notice))) {
		throw new Error('degradation policy: ' + label + '.bannerCopy must be well-formed plain text containing a non-ignorable letter or number, at most 2048 UTF-8 bytes, without control characters or line breaks');
	}
	if (env.messageKey !== undefined && !isMessageKey(env.messageKey)) {
		throw new Error('degradation policy: ' + label + '.messageKey must contain only ASCII letters, digits, dot, underscore, or hyphen and be at most 128 UTF-8 bytes');
	}
	if (env.messageParams !== undefined) validateMessageParams(env.messageParams, label);
	if (env.refetch !== undefined && typeof env.refetch !== 'boolean') throw new Error('degradation policy: ' + label + '.refetch must be a boolean');
	if (env.clearCache !== undefined && typeof env.clearCache !== 'boolean') throw new Error('degradation policy: ' + label + '.clearCache must be a boolean');
	return env;
}


/**
 * The bus-owned control topic carries the producer's two health events.
 * A valid shape is not proof of origin: Redis publishers remain trusted.
 * @param {unknown} event
 * @param {any} data
 * @returns {boolean}
 */
export function isHealthEvent(event, data) {
	if (event !== 'degraded' && event !== 'recovered') return false;
	if (data === null || typeof data !== 'object' || Array.isArray(data)) return false;
	if (!Object.hasOwn(data, 'at') || typeof data.at !== 'number' || !Number.isFinite(data.at) || data.at < 0) return false;
	const detail = event === 'degraded' ? 'mitigation' : 'recovery';
	if (Object.keys(data).some(key => key !== 'at' && key !== detail)) return false;
	const value = data[detail];
	try {
		// Accept every container the policy factory accepts, arrays included;
		// the surrounding health data was shape-checked above.
		validatePolicyEnvelope(value, detail);
		return true;
	} catch {
		return false;
	}
}
