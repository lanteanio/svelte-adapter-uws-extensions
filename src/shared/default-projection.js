/**
 * Sensitive-name policy for the zero-config presence and cursor projections.
 *
 * This is deliberately separate from stripInternal(): that public helper is a
 * credential/log redactor, while a broadcast default also has to remove common
 * personal data and request transport metadata. An explicit select callback is
 * the opt-in escape hatch for applications that intentionally share a field.
 */

import { stripInternal } from './sensitive.js';

const SENSITIVE_WORDS = new Set([
	'token', 'tokens', 'secret', 'secrets', 'password', 'passwords', 'passwd', 'pwd',
	'session', 'sessions', 'cookie', 'cookies', 'jwt', 'jwts',
	'credential', 'credentials', 'email', 'emails', 'phone', 'phones', 'telephone',
	'iban', 'ssn', 'dob', 'cc', 'pin', 'pins', 'otp', 'mfa', 'totp',
	'fax', 'faxes', 'msisdn', 'e164', 'sig', 'sigs', 'csrf', 'xsrf',
	'dek', 'kek', 'cvv', 'cvc'
]);

const SENSITIVE_COMPOUNDS = [
	'creditcard', 'creditcards', 'debitcard', 'cardnumber', 'cardnumbers',
	'securitycode', 'accountnumber', 'routingnumber', 'sortcode',
	'bankaccount', 'bankrouting', 'accountkey', 'subscriptionkey',
	'aeskey', 'wrappingkey', 'presharedkey', 'derivedkey',
	'sharedaccesssignature', 'backupcode', 'backupcodes', 'recoverycode',
	'recoverycodes', 'invitecode', 'invitecodes', 'magiclink', 'magiclinks',
	'securityanswer', 'seedphrase', 'recoveryphrase', 'walletseed',
	'codeverifier', 'oobcode', 'taxid', 'taxpayerid', 'nationalid',
	'passportnumber', 'passportno', 'socialsecurity', 'driverslicense',
	'driverlicense', 'driverslicence', 'driverlicence', 'drivinglicense',
	'drivinglicence', 'nationalinsurance', 'aadhaarnumber', 'nhsnumber',
	'medicalrecordnumber', 'maidenname', 'placeofbirth', 'dateofbirth',
	'birthdate', 'birthday', 'phonenumber', 'mobilenumber', 'homeaddress',
	'billingaddress', 'postaladdress', 'streetaddress', 'mailingaddress'
];

const EXACT_SENSITIVE_NAMES = new Set([
	'hmac', 'hmacs', 'signature', 'signatures', 'nonce', 'nonces'
]);

const SENSITIVE_SUBSTRINGS = [
	'token', 'secret', 'password', 'passwd', 'passphrase', 'session', 'cookie',
	'jwt', 'credential', 'email', 'iban', 'creditcard', 'apikey', 'privatekey',
	'privkey', 'accesskey', 'signingkey', 'secretkey', 'bearer', 'mnemonic',
	'keystore', 'connectionstring', 'passcode', 'csrf', 'xsrf', 'msisdn', 'e164'
];

const CREDENTIAL_KEY_QUALIFIERS = new Set([
	'api', 'apis', 'access', 'secret', 'private', 'priv', 'auth', 'session',
	'bearer', 'refresh', 'pass', 'master', 'root', 'admin', 'super', 'signing',
	'sign', 'signature', 'crypto', 'hmac', 'encryption', 'decryption', 'cipher',
	'symmetric', 'asymmetric', 'ssh', 'gpg', 'pgp', 'rsa', 'ecdsa', 'ed25519',
	'tls', 'ssl', 'cert', 'certificate', 'seed', 'salt', 'nonce', 'stream',
	'server', 'device', 'webhook', 'recovery', 'pairing', 'vapid', 'idempotency',
	'license', 'licence', 'deploy', 'activation', 'client', 'consumer',
	'publishable', 'restricted', 'live', 'test', 'sandbox', 'aws', 'stripe',
	'host', 'service'
]);

const BENIGN_KEY_NOUNS = new Set([
	'primary', 'foreign', 'composite', 'candidate', 'natural', 'surrogate',
	'unique', 'sort', 'hash', 'range', 'partition', 'row', 'column', 'index',
	'cache', 'lookup', 'map', 'dedup', 'shard', 'bucket', 'object', 'storage',
	'cluster', 'translation', 'i18n', 'locale', 'message', 'route', 'react',
	'list', 'tab', 'music', 'group', 'aggregate', 'sibling', 'parent', 'child'
]);

const AUTHOR_WORDS = new Set(['author', 'authors', 'authored', 'authoring']);
const AUTHOR_FLAT_SUFFIXES = new Set([
	'id', 'ids', 'uid', 'uuid', 'name', 'names', 'slug', 'handle', 'label',
	'title', 'at', 'by', 'on', 'avatar', 'initials', 'email', 'key', 'ref'
]);
const KEY_SECRET_SUFFIXES = new Set([
	'pair', 'pairs', 'vault', 'chain', 'ring', 'material', 'store', 'file',
	'seed', 'phrase', 'passphrase', 'backup', 'export', 'archive', 'bytes',
	'blob', 'share', 'shares', 'slot', 'handle', 'dump', 'secret', 'data'
]);
const FLAT_SENSITIVE_TOKENS = ['pwd', 'dob', 'otp', 'mfa', 'totp', 'cvv', 'cvc'];
const FLAT_SSN_PREFIXES = [
	'user', 'customer', 'client', 'employee', 'member', 'person', 'patient',
	'taxpayer', 'account', 'profile', 'owner'
];
const FLAT_SSN_SUFFIXES = [
	'last4', 'number', 'hash', 'id', 'value', 'masked', 'suffix', 'digits'
];
const FLAT_CONTACT_PREFIXES = [
	'user', 'customer', 'client', 'employee', 'member', 'person', 'patient',
	'contact', 'home', 'work', 'office', 'mobile', 'cell', 'primary', 'secondary',
	'emergency', 'guardian', 'parent', 'billing', 'shipping', 'profile', 'account',
	'owner'
];
const FLAT_CONTACT_SUFFIXES = [
	's', 'number', 'numbers', 'no', 'value', 'values', 'last4', 'digits', 'hash',
	'hashed', 'masked', 'verified', 'verification', 'countrycode', 'extension', 'ext'
];
const FLAT_KEY_SUBJECTS = [
	'user', 'node', 'group', 'record', 'item', 'entity', 'service', 'storage',
	'account', 'client', 'customer', 'tenant', 'project', 'workspace', 'app',
	'application', 'role', 'grid', 'device', 'team', 'organization', 'organisation',
	'org', 'member', 'owner', 'author', 'bot', 'peer', 'host', 'server', 'vendor',
	'provider', 'integration', 'environment', 'env', 'deployment', 'worker', 'agent'
];

function fieldNameWords(name) {
	return name
		.replace(/([a-z0-9])([A-Z])/g, '$1 $2')
		.replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
		.split(/[^A-Za-z0-9]+/)
		.filter(Boolean)
		.map((word) => word.toLowerCase());
}

function joinedFieldName(name) {
	return name.toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function endsWithDigit(word) {
	const code = word.charCodeAt(word.length - 1);
	return code >= 48 && code <= 57;
}

function stripTrailingDigits(word) {
	let end = word.length;
	while (end > 0 && word.charCodeAt(end - 1) >= 48 && word.charCodeAt(end - 1) <= 57) end--;
	return end === word.length ? word : word.slice(0, end);
}

function stripWordOrdinal(word) {
	return endsWithDigit(word) ? stripTrailingDigits(word) : word;
}

function flatAuthorWords(flat) {
	for (const stem of AUTHOR_WORDS) {
		if (flat.length > stem.length && flat.startsWith(stem)) {
			const rest = flat.slice(stem.length);
			if (AUTHOR_FLAT_SUFFIXES.has(rest)) return [stem, rest];
		}
	}
	return null;
}

function flatContactIsSensitive(flat) {
	for (const token of ['telephone', 'phone', 'fax']) {
		let at = flat.indexOf(token);
		while (at !== -1) {
			const head = flat.slice(0, at);
			const tail = flat.slice(at + token.length);
			let owned = head === '';
			if (!owned) {
				for (const prefix of FLAT_CONTACT_PREFIXES) {
					if (head.endsWith(prefix)) { owned = true; break; }
				}
			}
			let described = tail === '';
			if (!described) {
				for (const suffix of FLAT_CONTACT_SUFFIXES) {
					if (tail.startsWith(suffix)) { described = true; break; }
				}
			}
			if (owned && described) return true;
			at = flat.indexOf(token, at + 1);
		}
	}
	return false;
}

function flatSsnIsSensitive(flat) {
	let at = flat.indexOf('ssn');
	while (at !== -1) {
		const end = at + 3;
		if (at === 0 || end === flat.length) return true;
		const head = flat.slice(0, at);
		const tail = flat.slice(end);
		if (FLAT_SSN_PREFIXES.some((prefix) => head.endsWith(prefix))) return true;
		if (FLAT_SSN_SUFFIXES.some((suffix) => tail.startsWith(suffix))) return true;
		at = flat.indexOf('ssn', at + 1);
	}
	return false;
}

function flatHeadHasCredentialQualifier(head) {
	for (const qualifier of CREDENTIAL_KEY_QUALIFIERS) {
		if (head === qualifier) return true;
		if (head.endsWith(qualifier) && head.slice(0, -qualifier.length).includes('key')) return true;
		for (const subject of FLAT_KEY_SUBJECTS) {
			if (head.startsWith(qualifier) && head.slice(qualifier.length).startsWith(subject)) return true;
			if (head.startsWith(subject) && head.slice(subject.length).startsWith(qualifier)) return true;
			if (head.endsWith(qualifier) && head.slice(0, -qualifier.length).endsWith(subject)) return true;
		}
	}
	return false;
}

function matchesAtWordStart(joined, starts, needles) {
	for (const needle of needles) {
		let at = joined.indexOf(needle);
		while (at !== -1) {
			for (const start of starts) {
				if (start === at) return true;
				if (start > at) break;
			}
			at = joined.indexOf(needle, at + 1);
		}
	}
	return false;
}

function keyIsCredential(words, index) {
	if (words.length === 1) return true;
	if (index + 1 < words.length && KEY_SECRET_SUFFIXES.has(stripWordOrdinal(words[index + 1]))) return true;
	const before = index > 0 ? stripWordOrdinal(words[index - 1]) : null;
	if (before !== null && CREDENTIAL_KEY_QUALIFIERS.has(before)) return true;
	if (before !== null && BENIGN_KEY_NOUNS.has(before)) return false;
	for (let i = 0; i < words.length; i++) {
		if (i !== index && CREDENTIAL_KEY_QUALIFIERS.has(stripWordOrdinal(words[i]))) return true;
	}
	return false;
}

export function isSensitiveProjectionFieldName(name) {
	const joined = joinedFieldName(name);
	if (EXACT_SENSITIVE_NAMES.has(joined)) return true;
	if (SENSITIVE_SUBSTRINGS.some((token) => joined.includes(token))) return true;

	let words = fieldNameWords(name);
	if (words.length === 1) {
		const flat = words[0];
		const authorWords = flatAuthorWords(flat);
		if (authorWords !== null) {
			words = authorWords;
		} else {
			if (FLAT_SENSITIVE_TOKENS.some((token) => flat.includes(token))) return true;
			if (flatSsnIsSensitive(flat) || flatContactIsSensitive(flat)) return true;
			const ordinalFlat = stripWordOrdinal(flat);
			if (ordinalFlat === 'key' || ordinalFlat === 'keys') return true;
			const keyAt = flat.lastIndexOf('key');
			if (keyAt !== -1) {
				const head = flat.slice(0, keyAt);
				const tail = flat.slice(keyAt + 3);
				const ordinalHead = stripWordOrdinal(head);
				const parts = [];
				for (const qualifier of CREDENTIAL_KEY_QUALIFIERS) {
					if (ordinalHead === qualifier) parts.push(qualifier);
					for (const subject of FLAT_KEY_SUBJECTS) {
						if (ordinalHead.endsWith(qualifier) && ordinalHead.slice(0, -qualifier.length).endsWith(subject)) {
							parts.push(qualifier);
							break;
						}
					}
				}
				for (const noun of BENIGN_KEY_NOUNS) if (ordinalHead.endsWith(noun)) parts.push(noun);
				const before = parts.length > 0
					? parts.reduce((a, b) => (b.length > a.length ? b : a))
					: ordinalHead;
				const segmented = [];
				if (before !== '') segmented.push(before);
				const keyIndex = segmented.length;
				segmented.push('key');
				if (tail !== '') segmented.push(tail);
				if (keyIsCredential(segmented, keyIndex)) return true;
				if (!BENIGN_KEY_NOUNS.has(before) && flatHeadHasCredentialQualifier(ordinalHead)) return true;
			}
		}
	}

	if (words.length === 1) {
		if (SENSITIVE_COMPOUNDS.some((compound) => joined.includes(compound))) return true;
	} else {
		const starts = [];
		let offset = 0;
		for (const word of words) {
			starts.push(offset);
			offset += word.length;
		}
		if (matchesAtWordStart(joined, starts, SENSITIVE_COMPOUNDS)) return true;
	}

	for (let i = 0; i < words.length; i++) {
		const word = words[i];
		if (SENSITIVE_WORDS.has(word)) return true;
		if (endsWithDigit(word) && SENSITIVE_WORDS.has(stripTrailingDigits(word))) return true;
		if (word.includes('auth') && !AUTHOR_WORDS.has(word)) return true;
		const ordinalWord = stripWordOrdinal(word);
		if ((ordinalWord === 'key' || ordinalWord === 'keys') && keyIsCredential(words, i)) return true;
	}
	return false;
}

const TRANSPORT_EXACT_NAMES = new Set([
	'url', 'requesturl', 'originalurl', 'fullurl', 'requestid', 'useragent',
	'xforwardedfor', 'forwardedfor', 'forwarded', 'xrealip', 'xforwardedproto',
	'xoriginalforwardedfor', 'xvercelforwardedfor', 'xenvoyexternaladdress',
	'xazureclientip', 'xforwardedhost', 'remoteport', 'localport', 'headers',
	'header', 'httpheaders', 'requestheaders', 'address', 'addresses',
	'remoteaddress', 'clientaddress', 'peeraddress', 'socketaddress',
	'ipaddress', 'ipaddr'
]);
const TRANSPORT_WORDS = new Set(['ip', 'ips', 'ipv4', 'ipv6', 'addr', 'referer', 'referrer']);
const FLAT_TRANSPORT_OWNERS = [
	'remote', 'client', 'peer', 'socket', 'source', 'origin', 'request', 'local',
	'public', 'private', 'server', 'host', 'proxy', 'upstream', 'downstream',
	'destination', 'dest', 'forwarded', 'real', 'external', 'connecting',
	'cfconnecting', 'trueclient', 'fastlyclient', 'flyclient', 'azureclient',
	'envoyexternal', 'xclient', 'xclusterclient'
];
const FLAT_TRANSPORT_SUFFIXES = [
	'address', 'addr', 'hash', 'hashed', 'mask', 'masked', 'last4', 'value',
	'string', 'bytes', 'version', 'family', 'country', 'city', 'geo',
	'geolocation', 'asn', 'range', 'prefix', 'network', 'subnet', 'v4', 'v6'
];

function flatTransportIsUnsafe(flat) {
	for (const token of ['ip', 'addr']) {
		let at = flat.indexOf(token);
		while (at !== -1) {
			const head = flat.slice(0, at);
			const tail = flat.slice(at + token.length);
			let owned = head === '';
			if (!owned) {
				for (const owner of FLAT_TRANSPORT_OWNERS) {
					if (head.endsWith(owner)) { owned = true; break; }
				}
			}
			if (owned) {
				if (tail === '') return true;
				for (const suffix of FLAT_TRANSPORT_SUFFIXES) {
					if (tail.startsWith(suffix)) return true;
				}
			}
			at = flat.indexOf(token, at + 1);
		}
	}
	return false;
}

export function isStructurallyUnsafeProjectionFieldName(name) {
	if (name.startsWith('__') || name === 'constructor' || name === 'prototype') return true;
	const joined = joinedFieldName(name);
	if (TRANSPORT_EXACT_NAMES.has(joined)) return true;
	if (endsWithDigit(joined) && TRANSPORT_EXACT_NAMES.has(stripTrailingDigits(joined))) return true;
	const words = fieldNameWords(name);
	if (words.length === 1 && flatTransportIsUnsafe(words[0])) return true;
	return words.some((word) => TRANSPORT_WORDS.has(word));
}

const VERDICT_CACHE = new Map();
const VERDICT_CACHE_MAX = 1024;
const VERDICT_CACHE_MAX_NAME_LENGTH = 64;
let verdictCursor = null;

function evictOneVerdict() {
	for (let wrapped = 0; wrapped < 2; wrapped++) {
		if (verdictCursor === null) verdictCursor = VERDICT_CACHE.keys();
		const step = verdictCursor.next();
		if (step.done) { verdictCursor = null; continue; }
		VERDICT_CACHE.delete(step.value);
		return;
	}
}

export function isUnsafeProjectionFieldName(name) {
	const cached = VERDICT_CACHE.get(name);
	if (cached !== undefined) return cached;
	if (name.length > VERDICT_CACHE_MAX_NAME_LENGTH) return true;
	const verdict = isStructurallyUnsafeProjectionFieldName(name)
		|| isSensitiveProjectionFieldName(name);
	if (VERDICT_CACHE.size >= VERDICT_CACHE_MAX) evictOneVerdict();
	VERDICT_CACHE.set(name, verdict);
	return verdict;
}

/** Apply the broadcast policy through stripInternal's bounded graph walk. */
export function projectDefaultUserData(value) {
	return stripInternal(value, undefined, undefined, undefined, isUnsafeProjectionFieldName);
}
