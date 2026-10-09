// Field-name defense in depth for the presence and cursor plugins.
//
// Two failure directions, and both are real. Letting a credential through is a
// leak; dropping an ordinary display field is a product bug an app debugs from
// the client against a server working as designed. The name rules are biased
// toward dropping, so the second direction is pinned here just as carefully as
// the first.

import { describe, it, expect } from 'vitest';
import { isUnsafeProjectionFieldName } from '../../src/shared/sensitive-names.js';

/**
 * The exact question the projections ask. Asking the two underlying predicates
 * instead would test a path the shipped code does not take - and would leave
 * the memo in front of them, which is what actually answers, untested.
 */
const dropped = (name) => isUnsafeProjectionFieldName(name);

describe('flat lowercase spellings', () => {
	// A name with no separator and no camelCase hump is ONE word, so word
	// matching alone found nothing in it. Assuming otherwise reopened a hole the
	// substring rule had closed: `apiKey` and `api_key` were dropped while
	// `apikey` - the same value, a third spelling - was broadcast.
	const flat = [
		'apikey', 'APIKEY', 'secretkey', 'privatekey', 'privkey', 'accesskey',
		'sessionid', 'sessionkey', 'sessiontoken', 'accesstoken', 'refreshtoken',
		'idtoken', 'jwtsecret', 'clientsecret', 'webhooksecret', 'signingsecret',
		'apisecret', 'totpsecret', 'dbpassword', 'passwordhash', 'tokenhash',
		'setcookie', 'cookieheader', 'credentialid', 'passphrase', 'bearer',
		'mnemonic', 'keystore', 'connectionstring'
	];
	for (const name of flat) {
		it(`drops ${name}`, () => expect(dropped(name)).toBe(true));
	}
});

describe('a structural qualifier only counts immediately before the key', () => {
	// Accepting one anywhere let a credential hide behind it.
	const hidden = [
		'userApiKey', 'userAccessKey', 'userPrivateKey', 'nodePrivateKey',
		'groupPrivateKey', 'recordEncryptionKey', 'itemApiKey', 'entitySigningKey'
	];
	for (const name of hidden) {
		it(`drops ${name}`, () => expect(dropped(name)).toBe(true));
	}

	it('still passes the plain subject-qualified identifier', () => {
		// `userKey` is a supported presence dedup key field; an existing suite
		// pins it, so the rule has to keep it while catching `userApiKey`.
		expect(dropped('userKey')).toBe(false);
		expect(dropped('groupKey')).toBe(false);
	});
});

describe('credential-shaped key names', () => {
	// The previous rule listed the qualifiers that make a key secret and let
	// every unlisted one through, so all of these rode the roster - while their
	// snake_case twins were correctly dropped. Same value, two spellings,
	// opposite verdicts.
	const credentials = [
		'key', 'KEY', 'api_key', 'x-api-key', 'apiKey', 'accessKey', 'privateKey',
		'signingKey', 'licenseKey', 'masterKey',
		'streamKey', 'serverKey', 'hmacKey', 'deviceKey', 'webhookKey', 'signKey',
		'cryptoKey', 'symmetricKey', 'recoveryKey', 'pairingKey', 'seedKey', 'vapidKey',
		'idempotencyKey'
	];

	// `authorKey` is NOT here. It used to be, because the rule dropped every
	// qualifier it did not recognize, so listing it recorded a side effect
	// rather than a judgement. It is the author's identity key - the same shape
	// as `userKey`, which this plugin documents as a supported dedup field - and
	// the `author` family is deliberately kept riding a roster.
	it('keeps authorKey, which is identity rather than a credential', () => {
		expect(dropped('authorKey')).toBe(false);
		expect(dropped('userKey')).toBe(false);
	});

	for (const name of credentials) {
		it(`drops ${name}`, () => {
			expect(dropped(name)).toBe(true);
		});
	}

	it('drops the camelCase and snake_case spellings alike', () => {
		for (const camel of ['streamKey', 'hmacKey', 'deviceKey', 'serverKey']) {
			const snake = camel.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase();
			expect(dropped(camel), camel).toBe(dropped(snake));
			expect(dropped(camel), camel).toBe(true);
		}
	});

	// THE FLAT SPELLING, which is the one that actually leaked. The parity test
	// above was written to the shape of the fix rather than the shape of the
	// threat: a lowercase name with no separator and no hump is a SINGLE word,
	// so the word rule never saw `key` inside it and `streamkey` rode the roster
	// while `streamKey` and `stream_key` were dropped. A database column, a SQL
	// row and plenty of JSON payloads give you exactly that spelling.
	it('drops the flat and SCREAMING spellings too', () => {
		const qualifiers = [
			'stream', 'server', 'hmac', 'device', 'webhook', 'sign', 'crypto',
			'recovery', 'pairing', 'vapid', 'idempotency', 'license', 'master',
			'root', 'deploy', 'api', 'private', 'access', 'signing', 'encryption'
		];
		for (const q of qualifiers) {
			for (const spelling of [`${q}key`, `${q}keys`, `${q}KEY`.toUpperCase()]) {
				expect(dropped(spelling), spelling).toBe(true);
			}
		}
	});

	it('keeps English words that merely end in key', () => {
		// The flat rule has to treat an unrecognized `<x>key` as a credential,
		// because the space of credential qualifiers is open. The space of
		// ordinary words ending in "key" is closed, so it is an allowlist.
		for (const word of ['monkey', 'monkeys', 'donkey', 'turnkey', 'whiskey', 'jockey', 'hockey', 'lackey', 'malarkey', 'hotkey', 'hotkeys']) {
			expect(dropped(word), word).toBe(false);
		}
	});

	it('keeps the flat structural identities', () => {
		for (const word of ['userkey', 'primarykey', 'foreignkey', 'sortkey', 'partitionkey', 'rowkey', 'cachekey', 'publickey']) {
			expect(dropped(word), word).toBe(false);
		}
	});

	// The input-word exception has to work from BOTH sides. Leading-only missed
	// the trailing half, so `heldKeys` - the natural spelling of the held-key
	// state the exception exists to save - was still dropped.
	it('keeps the keyboard and game input family on either side of key', () => {
		for (const word of [
			'keyCode', 'keyDown', 'keyUp', 'keyPress', 'keyMap', 'keyBinding', 'keyFrame',
			'keyState', 'heldKeys', 'pressedKeys', 'arrowKeys', 'modifierKeys', 'keysDown',
			'keysHeld', 'onKeyDown', 'handleKeyDown', 'keyMapping', 'keyPresses', 'keyName'
		]) {
			expect(dropped(word), word).toBe(false);
		}
	});

	it('still drops a credential that merely sits next to an input word', () => {
		for (const word of ['keyMaterial', 'keyData', 'keyVault', 'keyMapToken', 'keyPressSecret']) {
			expect(dropped(word), word).toBe(true);
		}
	});

	// Removed once on the argument that the word pass already covered it. Counted,
	// that was wrong by a factor of fifty, on financial PII.
	it('drops the flat iban spellings', () => {
		for (const word of ['useriban', 'customeriban', 'payeeiban', 'accountiban', 'ibannumber', 'ibanaccount', 'ibanlast4', 'primaryiban']) {
			expect(dropped(word), word).toBe(true);
		}
	});
});

describe('structural key names keep riding a roster', () => {
	const identifiers = [
		'primaryKey', 'foreignKey', 'sortKey', 'partitionKey', 'rowKey', 'cacheKey',
		'publicKey', 'userKey', 'groupKey', 'compositeKey'
	];
	for (const name of identifiers) {
		it(`passes ${name}`, () => expect(dropped(name)).toBe(false));
	}

	it('never reaches the key rule for a word that merely contains "key"', () => {
		for (const name of ['monkey', 'keyboard', 'turnkey', 'donkey']) {
			expect(dropped(name), name).toBe(false);
		}
	});
});

describe('a sensitive token inside a longer word is not a match', () => {
	// The substring rule dropped every one of these. On a huddle or voice
	// surface the mic-state field is exactly what a presence roster carries.
	const ordinary = [
		'microphone', 'microphoneOn', 'microphoneEnabled', 'headphones',
		'phonetic', 'phoneticName', 'spinner', 'pinned', 'account', 'success',
		'accuracy', 'zip', 'zipCode', 'description', 'recipient', 'cardId', 'agentId'
	];
	for (const name of ordinary) {
		it(`passes ${name}`, () => expect(dropped(name)).toBe(false));
	}

	it('still drops the token when it IS the word', () => {
		for (const name of ['phone', 'phoneNumber', 'userPhone', 'token', 'sessionToken', 'pin', 'cc', 'ssn', 'dob']) {
			expect(dropped(name), name).toBe(true);
		}
	});
});

describe('the author family', () => {
	it('passes ordinary display identity', () => {
		for (const name of ['author', 'authorId', 'authorName', 'authoredAt', 'authors']) {
			expect(dropped(name), name).toBe(false);
		}
	});

	it('still drops real auth names', () => {
		for (const name of ['authorization', 'oauth', 'authToken', 'authentic', 'AUTH']) {
			expect(dropped(name), name).toBe(true);
		}
	});
});

describe('transport identity', () => {
	// Matching only the bare spellings left the IP crossing under every
	// qualified one.
	const transport = [
		'ip', 'clientIp', 'ipAddress', 'remoteIp', 'peerIp', 'ip_address', 'ipv4', 'ipv6',
		'address', 'clientAddress', 'remoteAddr', 'remote_addr', 'remoteAddress',
		'x-forwarded-for', 'xForwardedFor', 'forwardedFor',
		'x-original-forwarded-for', 'x-vercel-forwarded-for', 'x-envoy-external-address',
		'x-azure-clientip', 'x-forwarded-host',
		'userAgent', 'user-agent', 'referer', 'referrer', 'headers', 'url', 'requestId'
	];
	for (const name of transport) {
		it(`drops ${name}`, () => expect(dropped(name)).toBe(true));
	}

	it('does not eat the display fields that share those words', () => {
		// Matched whole rather than per word, because their words are ordinary:
		// `url` as a WORD takes avatarUrl and imageUrl, which is what a roster is
		// for; `address` takes shippingAddress and walletAddress; `header` takes
		// columnHeader. `host` is not matched at all - on a meeting surface
		// `host: true` is first-class identity, and the Host header is the
		// server's own name rather than anything about the client.
		for (const name of [
			'avatarUrl', 'imageUrl', 'profileUrl', 'hostId', 'host', 'hostName',
			'shippingAddress', 'walletAddress', 'addressBook', 'columnHeader',
			'sectionHeader', 'headerColor'
		]) {
			expect(dropped(name), name).toBe(false);
		}
	});

	it('drops qualified IP and addr fields in the flat spelling too', () => {
		// A flat database/JSON field has no word boundary: the word rule saw
		// `remoteIp`, `remote_ip` and `remote-ip`, but not `remoteip`.
		const families = [
			['remote', 'ip'], ['client', 'ip'], ['peer', 'ip'], ['socket', 'ip'],
			['source', 'ip'], ['local', 'ip'], ['public', 'ip'], ['private', 'ip'],
			['server', 'ip'], ['proxy', 'ip'], ['upstream', 'ip'],
			['downstream', 'ip'], ['cf', 'connecting', 'ip'],
			['true', 'client', 'ip'], ['fastly', 'client', 'ip'],
			['fly', 'client', 'ip'], ['x', 'client', 'ip'],
			['remote', 'addr'], ['client', 'addr'], ['peer', 'addr'],
			['socket', 'addr'], ['remote', 'ip', 'hash'], ['ip', 'last4'],
			['addr', 'family']
		];
		const cap = (word) => word[0].toUpperCase() + word.slice(1);
		for (const parts of families) {
			const spellings = [
				parts.join(''),
				parts[0] + parts.slice(1).map(cap).join(''),
				parts.join('_'),
				parts.join('-'),
				parts.join('_').toUpperCase()
			];
			for (const spelling of spellings) {
				expect(dropped(spelling), spelling).toBe(true);
			}
		}
	});

	it('does not mistake ordinary flat words containing ip for transport data', () => {
		// The flat repair must recover a high-signal boundary, not turn `ip`
		// into a substring rule.
		for (const name of [
			'zip', 'shipping', 'relationship', 'ownership', 'leadership',
			'snippet', 'iphone', 'ipad', 'ipod', 'ipfs', 'clientiphone'
		]) {
			expect(dropped(name), name).toBe(false);
		}
	});

	it('still drops the bare transport spellings those rules exist for', () => {
		for (const name of [
			'address', 'addresses', 'remoteAddress', 'ipAddress', 'headers', 'header',
			'rawHeaders', 'rawHeader', 'requestRawHeaders', 'httpRawHeaders'
		]) {
			expect(dropped(name), name).toBe(true);
		}
	});
});
