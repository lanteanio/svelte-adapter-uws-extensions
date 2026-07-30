import { describe, it, expect } from 'vitest';
import {
	isUnsafeProjectionFieldName,
	projectDefaultUserData
} from '../../src/shared/default-projection.js';

describe('default broadcast projection policy', () => {
	it('drops credentials, personal data, and transport metadata', () => {
		const names = [
			'apiKey', 'apikey', 'serviceRoleKey', 'sessionToken', 'dbPassword',
			'connectionString', 'email', 'userEmail', 'phoneNumber', 'userphone',
			'contactphone', 'faxNumber', 'msisdn', 'e164', 'ssn', 'userSsn',
			'dateOfBirth', 'userdob', 'passportNumber', 'nationalId', 'taxId',
			'bankAccountNumber', 'routingNumber', 'iban', 'cardNumber', 'cardCvv',
			'homeAddress', 'billingAddressLine1', 'remoteAddress', 'clientIp',
			'remoteip', 'ipAddress', 'address', 'headers', 'requestUrl', 'requestId'
		];
		for (const name of names) expect(isUnsafeProjectionFieldName(name), name).toBe(true);
	});

	it('keeps ordinary roster and product fields', () => {
		const names = [
			'id', 'name', 'role', 'author', 'authorId', 'authorName', 'authoredAt',
			'microphone', 'microphoneOn', 'headphones', 'smartphone', 'halifax',
			'avatarUrl', 'imageUrl', 'profileUrl', 'shippingAddress', 'walletAddress',
			'addressBook', 'primaryKey', 'sortKey', 'hashKey', 'translationKey',
			'clientSortKey', 'serverCacheKey', 'keyCode', 'keyboard', 'account',
			'spinner', 'zipCode', 'classSnapshot', 'processname', 'businessname'
		];
		for (const name of names) expect(isUnsafeProjectionFieldName(name), name).toBe(false);
	});

	it('gives one datum the same verdict across common spellings', () => {
		const families = [
			['api', 'key'], ['service', 'role', 'key'], ['phone', 'number'],
			['date', 'of', 'birth'], ['tax', 'id'], ['home', 'address'],
			['author', 'id'], ['sort', 'key'], ['avatar', 'url']
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
			expect(new Set(spellings.map(isUnsafeProjectionFieldName)).size, parts.join(' ')).toBe(1);
		}
	});

	it('recursively projects a bounded clone without mutating the source', () => {
		const input = {
			id: '1',
			authorId: 'a-1',
			email: 'alice@example.test',
			profile: {
				name: 'Alice',
				remoteAddress: '10.0.0.4',
				avatarUrl: '/a.png'
			}
		};
		const projected = projectDefaultUserData(input);
		expect(projected).toEqual({
			id: '1',
			authorId: 'a-1',
			profile: { name: 'Alice', avatarUrl: '/a.png' }
		});
		expect(input.email).toBe('alice@example.test');
		expect(input.profile.remoteAddress).toBe('10.0.0.4');
	});
});
