import { describe, expect, it } from 'vitest';
import { runInNewContext } from 'node:vm';
import { WsClosedError, SubscribeDeniedError } from '../../src/redis/cursor.js';

for (const [name, ErrorType, brand] of [
	['closed', WsClosedError, Symbol.for('lantean.error.ws-closed.v1')],
	['denied', SubscribeDeniedError, Symbol.for('lantean.error.subscribe-denied.v1')]
]) describe(`${name} error identity`, () => {
	it('sets a non-enumerable registered brand on a native error', () => {
		const error = new ErrorType('cursor.attach', 'room', 'FORBIDDEN');
		expect(error).toBeInstanceOf(Error);
		expect(error).toBeInstanceOf(ErrorType);
		expect(Object.getOwnPropertyDescriptor(error, brand)).toEqual({ value: true, enumerable: false, writable: false, configurable: false });
	});

	it('recognizes native errors from another realm with the same brand', () => {
		const error = runInNewContext('Object.defineProperty(new Error("closed"), brand, { value: true })', { brand });
		expect(error).toBeInstanceOf(ErrorType);
	});

	it('recognizes another copy of the package in both directions', async () => {
		const duplicate = await import('../../src/shared/errors.js?identity-copy');
		const DuplicateType = name === 'closed' ? duplicate.WsClosedError : duplicate.SubscribeDeniedError;
		expect(new ErrorType('cursor.attach', 'room', 'FORBIDDEN')).toBeInstanceOf(DuplicateType);
		expect(new DuplicateType('cursor.attach', 'room', 'FORBIDDEN')).toBeInstanceOf(ErrorType);
	});

	it('refuses branded objects, inherited Error prototypes, and unrelated native errors', () => {
		const prototypeForgery = Object.create(Error.prototype);
		Object.defineProperty(prototypeForgery, brand, { value: true });
		for (const value of [null, {}, new Error('other'), { [brand]: true }, prototypeForgery, { [brand]: true, [Symbol.toStringTag]: 'Error' }]) {
			expect(value instanceof ErrorType).toBe(false);
		}
	});

	it('preserves ordinary instanceof rules for subclasses', () => {
		class Derived extends ErrorType {}
		expect(new Derived('cursor.attach', 'room', 'FORBIDDEN')).toBeInstanceOf(Derived);
		expect(new ErrorType('cursor.attach', 'room', 'FORBIDDEN')).not.toBeInstanceOf(Derived);
	});
});
