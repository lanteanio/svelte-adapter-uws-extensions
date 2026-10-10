import { LockLostError } from '../../../src/redis/lock.js';

export default async function throwCodedError() {
	throw new LockLostError('worker-resource');
}
