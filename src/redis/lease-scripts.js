/**
 * Public Redis lease-script constants shared by the lock, leader, and
 * smooth-cluster primitives. Most applications should use those higher-level
 * APIs; this subpath is for tooling that deliberately calls `redis.eval`.
 *
 * @module svelte-adapter-uws-extensions/redis/lease-scripts
 */

export {
	LEASE_RENEW_SCRIPT,
	LEASE_RELEASE_SCRIPT
} from '../shared/lease-scripts.js';
