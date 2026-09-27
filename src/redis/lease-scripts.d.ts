/**
 * Compare-and-pexpire a lease when `KEYS[1]` still contains `ARGV[1]`.
 * `ARGV[2]` is the new TTL in milliseconds. Returns `1` when renewed and `0`
 * when the lease is absent or belongs to another holder.
 */
export declare const LEASE_RENEW_SCRIPT: string;

/**
 * Compare-and-delete a lease when `KEYS[1]` still contains `ARGV[1]`.
 * Returns `1` when released and `0` when the lease is absent or belongs to
 * another holder.
 */
export declare const LEASE_RELEASE_SCRIPT: string;
