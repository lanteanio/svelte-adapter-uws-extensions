/**
 * A tenant-qualified erasure identity. `tenantId` is server-trusted: 1 to 64
 * characters of `[a-zA-Z0-9_-]`, or exactly `null` for the untenanted scope
 * (never every tenant). `userId` is a non-empty string, kept exactly.
 */
export interface ForgetIdentity {
	tenantId: string | null;
	userId: string;
}
