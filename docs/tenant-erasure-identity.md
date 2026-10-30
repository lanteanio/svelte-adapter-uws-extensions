# Tenant-qualified erasure identity

[Package overview](../README.md) | [Migration guide](../MIGRATION.md)

This is the contract for stores whose records carry no topic: the Redis
session store, the Redis connection registry and the Postgres task runner.
Their legacy options key a user by a bare user id; the qualified options
below key it by the exact pair of tenant and user.

## Identity

```ts
interface ForgetIdentity {
  tenantId: string | null;
  userId: string;
}
```

`tenantId` is server-trusted and follows svelte-realtime's tenant id rule: 1 to
64 ASCII letters, digits, `_` or `-`, or exactly `null`. `userId` is a nonempty
string, kept exactly as given, without coercion, trimming or case folding.
`null` or `undefined` instead of an object is the absent identity. In an object,
`{ tenantId: null, userId }` is a user in the untenanted scope; it never means
every tenant. An object without `tenantId` is invalid, not the untenanted scope.
The type is exported from `svelte-adapter-uws-extensions/forget-store` and from
the session, registry and task runner subpaths.

## Options

| Store | Legacy option | Qualified option | Called with |
| --- | --- | --- | --- |
| Redis session | `forgetUserId` | `forgetIdentity` | the session data |
| Postgres task runner | `forgetUserId` | `forgetIdentity` | `(input, name)` |
| Redis connection registry | `identify` | `identifyIdentity` | the socket |

Each pair is mutually exclusive; configuring both throws at construction. The
identity is extracted once before a write is accepted, validated, and stored
with the record. A throwing or malformed extractor rejects the write (a socket
that opens with one registers nothing, and its close releases nothing); it never
creates an unattributed record. An explicit absent identity stores an
unattributed record that no erasure reaches.

A qualified registry takes the identity object as the target of `lookup`,
`send`, `sendCoalesced` and `request`, and rejects a bare string with a
`TypeError`. Attribute cohorts for `sendTo` may span identities, and every
member keeps its full identity. An attribute named `tenantId` is not an identity
and not an authorization boundary. `requestSession` keeps routing by the opaque,
globally unique app session id.

## Erasure

The erasure method stays `purgeUser(tenantId, userId)`. A qualified store
validates the two arguments as one identity, with the same rule whether it is
called directly or through `createForgetStore`, and removes only what is stored
under exactly that identity. An invalid or missing `tenantId` throws; pass
`null` for the untenanted scope.

A qualified purge cannot report success while records written without an
identity may still hold the user. It rejects with `code: 'ERASURE_INCOMPLETE'`,
`erasureIncomplete: true`, `legacyResidue: true` and `removed`, the count this
call committed, and leaves those records in place, because their tenant is
unknown:

- Postgres tasks: any row with no identity stamp whose `user_id` is this user id
  or is null.
- Redis sessions: any session in the legacy layout during the first `ttlMs`
  after the store's keyspace first ran in qualified mode (recorded at
  `sess:identity:v1:since`), because such sessions carry no index the purge
  could read; afterwards, a legacy session still listed in the `forgetUserId`
  index of this user id.
- Redis registry: a `conns:<userId>` row written by `identify`.

Every other failure rejects with `removed` as well, and `createForgetStore`
adds it to `partialCounts`. Retry the same identity after repairing the cause.

A write in flight when its identity is erased does not survive the erasure. A
stamped task insert holds a shared transaction-level advisory lock on its
identity, and the purge takes it exclusively before deleting, so an insert in
flight commits first and is deleted. A session purge increments a counter in
the identity's index before reading it; a session write reads the counter with
its index write and checks it again after storing the record, and deletes the
record and rejects with `ERASURE_INDEX_INCOMPLETE` when it moved. A registry
purge clears the identity on every instance through the versioned forget
event. Writes that start after an erasure are ordinary writes: erasure is not
a ban, and the application stops new writes for an erased user.

## Storage and transport

The canonical encoding of an identity is `JSON.stringify([tenantId, userId])`,
which keeps `null` distinct from every string and has no delimiter to collide
on. Redis keys embed its UTF-8 bytes as unpadded base64url.

| Store | Qualified keys and channels |
| --- | --- |
| Session | record `{"identity:v1":[tenantId,userId],"r":<record>}` at the token key; index `sess:identity:v1:byuser:<base64url>` |
| Registry | rows `identity:v1:conns:<base64url>`; events on `identity:v1:registry-events`; pushes on `identity:v1:push:<instanceId>` |
| Tasks | `identity_version = 1`, `tenant_id`, `user_id`; index `(identity_version, tenant_id, user_id)` |

Registry events and user-targeted push envelopes carry `v: 1` and
`identity: { tenantId, userId }` instead of `userId`. A registry created with
`identify` neither reads nor writes these channels, so it never reads a
qualified event as a bare user id; a receiving instance delivers a push only
when the envelope's identity owns the target session there. Bootstrap scans
read only keys whose segment decodes to a valid identity.

Session tokens beginning with `identity:` are reserved in both modes, as
`byuser:` is. A replacement that stores a session for another identity removes
the token from the previous identity's index. A qualified store adopts a session
written in the legacy layout where a legacy store would extend it (`touch`, and
a `get` or lifecycle load with `refreshOnGet`): it extracts the identity,
indexes it, and rewrites the record in the qualified layout with a fresh TTL,
so active users stay signed in. A legacy session whose identity cannot be
extracted stays readable, is not extended, and expires within its TTL; `touch`
resolves `false` for it.

Task rows written without `forgetIdentity` keep `identity_version` null; a
stamped row with a `null` tenant is the untenanted scope. A check constraint
refuses a tenant on an unstamped row. Retries, recovery and cleanup update rows
in place and keep the stamp. The schema is version 4 of the task store's
migration ledger.

## Rollout

1. Apply task schema version 4 (automatic unless `autoMigrate: false`) and
   upgrade every instance, still with the legacy options.
2. Stop every instance of an earlier release: it neither reserves the
   `identity:` session prefix nor reads qualified records, and its task purge
   deletes by raw user id across tenants.
3. Drain registries created with `identify` and enable `identifyIdentity`
   everywhere at once; connected users re-register when they reconnect. Do not
   derive identities from legacy rows.
4. Enable `forgetIdentity` on the session store and task runner.
5. Backfill unstamped task rows only from authoritative ownership data
   (`identity_version = 1` with the owning `tenant_id` and `user_id`), or let
   them finish and expire. Rows of unknown ownership stay unstamped and keep
   qualified erasures incomplete; do not relabel them as the `null` tenant.
   Legacy sessions are adopted on their next refresh or expire within `ttlMs`
   (keep `ttlMs` at least as long as before the switch); session purges report
   `legacyResidue` until that window has passed. Legacy registry rows expire
   within the registry `ttl`.

Deployments that keep the legacy options keep their behaviour: a bare user id is
global within the configured prefix or table, and `tenantId` does not scope the
purge. Combining tenants with overlapping user ids in one legacy store is not
supported; give each tenant its own prefix or table, or use the qualified
options.
