# Database credential rotation

The application owns database credentials and client replacement. Updating a
secret in a vault or deployment environment does not reconfigure a running Redis
client, its subscriber duplicates, a PostgreSQL pool, or a dedicated notification
session. Treat rotation as a deployment with an overlap window and a measured
rollback path.

## Prepare the replacement

1. Inventory every credential consumer: application replicas and workers, Redis
   command and subscription connections, every Cluster node, Sentinel when used,
   PostgreSQL pools, notification bridges, migration jobs and backup operators.
   Record credential references and owners, never passwords or full connection
   URLs. Preserve TLS verification and the existing network restrictions.
2. Provision replacement access with the same required command, key, channel,
   table and sequence permissions. Keep schema ownership separate from the
   runtime role when using `autoMigrate: false`. Verify tenant restrictions,
   `search_path`, row security and role defaults as part of the replacement.
3. Establish an overlap supported by the actual server or provider. Redis ACL
   users can hold multiple passwords; apply and persist the change on every
   relevant node. PostgreSQL password authentication has one password per role:
   use a separately authorized login for overlap, or plan an interruption if the
   provider only allows replacing one login's password. A second login must not
   gain broader privileges to make a readiness check pass. See the official
   [Redis ACL rules](https://redis.io/docs/latest/commands/acl-setuser/) and
   [PostgreSQL role controls](https://www.postgresql.org/docs/16/sql-alterrole.html).
4. Reserve connection capacity for both client generations, reconnects and an
   administrative connection. Set finite connection and operation deadlines.
   Define the traffic drain deadline and an explicit decision for work still
   running at that deadline; cancelling a caller does not undo a committed write.
5. Keep a tested rollback credential available until validation ends, unless it
   is compromised. During an incident, use independently provisioned recovery
   access instead of restoring a credential known to be exposed.

## Prove fresh connections

Construct a new client using the replacement secret through the deployment's
normal secret injection path. A successful operation on an old pooled connection
does not prove that a new connection can authenticate. Do not put secrets in
command arguments, shell history, test output or evidence attachments.

For Redis, verify authentication and representative permitted operations from
the application network, including a fresh subscriber and delivery from another
connection. A successful `PING` alone does not prove key, script or channel
permissions. Exercise the configured Cluster routing and each node, or the
Sentinel discovery and data-server credentials, as applicable. Use an isolated
test namespace with expiry and remove only its own records.

For PostgreSQL, connect with the replacement login through the actual pooler
route. Check the server version and the required existing schema without running
automatic migrations. Verify representative reads and writes on isolated test
records, including required sequence privileges. Separately open the direct or
session-pooled connection used by notification bridges and verify a subscription
and notification between two sessions. A transaction-pooled query connection
does not qualify a persistent `LISTEN` connection. The
[PostgreSQL guide](./postgres-support.md) describes the preflight, connection
budget and ownership rules.

## Replace and drain

1. Start a canary with newly constructed clients and await its store and bridge
   initialization before routing traffic to it. Confirm authorization, delivery,
   bounded command latency, pool waits and task/lease renewal under normal load.
2. Roll through replicas while both credentials are valid. Stop admitting work
   to each old replica, stop its producers and scheduled work, and let its
   accepted operations settle within the drain budget. Reconcile ambiguous
   writes before retrying them. Notifications and Redis pub/sub are not durable;
   use the application's snapshot or durable replay recovery after a handover
   gap. Do not promise lossless overlap or duplicate-free delivery.
3. Deactivate modules before closing their backing connections. Close subscriber
   duplicates as well as Redis command connections; replacing only the command
   client leaves subscription connections on their old configuration. Retain
   bounded teardown deadlines and report failure to drain.
4. For an owned PostgreSQL wrapper, await `pg.end()`: it stops registered bridges,
   closes tracked standalone sessions and then closes the pool. Release checked
   out clients so shutdown can complete. For a supplied external pool, wrapper
   `end()` is a no-op: the application must await bridge deactivation, close its
   standalone clients and then await the external pool's `end()` itself. See
   [notification shutdown](./postgres-support.md#notification-sessions-and-shutdown).
5. Confirm every replica, worker, subscriber and administrative job has switched.
   Test another genuinely fresh connection after a controlled reconnect. Remove
   the old credential only after that succeeds. Verify the old credential now
   fails on a fresh connection without printing it. Credential removal does not
   prove that an already authenticated connection has closed; inspect and retire
   remaining old connections using the server/provider's controls.

If the canary or reconnect check fails, stop the rollout, preserve the overlap,
and reconstruct clients with the tested rollback credential. Investigate the
specific authentication, authorization, TLS or routing failure. Do not widen
permissions, disable TLS verification or turn command queues into indefinite
buffers to hide a rotation failure. The [Redis outage policy](./redis-outages.md)
describes finite waits and ambiguous commands.

## Recovery access and rehearsal

Keep emergency database administration reachable without the primary secret
store, application database or application login. Assign custodians, approvals,
an independent authentication factor and an expiry for emergency access. Store
recovery material through the organization's approved offline or separately
administered mechanism. Verify access from the recovery network and retain audit
records outside the failing service. Revoke temporary access and rotate exposed
credentials after use.

Rehearse a rejected replacement password, missing subscriber/table permission,
an old pool that still works while fresh connections fail, an unavailable secret
store and an interrupted rollout. In each case prove readiness blocks new
traffic, accepted work has an explicit outcome, and rollback or emergency access
restores fresh connections within the deployment's target. Record the artifact
and backend versions, credential reference/version identifiers, participating
roles, start/end times, measured recovery time, fresh-connection results and
revocation outcome. Keep credentials, personal data and message payloads out of
the record. Repeat after changing the provider, topology, authentication method
or client lifecycle; a documentation check is not a production rotation drill.
