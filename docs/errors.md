# Error reference

Search this page with the exact stable ID, event, or beginning of the line you saw.
Three kinds of failure are indexed here, each with its cause, what it means for traffic,
whether anything recovers on its own, and what to do next:

- 13 thrown errors. The Error message starts with the indexed prefix and ends with the
  stable ID tag and this document's route.
- 68 diagnostic events. Each is one canonical line
  `[lantean/diagnostic source=svelte-adapter-uws-extensions component=<component> event=<event> severity=<severity>] <message> {json}`,
  the same grammar the adapter prints, so `parseDiagnostic()` from `svelte-adapter-uws/observability`
  parses it and a sink installed through either package receives the record. The indexed prefix is
  that line up to and including the invariant part of the message.
- 4 plain console lines, printed through the registry so each carries its stable ID tag
  and the printed text cannot drift from the indexed prefix.

The diagnostic inventory is derived at generation time by scanning `src/` for emitted events: a
failure event cannot be added to the runtime without an entry here, because the generator fails
the build until one exists, and an entry whose event nothing emits fails it too. Plain console
output is held to the same rule from the other side: `scripts/check-console-index.js` walks every
`console.warn` and `console.error` under `src/` and fails the build unless the line prints through
this registry or carries a recorded reason an operator who read it needs nothing more.

The adapter ships its own reference for its runtime at
[svelte-adapter-uws docs/errors.md](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/errors.md).

| Stable ID | Code or event | Searchable message prefix |
|---|---|---|
| [EXTENSIONS-ERR-REGISTRY-REQUEST-TARGET](#extensions-err-registry-request-target) | `registry.request.invalid-target` | `registry.request: target must be a non-empty userId string` |
| [EXTENSIONS-ERR-REGISTRY-REQUEST-EVENT](#extensions-err-registry-request-event) | `registry.request.invalid-event` | `registry.request: event must be a non-empty string` |
| [EXTENSIONS-ERR-REGISTRY-REQUEST-OFFLINE](#extensions-err-registry-request-offline) | `registry.request.offline` | `registry.request: target user "` |
| [EXTENSIONS-ERR-REGISTRY-REQUEST-PENDING](#extensions-err-registry-request-pending) | `registry.request.pending-cap` | `registry.request: pending requests exceeded ` |
| [EXTENSIONS-ERR-REGISTRY-REQUEST-TIMEOUT](#extensions-err-registry-request-timeout) | `registry.request.timeout` | `registry.request: timed out after ` |
| [EXTENSIONS-ERR-REGISTRY-SESSION-TARGET](#extensions-err-registry-session-target) | `registry.request-session.invalid-target` | `registry.requestSession: target must be a non-empty sessionId string` |
| [EXTENSIONS-ERR-REGISTRY-SESSION-EVENT](#extensions-err-registry-session-event) | `registry.request-session.invalid-event` | `registry.requestSession: event must be a non-empty string` |
| [EXTENSIONS-ERR-REGISTRY-SESSION-OFFLINE](#extensions-err-registry-session-offline) | `registry.request-session.offline` | `registry.requestSession: target session "` |
| [EXTENSIONS-ERR-REGISTRY-SESSION-PENDING](#extensions-err-registry-session-pending) | `registry.request-session.pending-cap` | `registry.requestSession: pending requests exceeded ` |
| [EXTENSIONS-ERR-REGISTRY-SESSION-TIMEOUT](#extensions-err-registry-session-timeout) | `registry.request-session.timeout` | `registry.requestSession: timed out after ` |
| [EXTENSIONS-ERR-REGISTRY-SEND-TARGET](#extensions-err-registry-send-target) | `registry.send.invalid-target` | `registry.send: target must be a non-empty userId string` |
| [EXTENSIONS-ERR-REGISTRY-SEND-COALESCED-TARGET](#extensions-err-registry-send-coalesced-target) | `registry.send-coalesced.invalid-target` | `registry.sendCoalesced: target must be a non-empty userId string` |
| [EXTENSIONS-ERR-CIRCUIT-OPEN](#extensions-err-circuit-open) | `breaker.circuit-open` | `circuit breaker is open - backend unavailable` |
| [EXTENSIONS-ERR-CAPABILITY-COOKIE-METRICS](#extensions-err-capability-cookie-metrics) | `capability-cookie.metrics-instrument.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.capability-cookie event=capability-cookie.metrics-instrument.failed severity=error] The capability-cookie metrics instrument threw; further instrument failures are not reported.` |
| [EXTENSIONS-ERR-POSTGRES-POOL-IDLE-CLIENT](#extensions-err-postgres-pool-idle-client) | `postgres.pool.idle-client.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres event=postgres.pool.idle-client.failed severity=error] An idle Postgres pool client reported an error.` |
| [EXTENSIONS-ERR-POSTGRES-TRANSACTION-CLIENT](#extensions-err-postgres-transaction-client) | `postgres.transaction.client.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres event=postgres.transaction.client.failed severity=error] A Postgres connection checked out for a transaction reported an error.` |
| [EXTENSIONS-ERR-POSTGRES-NOTIFY-PAYLOAD-NEAR-LIMIT](#extensions-err-postgres-notify-payload-near-limit) | `postgres.notify.payload-near-limit` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.notify event=postgres.notify.payload-near-limit severity=warn] A Postgres NOTIFY payload is approaching the 8000-byte transport limit.` |
| [EXTENSIONS-ERR-POSTGRES-NOTIFY-PARSE](#extensions-err-postgres-notify-parse) | `postgres.notify.parse.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.notify event=postgres.notify.parse.failed severity=warn] The application-owned Postgres NOTIFY parser, or the publish of its result, threw.` |
| [EXTENSIONS-ERR-POSTGRES-TASKS-STATE-LISTENER](#extensions-err-postgres-tasks-state-listener) | `postgres.tasks.state-listener.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.tasks event=postgres.tasks.state-listener.failed severity=warn] The task state-change listener failed; task execution continues.` |
| [EXTENSIONS-ERR-CLOCK-SKEW-SAMPLE](#extensions-err-clock-skew-sample) | `redis.clock-skew.sample.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock-skew event=redis.clock-skew.sample.failed severity=error] A Redis clock-skew sample failed; the last good gauge value remains active.` |
| [EXTENSIONS-ERR-CLOCK-SAMPLE](#extensions-err-clock-sample) | `redis.clock.sample.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock event=redis.clock.sample.failed severity=error] A Redis cluster-clock sample failed; the last good state remains active.` |
| [EXTENSIONS-ERR-CLOCK-NTP-SAMPLE](#extensions-err-clock-ntp-sample) | `redis.clock.ntp-sample.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock event=redis.clock.ntp-sample.failed severity=warn] The auxiliary NTP clock sample failed and was excluded from this round.` |
| [EXTENSIONS-ERR-CLOCK-LEADER-OFFSET](#extensions-err-clock-leader-offset) | `redis.clock.leader-offset-read.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock event=redis.clock.leader-offset-read.failed severity=warn] The cluster-clock leader offset could not be published or read; the prior state remains active.` |
| [EXTENSIONS-ERR-CURSOR-SUBSCRIBER](#extensions-err-cursor-subscriber) | `redis.cursor.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.subscriber.failed severity=error] The Redis cursor subscriber connection reported an error.` |
| [EXTENSIONS-ERR-CURSOR-CLEANUP](#extensions-err-cursor-cleanup) | `redis.cursor.cleanup.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.cleanup.failed severity=warn] Cursor stale-entry cleanup failed; the next sweep retries.` |
| [EXTENSIONS-ERR-CURSOR-FLUSH](#extensions-err-cursor-flush) | `redis.cursor.flush.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.flush.failed severity=warn] A cursor tick could not publish one or more topics; the other topics were flushed.` |
| [EXTENSIONS-ERR-GROUPS-META-INITIALIZE](#extensions-err-groups-meta-initialize) | `redis.groups.meta-initialize.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.groups event=redis.groups.meta-initialize.failed severity=warn] The initial Redis group metadata write failed.` |
| [EXTENSIONS-ERR-GROUPS-HEARTBEAT](#extensions-err-groups-heartbeat) | `redis.groups.heartbeat.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.groups event=redis.groups.heartbeat.failed severity=warn] A Redis group heartbeat pipeline failed.` |
| [EXTENSIONS-ERR-GROUPS-SUBSCRIBER](#extensions-err-groups-subscriber) | `redis.groups.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.groups event=redis.groups.subscriber.failed severity=error] The Redis group subscriber connection reported an error.` |
| [EXTENSIONS-ERR-LEADER-TICK](#extensions-err-leader-tick) | `redis.leader.tick.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.leader event=redis.leader.tick.failed severity=error] A Redis leader election tick failed; leadership is fail-closed.` |
| [EXTENSIONS-ERR-LEADER-READ-CURRENT](#extensions-err-leader-read-current) | `redis.leader.read-current.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.leader event=redis.leader.read-current.failed severity=warn] The current Redis leader could not be read.` |
| [EXTENSIONS-ERR-LEADER-RELEASE](#extensions-err-leader-release) | `redis.leader.release.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.leader event=redis.leader.release.failed severity=warn] The Redis leader lease could not be released; it expires by TTL.` |
| [EXTENSIONS-ERR-PRESENCE-HEARTBEAT-PUBLISH](#extensions-err-presence-heartbeat-publish) | `redis.presence.heartbeat.publish-failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.heartbeat.publish-failed severity=warn] A presence heartbeat could not be published for one or more topics; the TTL refresh and the other topics went ahead.` |
| [EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBER](#extensions-err-presence-keyspace-subscriber) | `redis.presence.keyspace-subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-subscriber.failed severity=error] A presence keyspace-notification subscriber connection reported an error.` |
| [EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBE](#extensions-err-presence-keyspace-subscribe) | `redis.presence.keyspace-subscribe.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-subscribe.failed severity=warn] The presence keyspace cleanup could not subscribe to key-deletion events.` |
| [EXTENSIONS-ERR-PRESENCE-KEYSPACE-EVENTS-OFF](#extensions-err-presence-keyspace-events-off) | `redis.presence.keyspace-events.off` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-events.off severity=warn] A Redis server's keyspace notification setting cannot deliver the key deletions the presence cleanup listens for.` |
| [EXTENSIONS-ERR-PRESENCE-KEYSPACE-RECONCILE](#extensions-err-presence-keyspace-reconcile) | `redis.presence.keyspace-reconcile.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-reconcile.failed severity=error] Presence keyspace subscriber reconciliation failed; it is retried with a backoff.` |
| [EXTENSIONS-ERR-PRESENCE-SUBSCRIBER](#extensions-err-presence-subscriber) | `redis.presence.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.subscriber.failed severity=error] The Redis presence subscriber connection reported an error.` |
| [EXTENSIONS-ERR-PUBLISH-RATE-SUBSCRIBER](#extensions-err-publish-rate-subscriber) | `redis.publish-rate.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.publish-rate event=redis.publish-rate.subscriber.failed severity=error] The cluster publish-rate subscriber connection reported an error.` |
| [EXTENSIONS-ERR-PUBLISH-RATE-LISTENER](#extensions-err-publish-rate-listener) | `redis.publish-rate.listener.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.publish-rate event=redis.publish-rate.listener.failed severity=error] A publish-rate threshold listener threw; aggregation continues.` |
| [EXTENSIONS-ERR-RATELIMIT-PROXY-COLLAPSE](#extensions-err-ratelimit-proxy-collapse) | `redis.ratelimit.proxy-collapse-risk` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.ratelimit event=redis.ratelimit.proxy-collapse-risk severity=warn] A private or loopback rate-limit key was denied while no trusted address header is configured.` |
| [EXTENSIONS-ERR-REGISTRY-SUBSCRIBER](#extensions-err-registry-subscriber) | `redis.registry.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.registry event=redis.registry.subscriber.failed severity=error] The connection-registry subscriber connection reported an error.` |
| [EXTENSIONS-ERR-SHARDED-SUBSCRIBER](#extensions-err-sharded-subscriber) | `redis.sharded-pubsub.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.subscriber.failed severity=error] A sharded pub/sub subscriber connection reported an error.` |
| [EXTENSIONS-ERR-SHARDED-CHANNEL-RECONCILE](#extensions-err-sharded-channel-reconcile) | `redis.sharded-pubsub.channel-reconcile.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.channel-reconcile.failed severity=error] A sharded pub/sub channel could not be re-subscribed on its new owner.` |
| [EXTENSIONS-ERR-SHARDED-RECONCILE](#extensions-err-sharded-reconcile) | `redis.sharded-pubsub.reconcile.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.reconcile.failed severity=error] Sharded pub/sub reconciliation could not refresh slot ownership.` |
| [EXTENSIONS-ERR-PUBSUB-ENVELOPE-OVERSIZED](#extensions-err-pubsub-envelope-oversized) | `redis.pubsub.envelope.oversized` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.envelope.oversized severity=warn] A publish envelope exceeds maxEnvelopeBytes and was not relayed.` |
| [EXTENSIONS-ERR-TOPIC-BROADCAST-ENVELOPE-OVERSIZED](#extensions-err-topic-broadcast-envelope-oversized) | `redis.topic-broadcast.envelope.oversized` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.topic-broadcast event=redis.topic-broadcast.envelope.oversized severity=warn] A topic broadcast reply exceeds maxEnvelopeBytes and was not relayed.` |
| [EXTENSIONS-ERR-PUBSUB-TRACE-OMITTED](#extensions-err-pubsub-trace-omitted) | `redis.pubsub.trace-context.omitted` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.trace-context.omitted severity=warn] Trace context was omitted from a relay envelope that would otherwise exceed maxEnvelopeBytes.` |
| [EXTENSIONS-ERR-SHARDED-TRACE-OMITTED](#extensions-err-sharded-trace-omitted) | `redis.sharded-pubsub.trace-context.omitted` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.trace-context.omitted severity=warn] Trace context was omitted from a shard relay envelope that would otherwise exceed maxEnvelopeBytes.` |
| [EXTENSIONS-ERR-REDIS-REPLAY-STORAGE-FALLBACK](#extensions-err-redis-replay-storage-fallback) | `redis.replay.storage-fallback` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.replay event=redis.replay.storage-fallback severity=warn] Replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.` |
| [EXTENSIONS-ERR-REDIS-REPLAY-STREAM-STORAGE-FALLBACK](#extensions-err-redis-replay-stream-storage-fallback) | `redis.replay-stream.storage-fallback` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.replay-stream event=redis.replay-stream.storage-fallback severity=warn] Stream replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.` |
| [EXTENSIONS-ERR-POSTGRES-REPLAY-STORAGE-FALLBACK](#extensions-err-postgres-replay-storage-fallback) | `postgres.replay.storage-fallback` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.replay event=postgres.replay.storage-fallback severity=warn] Replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.` |
| [EXTENSIONS-ERR-RATELIMIT-STORE-FLOOR](#extensions-err-ratelimit-store-floor) | `redis.ratelimit.store-unreachable` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.ratelimit event=redis.ratelimit.store-unreachable severity=warn] The rate-limit store is unreachable; verdicts are decided on the in-process floor.` |
| [EXTENSIONS-ERR-COMPOSITE-RATELIMIT-STORE-FLOOR](#extensions-err-composite-ratelimit-store-floor) | `redis.composite-ratelimit.store-unreachable` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.composite-ratelimit event=redis.composite-ratelimit.store-unreachable severity=warn] The composite rate-limit store is unreachable; verdicts are decided on the in-process floor.` |
| [EXTENSIONS-ERR-PRESENCE-ENVELOPE-OVERSIZED](#extensions-err-presence-envelope-oversized) | `redis.presence.envelope-oversized` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.envelope-oversized severity=warn] A presence event envelope exceeds maxEnvelopeBytes and was not relayed to other instances.` |
| [EXTENSIONS-ERR-PUBLISH-RATE-REMOTE-CAP](#extensions-err-publish-rate-remote-cap) | `redis.publish-rate.remote-instances-capped` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.publish-rate event=redis.publish-rate.remote-instances-capped severity=warn] The publish-rate aggregator reached its sibling-instance bound; stale entries are evicted and envelopes past the bound are dropped.` |
| [EXTENSIONS-ERR-REGISTRY-INDEX-BUCKET](#extensions-err-registry-index-bucket) | `redis.registry.index-bucket.large` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.registry event=redis.registry.index-bucket.large severity=warn] A registry secondary-index bucket is very large and keeps growing.` |
| [EXTENSIONS-ERR-REGISTRY-USER-INDEX](#extensions-err-registry-user-index) | `redis.registry.user-index.large` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.registry event=redis.registry.user-index.large severity=warn] The cluster-wide registry user index is very large and keeps growing.` |
| [EXTENSIONS-ERR-PUBSUB-RELAY-BURST](#extensions-err-pubsub-relay-burst) | `redis.pubsub.relay-batch.burst` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.relay-batch.burst severity=warn] One event-loop iteration queued more relay publishes than the per-tick bound.` |
| [EXTENSIONS-ERR-PUBSUB-SYSTEM-CHANNEL-REFUSED](#extensions-err-pubsub-system-channel-refused) | `redis.pubsub.system-channel.refused` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.system-channel.refused severity=warn] A connection was not subscribed to the bus's systemChannel, so it gets no degraded or recovered events.` |
| [EXTENSIONS-ERR-SHARDED-RELAY-BURST](#extensions-err-sharded-relay-burst) | `redis.sharded-pubsub.relay-batch.channel-burst` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.relay-batch.channel-burst severity=warn] One event-loop iteration queued publishes for more distinct shard channels than the per-tick bound.` |
| [EXTENSIONS-ERR-REDIS-DUPLICATES](#extensions-err-redis-duplicates) | `redis.client.duplicates.excessive` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.client event=redis.client.duplicates.excessive severity=warn] One Redis client has spawned more duplicate connections than a process should need.` |
| [EXTENSIONS-ERR-SMOOTH-OWNER-FALLBACK](#extensions-err-smooth-owner-fallback) | `redis.smooth.relay-owner.unresolved` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.smooth event=redis.smooth.relay-owner.unresolved severity=warn] The smooth relay channel owner could not be resolved; publishes are pinned to a fallback master.` |
| [EXTENSIONS-ERR-RESUME-TOPICS-TRUNCATED](#extensions-err-resume-topics-truncated) | `replay.resume.topics-truncated` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.replay event=replay.resume.topics-truncated severity=warn] A resume request presented more topics than the per-request bound; the remainder were not resumed.` |
| [EXTENSIONS-ERR-PROMETHEUS-SERIES-CAP](#extensions-err-prometheus-series-cap) | `prometheus.series.cap-reached` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.prometheus event=prometheus.series.cap-reached severity=warn] A metric reached its maxSeries cap; new label sets for it are dropped.` |
| [EXTENSIONS-ERR-POSTGRES-TASKS-SWEEP](#extensions-err-postgres-tasks-sweep) | `postgres.tasks.sweep.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.tasks event=postgres.tasks.sweep.failed severity=error] A background task sweep failed.` |
| [EXTENSIONS-ERR-REDIS-CLIENT](#extensions-err-redis-client) | `redis.client.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.client event=redis.client.failed severity=error] The Redis client created by createRedisClient reported an error.` |
| [EXTENSIONS-ERR-REDIS-RESUBSCRIBE](#extensions-err-redis-resubscribe) | `redis.subscriber.resubscribe.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.subscriber event=redis.subscriber.resubscribe.failed severity=error] Redis refused a subscription restored after a reconnect.` |
| [EXTENSIONS-ERR-REDIS-SUBSCRIBER-SILENT](#extensions-err-redis-subscriber-silent) | `redis.subscriber.silent` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.subscriber event=redis.subscriber.silent severity=error] A Redis subscriber connection stopped answering and was closed.` |
| [EXTENSIONS-ERR-REDIS-SUBSCRIBER-UNREACHABLE](#extensions-err-redis-subscriber-unreachable) | `redis.subscriber.unreachable` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.subscriber event=redis.subscriber.unreachable severity=error] No node of the Redis Cluster accepted a subscriber connection.` |
| [EXTENSIONS-ERR-PUBSUB-SUBSCRIBER](#extensions-err-pubsub-subscriber) | `redis.pubsub.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.subscriber.failed severity=error] The pub/sub bus subscriber connection reported an error.` |
| [EXTENSIONS-ERR-CRDT-SUBSCRIBER](#extensions-err-crdt-subscriber) | `redis.crdt.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.crdt event=redis.crdt.subscriber.failed severity=error] The CRDT relay subscriber connection reported an error.` |
| [EXTENSIONS-ERR-SMOOTH-SUBSCRIBER](#extensions-err-smooth-subscriber) | `redis.smooth.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.smooth event=redis.smooth.subscriber.failed severity=error] The smooth relay subscriber connection reported an error.` |
| [EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBER](#extensions-err-topic-broadcast-subscriber) | `redis.topic-broadcast.subscriber.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.topic-broadcast event=redis.topic-broadcast.subscriber.failed severity=error] The topic-broadcast subscriber connection reported an error.` |
| [EXTENSIONS-ERR-CRDT-SUBSCRIBE](#extensions-err-crdt-subscribe) | `redis.crdt.subscribe.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.crdt event=redis.crdt.subscribe.failed severity=error] The CRDT relay could not subscribe to its channel.` |
| [EXTENSIONS-ERR-SMOOTH-SUBSCRIBE](#extensions-err-smooth-subscribe) | `redis.smooth.subscribe.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.smooth event=redis.smooth.subscribe.failed severity=error] The smooth relay could not subscribe to its channel.` |
| [EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBE](#extensions-err-topic-broadcast-subscribe) | `redis.topic-broadcast.subscribe.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.topic-broadcast event=redis.topic-broadcast.subscribe.failed severity=error] The topic broadcast could not subscribe to its channel.` |
| [EXTENSIONS-ERR-CURSOR-SUBSCRIBE](#extensions-err-cursor-subscribe) | `redis.cursor.subscribe.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.subscribe.failed severity=error] The cursor relay could not subscribe to its channel.` |
| [EXTENSIONS-ERR-POSTGRES-NOTIFY-CONNECTION](#extensions-err-postgres-notify-connection) | `postgres.notify.connection.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.notify event=postgres.notify.connection.failed severity=error] The Postgres LISTEN connection of a NOTIFY bridge reported an error and was closed.` |
| [EXTENSIONS-ERR-SINK-FAILED](#extensions-err-sink-failed) | `operational.sink.failed` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.observability event=operational.sink.failed severity=error] The configured operational event sink failed; console fallback was restored for this event.` |
| [EXTENSIONS-ERR-INVARIANT](#extensions-err-invariant) | `invariant.violated` | `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.assertion event=invariant.violated severity=` |
| [EXTENSIONS-ERR-DIAGNOSTIC-RECORD-SHAPE](#extensions-err-diagnostic-record-shape) | `observability.diagnostic.record-shape` | `[extensions] operational event dropped, invalid record shape` |
| [EXTENSIONS-ERR-DIAGNOSTIC-RENDER-COLLAPSE](#extensions-err-diagnostic-render-collapse) | `observability.diagnostic.render-collapse` | `[extensions] diagnostic render failed: ` |
| [EXTENSIONS-ERR-DIAGNOSTIC-CONSOLE-WRITE](#extensions-err-diagnostic-console-write) | `observability.diagnostic.console-write` | `[extensions] diagnostic rendered but the console refused it: ` |
| [EXTENSIONS-ERR-DIAGNOSTIC-SINK-NOTICE](#extensions-err-diagnostic-sink-notice) | `observability.diagnostic.sink-notice` | `[extensions] operational sink failed and its failure notice could not be built: ` |

## Emitted diagnostic event coverage

The runtime emits 68 distinct diagnostic events. Every one of them is indexed above with a stable ID and full operator guidance.
The split is enforced by severity rather than by a list: an emitted event is exempt from the
indexed reference only while every severity it is emitted at is informational, so promoting one
to a warning or an error fails generation until it is indexed.

Indexed events:

- `capability-cookie.metrics-instrument.failed` - [EXTENSIONS-ERR-CAPABILITY-COOKIE-METRICS](#extensions-err-capability-cookie-metrics)
- `postgres.pool.idle-client.failed` - [EXTENSIONS-ERR-POSTGRES-POOL-IDLE-CLIENT](#extensions-err-postgres-pool-idle-client)
- `postgres.transaction.client.failed` - [EXTENSIONS-ERR-POSTGRES-TRANSACTION-CLIENT](#extensions-err-postgres-transaction-client)
- `postgres.notify.payload-near-limit` - [EXTENSIONS-ERR-POSTGRES-NOTIFY-PAYLOAD-NEAR-LIMIT](#extensions-err-postgres-notify-payload-near-limit)
- `postgres.notify.parse.failed` - [EXTENSIONS-ERR-POSTGRES-NOTIFY-PARSE](#extensions-err-postgres-notify-parse)
- `postgres.tasks.state-listener.failed` - [EXTENSIONS-ERR-POSTGRES-TASKS-STATE-LISTENER](#extensions-err-postgres-tasks-state-listener)
- `redis.clock-skew.sample.failed` - [EXTENSIONS-ERR-CLOCK-SKEW-SAMPLE](#extensions-err-clock-skew-sample)
- `redis.clock.sample.failed` - [EXTENSIONS-ERR-CLOCK-SAMPLE](#extensions-err-clock-sample)
- `redis.clock.ntp-sample.failed` - [EXTENSIONS-ERR-CLOCK-NTP-SAMPLE](#extensions-err-clock-ntp-sample)
- `redis.clock.leader-offset-read.failed` - [EXTENSIONS-ERR-CLOCK-LEADER-OFFSET](#extensions-err-clock-leader-offset)
- `redis.cursor.subscriber.failed` - [EXTENSIONS-ERR-CURSOR-SUBSCRIBER](#extensions-err-cursor-subscriber)
- `redis.cursor.cleanup.failed` - [EXTENSIONS-ERR-CURSOR-CLEANUP](#extensions-err-cursor-cleanup)
- `redis.cursor.flush.failed` - [EXTENSIONS-ERR-CURSOR-FLUSH](#extensions-err-cursor-flush)
- `redis.groups.meta-initialize.failed` - [EXTENSIONS-ERR-GROUPS-META-INITIALIZE](#extensions-err-groups-meta-initialize)
- `redis.groups.heartbeat.failed` - [EXTENSIONS-ERR-GROUPS-HEARTBEAT](#extensions-err-groups-heartbeat)
- `redis.groups.subscriber.failed` - [EXTENSIONS-ERR-GROUPS-SUBSCRIBER](#extensions-err-groups-subscriber)
- `redis.leader.tick.failed` - [EXTENSIONS-ERR-LEADER-TICK](#extensions-err-leader-tick)
- `redis.leader.read-current.failed` - [EXTENSIONS-ERR-LEADER-READ-CURRENT](#extensions-err-leader-read-current)
- `redis.leader.release.failed` - [EXTENSIONS-ERR-LEADER-RELEASE](#extensions-err-leader-release)
- `redis.presence.heartbeat.publish-failed` - [EXTENSIONS-ERR-PRESENCE-HEARTBEAT-PUBLISH](#extensions-err-presence-heartbeat-publish)
- `redis.presence.keyspace-subscriber.failed` - [EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBER](#extensions-err-presence-keyspace-subscriber)
- `redis.presence.keyspace-subscribe.failed` - [EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBE](#extensions-err-presence-keyspace-subscribe)
- `redis.presence.keyspace-events.off` - [EXTENSIONS-ERR-PRESENCE-KEYSPACE-EVENTS-OFF](#extensions-err-presence-keyspace-events-off)
- `redis.presence.keyspace-reconcile.failed` - [EXTENSIONS-ERR-PRESENCE-KEYSPACE-RECONCILE](#extensions-err-presence-keyspace-reconcile)
- `redis.presence.subscriber.failed` - [EXTENSIONS-ERR-PRESENCE-SUBSCRIBER](#extensions-err-presence-subscriber)
- `redis.publish-rate.subscriber.failed` - [EXTENSIONS-ERR-PUBLISH-RATE-SUBSCRIBER](#extensions-err-publish-rate-subscriber)
- `redis.publish-rate.listener.failed` - [EXTENSIONS-ERR-PUBLISH-RATE-LISTENER](#extensions-err-publish-rate-listener)
- `redis.ratelimit.proxy-collapse-risk` - [EXTENSIONS-ERR-RATELIMIT-PROXY-COLLAPSE](#extensions-err-ratelimit-proxy-collapse)
- `redis.registry.subscriber.failed` - [EXTENSIONS-ERR-REGISTRY-SUBSCRIBER](#extensions-err-registry-subscriber)
- `redis.sharded-pubsub.subscriber.failed` - [EXTENSIONS-ERR-SHARDED-SUBSCRIBER](#extensions-err-sharded-subscriber)
- `redis.sharded-pubsub.channel-reconcile.failed` - [EXTENSIONS-ERR-SHARDED-CHANNEL-RECONCILE](#extensions-err-sharded-channel-reconcile)
- `redis.sharded-pubsub.reconcile.failed` - [EXTENSIONS-ERR-SHARDED-RECONCILE](#extensions-err-sharded-reconcile)
- `redis.pubsub.envelope.oversized` - [EXTENSIONS-ERR-PUBSUB-ENVELOPE-OVERSIZED](#extensions-err-pubsub-envelope-oversized)
- `redis.topic-broadcast.envelope.oversized` - [EXTENSIONS-ERR-TOPIC-BROADCAST-ENVELOPE-OVERSIZED](#extensions-err-topic-broadcast-envelope-oversized)
- `redis.pubsub.trace-context.omitted` - [EXTENSIONS-ERR-PUBSUB-TRACE-OMITTED](#extensions-err-pubsub-trace-omitted)
- `redis.sharded-pubsub.trace-context.omitted` - [EXTENSIONS-ERR-SHARDED-TRACE-OMITTED](#extensions-err-sharded-trace-omitted)
- `redis.replay.storage-fallback` - [EXTENSIONS-ERR-REDIS-REPLAY-STORAGE-FALLBACK](#extensions-err-redis-replay-storage-fallback)
- `redis.replay-stream.storage-fallback` - [EXTENSIONS-ERR-REDIS-REPLAY-STREAM-STORAGE-FALLBACK](#extensions-err-redis-replay-stream-storage-fallback)
- `postgres.replay.storage-fallback` - [EXTENSIONS-ERR-POSTGRES-REPLAY-STORAGE-FALLBACK](#extensions-err-postgres-replay-storage-fallback)
- `redis.ratelimit.store-unreachable` - [EXTENSIONS-ERR-RATELIMIT-STORE-FLOOR](#extensions-err-ratelimit-store-floor)
- `redis.composite-ratelimit.store-unreachable` - [EXTENSIONS-ERR-COMPOSITE-RATELIMIT-STORE-FLOOR](#extensions-err-composite-ratelimit-store-floor)
- `redis.presence.envelope-oversized` - [EXTENSIONS-ERR-PRESENCE-ENVELOPE-OVERSIZED](#extensions-err-presence-envelope-oversized)
- `redis.publish-rate.remote-instances-capped` - [EXTENSIONS-ERR-PUBLISH-RATE-REMOTE-CAP](#extensions-err-publish-rate-remote-cap)
- `redis.registry.index-bucket.large` - [EXTENSIONS-ERR-REGISTRY-INDEX-BUCKET](#extensions-err-registry-index-bucket)
- `redis.registry.user-index.large` - [EXTENSIONS-ERR-REGISTRY-USER-INDEX](#extensions-err-registry-user-index)
- `redis.pubsub.relay-batch.burst` - [EXTENSIONS-ERR-PUBSUB-RELAY-BURST](#extensions-err-pubsub-relay-burst)
- `redis.pubsub.system-channel.refused` - [EXTENSIONS-ERR-PUBSUB-SYSTEM-CHANNEL-REFUSED](#extensions-err-pubsub-system-channel-refused)
- `redis.sharded-pubsub.relay-batch.channel-burst` - [EXTENSIONS-ERR-SHARDED-RELAY-BURST](#extensions-err-sharded-relay-burst)
- `redis.client.duplicates.excessive` - [EXTENSIONS-ERR-REDIS-DUPLICATES](#extensions-err-redis-duplicates)
- `redis.smooth.relay-owner.unresolved` - [EXTENSIONS-ERR-SMOOTH-OWNER-FALLBACK](#extensions-err-smooth-owner-fallback)
- `replay.resume.topics-truncated` - [EXTENSIONS-ERR-RESUME-TOPICS-TRUNCATED](#extensions-err-resume-topics-truncated)
- `prometheus.series.cap-reached` - [EXTENSIONS-ERR-PROMETHEUS-SERIES-CAP](#extensions-err-prometheus-series-cap)
- `postgres.tasks.sweep.failed` - [EXTENSIONS-ERR-POSTGRES-TASKS-SWEEP](#extensions-err-postgres-tasks-sweep)
- `redis.client.failed` - [EXTENSIONS-ERR-REDIS-CLIENT](#extensions-err-redis-client)
- `redis.subscriber.resubscribe.failed` - [EXTENSIONS-ERR-REDIS-RESUBSCRIBE](#extensions-err-redis-resubscribe)
- `redis.subscriber.silent` - [EXTENSIONS-ERR-REDIS-SUBSCRIBER-SILENT](#extensions-err-redis-subscriber-silent)
- `redis.subscriber.unreachable` - [EXTENSIONS-ERR-REDIS-SUBSCRIBER-UNREACHABLE](#extensions-err-redis-subscriber-unreachable)
- `redis.pubsub.subscriber.failed` - [EXTENSIONS-ERR-PUBSUB-SUBSCRIBER](#extensions-err-pubsub-subscriber)
- `redis.crdt.subscriber.failed` - [EXTENSIONS-ERR-CRDT-SUBSCRIBER](#extensions-err-crdt-subscriber)
- `redis.smooth.subscriber.failed` - [EXTENSIONS-ERR-SMOOTH-SUBSCRIBER](#extensions-err-smooth-subscriber)
- `redis.topic-broadcast.subscriber.failed` - [EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBER](#extensions-err-topic-broadcast-subscriber)
- `redis.crdt.subscribe.failed` - [EXTENSIONS-ERR-CRDT-SUBSCRIBE](#extensions-err-crdt-subscribe)
- `redis.smooth.subscribe.failed` - [EXTENSIONS-ERR-SMOOTH-SUBSCRIBE](#extensions-err-smooth-subscribe)
- `redis.topic-broadcast.subscribe.failed` - [EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBE](#extensions-err-topic-broadcast-subscribe)
- `redis.cursor.subscribe.failed` - [EXTENSIONS-ERR-CURSOR-SUBSCRIBE](#extensions-err-cursor-subscribe)
- `postgres.notify.connection.failed` - [EXTENSIONS-ERR-POSTGRES-NOTIFY-CONNECTION](#extensions-err-postgres-notify-connection)
- `operational.sink.failed` - [EXTENSIONS-ERR-SINK-FAILED](#extensions-err-sink-failed)
- `invariant.violated` - [EXTENSIONS-ERR-INVARIANT](#extensions-err-invariant)

Indexed console lines (no diagnostic event; the searchable key is the printed prefix and
the stable ID tag on the line):

- `[extensions] operational event dropped, invalid record shape` - [EXTENSIONS-ERR-DIAGNOSTIC-RECORD-SHAPE](#extensions-err-diagnostic-record-shape)
- `[extensions] diagnostic render failed: ` - [EXTENSIONS-ERR-DIAGNOSTIC-RENDER-COLLAPSE](#extensions-err-diagnostic-render-collapse)
- `[extensions] diagnostic rendered but the console refused it: ` - [EXTENSIONS-ERR-DIAGNOSTIC-CONSOLE-WRITE](#extensions-err-diagnostic-console-write)
- `[extensions] operational sink failed and its failure notice could not be built: ` - [EXTENSIONS-ERR-DIAGNOSTIC-SINK-NOTICE](#extensions-err-diagnostic-sink-notice)

<a id="extensions-err-registry-request-target"></a>
## `EXTENSIONS-ERR-REGISTRY-REQUEST-TARGET`

- **Code/event:** `registry.request.invalid-target`
- **Message prefix:** `registry.request: target must be a non-empty userId string`
- **Cause:** request() received an empty or non-string user identifier.
- **Consequence:** The request is rejected before registry or network work begins.
- **Automatic recovery:** None; invalid input is not retried.
- **Next action:** Pass the canonical non-empty user ID recorded by the registry identify callback.
- **Runtime help:** `docs/errors.md#extensions-err-registry-request-target`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-request-event"></a>
## `EXTENSIONS-ERR-REGISTRY-REQUEST-EVENT`

- **Code/event:** `registry.request.invalid-event`
- **Message prefix:** `registry.request: event must be a non-empty string`
- **Cause:** request() received an empty or non-string event name.
- **Consequence:** The request is rejected before registry or network work begins.
- **Automatic recovery:** None; invalid input is not retried.
- **Next action:** Pass the non-empty client onRequest event name.
- **Runtime help:** `docs/errors.md#extensions-err-registry-request-event`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-request-offline"></a>
## `EXTENSIONS-ERR-REGISTRY-REQUEST-OFFLINE`

- **Code/event:** `registry.request.offline`
- **Message prefix:** `registry.request: target user "`
- **Cause:** No live registry owner exists for the requested user, or its local socket has closed.
- **Consequence:** The request is not delivered and the caller promise rejects.
- **Automatic recovery:** None; the registry waits for a future connection open event.
- **Next action:** Treat the user as offline and retry only after a new registry open event.
- **Runtime help:** `docs/errors.md#extensions-err-registry-request-offline`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-request-pending"></a>
## `EXTENSIONS-ERR-REGISTRY-REQUEST-PENDING`

- **Code/event:** `registry.request.pending-cap`
- **Message prefix:** `registry.request: pending requests exceeded `
- **Cause:** This instance reached its bounded number of cluster requests awaiting replies.
- **Consequence:** The new request is rejected so pending memory remains bounded.
- **Automatic recovery:** Capacity returns as existing pending requests settle.
- **Next action:** Apply caller backpressure and investigate slow or missing recipients before retrying.
- **Runtime help:** `docs/errors.md#extensions-err-registry-request-pending`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-request-timeout"></a>
## `EXTENSIONS-ERR-REGISTRY-REQUEST-TIMEOUT`

- **Code/event:** `registry.request.timeout`
- **Message prefix:** `registry.request: timed out after `
- **Cause:** The remote owner did not return a reply inside the configured timeout.
- **Consequence:** The caller promise rejects while the remote operation outcome remains unknown.
- **Automatic recovery:** None; automatic replay could duplicate the operation.
- **Next action:** Reconcile state or retry only through an idempotent operation.
- **Runtime help:** `docs/errors.md#extensions-err-registry-request-timeout`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-session-target"></a>
## `EXTENSIONS-ERR-REGISTRY-SESSION-TARGET`

- **Code/event:** `registry.request-session.invalid-target`
- **Message prefix:** `registry.requestSession: target must be a non-empty sessionId string`
- **Cause:** requestSession() received an empty or non-string application session identifier.
- **Consequence:** The request is rejected before registry or network work begins.
- **Automatic recovery:** None; invalid input is not retried.
- **Next action:** Pass the canonical non-empty session ID recorded by sessionIdentify.
- **Runtime help:** `docs/errors.md#extensions-err-registry-session-target`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-session-event"></a>
## `EXTENSIONS-ERR-REGISTRY-SESSION-EVENT`

- **Code/event:** `registry.request-session.invalid-event`
- **Message prefix:** `registry.requestSession: event must be a non-empty string`
- **Cause:** requestSession() received an empty or non-string event name.
- **Consequence:** The request is rejected before registry or network work begins.
- **Automatic recovery:** None; invalid input is not retried.
- **Next action:** Pass the non-empty client onRequest event name.
- **Runtime help:** `docs/errors.md#extensions-err-registry-session-event`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-session-offline"></a>
## `EXTENSIONS-ERR-REGISTRY-SESSION-OFFLINE`

- **Code/event:** `registry.request-session.offline`
- **Message prefix:** `registry.requestSession: target session "`
- **Cause:** No live registry owner exists for the requested application session, or its socket has closed.
- **Consequence:** The request is not delivered and the caller promise rejects.
- **Automatic recovery:** None; the registry waits for a future connection open event.
- **Next action:** Treat the session as offline and retry only after a new registry open event.
- **Runtime help:** `docs/errors.md#extensions-err-registry-session-offline`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-session-pending"></a>
## `EXTENSIONS-ERR-REGISTRY-SESSION-PENDING`

- **Code/event:** `registry.request-session.pending-cap`
- **Message prefix:** `registry.requestSession: pending requests exceeded `
- **Cause:** This instance reached its bounded number of session-routed requests awaiting replies.
- **Consequence:** The new request is rejected so pending memory remains bounded.
- **Automatic recovery:** Capacity returns as existing pending requests settle.
- **Next action:** Apply caller backpressure and investigate slow or missing recipients before retrying.
- **Runtime help:** `docs/errors.md#extensions-err-registry-session-pending`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-session-timeout"></a>
## `EXTENSIONS-ERR-REGISTRY-SESSION-TIMEOUT`

- **Code/event:** `registry.request-session.timeout`
- **Message prefix:** `registry.requestSession: timed out after `
- **Cause:** The remote session owner did not return a reply inside the configured timeout.
- **Consequence:** The caller promise rejects while the remote operation outcome remains unknown.
- **Automatic recovery:** None; automatic replay could duplicate the operation.
- **Next action:** Reconcile state or retry only through an idempotent operation.
- **Runtime help:** `docs/errors.md#extensions-err-registry-session-timeout`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-send-target"></a>
## `EXTENSIONS-ERR-REGISTRY-SEND-TARGET`

- **Code/event:** `registry.send.invalid-target`
- **Message prefix:** `registry.send: target must be a non-empty userId string`
- **Cause:** send() received an empty or non-string user identifier.
- **Consequence:** Nothing is delivered locally or across the cluster; the call throws synchronously.
- **Automatic recovery:** None; invalid input is not retried.
- **Next action:** Pass the canonical non-empty user ID recorded by the registry identify callback.
- **Runtime help:** `docs/errors.md#extensions-err-registry-send-target`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-send-coalesced-target"></a>
## `EXTENSIONS-ERR-REGISTRY-SEND-COALESCED-TARGET`

- **Code/event:** `registry.send-coalesced.invalid-target`
- **Message prefix:** `registry.sendCoalesced: target must be a non-empty userId string`
- **Cause:** sendCoalesced() received an empty or non-string user identifier.
- **Consequence:** Nothing is coalesced locally or across the cluster; the call throws synchronously.
- **Automatic recovery:** None; invalid input is not retried.
- **Next action:** Pass the canonical non-empty user ID recorded by the registry identify callback.
- **Runtime help:** `docs/errors.md#extensions-err-registry-send-coalesced-target`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-circuit-open"></a>
## `EXTENSIONS-ERR-CIRCUIT-OPEN`

- **Code/event:** `breaker.circuit-open`
- **Message prefix:** `circuit breaker is open - backend unavailable`
- **Cause:** The optional breaker crossed its failure threshold and is protecting an unhealthy backend.
- **Consequence:** Awaited operations fail fast and fire-and-forget operations are skipped.
- **Automatic recovery:** After resetTimeout, bounded probes test the backend and the first success closes the circuit.
- **Next action:** Restore the backend and inspect breaker state transitions before increasing thresholds.
- **Runtime help:** `docs/errors.md#extensions-err-circuit-open`
- **Runtime sources:** [src/shared/breaker.js](../src/shared/breaker.js)

<a id="extensions-err-capability-cookie-metrics"></a>
## `EXTENSIONS-ERR-CAPABILITY-COOKIE-METRICS`

- **Code/event:** `capability-cookie.metrics-instrument.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.capability-cookie event=capability-cookie.metrics-instrument.failed severity=error] The capability-cookie metrics instrument threw; further instrument failures are not reported.`
- **Cause:** The application-supplied metrics counter for capability-cookie misses threw when incremented.
- **Consequence:** Cookie validation continues unchanged. Every miss still calls the instrument, so the counter resumes if the fault clears, but later failures are silent on this instance.
- **Automatic recovery:** None needed for validation; the instrument is called again on every miss.
- **Next action:** Fix the capability_cookie_misses_total counter of the registry passed as the metrics option; the error attribute carries what its inc() threw.
- **Runtime help:** `docs/errors.md#extensions-err-capability-cookie-metrics`
- **Runtime sources:** [src/capability-cookie.js](../src/capability-cookie.js)

<a id="extensions-err-postgres-pool-idle-client"></a>
## `EXTENSIONS-ERR-POSTGRES-POOL-IDLE-CLIENT`

- **Code/event:** `postgres.pool.idle-client.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres event=postgres.pool.idle-client.failed severity=error] An idle Postgres pool client reported an error.`
- **Cause:** A pooled connection failed while idle: the server closed it, the network dropped, authentication expired, or TLS renegotiation failed.
- **Consequence:** That connection is discarded by the pool. In-flight queries on other connections are unaffected; the next checkout opens a fresh connection.
- **Automatic recovery:** pg.Pool reconnects on demand; nothing is retried by this package.
- **Next action:** Read the error attribute (connection strings are redacted) and check Postgres logs, network stability, and idle timeouts between the instance and the database.
- **Runtime help:** `docs/errors.md#extensions-err-postgres-pool-idle-client`
- **Runtime sources:** [src/postgres/index.js](../src/postgres/index.js)

<a id="extensions-err-postgres-transaction-client"></a>
## `EXTENSIONS-ERR-POSTGRES-TRANSACTION-CLIENT`

- **Code/event:** `postgres.transaction.client.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres event=postgres.transaction.client.failed severity=error] A Postgres connection checked out for a transaction reported an error.`
- **Cause:** The server ended the connection, or the network dropped it, while a transaction held it: a failover or restart, idle_in_transaction_session_timeout, or pg_terminate_backend.
- **Consequence:** The statement in flight, or the next one, fails and the operation that ran the transaction rejects. The server rolls the transaction back unless the drop hit COMMIT, whose outcome is then unknown. The connection is discarded rather than returned to the pool. Reported once per connection.
- **Automatic recovery:** pg.Pool opens a fresh connection on the next checkout. The failed operation is not retried by this package.
- **Next action:** The error attribute often says only that the connection terminated; the reason is in the Postgres log. Retry the failed operation if it is safe to, and after a drop during COMMIT check first whether it took effect.
- **Runtime help:** `docs/errors.md#extensions-err-postgres-transaction-client`
- **Runtime sources:** [src/shared/pg-tx.js](../src/shared/pg-tx.js)

<a id="extensions-err-postgres-notify-payload-near-limit"></a>
## `EXTENSIONS-ERR-POSTGRES-NOTIFY-PAYLOAD-NEAR-LIMIT`

- **Code/event:** `postgres.notify.payload-near-limit`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.notify event=postgres.notify.payload-near-limit severity=warn] A Postgres NOTIFY payload is approaching the 8000-byte transport limit.`
- **Cause:** A notification longer than 7500 bytes arrived on the bridged channel; Postgres rejects payloads above roughly 8000 bytes at pg_notify time.
- **Consequence:** This notification is still processed. A slightly larger one makes pg_notify raise "payload string too long", which aborts the transaction that sent it: with a notify trigger, the application's own INSERT or UPDATE fails, and nothing reaches the bridge.
- **Automatic recovery:** None; the limit is a server constant.
- **Next action:** Shrink the notified row (send an id and let clients fetch) or split the event; see the shortlink.
- **Operator shortlink:** `https://svti.me/pg-notify-limit`
- **Runtime help:** `docs/errors.md#extensions-err-postgres-notify-payload-near-limit`
- **Runtime sources:** [src/postgres/notify.js](../src/postgres/notify.js)

<a id="extensions-err-postgres-notify-parse"></a>
## `EXTENSIONS-ERR-POSTGRES-NOTIFY-PARSE`

- **Code/event:** `postgres.notify.parse.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.notify event=postgres.notify.parse.failed severity=warn] The application-owned Postgres NOTIFY parser, or the publish of its result, threw.`
- **Cause:** The custom parse() passed to createNotifyBridge threw for this payload, or publishing the result it returned threw. For a parse() failure the error attribute carries only the name and code, since the message can quote the payload; a publish failure carries its message.
- **Consequence:** The notification is dropped and counted in notify_parse_errors_total; delivery of later notifications continues.
- **Automatic recovery:** None for the dropped notification.
- **Next action:** Make parse() return null for payloads it cannot handle instead of throwing, and fix the producer that emits the unexpected shape.
- **Runtime help:** `docs/errors.md#extensions-err-postgres-notify-parse`
- **Runtime sources:** [src/postgres/notify.js](../src/postgres/notify.js)

<a id="extensions-err-postgres-tasks-state-listener"></a>
## `EXTENSIONS-ERR-POSTGRES-TASKS-STATE-LISTENER`

- **Code/event:** `postgres.tasks.state-listener.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.tasks event=postgres.tasks.state-listener.failed severity=warn] The task state-change listener failed; task execution continues.`
- **Cause:** The onStateChange callback threw synchronously or returned a promise that rejected.
- **Consequence:** The state transition itself was already committed; only the observer missed it. Later transitions still invoke the listener.
- **Automatic recovery:** None; the listener is invoked again on the next transition.
- **Next action:** Fix the listener; it must not throw, and it should catch its own asynchronous failures.
- **Runtime help:** `docs/errors.md#extensions-err-postgres-tasks-state-listener`
- **Runtime sources:** [src/postgres/tasks.js](../src/postgres/tasks.js)

<a id="extensions-err-clock-skew-sample"></a>
## `EXTENSIONS-ERR-CLOCK-SKEW-SAMPLE`

- **Code/event:** `redis.clock-skew.sample.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock-skew event=redis.clock-skew.sample.failed severity=error] A Redis clock-skew sample failed; the last good gauge value remains active.`
- **Cause:** The TIME round trip to Redis threw, typically because the connection is down or the breaker is open.
- **Consequence:** The skew gauge keeps its previous value instead of reporting zero, and no trip callback fires from this round.
- **Automatic recovery:** The next scheduled sample retries; the Redis client owns reconnection.
- **Next action:** Check Redis connectivity; if samples keep failing, the skew alert is blind and the clock should not be trusted for fencing decisions.
- **Runtime help:** `docs/errors.md#extensions-err-clock-skew-sample`
- **Runtime sources:** [src/redis/clock-skew.js](../src/redis/clock-skew.js)

<a id="extensions-err-clock-sample"></a>
## `EXTENSIONS-ERR-CLOCK-SAMPLE`

- **Code/event:** `redis.clock.sample.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock event=redis.clock.sample.failed severity=error] A Redis cluster-clock sample failed; the last good state remains active.`
- **Cause:** The TIME round trip to Redis threw, typically because the connection is down or the breaker is open.
- **Consequence:** The clock keeps its previous offset and skew instead of fabricating zero; disagreement checks skip this round.
- **Automatic recovery:** The next scheduled sample retries; the Redis client owns reconnection.
- **Next action:** Check Redis connectivity; repeated failures mean the cluster clock is running on a stale offset.
- **Runtime help:** `docs/errors.md#extensions-err-clock-sample`
- **Runtime sources:** [src/redis/clock.js](../src/redis/clock.js)

<a id="extensions-err-clock-ntp-sample"></a>
## `EXTENSIONS-ERR-CLOCK-NTP-SAMPLE`

- **Code/event:** `redis.clock.ntp-sample.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock event=redis.clock.ntp-sample.failed severity=warn] The auxiliary NTP clock sample failed and was excluded from this round.`
- **Cause:** The configured ntp() source threw or rejected.
- **Consequence:** This round votes with the local and Redis sources only; a stale NTP offset never keeps voting.
- **Automatic recovery:** The next scheduled sample calls ntp() again.
- **Next action:** Check the NTP source the ntp option calls; the cluster clock stays healthy without it, but the third vote is missing.
- **Runtime help:** `docs/errors.md#extensions-err-clock-ntp-sample`
- **Runtime sources:** [src/redis/clock.js](../src/redis/clock.js)

<a id="extensions-err-clock-leader-offset"></a>
## `EXTENSIONS-ERR-CLOCK-LEADER-OFFSET`

- **Code/event:** `redis.clock.leader-offset-read.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.clock event=redis.clock.leader-offset-read.failed severity=warn] The cluster-clock leader offset could not be published or read; the prior state remains active.`
- **Cause:** On the leader, writing its offset key to Redis threw. On a follower, reading the key threw or its value did not parse.
- **Consequence:** A failing leader leaves followers on the last offset it published until the key expires. A failing follower keeps using the last offset it read for at most leaderTtlMs after that read, then stamps from its own consistent() clock until a read succeeds.
- **Automatic recovery:** The next scheduled sample publishes or reads the key again.
- **Next action:** Check Redis connectivity and confirm the leader instance is alive and publishing.
- **Runtime help:** `docs/errors.md#extensions-err-clock-leader-offset`
- **Runtime sources:** [src/redis/clock.js](../src/redis/clock.js)

<a id="extensions-err-cursor-subscriber"></a>
## `EXTENSIONS-ERR-CURSOR-SUBSCRIBER`

- **Code/event:** `redis.cursor.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.subscriber.failed severity=error] The Redis cursor subscriber connection reported an error.`
- **Cause:** The dedicated pub/sub connection that receives remote cursor updates emitted an error.
- **Consequence:** Remote cursor positions may be delayed or missed on this instance while the subscriber is unhealthy.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure.
- **Next action:** Inspect the Redis error and verify cursor propagation after reconnection.
- **Runtime help:** `docs/errors.md#extensions-err-cursor-subscriber`
- **Runtime sources:** [src/redis/cursor/redis-io.js](../src/redis/cursor/redis-io.js)

<a id="extensions-err-cursor-cleanup"></a>
## `EXTENSIONS-ERR-CURSOR-CLEANUP`

- **Code/event:** `redis.cursor.cleanup.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.cleanup.failed severity=warn] Cursor stale-entry cleanup failed; the next sweep retries.`
- **Cause:** The cleanup Lua script threw for at least one active topic in a sweep. The failed attribute counts those topics out of the topics attribute, and the error attribute is the first failure.
- **Consequence:** Stale cursor entries on the failed topics linger until a later sweep succeeds; live updates are unaffected. Reported once per failure streak, and once more if the streak narrows from every topic failing to only some; a sweep with no failure re-arms it.
- **Automatic recovery:** The periodic sweep runs the script again on every active topic.
- **Next action:** Check Redis health; failed equal to topics usually means Redis is unreachable. Topics are not logged, since they can carry user ids; if only some topics keep failing, scan the cursor hashes for one with a corrupt entry.
- **Runtime help:** `docs/errors.md#extensions-err-cursor-cleanup`
- **Runtime sources:** [src/redis/cursor/redis-io.js](../src/redis/cursor/redis-io.js)

<a id="extensions-err-cursor-flush"></a>
## `EXTENSIONS-ERR-CURSOR-FLUSH`

- **Code/event:** `redis.cursor.flush.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.flush.failed severity=warn] A cursor tick could not publish one or more topics; the other topics were flushed.`
- **Cause:** Publishing a topic's coalesced cursor frame threw during a tick. The failed attribute counts those topics out of the flushed attribute, and the error attribute is the first failure.
- **Consequence:** Viewers on the failed topics miss those positions; the next move on such a topic is tried again on a later tick. Reported once per failure streak; a streak ends after ten seconds with no failed flush.
- **Automatic recovery:** Every later tick publishes again.
- **Next action:** Read the error. A platform that refuses the publish itself fails every topic; a failure on only some topics points at their cursor data. Topics are not logged, since they can carry user ids.
- **Runtime help:** `docs/errors.md#extensions-err-cursor-flush`
- **Runtime sources:** [src/redis/cursor/scheduler.js](../src/redis/cursor/scheduler.js)

<a id="extensions-err-groups-meta-initialize"></a>
## `EXTENSIONS-ERR-GROUPS-META-INITIALIZE`

- **Code/event:** `redis.groups.meta-initialize.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.groups event=redis.groups.meta-initialize.failed severity=warn] The initial Redis group metadata write failed.`
- **Cause:** Writing the group meta hash at construction threw.
- **Consequence:** getMeta() on this instance returns the initial in-memory metadata until a later write succeeds; other instances do not see it yet.
- **Automatic recovery:** The next getMeta() or setMeta() call retries the write.
- **Next action:** Check Redis connectivity; metadata converges once a write succeeds.
- **Runtime help:** `docs/errors.md#extensions-err-groups-meta-initialize`
- **Runtime sources:** [src/redis/groups.js](../src/redis/groups.js)

<a id="extensions-err-groups-heartbeat"></a>
## `EXTENSIONS-ERR-GROUPS-HEARTBEAT`

- **Code/event:** `redis.groups.heartbeat.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.groups event=redis.groups.heartbeat.failed severity=warn] A Redis group heartbeat pipeline failed.`
- **Cause:** The periodic pipeline that refreshes member timestamps and key expiry was rejected, or Redis refused one of its commands (READONLY after a failover, OOM, NOPERM).
- **Consequence:** If heartbeats keep failing past the member TTL, other instances treat this instance's members as gone and the group keys may expire. Reported once per failure streak per group; a successful heartbeat re-arms it.
- **Automatic recovery:** The next heartbeat tick retries; a single failure inside the TTL window has no visible effect.
- **Next action:** Check Redis connectivity; sustained failures on one instance mean its members are being evicted cluster-wide.
- **Runtime help:** `docs/errors.md#extensions-err-groups-heartbeat`
- **Runtime sources:** [src/redis/groups.js](../src/redis/groups.js)

<a id="extensions-err-groups-subscriber"></a>
## `EXTENSIONS-ERR-GROUPS-SUBSCRIBER`

- **Code/event:** `redis.groups.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.groups event=redis.groups.subscriber.failed severity=error] The Redis group subscriber connection reported an error.`
- **Cause:** The dedicated pub/sub connection that receives cross-instance group events emitted an error.
- **Consequence:** Cross-instance group events may be delayed or missed on this instance while the subscriber is unhealthy.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure.
- **Next action:** Inspect the Redis error and connection health, then verify group event delivery.
- **Runtime help:** `docs/errors.md#extensions-err-groups-subscriber`
- **Runtime sources:** [src/redis/groups.js](../src/redis/groups.js)

<a id="extensions-err-leader-tick"></a>
## `EXTENSIONS-ERR-LEADER-TICK`

- **Code/event:** `redis.leader.tick.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.leader event=redis.leader.tick.failed severity=error] A Redis leader election tick failed; leadership is fail-closed.`
- **Cause:** Acquiring or renewing the leader lease threw, typically because Redis is unreachable or the breaker is open.
- **Consequence:** A current leader steps down on the failed renewal being reported rather than assuming it still holds the lease; a candidate does not acquire. If the failure is local to this instance, another instance acquires the lease once it expires; if every instance fails, leader-gated work pauses until a tick succeeds.
- **Automatic recovery:** The next tick retries; the Redis client owns reconnection.
- **Next action:** Check Redis connectivity. The operation attribute says whether the failing step was acquire or renew.
- **Runtime help:** `docs/errors.md#extensions-err-leader-tick`
- **Runtime sources:** [src/redis/leader.js](../src/redis/leader.js)

<a id="extensions-err-leader-read-current"></a>
## `EXTENSIONS-ERR-LEADER-READ-CURRENT`

- **Code/event:** `redis.leader.read-current.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.leader event=redis.leader.read-current.failed severity=warn] The current Redis leader could not be read.`
- **Cause:** currentLeader() could not read the lease key.
- **Consequence:** The caller receives null for this call; election itself is unaffected.
- **Automatic recovery:** None; the caller decides whether to retry.
- **Next action:** Check Redis connectivity if this repeats.
- **Runtime help:** `docs/errors.md#extensions-err-leader-read-current`
- **Runtime sources:** [src/redis/leader.js](../src/redis/leader.js)

<a id="extensions-err-leader-release"></a>
## `EXTENSIONS-ERR-LEADER-RELEASE`

- **Code/event:** `redis.leader.release.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.leader event=redis.leader.release.failed severity=warn] The Redis leader lease could not be released; it expires by TTL.`
- **Cause:** The compare-and-delete release on stop() threw.
- **Consequence:** No other instance can become leader until the lease TTL elapses; the stopping instance already treats itself as a non-leader.
- **Automatic recovery:** The lease expires on its own; no other instance can accidentally release someone else's lease.
- **Next action:** Nothing unless the failover delay is unacceptable; then check Redis connectivity during shutdown.
- **Runtime help:** `docs/errors.md#extensions-err-leader-release`
- **Runtime sources:** [src/redis/leader.js](../src/redis/leader.js)

<a id="extensions-err-presence-heartbeat-publish"></a>
## `EXTENSIONS-ERR-PRESENCE-HEARTBEAT-PUBLISH`

- **Code/event:** `redis.presence.heartbeat.publish-failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.heartbeat.publish-failed severity=warn] A presence heartbeat could not be published for one or more topics; the TTL refresh and the other topics went ahead.`
- **Cause:** Publishing a topic's heartbeat roster threw. The failed attribute counts those topics out of the topics attribute, and the error attribute is the first failure.
- **Consequence:** Clients on the failed topics get no heartbeat, so a client maxAge sweep can drop users who are still present; the Redis TTL refresh is unaffected. Reported once per failure streak; a heartbeat with no failure re-arms it.
- **Automatic recovery:** Every heartbeat publishes again.
- **Next action:** Read the error. A platform that refuses the publish itself fails every topic; a failure on only some topics points at their roster data. Topics are not logged, since they can carry user ids.
- **Runtime help:** `docs/errors.md#extensions-err-presence-heartbeat-publish`
- **Runtime sources:** [src/redis/presence.js](../src/redis/presence.js)

<a id="extensions-err-presence-keyspace-subscriber"></a>
## `EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBER`

- **Code/event:** `redis.presence.keyspace-subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-subscriber.failed severity=error] A presence keyspace-notification subscriber connection reported an error.`
- **Cause:** A per-master connection subscribed to key-deletion events, which the presence cleanup of expired topics listens on, emitted an error after its subscription stood. An error before that is reported as EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBE instead.
- **Consequence:** Presence expiry deletions from that master may be missed until reconciliation repairs the subscriber set.
- **Automatic recovery:** A sweep of the masters runs at once. These connections do not reconnect: one that ended is replaced by a new connection to its master. One that stops answering is closed and reported as EXTENSIONS-ERR-REDIS-SUBSCRIBER-SILENT, then replaced the same way.
- **Next action:** Inspect the Redis error and confirm reconciliation restores expired presence state.
- **Runtime help:** `docs/errors.md#extensions-err-presence-keyspace-subscriber`
- **Runtime sources:** [src/redis/presence/subscriber.js](../src/redis/presence/subscriber.js)

<a id="extensions-err-presence-keyspace-subscribe"></a>
## `EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBE`

- **Code/event:** `redis.presence.keyspace-subscribe.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-subscribe.failed severity=warn] The presence keyspace cleanup could not subscribe to key-deletion events.`
- **Cause:** psubscribe to __keyevent@*__:del failed: an ACL denied the user PSUBSCRIBE or that channel pattern (NOPERM), the connection failed or dropped during the call, in which case the error is the one the connection raised (a refused login, for example), or on a cluster the master did not answer within 10 seconds and the connection was closed. Redis accepts the subscription even when keyspace notifications are disabled, so a disabled notify-keyspace-events setting never produces this event; the subscription then succeeds and receives nothing, and EXTENSIONS-ERR-PRESENCE-KEYSPACE-EVENTS-OFF reports the setting where the server lets it be read.
- **Consequence:** This instance is not told when a topic's presence hash is deleted because the last instance presenting it stopped heartbeating, so its sync-only observers keep showing the last roster until new presence traffic or a reload.
- **Automatic recovery:** On a cluster the master is subscribed again with a backoff from 1 to 30 seconds, sooner when the topology changes, and each kind of failure is reported once per streak per master. On standalone Redis the subscription is attempted again only when the presence subscriber connection is set up again.
- **Next action:** For NOPERM, grant the Redis user +psubscribe and the pattern &__keyevent@*__:del; otherwise check connectivity to the server (on a cluster, the master attribute names it). On a cluster the next retry subscribes again; on standalone Redis restart the instance. For the cleanup to receive anything once subscribed, notify-keyspace-events must include E and g (for example Eg) on every master; see the shortlink.
- **Operator shortlink:** `https://svti.me/redis-keyspace`
- **Runtime help:** `docs/errors.md#extensions-err-presence-keyspace-subscribe`
- **Runtime sources:** [src/redis/presence/subscriber.js](../src/redis/presence/subscriber.js)

<a id="extensions-err-presence-keyspace-events-off"></a>
## `EXTENSIONS-ERR-PRESENCE-KEYSPACE-EVENTS-OFF`

- **Code/event:** `redis.presence.keyspace-events.off`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-events.off severity=warn] A Redis server's keyspace notification setting cannot deliver the key deletions the presence cleanup listens for.`
- **Cause:** notify-keyspace-events on that server lacks E (keyevent notifications) or g (generic commands, also included in A); the flags attribute carries its value. Redis accepts the subscription regardless, so nothing else reports this.
- **Consequence:** The keyspace cleanup never fires for topics whose presence hash lives on that server: when the last instance presenting a topic dies, its sync-only observers keep showing the last roster until new presence traffic or a reload.
- **Automatic recovery:** None; this is server configuration. The setting is read when the subscription is set up, so a corrected server is checked again the next time it is.
- **Next action:** Run CONFIG SET notify-keyspace-events Eg on every master (or add E and g to the flags it already has), and set it in redis.conf so a restart keeps it; see the shortlink.
- **Operator shortlink:** `https://svti.me/redis-keyspace`
- **Runtime help:** `docs/errors.md#extensions-err-presence-keyspace-events-off`
- **Runtime sources:** [src/redis/presence/subscriber.js](../src/redis/presence/subscriber.js)

<a id="extensions-err-presence-keyspace-reconcile"></a>
## `EXTENSIONS-ERR-PRESENCE-KEYSPACE-RECONCILE`

- **Code/event:** `redis.presence.keyspace-reconcile.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.keyspace-reconcile.failed severity=error] Presence keyspace subscriber reconciliation failed; it is retried with a backoff.`
- **Cause:** Re-reading the cluster master set or re-subscribing a master threw, or a master's CLUSTER MYID failed or was not answered within 10 seconds.
- **Consequence:** Masters added by a failover may not have a keyspace subscriber yet, so expiry cleanup from them lags.
- **Automatic recovery:** Reconciliation reruns after a backoff from 1 to 30 seconds, and at once on the next topology event or subscriber error; while sweeps keep failing, a node joining (+node) starts at most one sweep between two retries. A master ioredis dropped from its node list is swept again once a refreshed slot map lists it: when a sweep missed a master that did not answer, or one the last slot map listed that ioredis no longer does, its retry reads the slot map first. A failed sweep is reported once per failure streak, a master that does not answer once until it answers or the slot map drops it.
- **Next action:** Check cluster health; if it persists, inspect connectivity to individual masters, and that the Redis user may run CLUSTER MYID (+cluster|myid).
- **Runtime help:** `docs/errors.md#extensions-err-presence-keyspace-reconcile`
- **Runtime sources:** [src/redis/presence/subscriber.js](../src/redis/presence/subscriber.js)

<a id="extensions-err-presence-subscriber"></a>
## `EXTENSIONS-ERR-PRESENCE-SUBSCRIBER`

- **Code/event:** `redis.presence.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.subscriber.failed severity=error] The Redis presence subscriber connection reported an error.`
- **Cause:** The dedicated pub/sub connection that receives cross-instance presence events emitted an error.
- **Consequence:** Cross-instance presence diffs may be delayed or missed on this instance while the subscriber is unhealthy.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure.
- **Next action:** Inspect Redis connectivity and verify presence diffs resume.
- **Runtime help:** `docs/errors.md#extensions-err-presence-subscriber`
- **Runtime sources:** [src/redis/presence/subscriber.js](../src/redis/presence/subscriber.js)

<a id="extensions-err-publish-rate-subscriber"></a>
## `EXTENSIONS-ERR-PUBLISH-RATE-SUBSCRIBER`

- **Code/event:** `redis.publish-rate.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.publish-rate event=redis.publish-rate.subscriber.failed severity=error] The cluster publish-rate subscriber connection reported an error.`
- **Cause:** The dedicated pub/sub connection that receives remote rate samples emitted an error.
- **Consequence:** Cluster-wide rate views go stale and admission decisions lose remote samples until the subscriber recovers.
- **Automatic recovery:** The Redis client owns reconnection; stale remote samples age out.
- **Next action:** Inspect Redis connectivity and verify remote rate samples resume.
- **Runtime help:** `docs/errors.md#extensions-err-publish-rate-subscriber`
- **Runtime sources:** [src/redis/publish-rate.js](../src/redis/publish-rate.js)

<a id="extensions-err-publish-rate-listener"></a>
## `EXTENSIONS-ERR-PUBLISH-RATE-LISTENER`

- **Code/event:** `redis.publish-rate.listener.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.publish-rate event=redis.publish-rate.listener.failed severity=error] A publish-rate threshold listener threw; aggregation continues.`
- **Cause:** An onPublishRate callback threw while being told about crossings.
- **Consequence:** That listener missed this set of crossings; other listeners and the aggregation loop are unaffected.
- **Automatic recovery:** None; the listener is invoked again on the next crossing.
- **Next action:** Fix the listener so it does not throw.
- **Runtime help:** `docs/errors.md#extensions-err-publish-rate-listener`
- **Runtime sources:** [src/redis/publish-rate.js](../src/redis/publish-rate.js)

<a id="extensions-err-ratelimit-proxy-collapse"></a>
## `EXTENSIONS-ERR-RATELIMIT-PROXY-COLLAPSE`

- **Code/event:** `redis.ratelimit.proxy-collapse-risk`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.ratelimit event=redis.ratelimit.proxy-collapse-risk severity=warn] A private or loopback rate-limit key was denied while no trusted address header is configured.`
- **Cause:** keyBy is ip (the default), no ADDRESS_HEADER is set, and a denied bucket key was a private or loopback address, which is what every client looks like behind an unconfigured reverse proxy.
- **Consequence:** All clients share one bucket: one busy client rate-limits everyone. Reported once per limiter; the address itself is never printed.
- **Automatic recovery:** None; this is configuration.
- **Next action:** Configure ADDRESS_HEADER and XFF_DEPTH on the adapter so the real client address is resolved, or pass an explicit keyBy resolver.
- **Runtime help:** `docs/errors.md#extensions-err-ratelimit-proxy-collapse`
- **Runtime sources:** [src/redis/ratelimit.js](../src/redis/ratelimit.js)

<a id="extensions-err-registry-subscriber"></a>
## `EXTENSIONS-ERR-REGISTRY-SUBSCRIBER`

- **Code/event:** `redis.registry.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.registry event=redis.registry.subscriber.failed severity=error] The connection-registry subscriber connection reported an error.`
- **Cause:** The dedicated pub/sub connection that receives cross-instance requests and replies emitted an error.
- **Consequence:** Cross-instance requests and replies may be delayed or lost while the subscriber is unhealthy; pending requests still time out normally.
- **Automatic recovery:** The Redis client owns reconnection; pending requests remain bounded by timeout.
- **Next action:** Inspect Redis connectivity and verify cross-instance request/reply delivery.
- **Runtime help:** `docs/errors.md#extensions-err-registry-subscriber`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-sharded-subscriber"></a>
## `EXTENSIONS-ERR-SHARDED-SUBSCRIBER`

- **Code/event:** `redis.sharded-pubsub.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.subscriber.failed severity=error] A sharded pub/sub subscriber connection reported an error.`
- **Cause:** A shard-channel subscriber emitted an error, commonly the server-initiated unsubscribe during a slot migration.
- **Consequence:** Delivery on the affected shard channels is interrupted until ownership is refreshed and the subscriptions are restored.
- **Automatic recovery:** The bus marks the slot map stale, refreshes ownership, and re-subscribes wanted channels.
- **Next action:** Inspect the Redis error and verify the topology refresh and shard delivery complete.
- **Runtime help:** `docs/errors.md#extensions-err-sharded-subscriber`
- **Runtime sources:** [src/redis/sharded-pubsub.js](../src/redis/sharded-pubsub.js)

<a id="extensions-err-sharded-channel-reconcile"></a>
## `EXTENSIONS-ERR-SHARDED-CHANNEL-RECONCILE`

- **Code/event:** `redis.sharded-pubsub.channel-reconcile.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.channel-reconcile.failed severity=error] A sharded pub/sub channel could not be re-subscribed on its new owner.`
- **Cause:** SSUBSCRIBE on the channel's new slot owner threw or timed out after a reshard, the slot had no owner in the cluster's slot map, or the bus has no connection to its owner: ioredis does not list it as a master yet, which the bus has the slot map read again for, or a master did not give its id (CLUSTER MYID failed or took longer than 5 seconds), which the error then mentions. The channel is not logged, since it is built from a topic; the slot attribute identifies the first failed one and the failed attribute counts them.
- **Consequence:** Messages on those channels are missed on this instance until a retry re-subscribes them; the other moved channels were handled. Reported once per failure streak; a reconciliation that completes re-arms it.
- **Automatic recovery:** Yes: the reconciliation is retried after 1 s, then with the delay doubling up to 30 s, until every moved channel is subscribed on its owner. A subscriber error retries at once.
- **Next action:** Check the health of the node that owns the slot attribute (CLUSTER SHARDS or CLUSTER SLOTS), and for NOPERM the Redis user's channel permissions. Delivery resumes on its own once the node accepts the subscription.
- **Runtime help:** `docs/errors.md#extensions-err-sharded-channel-reconcile`
- **Runtime sources:** [src/redis/sharded-pubsub.js](../src/redis/sharded-pubsub.js)

<a id="extensions-err-sharded-reconcile"></a>
## `EXTENSIONS-ERR-SHARDED-RECONCILE`

- **Code/event:** `redis.sharded-pubsub.reconcile.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.reconcile.failed severity=error] Sharded pub/sub reconciliation could not refresh slot ownership.`
- **Cause:** Reading CLUSTER SLOTS threw or took longer than 5 seconds, or no master gave its id (CLUSTER MYID failed or took longer than 5 seconds on every one), while reconciling after a subscriber error, after a master's subscriber connection ended (one closed for not answering included), or on a retry.
- **Consequence:** Channels that moved during the reshard stay unsubscribed on this instance until a reconciliation succeeds. Reported once per failure streak; a reconciliation that completes re-arms it.
- **Automatic recovery:** Yes: the reconciliation is retried after 1 s, then with the delay doubling up to 30 s, until it completes. A subscriber error retries at once.
- **Next action:** Check cluster health and run CLUSTER SLOTS from the instance, and check that the Redis user may run CLUSTER MYID (+cluster|myid); delivery resumes on its own once the slot map and the masters' ids can be read.
- **Runtime help:** `docs/errors.md#extensions-err-sharded-reconcile`
- **Runtime sources:** [src/redis/sharded-pubsub.js](../src/redis/sharded-pubsub.js)

<a id="extensions-err-pubsub-envelope-oversized"></a>
## `EXTENSIONS-ERR-PUBSUB-ENVELOPE-OVERSIZED`

- **Code/event:** `redis.pubsub.envelope.oversized`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.envelope.oversized severity=warn] A publish envelope exceeds maxEnvelopeBytes and was not relayed.`
- **Cause:** The serialized envelope exceeds the byte limit enforced by peers. The limit includes the whole batch envelope.
- **Consequence:** Local delivery is retained, but remote instances and sibling workers miss this envelope. Reported once per oversized streak.
- **Automatic recovery:** The next envelope within the limit is relayed normally and re-arms the report.
- **Next action:** Reduce publish or batch payload sizes, or raise maxEnvelopeBytes consistently on every instance.
- **Runtime help:** `docs/errors.md#extensions-err-pubsub-envelope-oversized`
- **Runtime sources:** [src/redis/pubsub.js](../src/redis/pubsub.js), [src/redis/sharded-pubsub.js](../src/redis/sharded-pubsub.js)

<a id="extensions-err-topic-broadcast-envelope-oversized"></a>
## `EXTENSIONS-ERR-TOPIC-BROADCAST-ENVELOPE-OVERSIZED`

- **Code/event:** `redis.topic-broadcast.envelope.oversized`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.topic-broadcast event=redis.topic-broadcast.envelope.oversized severity=warn] A topic broadcast reply exceeds maxEnvelopeBytes and was not relayed.`
- **Cause:** The serialized reply envelope exceeds the byte limit enforced by peers.
- **Consequence:** The requesting instance cannot collect these replies and may wait until its timeout. Reported once per oversized streak.
- **Automatic recovery:** The next envelope within the limit is relayed normally and re-arms the report.
- **Next action:** Reduce subscriber reply sizes, or raise maxEnvelopeBytes consistently on every instance.
- **Runtime help:** `docs/errors.md#extensions-err-topic-broadcast-envelope-oversized`
- **Runtime sources:** [src/redis/topic-broadcast.js](../src/redis/topic-broadcast.js)

<a id="extensions-err-pubsub-trace-omitted"></a>
## `EXTENSIONS-ERR-PUBSUB-TRACE-OMITTED`

- **Code/event:** `redis.pubsub.trace-context.omitted`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.trace-context.omitted severity=warn] Trace context was omitted from a relay envelope that would otherwise exceed maxEnvelopeBytes.`
- **Cause:** The application payload alone fits under maxEnvelopeBytes, but adding the traceparent and tracestate fields would push the envelope over it, and receiving instances drop oversized envelopes before parsing.
- **Consequence:** The message is relayed and delivered without tracing fields; the consumer operation on other instances starts a new trace instead of continuing this one. Reported once per bus.
- **Automatic recovery:** None; each affected envelope is relayed untraced.
- **Next action:** Raise maxEnvelopeBytes on every instance of the bus, or shrink payloads that sit within a few hundred bytes of the cap.
- **Runtime help:** `docs/errors.md#extensions-err-pubsub-trace-omitted`
- **Runtime sources:** [src/redis/pubsub.js](../src/redis/pubsub.js)

<a id="extensions-err-sharded-trace-omitted"></a>
## `EXTENSIONS-ERR-SHARDED-TRACE-OMITTED`

- **Code/event:** `redis.sharded-pubsub.trace-context.omitted`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.trace-context.omitted severity=warn] Trace context was omitted from a shard relay envelope that would otherwise exceed maxEnvelopeBytes.`
- **Cause:** The application payload alone fits under maxEnvelopeBytes, but adding the traceparent and tracestate fields would push the envelope over it, and receiving instances drop oversized envelopes before parsing.
- **Consequence:** The message is relayed and delivered without tracing fields; the consumer operation on other instances starts a new trace instead of continuing this one. Reported once per bus.
- **Automatic recovery:** None; each affected envelope is relayed untraced.
- **Next action:** Raise maxEnvelopeBytes on every instance of the bus, or shrink payloads that sit within a few hundred bytes of the cap.
- **Runtime help:** `docs/errors.md#extensions-err-sharded-trace-omitted`
- **Runtime sources:** [src/redis/sharded-pubsub.js](../src/redis/sharded-pubsub.js)

<a id="extensions-err-redis-replay-storage-fallback"></a>
## `EXTENSIONS-ERR-REDIS-REPLAY-STORAGE-FALLBACK`

- **Code/event:** `redis.replay.storage-fallback`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.replay event=redis.replay.storage-fallback severity=warn] Replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.`
- **Cause:** A replay publish could not write to Redis (unreachable, breaker open, or the publish script failed) and the buffer was created with localFanoutOnStorageFailure.
- **Consequence:** Subscribers connected now receive the message, but it is not in the replay buffer, so a client that reconnects and resumes cannot recover it. Only the first fallback is reported; replay_storage_fallbacks_total counts every one.
- **Automatic recovery:** Yes for later publishes: each one tries the store first and is stored again once Redis answers. Messages that fell back are not backfilled.
- **Next action:** Check Redis connectivity and the breaker, then read replay_storage_fallbacks_total for how many publishes went unstored. The requestId attribute correlates the first one with its request.
- **Runtime help:** `docs/errors.md#extensions-err-redis-replay-storage-fallback`
- **Runtime sources:** [src/redis/replay.js](../src/redis/replay.js)

<a id="extensions-err-redis-replay-stream-storage-fallback"></a>
## `EXTENSIONS-ERR-REDIS-REPLAY-STREAM-STORAGE-FALLBACK`

- **Code/event:** `redis.replay-stream.storage-fallback`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.replay-stream event=redis.replay-stream.storage-fallback severity=warn] Stream replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.`
- **Cause:** A stream replay publish could not write to Redis (unreachable, breaker open, or the publish script failed) and the buffer was created with localFanoutOnStorageFailure.
- **Consequence:** Subscribers connected now receive the message, but it is not in the stream, so a client that reconnects and resumes cannot recover it. Only the first fallback is reported; replay_storage_fallbacks_total counts every one.
- **Automatic recovery:** Yes for later publishes: each one tries the store first and is stored again once Redis answers. Messages that fell back are not backfilled.
- **Next action:** Check Redis connectivity and the breaker, then read replay_storage_fallbacks_total for how many publishes went unstored. The requestId attribute correlates the first one with its request.
- **Runtime help:** `docs/errors.md#extensions-err-redis-replay-stream-storage-fallback`
- **Runtime sources:** [src/redis/replay-stream.js](../src/redis/replay-stream.js)

<a id="extensions-err-postgres-replay-storage-fallback"></a>
## `EXTENSIONS-ERR-POSTGRES-REPLAY-STORAGE-FALLBACK`

- **Code/event:** `postgres.replay.storage-fallback`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.replay event=postgres.replay.storage-fallback severity=warn] Replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.`
- **Cause:** A replay publish could not write to Postgres (unreachable, breaker open, or the insert failed) and the buffer was created with localFanoutOnStorageFailure.
- **Consequence:** Subscribers connected now receive the message, but it is not in the replay table, so a client that reconnects and resumes cannot recover it. Only the first fallback is reported; replay_storage_fallbacks_total counts every one.
- **Automatic recovery:** Yes for later publishes: each one tries the store first and is stored again once Postgres answers. Messages that fell back are not backfilled.
- **Next action:** Check Postgres connectivity and the breaker, then read replay_storage_fallbacks_total for how many publishes went unstored. The requestId attribute correlates the first one with its request.
- **Runtime help:** `docs/errors.md#extensions-err-postgres-replay-storage-fallback`
- **Runtime sources:** [src/postgres/replay.js](../src/postgres/replay.js)

<a id="extensions-err-ratelimit-store-floor"></a>
## `EXTENSIONS-ERR-RATELIMIT-STORE-FLOOR`

- **Code/event:** `redis.ratelimit.store-unreachable`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.ratelimit event=redis.ratelimit.store-unreachable severity=warn] The rate-limit store is unreachable; verdicts are decided on the in-process floor.`
- **Cause:** A rate-limit decision could not reach Redis, and the limiter was created with an in-process floor, so this instance decided on its own.
- **Consequence:** Limits hold per instance while degraded: with N instances a client can be allowed up to N times the floor budget. Reported once per limiter; ratelimit_storage_fallbacks_total counts every local verdict.
- **Automatic recovery:** Yes: every decision tries Redis first, so cluster-wide limiting resumes as soon as the store answers.
- **Next action:** Check Redis connectivity and the breaker. If per-instance limits are not acceptable for this route, create the limiter without a floor so an unreachable store fails the call instead.
- **Runtime help:** `docs/errors.md#extensions-err-ratelimit-store-floor`
- **Runtime sources:** [src/redis/ratelimit.js](../src/redis/ratelimit.js)

<a id="extensions-err-composite-ratelimit-store-floor"></a>
## `EXTENSIONS-ERR-COMPOSITE-RATELIMIT-STORE-FLOOR`

- **Code/event:** `redis.composite-ratelimit.store-unreachable`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.composite-ratelimit event=redis.composite-ratelimit.store-unreachable severity=warn] The composite rate-limit store is unreachable; verdicts are decided on the in-process floor.`
- **Cause:** A composite rate-limit decision could not reach Redis, and the limiter was created with an in-process floor, so this instance decided on its own.
- **Consequence:** Limits hold per instance while degraded: with N instances a client can be allowed up to N times each floor budget. Reported once per limiter; ratelimit_composite_storage_fallbacks_total counts every local verdict.
- **Automatic recovery:** Yes: every decision tries Redis first, so cluster-wide limiting resumes as soon as the store answers.
- **Next action:** Check Redis connectivity and the breaker. If per-instance limits are not acceptable for this route, create the limiter without a floor so an unreachable store fails the call instead.
- **Runtime help:** `docs/errors.md#extensions-err-composite-ratelimit-store-floor`
- **Runtime sources:** [src/redis/composite-ratelimit.js](../src/redis/composite-ratelimit.js)

<a id="extensions-err-presence-envelope-oversized"></a>
## `EXTENSIONS-ERR-PRESENCE-ENVELOPE-OVERSIZED`

- **Code/event:** `redis.presence.envelope-oversized`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.presence event=redis.presence.envelope-oversized severity=warn] A presence event envelope exceeds maxEnvelopeBytes and was not relayed to other instances.`
- **Cause:** A presence event (join, leave, or update) serialized larger than maxEnvelopeBytes, the bound every peer enforces before it parses an envelope. The event and size are in the attributes.
- **Consequence:** The event applied on this instance only. Other instances never received it, so their rosters are missing this change: a partial roster rather than an error anywhere else.
- **Automatic recovery:** None for this event. A later event for the same entry that fits under the bound is relayed normally.
- **Next action:** Shrink the presence data - select fewer or smaller fields - or raise maxEnvelopeBytes on every instance together.
- **Runtime help:** `docs/errors.md#extensions-err-presence-envelope-oversized`
- **Runtime sources:** [src/redis/presence.js](../src/redis/presence.js)

<a id="extensions-err-publish-rate-remote-cap"></a>
## `EXTENSIONS-ERR-PUBLISH-RATE-REMOTE-CAP`

- **Code/event:** `redis.publish-rate.remote-instances-capped`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.publish-rate event=redis.publish-rate.remote-instances-capped severity=warn] The publish-rate aggregator reached its sibling-instance bound; stale entries are evicted and envelopes past the bound are dropped.`
- **Cause:** The aggregator is tracking as many sibling instances as its bound allows (the maxRemoteInstances attribute).
- **Consequence:** Cluster-wide publish rates can under-count while the bound holds, because evicted and dropped instances are missing from the sum. Reported once per aggregator.
- **Automatic recovery:** Partial: an evicted instance is counted again once it publishes, if a slot is free.
- **Next action:** A cluster this large usually means a deployment misconfiguration or instances that exit without deactivating. Read cluster_publish_rate_remote_evicted_total and cluster_publish_rate_remote_dropped_total.
- **Operator shortlink:** `https://svti.me/publish-rate-aggregator`
- **Runtime help:** `docs/errors.md#extensions-err-publish-rate-remote-cap`
- **Runtime sources:** [src/redis/publish-rate.js](../src/redis/publish-rate.js)

<a id="extensions-err-registry-index-bucket"></a>
## `EXTENSIONS-ERR-REGISTRY-INDEX-BUCKET`

- **Code/event:** `redis.registry.index-bucket.large`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.registry event=redis.registry.index-bucket.large severity=warn] A registry secondary-index bucket is very large and keeps growing.`
- **Cause:** One value of an indexed attribute (named in the attributes) is shared by more userIds than the per-bucket threshold.
- **Consequence:** Nothing is dropped - evicting from the index would make sendTo miss recipients - so the bucket and its memory keep growing. Reported once per attribute.
- **Automatic recovery:** None; the bucket shrinks as those users disconnect.
- **Next action:** Index only the attributes sendTo filters on. A broad attribute that almost every user shares, such as a default role, makes one huge bucket without narrowing any sendTo.
- **Operator shortlink:** `https://svti.me/registry-index`
- **Runtime help:** `docs/errors.md#extensions-err-registry-index-bucket`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-registry-user-index"></a>
## `EXTENSIONS-ERR-REGISTRY-USER-INDEX`

- **Code/event:** `redis.registry.user-index.large`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.registry event=redis.registry.user-index.large severity=warn] The cluster-wide registry user index is very large and keeps growing.`
- **Cause:** The user index, kept on every instance, reached its size threshold.
- **Consequence:** Nothing is evicted - eviction would mis-route sendTo - so memory keeps growing: roughly 32 bytes of map state per entry on every instance. Reported once.
- **Automatic recovery:** None; the index shrinks as users disconnect.
- **Next action:** Stop registering identities that are unique per connection (anonymous or random ids), or split the cluster.
- **Operator shortlink:** `https://svti.me/registry-index`
- **Runtime help:** `docs/errors.md#extensions-err-registry-user-index`
- **Runtime sources:** [src/redis/registry.js](../src/redis/registry.js)

<a id="extensions-err-pubsub-relay-burst"></a>
## `EXTENSIONS-ERR-PUBSUB-RELAY-BURST`

- **Code/event:** `redis.pubsub.relay-batch.burst`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.relay-batch.burst severity=warn] One event-loop iteration queued more relay publishes than the per-tick bound.`
- **Cause:** Code published in a tight synchronous loop without yielding, so the whole burst landed in one tick.
- **Consequence:** Everything is still relayed, as one pipelined batch at the end of the iteration; memory and latency spike for that tick. Reported once per bus.
- **Automatic recovery:** Yes; the batch drains every tick.
- **Next action:** Find the publish-in-loop and yield between chunks, or send the list as one envelope with publishBatched.
- **Operator shortlink:** `https://svti.me/pubsub-burst`
- **Runtime help:** `docs/errors.md#extensions-err-pubsub-relay-burst`
- **Runtime sources:** [src/redis/pubsub.js](../src/redis/pubsub.js)

<a id="extensions-err-pubsub-system-channel-refused"></a>
## `EXTENSIONS-ERR-PUBSUB-SYSTEM-CHANNEL-REFUSED`

- **Code/event:** `redis.pubsub.system-channel.refused`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.system-channel.refused severity=warn] A connection was not subscribed to the bus's systemChannel, so it gets no degraded or recovered events.`
- **Cause:** The bus's open hook subscribes each connection with platform.subscribe(ws, systemChannel), which runs the application's subscribeBatch hook when it exports one and its subscribe hook otherwise, and the platform refused with the reason attribute. FORBIDDEN, UNAUTHENTICATED or APP_DEFINED (a reason the application wrote) mean that hook denied the topic, and INTERNAL_ERROR that it threw. RATE_LIMITED means the connection was at one of the adapter's per-connection subscribe limits, or the hook returned it. INVALID_TOPIC means the hook returned it, the adapter's topic check refused the name (over 256 characters, a control character, a quote or a backslash), or the adapter does not take a server-side subscribe to a __-prefixed topic. Reported only for a bus with a breaker, the only kind that publishes degraded and recovered events.
- **Consequence:** Connections refused this way get every other message, but not the degraded and recovered events this bus publishes on the systemChannel. Reported once per bus.
- **Automatic recovery:** None; a connection is subscribed once, when it opens.
- **Next action:** Let the subscribeBatch or subscribe hook, whichever the application exports, allow the systemChannel topic (__realtime unless configured). For an adapter RATE_LIMITED, run the bus's open hook before code that subscribes the connection to its other topics; for an adapter INVALID_TOPIC on a custom systemChannel, use a plain name. If the hook allows the topic and the adapter still refuses a __-prefixed one, create the bus with systemChannel: false, as when no client needs the events.
- **Runtime help:** `docs/errors.md#extensions-err-pubsub-system-channel-refused`
- **Runtime sources:** [src/redis/pubsub.js](../src/redis/pubsub.js)

<a id="extensions-err-sharded-relay-burst"></a>
## `EXTENSIONS-ERR-SHARDED-RELAY-BURST`

- **Code/event:** `redis.sharded-pubsub.relay-batch.channel-burst`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.sharded-pubsub event=redis.sharded-pubsub.relay-batch.channel-burst severity=warn] One event-loop iteration queued publishes for more distinct shard channels than the per-tick bound.`
- **Cause:** One synchronous burst published to that many distinct topics, which usually means a topic name built from a value that is unique per message.
- **Consequence:** Everything is still published; the tick batch grows with the number of channels. Reported once per bus.
- **Automatic recovery:** Yes; the batch drains every tick.
- **Next action:** Look for a topic-cardinality leak: a topic name that embeds a per-message id instead of a per-room one.
- **Operator shortlink:** `https://svti.me/sharded-bus-burst`
- **Runtime help:** `docs/errors.md#extensions-err-sharded-relay-burst`
- **Runtime sources:** [src/redis/sharded-pubsub.js](../src/redis/sharded-pubsub.js)

<a id="extensions-err-redis-duplicates"></a>
## `EXTENSIONS-ERR-REDIS-DUPLICATES`

- **Code/event:** `redis.client.duplicates.excessive`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.client event=redis.client.duplicates.excessive severity=warn] One Redis client has spawned more duplicate connections than a process should need.`
- **Cause:** Modules that each take their own subscriber connection were constructed many times, usually per request or per connection instead of once at startup.
- **Consequence:** Every duplicate is a real socket to Redis, so a leak ends at the server connection limit. Reported once per client.
- **Automatic recovery:** Duplicates are released as they end; the leak itself continues until the code changes.
- **Next action:** Find the module constructed repeatedly without a matching deactivate or destroy, and construct it once at startup.
- **Operator shortlink:** `https://svti.me/redis-duplicates`
- **Runtime help:** `docs/errors.md#extensions-err-redis-duplicates`
- **Runtime sources:** [src/redis/index.js](../src/redis/index.js)

<a id="extensions-err-smooth-owner-fallback"></a>
## `EXTENSIONS-ERR-SMOOTH-OWNER-FALLBACK`

- **Code/event:** `redis.smooth.relay-owner.unresolved`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.smooth event=redis.smooth.relay-owner.unresolved severity=warn] The smooth relay channel owner could not be resolved; publishes are pinned to a fallback master.`
- **Cause:** The cluster did not report which master owns the relay channel, typically while the topology is changing.
- **Consequence:** Publishes go through the first master so broadcast order is kept; delivery continues. Reported once per relay.
- **Automatic recovery:** Delivery continues through the fallback master without intervention.
- **Next action:** None if the cluster was resharding. On a stable cluster, check that every master is reachable from this instance.
- **Runtime help:** `docs/errors.md#extensions-err-smooth-owner-fallback`
- **Runtime sources:** [src/redis/smooth-cluster.js](../src/redis/smooth-cluster.js)

<a id="extensions-err-resume-topics-truncated"></a>
## `EXTENSIONS-ERR-RESUME-TOPICS-TRUNCATED`

- **Code/event:** `replay.resume.topics-truncated`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.replay event=replay.resume.topics-truncated severity=warn] A resume request presented more topics than the per-request bound; the remainder were not resumed.`
- **Cause:** A client asked to resume more topics in one request than the accepted attribute allows.
- **Consequence:** The first topics up to the bound were resumed; the rest were neither inspected nor resumed, so they start without replay. Reported at most once per interval (intervalMs attribute) per store.
- **Automatic recovery:** None for that request.
- **Next action:** Give the client fewer topics per connection, or spread its subscriptions across connections.
- **Runtime help:** `docs/errors.md#extensions-err-resume-topics-truncated`
- **Runtime sources:** [src/shared/replay-helpers.js](../src/shared/replay-helpers.js)

<a id="extensions-err-prometheus-series-cap"></a>
## `EXTENSIONS-ERR-PROMETHEUS-SERIES-CAP`

- **Code/event:** `prometheus.series.cap-reached`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.prometheus event=prometheus.series.cap-reached severity=warn] A metric reached its maxSeries cap; new label sets for it are dropped.`
- **Cause:** The metric named in the attributes has as many distinct label sets as its maxSeries allows.
- **Consequence:** Existing series keep updating; observations for any new label set are dropped. prometheus_series_dropped_total counts the drops. Reported once per metric.
- **Automatic recovery:** None while the cap holds.
- **Next action:** Raise maxSeries if the cardinality is legitimate, or pass mapTopic so topic labels stay bounded.
- **Runtime help:** `docs/errors.md#extensions-err-prometheus-series-cap`
- **Runtime sources:** [src/prometheus/index.js](../src/prometheus/index.js)

<a id="extensions-err-postgres-tasks-sweep"></a>
## `EXTENSIONS-ERR-POSTGRES-TASKS-SWEEP`

- **Code/event:** `postgres.tasks.sweep.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.tasks event=postgres.tasks.sweep.failed severity=error] A background task sweep failed.`
- **Cause:** The dispatch, recovery, or cleanup sweep (named in the sweep attribute) threw: Postgres unreachable, or a schema mismatch such as a table managed with autoMigrate: false that lacks a column this release reads.
- **Consequence:** That sweep did nothing: pending tasks are not dispatched, stuck ones are not reclaimed, or terminal rows are not deleted. Reported once per failure streak per sweep.
- **Automatic recovery:** Yes: every interval retries, and the first success resumes normal work and re-arms this report.
- **Next action:** Read the error attribute. A missing-column error means the table needs the ALTER TABLE statements from the migration guide; otherwise check Postgres connectivity and the breaker.
- **Runtime help:** `docs/errors.md#extensions-err-postgres-tasks-sweep`
- **Runtime sources:** [src/postgres/tasks.js](../src/postgres/tasks.js)

<a id="extensions-err-redis-client"></a>
## `EXTENSIONS-ERR-REDIS-CLIENT`

- **Code/event:** `redis.client.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.client event=redis.client.failed severity=error] The Redis client created by createRedisClient reported an error.`
- **Cause:** The main ioredis connection emitted an error: the server is unreachable, refused the connection, or dropped it.
- **Consequence:** Commands on this client fail or queue until the connection is restored; modules report their own failures on top of this one.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure.
- **Next action:** Check Redis reachability and credentials; the error attribute carries the redacted cause.
- **Runtime help:** `docs/errors.md#extensions-err-redis-client`
- **Runtime sources:** [src/redis/index.js](../src/redis/index.js)

<a id="extensions-err-redis-resubscribe"></a>
## `EXTENSIONS-ERR-REDIS-RESUBSCRIBE`

- **Code/event:** `redis.subscriber.resubscribe.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.subscriber event=redis.subscriber.resubscribe.failed severity=error] Redis refused a subscription restored after a reconnect.`
- **Cause:** A subscriber connection reconnected, or on a Redis Cluster its subscriptions moved to a new node connection after the previous one ended, and Redis refused the SUBSCRIBE, PSUBSCRIBE or SSUBSCRIBE that restores one of its channels, for example NOPERM after the Redis user lost access to the channel, which is itself what makes Redis close a subscribed connection.
- **Consequence:** Messages on the refused channels do not reach this instance, while the module that owns the connection still treats them as subscribed. The owner attribute names that module, kind the subscription type of the first refused channel and refused how many channels were refused; channel names are not logged. Reported once per failure streak per subscriber.
- **Automatic recovery:** Refused channels are retried with a backoff from 1 to 30 seconds while the connection stays up, and again after every reconnect or move to a new cluster node connection, until Redis accepts them or the module releases them. Channels Redis accepts are restored at once.
- **Next action:** Grant the Redis user the channels this package subscribes to, or restore the server configuration that changed; the next retry restores the subscription.
- **Runtime help:** `docs/errors.md#extensions-err-redis-resubscribe`
- **Runtime sources:** [src/shared/redis-subscriber.js](../src/shared/redis-subscriber.js)

<a id="extensions-err-redis-subscriber-silent"></a>
## `EXTENSIONS-ERR-REDIS-SUBSCRIBER-SILENT`

- **Code/event:** `redis.subscriber.silent`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.subscriber event=redis.subscriber.silent severity=error] A Redis subscriber connection stopped answering and was closed.`
- **Cause:** Nothing came in over the connection within timeoutMs milliseconds of a PING, or a new connection stayed in its handshake for timeoutMs (a whole PING round). Subscriber connections are asked every 30 seconds because one that only receives notices no failure on its own: a host that died or was cut off without closing the socket, or a NAT or load balancer that dropped the idle flow, leaves it open and silent until TCP keepalive gives up, about two hours at the operating system default. A server blocked for longer than timeoutMs by one command reads the same way.
- **Consequence:** Messages published on the connection's channels while it was silent, or before its channels are restored, may not have reached this instance. The owner attribute names the module that owns it, and master the node id of the master a per-master connection belongs to. Reported once per streak per subscriber, or per master for the per-master connections, until a PING there is answered again.
- **Automatic recovery:** The connection is closed at once. A standalone subscriber reconnects and restores its channels; on a Redis Cluster a module subscriber moves its channels to another node, and the per-master connections of the sharded bus and of the presence keyspace cleanup are opened again on their master once the cluster lists it.
- **Next action:** Check the network path to the Redis node and its health. If it repeats while the server is healthy, look for commands that block it for seconds (SLOWLOG) and for a proxy that drops idle connections.
- **Runtime help:** `docs/errors.md#extensions-err-redis-subscriber-silent`
- **Runtime sources:** [src/shared/redis-subscriber.js](../src/shared/redis-subscriber.js)

<a id="extensions-err-redis-subscriber-unreachable"></a>
## `EXTENSIONS-ERR-REDIS-SUBSCRIBER-UNREACHABLE`

- **Code/event:** `redis.subscriber.unreachable`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.subscriber event=redis.subscriber.unreachable severity=error] No node of the Redis Cluster accepted a subscriber connection.`
- **Cause:** A connection to every node the subscriber's Cluster knows ended before a command on it succeeded: the nodes are down or unreachable from this host, refuse the login or the connection (maxclients), or every one of them moved to a new address, as a rolling restart without stable addresses does. The nodes attribute is how many nodes it knows, and the error attribute the last error such a connection raised, when one did.
- **Consequence:** Messages on the subscriber's channels do not reach this instance until a node accepts it. The owner attribute names the module that owns it. Reported once per failure streak per subscriber.
- **Automatic recovery:** The subscriber reads the cluster slot map again and keeps trying with a delay of up to 2 seconds. When no node it knows answers that read either, its Cluster reconnects through the startup nodes, which looks their addresses up again. The attempts of that reconnect, and of one a slot map read of the subscriber set off (a node with no slots answers with an empty map, on which ioredis drops every node), are spaced as the node connections are redialled, growing to 2 seconds, or longer when clusterRetryStrategy asks, and do not stop until the Cluster is ready again, even when the strategy would give up. ioredis raises an error for an attempt whose slot map read reaches no node, which the owning module reports as its subscriber error. A connection that works ends the streak and re-arms this report.
- **Next action:** Check that the cluster nodes are up, reachable from this host and accept the configured credentials. When node addresses change on restart, give the Cluster startup nodes that resolve to the current ones, such as a DNS name.
- **Runtime help:** `docs/errors.md#extensions-err-redis-subscriber-unreachable`
- **Runtime sources:** [src/shared/redis-subscriber.js](../src/shared/redis-subscriber.js)

<a id="extensions-err-pubsub-subscriber"></a>
## `EXTENSIONS-ERR-PUBSUB-SUBSCRIBER`

- **Code/event:** `redis.pubsub.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.pubsub event=redis.pubsub.subscriber.failed severity=error] The pub/sub bus subscriber connection reported an error.`
- **Cause:** The dedicated connection that receives relayed publishes from other instances emitted an error.
- **Consequence:** Publishes from other instances are not delivered locally while the subscriber is down; local publishes are unaffected.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure.
- **Next action:** Inspect Redis connectivity and verify cross-instance delivery resumes.
- **Runtime help:** `docs/errors.md#extensions-err-pubsub-subscriber`
- **Runtime sources:** [src/redis/pubsub.js](../src/redis/pubsub.js)

<a id="extensions-err-crdt-subscriber"></a>
## `EXTENSIONS-ERR-CRDT-SUBSCRIBER`

- **Code/event:** `redis.crdt.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.crdt event=redis.crdt.subscriber.failed severity=error] The CRDT relay subscriber connection reported an error.`
- **Cause:** The dedicated connection that receives CRDT updates from other instances emitted an error.
- **Consequence:** Updates from other instances are not applied locally while the subscriber is down, and they are not replayed when it reconnects: the relay keeps no backlog, and a replica syncs from its peers only when its document is loaded on this instance.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure. Updates missed during the outage are not recovered automatically.
- **Next action:** Restore Redis connectivity. Documents that received remote updates during the outage stay diverged on this instance until they are loaded again, which syncs them from live peers: after every local editor of the document has left, or after a restart.
- **Runtime help:** `docs/errors.md#extensions-err-crdt-subscriber`
- **Runtime sources:** [src/redis/crdt.js](../src/redis/crdt.js)

<a id="extensions-err-smooth-subscriber"></a>
## `EXTENSIONS-ERR-SMOOTH-SUBSCRIBER`

- **Code/event:** `redis.smooth.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.smooth event=redis.smooth.subscriber.failed severity=error] The smooth relay subscriber connection reported an error.`
- **Cause:** The dedicated connection that receives smooth relay commands, broadcasts and sync replies from other instances emitted an error.
- **Consequence:** Relay traffic from other instances is not received while the subscriber is down.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure.
- **Next action:** Inspect Redis connectivity and verify relay traffic resumes.
- **Runtime help:** `docs/errors.md#extensions-err-smooth-subscriber`
- **Runtime sources:** [src/redis/smooth-cluster.js](../src/redis/smooth-cluster.js)

<a id="extensions-err-topic-broadcast-subscriber"></a>
## `EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBER`

- **Code/event:** `redis.topic-broadcast.subscriber.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.topic-broadcast event=redis.topic-broadcast.subscriber.failed severity=error] The topic-broadcast subscriber connection reported an error.`
- **Cause:** The dedicated connection that receives broadcasts from other instances emitted an error.
- **Consequence:** Broadcasts from other instances are not delivered locally while the subscriber is down.
- **Automatic recovery:** The Redis client owns reconnection; this handler records the failure.
- **Next action:** Inspect Redis connectivity and verify cross-instance broadcasts resume.
- **Runtime help:** `docs/errors.md#extensions-err-topic-broadcast-subscriber`
- **Runtime sources:** [src/redis/topic-broadcast.js](../src/redis/topic-broadcast.js)

<a id="extensions-err-crdt-subscribe"></a>
## `EXTENSIONS-ERR-CRDT-SUBSCRIBE`

- **Code/event:** `redis.crdt.subscribe.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.crdt event=redis.crdt.subscribe.failed severity=error] The CRDT relay could not subscribe to its channel.`
- **Cause:** SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.
- **Consequence:** Updates and sync requests from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak; an onError option, when set, still sees every failure.
- **Automatic recovery:** The connection is closed and the next relay subscribes on a fresh one; a subscribe that succeeds re-arms this report.
- **Next action:** Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.
- **Runtime help:** `docs/errors.md#extensions-err-crdt-subscribe`
- **Runtime sources:** [src/redis/crdt.js](../src/redis/crdt.js)

<a id="extensions-err-smooth-subscribe"></a>
## `EXTENSIONS-ERR-SMOOTH-SUBSCRIBE`

- **Code/event:** `redis.smooth.subscribe.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.smooth event=redis.smooth.subscribe.failed severity=error] The smooth relay could not subscribe to its channel.`
- **Cause:** SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.
- **Consequence:** Relay commands, broadcasts and sync replies from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak; an onError option, when set, still sees every failure.
- **Automatic recovery:** The connection is closed and the next relay subscribes on a fresh one; a subscribe that succeeds re-arms this report.
- **Next action:** Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.
- **Runtime help:** `docs/errors.md#extensions-err-smooth-subscribe`
- **Runtime sources:** [src/redis/smooth-cluster.js](../src/redis/smooth-cluster.js)

<a id="extensions-err-topic-broadcast-subscribe"></a>
## `EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBE`

- **Code/event:** `redis.topic-broadcast.subscribe.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.topic-broadcast event=redis.topic-broadcast.subscribe.failed severity=error] The topic broadcast could not subscribe to its channel.`
- **Cause:** SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.
- **Consequence:** Broadcasts and requests from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak; an onError option, when set, still sees every failure.
- **Automatic recovery:** The connection is closed and the next broadcast subscribes on a fresh one; a subscribe that succeeds re-arms this report.
- **Next action:** Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.
- **Runtime help:** `docs/errors.md#extensions-err-topic-broadcast-subscribe`
- **Runtime sources:** [src/redis/topic-broadcast.js](../src/redis/topic-broadcast.js)

<a id="extensions-err-cursor-subscribe"></a>
## `EXTENSIONS-ERR-CURSOR-SUBSCRIBE`

- **Code/event:** `redis.cursor.subscribe.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.redis.cursor event=redis.cursor.subscribe.failed severity=error] The cursor relay could not subscribe to its channel.`
- **Cause:** SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.
- **Consequence:** Cursor positions from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak.
- **Automatic recovery:** The connection is closed and a fresh one subscribes on the next cursor update, attach or `__cursor:` subscribe, and on a retry that backs off from one to thirty seconds while a connection on this instance uses the cursor; a subscribe that succeeds re-arms this report.
- **Next action:** Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.
- **Runtime help:** `docs/errors.md#extensions-err-cursor-subscribe`
- **Runtime sources:** [src/redis/cursor/redis-io.js](../src/redis/cursor/redis-io.js)

<a id="extensions-err-postgres-notify-connection"></a>
## `EXTENSIONS-ERR-POSTGRES-NOTIFY-CONNECTION`

- **Code/event:** `postgres.notify.connection.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.postgres.notify event=postgres.notify.connection.failed severity=error] The Postgres LISTEN connection of a NOTIFY bridge reported an error and was closed.`
- **Cause:** The dedicated LISTEN connection emitted an error: the server went away, restarted, or dropped the connection.
- **Consequence:** Notifications on the channel attribute are missed until the bridge listens again; Postgres does not queue them for a closed listener.
- **Automatic recovery:** With autoReconnect the bridge reconnects after reconnectInterval; in advisory mode the next poll re-establishes the listener.
- **Next action:** Check Postgres availability. Notifications sent while the listener was down are lost, so a consumer that needs them must resynchronise from the table.
- **Runtime help:** `docs/errors.md#extensions-err-postgres-notify-connection`
- **Runtime sources:** [src/postgres/notify.js](../src/postgres/notify.js)

<a id="extensions-err-sink-failed"></a>
## `EXTENSIONS-ERR-SINK-FAILED`

- **Code/event:** `operational.sink.failed`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.observability event=operational.sink.failed severity=error] The configured operational event sink failed; console fallback was restored for this event.`
- **Cause:** The sink installed through setOperationalEventSink threw or returned a rejected promise for one record.
- **Consequence:** That record was written to console instead; the sink stays installed and receives the next record.
- **Automatic recovery:** Per record: console fallback. The sink is not removed.
- **Next action:** Fix the sink named by the originalSource and originalEvent attributes; a sink should never throw.
- **Runtime help:** `docs/errors.md#extensions-err-sink-failed`
- **Runtime sources:** [src/shared/diagnostic.js](../src/shared/diagnostic.js)

<a id="extensions-err-invariant"></a>
## `EXTENSIONS-ERR-INVARIANT`

- **Code/event:** `invariant.violated`
- **Message prefix:** `[lantean/diagnostic source=svelte-adapter-uws-extensions component=runtime.assertion event=invariant.violated severity=`
- **Cause:** A framework-internal assertion failed. For assert and fatal the message is the assertion category; for devAssert it is the text the call site passed. The severity is chosen by the call site, so both vary.
- **Consequence:** Depends on the tier. A soft violation is counted and logged; at the fatal tier the worker is scheduled to exit with status 78 and the supervisor replaces it. A development-only assertion is logged and never counted.
- **Automatic recovery:** None for the condition itself. A fatal-tier violation exits the worker, which is replacement rather than recovery.
- **Next action:** Read the severity first, because it selects the tier, then the category and context attributes. These are library-internal invariants, so a violation is an extensions defect rather than an application misconfiguration; report it with both attributes.
- **Runtime help:** `docs/errors.md#extensions-err-invariant`
- **Runtime sources:** [src/shared/assert.js](../src/shared/assert.js)

<a id="extensions-err-diagnostic-record-shape"></a>
## `EXTENSIONS-ERR-DIAGNOSTIC-RECORD-SHAPE`

- **Code/event:** `observability.diagnostic.record-shape`
- **Message prefix:** `[extensions] operational event dropped, invalid record shape`
- **Cause:** Something emitted an operational diagnostic the runtime could not build a record from: a malformed event name, an unknown severity or data class, or a bad timestamp. An unserializable attribute is a quieter failure handled by the renderer, which prints the envelope with the attributes stripped.
- **Consequence:** That diagnostic is dropped: it reaches neither the configured sink nor the log. Everything else keeps emitting normally.
- **Automatic recovery:** None for the dropped record. Telemetry never throws, so the emitting path continued as if it had been reported.
- **Next action:** Read the event name printed with this line and fix the emitter; if it is application code calling the diagnostic surface, check the record against the documented shape.
- **Runtime help:** `docs/errors.md#extensions-err-diagnostic-record-shape`
- **Runtime sources:** [src/shared/diagnostic.js](../src/shared/diagnostic.js)

<a id="extensions-err-diagnostic-render-collapse"></a>
## `EXTENSIONS-ERR-DIAGNOSTIC-RENDER-COLLAPSE`

- **Code/event:** `observability.diagnostic.render-collapse`
- **Message prefix:** `[extensions] diagnostic render failed: `
- **Cause:** Formatting the record failed twice: once with its attributes and once with them stripped, which means the process JSON serializer itself is failing.
- **Consequence:** The event is lost; only this line and the event name reach the console.
- **Automatic recovery:** None.
- **Next action:** Treat the process as unhealthy; a serializer that cannot render an envelope of strings is a runtime fault, not an application one.
- **Runtime help:** `docs/errors.md#extensions-err-diagnostic-render-collapse`
- **Runtime sources:** [src/shared/diagnostic.js](../src/shared/diagnostic.js)

<a id="extensions-err-diagnostic-console-write"></a>
## `EXTENSIONS-ERR-DIAGNOSTIC-CONSOLE-WRITE`

- **Code/event:** `observability.diagnostic.console-write`
- **Message prefix:** `[extensions] diagnostic rendered but the console refused it: `
- **Cause:** The record rendered, but the console method chosen for its severity threw: a host replaced the console or the write end of its transport is gone.
- **Consequence:** The rendered line was not written; this notice goes through console.error because that is a different method in every case but one.
- **Automatic recovery:** None.
- **Next action:** Check what replaced or wrapped the console in this process.
- **Runtime help:** `docs/errors.md#extensions-err-diagnostic-console-write`
- **Runtime sources:** [src/shared/diagnostic.js](../src/shared/diagnostic.js)

<a id="extensions-err-diagnostic-sink-notice"></a>
## `EXTENSIONS-ERR-DIAGNOSTIC-SINK-NOTICE`

- **Code/event:** `observability.diagnostic.sink-notice`
- **Message prefix:** `[extensions] operational sink failed and its failure notice could not be built: `
- **Cause:** A sink failed, the original record was printed to console, and then building the sink-failure notice itself threw, usually because the injected clock is broken.
- **Consequence:** The original event was already written; only the notice about the sink is missing.
- **Automatic recovery:** None.
- **Next action:** Fix the sink, then check the runtime clock injection if this line keeps appearing.
- **Runtime help:** `docs/errors.md#extensions-err-diagnostic-sink-notice`
- **Runtime sources:** [src/shared/diagnostic.js](../src/shared/diagnostic.js)
