/**
 * Stable search keys for operator-facing extension failures. messagePrefix is
 * the invariant beginning of the text an operator sees - for a diagnostic
 * emission the whole line head up to and including the message - and
 * problemPrefix is the message alone, which is what an emitter passes on.
 * Dynamic user ids, timeouts, and backend details follow them at runtime.
 *
 * `emission` says which shape the runtime actually produces, because they
 * differ and a prefix that never appears in a log is worse than no prefix:
 *
 * - `thrown`  - an Error message with no diagnostic line around it; the text
 *               carries the stable ID tag and the repo-relative help route.
 * - `direct`  - emitRegisteredFailure(), a canonical diagnostic line whose
 *               component, event, severity, and message all come from the
 *               entry; messagePrefix is derived from them, so the emitted
 *               line and the indexed key are one thing.
 * - `head`    - only the line head is invariant; severity and message vary by
 *               call site, so the prefix stops where the variation begins.
 * - `console` - a plain console line with no diagnostic head, printed through
 *               extensionsConsoleLine() so the emitted text IS the registry's
 *               prefix plus the call-site detail and the stable ID tag. These
 *               are the last-resort lines the diagnostic pipeline prints about
 *               itself when it cannot render or deliver a record.
 *
 * @module svelte-adapter-uws-extensions/error-registry
 */

const HEAD = '[lantean/diagnostic source=svelte-adapter-uws-extensions component=';

export const EXTENSIONS_ERROR_IDS = Object.freeze({
	REGISTRY_REQUEST_TARGET: 'EXTENSIONS-ERR-REGISTRY-REQUEST-TARGET',
	REGISTRY_REQUEST_EVENT: 'EXTENSIONS-ERR-REGISTRY-REQUEST-EVENT',
	REGISTRY_REQUEST_OFFLINE: 'EXTENSIONS-ERR-REGISTRY-REQUEST-OFFLINE',
	REGISTRY_REQUEST_PENDING: 'EXTENSIONS-ERR-REGISTRY-REQUEST-PENDING',
	REGISTRY_REQUEST_TIMEOUT: 'EXTENSIONS-ERR-REGISTRY-REQUEST-TIMEOUT',
	REGISTRY_SESSION_TARGET: 'EXTENSIONS-ERR-REGISTRY-SESSION-TARGET',
	REGISTRY_SESSION_EVENT: 'EXTENSIONS-ERR-REGISTRY-SESSION-EVENT',
	REGISTRY_SESSION_OFFLINE: 'EXTENSIONS-ERR-REGISTRY-SESSION-OFFLINE',
	REGISTRY_SESSION_PENDING: 'EXTENSIONS-ERR-REGISTRY-SESSION-PENDING',
	REGISTRY_SESSION_TIMEOUT: 'EXTENSIONS-ERR-REGISTRY-SESSION-TIMEOUT',
	REGISTRY_SEND_TARGET: 'EXTENSIONS-ERR-REGISTRY-SEND-TARGET',
	REGISTRY_SEND_COALESCED_TARGET: 'EXTENSIONS-ERR-REGISTRY-SEND-COALESCED-TARGET',
	CIRCUIT_OPEN: 'EXTENSIONS-ERR-CIRCUIT-OPEN',
	CAPABILITY_COOKIE_METRICS: 'EXTENSIONS-ERR-CAPABILITY-COOKIE-METRICS',
	POSTGRES_POOL_IDLE_CLIENT: 'EXTENSIONS-ERR-POSTGRES-POOL-IDLE-CLIENT',
	POSTGRES_TRANSACTION_CLIENT: 'EXTENSIONS-ERR-POSTGRES-TRANSACTION-CLIENT',
	POSTGRES_NOTIFY_PAYLOAD_NEAR_LIMIT: 'EXTENSIONS-ERR-POSTGRES-NOTIFY-PAYLOAD-NEAR-LIMIT',
	POSTGRES_NOTIFY_PARSE: 'EXTENSIONS-ERR-POSTGRES-NOTIFY-PARSE',
	POSTGRES_TASKS_STATE_LISTENER: 'EXTENSIONS-ERR-POSTGRES-TASKS-STATE-LISTENER',
	CLOCK_SKEW_SAMPLE: 'EXTENSIONS-ERR-CLOCK-SKEW-SAMPLE',
	CLOCK_SAMPLE: 'EXTENSIONS-ERR-CLOCK-SAMPLE',
	CLOCK_NTP_SAMPLE: 'EXTENSIONS-ERR-CLOCK-NTP-SAMPLE',
	CLOCK_LEADER_OFFSET: 'EXTENSIONS-ERR-CLOCK-LEADER-OFFSET',
	CURSOR_SUBSCRIBER: 'EXTENSIONS-ERR-CURSOR-SUBSCRIBER',
	CURSOR_CLEANUP: 'EXTENSIONS-ERR-CURSOR-CLEANUP',
	GROUPS_META_INITIALIZE: 'EXTENSIONS-ERR-GROUPS-META-INITIALIZE',
	GROUPS_HEARTBEAT: 'EXTENSIONS-ERR-GROUPS-HEARTBEAT',
	GROUPS_SUBSCRIBER: 'EXTENSIONS-ERR-GROUPS-SUBSCRIBER',
	LEADER_TICK: 'EXTENSIONS-ERR-LEADER-TICK',
	LEADER_READ_CURRENT: 'EXTENSIONS-ERR-LEADER-READ-CURRENT',
	LEADER_RELEASE: 'EXTENSIONS-ERR-LEADER-RELEASE',
	PRESENCE_KEYSPACE_SUBSCRIBER: 'EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBER',
	PRESENCE_KEYSPACE_SUBSCRIBE: 'EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBE',
	PRESENCE_KEYSPACE_RECONCILE: 'EXTENSIONS-ERR-PRESENCE-KEYSPACE-RECONCILE',
	PRESENCE_KEYSPACE_EVENTS_OFF: 'EXTENSIONS-ERR-PRESENCE-KEYSPACE-EVENTS-OFF',
	PRESENCE_SUBSCRIBER: 'EXTENSIONS-ERR-PRESENCE-SUBSCRIBER',
	PUBLISH_RATE_SUBSCRIBER: 'EXTENSIONS-ERR-PUBLISH-RATE-SUBSCRIBER',
	PUBLISH_RATE_LISTENER: 'EXTENSIONS-ERR-PUBLISH-RATE-LISTENER',
	RATELIMIT_PROXY_COLLAPSE: 'EXTENSIONS-ERR-RATELIMIT-PROXY-COLLAPSE',
	REGISTRY_SUBSCRIBER: 'EXTENSIONS-ERR-REGISTRY-SUBSCRIBER',
	SHARDED_SUBSCRIBER: 'EXTENSIONS-ERR-SHARDED-SUBSCRIBER',
	SHARDED_CHANNEL_RECONCILE: 'EXTENSIONS-ERR-SHARDED-CHANNEL-RECONCILE',
	SHARDED_RECONCILE: 'EXTENSIONS-ERR-SHARDED-RECONCILE',
	PUBSUB_TRACE_OMITTED: 'EXTENSIONS-ERR-PUBSUB-TRACE-OMITTED',
	SHARDED_TRACE_OMITTED: 'EXTENSIONS-ERR-SHARDED-TRACE-OMITTED',
	REDIS_REPLAY_STORAGE_FALLBACK: 'EXTENSIONS-ERR-REDIS-REPLAY-STORAGE-FALLBACK',
	REDIS_REPLAY_STREAM_STORAGE_FALLBACK: 'EXTENSIONS-ERR-REDIS-REPLAY-STREAM-STORAGE-FALLBACK',
	POSTGRES_REPLAY_STORAGE_FALLBACK: 'EXTENSIONS-ERR-POSTGRES-REPLAY-STORAGE-FALLBACK',
	RATELIMIT_STORE_FLOOR: 'EXTENSIONS-ERR-RATELIMIT-STORE-FLOOR',
	COMPOSITE_RATELIMIT_STORE_FLOOR: 'EXTENSIONS-ERR-COMPOSITE-RATELIMIT-STORE-FLOOR',
	PRESENCE_ENVELOPE_OVERSIZED: 'EXTENSIONS-ERR-PRESENCE-ENVELOPE-OVERSIZED',
	PUBLISH_RATE_REMOTE_CAP: 'EXTENSIONS-ERR-PUBLISH-RATE-REMOTE-CAP',
	REGISTRY_INDEX_BUCKET: 'EXTENSIONS-ERR-REGISTRY-INDEX-BUCKET',
	REGISTRY_USER_INDEX: 'EXTENSIONS-ERR-REGISTRY-USER-INDEX',
	PUBSUB_RELAY_BURST: 'EXTENSIONS-ERR-PUBSUB-RELAY-BURST',
	SHARDED_RELAY_BURST: 'EXTENSIONS-ERR-SHARDED-RELAY-BURST',
	REDIS_DUPLICATES: 'EXTENSIONS-ERR-REDIS-DUPLICATES',
	SMOOTH_OWNER_FALLBACK: 'EXTENSIONS-ERR-SMOOTH-OWNER-FALLBACK',
	RESUME_TOPICS_TRUNCATED: 'EXTENSIONS-ERR-RESUME-TOPICS-TRUNCATED',
	PROMETHEUS_SERIES_CAP: 'EXTENSIONS-ERR-PROMETHEUS-SERIES-CAP',
	POSTGRES_TASKS_SWEEP: 'EXTENSIONS-ERR-POSTGRES-TASKS-SWEEP',
	REDIS_CLIENT: 'EXTENSIONS-ERR-REDIS-CLIENT',
	REDIS_RESUBSCRIBE: 'EXTENSIONS-ERR-REDIS-RESUBSCRIBE',
	PUBSUB_SUBSCRIBER: 'EXTENSIONS-ERR-PUBSUB-SUBSCRIBER',
	CRDT_SUBSCRIBER: 'EXTENSIONS-ERR-CRDT-SUBSCRIBER',
	CRDT_SUBSCRIBE: 'EXTENSIONS-ERR-CRDT-SUBSCRIBE',
	SMOOTH_SUBSCRIBE: 'EXTENSIONS-ERR-SMOOTH-SUBSCRIBE',
	TOPIC_BROADCAST_SUBSCRIBE: 'EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBE',
	CURSOR_SUBSCRIBE: 'EXTENSIONS-ERR-CURSOR-SUBSCRIBE',
	SMOOTH_SUBSCRIBER: 'EXTENSIONS-ERR-SMOOTH-SUBSCRIBER',
	TOPIC_BROADCAST_SUBSCRIBER: 'EXTENSIONS-ERR-TOPIC-BROADCAST-SUBSCRIBER',
	POSTGRES_NOTIFY_CONNECTION: 'EXTENSIONS-ERR-POSTGRES-NOTIFY-CONNECTION',
	SINK_FAILED: 'EXTENSIONS-ERR-SINK-FAILED',
	INVARIANT: 'EXTENSIONS-ERR-INVARIANT',
	DIAGNOSTIC_RECORD_SHAPE: 'EXTENSIONS-ERR-DIAGNOSTIC-RECORD-SHAPE',
	DIAGNOSTIC_RENDER_COLLAPSE: 'EXTENSIONS-ERR-DIAGNOSTIC-RENDER-COLLAPSE',
	DIAGNOSTIC_CONSOLE_WRITE: 'EXTENSIONS-ERR-DIAGNOSTIC-CONSOLE-WRITE',
	DIAGNOSTIC_SINK_NOTICE: 'EXTENSIONS-ERR-DIAGNOSTIC-SINK-NOTICE'
});

const REDIS_RECONNECTS = 'The Redis client owns reconnection; this handler records the failure.';

function definition(entry) {
	return Object.freeze({
		code: null,
		component: null,
		severity: null,
		problemPrefix: null,
		dataClass: null,
		link: null,
		...entry,
		sources: Object.freeze(entry.sources),
		help: 'docs/errors.md#' + entry.anchor
	});
}

function thrown(entry) {
	return definition({ ...entry, emission: 'thrown' });
}

/** Builds the invariant head of a directly-emitted diagnostic line. */
function lineHead(component, event, severity) {
	return HEAD + component + ' event=' + event + ' severity=' + severity + '] ';
}

function direct(entry) {
	return definition({
		...entry,
		emission: 'direct',
		messagePrefix: lineHead(entry.component, entry.event, entry.severity) + entry.problemPrefix
	});
}

function consoleLine(entry) {
	return definition({ ...entry, emission: 'console' });
}

export const EXTENSIONS_ERROR_REGISTRY = Object.freeze([
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_REQUEST_TARGET,
		event: 'registry.request.invalid-target',
		messagePrefix: 'registry.request: target must be a non-empty userId string',
		cause: 'request() received an empty or non-string user identifier.',
		consequence: 'The request is rejected before registry or network work begins.',
		automaticRecovery: 'None; invalid input is not retried.',
		nextAction: 'Pass the canonical non-empty user ID recorded by the registry identify callback.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-request-target'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_REQUEST_EVENT,
		event: 'registry.request.invalid-event',
		messagePrefix: 'registry.request: event must be a non-empty string',
		cause: 'request() received an empty or non-string event name.',
		consequence: 'The request is rejected before registry or network work begins.',
		automaticRecovery: 'None; invalid input is not retried.',
		nextAction: 'Pass the non-empty client onRequest event name.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-request-event'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_REQUEST_OFFLINE,
		event: 'registry.request.offline', messagePrefix: 'registry.request: target user "',
		cause: 'No live registry owner exists for the requested user, or its local socket has closed.',
		consequence: 'The request is not delivered and the caller promise rejects.',
		automaticRecovery: 'None; the registry waits for a future connection open event.',
		nextAction: 'Treat the user as offline and retry only after a new registry open event.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-request-offline'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_REQUEST_PENDING,
		event: 'registry.request.pending-cap', messagePrefix: 'registry.request: pending requests exceeded ',
		cause: 'This instance reached its bounded number of cluster requests awaiting replies.',
		consequence: 'The new request is rejected so pending memory remains bounded.',
		automaticRecovery: 'Capacity returns as existing pending requests settle.',
		nextAction: 'Apply caller backpressure and investigate slow or missing recipients before retrying.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-request-pending'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_REQUEST_TIMEOUT,
		event: 'registry.request.timeout', messagePrefix: 'registry.request: timed out after ',
		cause: 'The remote owner did not return a reply inside the configured timeout.',
		consequence: 'The caller promise rejects while the remote operation outcome remains unknown.',
		automaticRecovery: 'None; automatic replay could duplicate the operation.',
		nextAction: 'Reconcile state or retry only through an idempotent operation.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-request-timeout'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SESSION_TARGET,
		event: 'registry.request-session.invalid-target',
		messagePrefix: 'registry.requestSession: target must be a non-empty sessionId string',
		cause: 'requestSession() received an empty or non-string application session identifier.',
		consequence: 'The request is rejected before registry or network work begins.',
		automaticRecovery: 'None; invalid input is not retried.',
		nextAction: 'Pass the canonical non-empty session ID recorded by sessionIdentify.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-session-target'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SESSION_EVENT,
		event: 'registry.request-session.invalid-event',
		messagePrefix: 'registry.requestSession: event must be a non-empty string',
		cause: 'requestSession() received an empty or non-string event name.',
		consequence: 'The request is rejected before registry or network work begins.',
		automaticRecovery: 'None; invalid input is not retried.',
		nextAction: 'Pass the non-empty client onRequest event name.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-session-event'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SESSION_OFFLINE,
		event: 'registry.request-session.offline', messagePrefix: 'registry.requestSession: target session "',
		cause: 'No live registry owner exists for the requested application session, or its socket has closed.',
		consequence: 'The request is not delivered and the caller promise rejects.',
		automaticRecovery: 'None; the registry waits for a future connection open event.',
		nextAction: 'Treat the session as offline and retry only after a new registry open event.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-session-offline'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SESSION_PENDING,
		event: 'registry.request-session.pending-cap', messagePrefix: 'registry.requestSession: pending requests exceeded ',
		cause: 'This instance reached its bounded number of session-routed requests awaiting replies.',
		consequence: 'The new request is rejected so pending memory remains bounded.',
		automaticRecovery: 'Capacity returns as existing pending requests settle.',
		nextAction: 'Apply caller backpressure and investigate slow or missing recipients before retrying.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-session-pending'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SESSION_TIMEOUT,
		event: 'registry.request-session.timeout', messagePrefix: 'registry.requestSession: timed out after ',
		cause: 'The remote session owner did not return a reply inside the configured timeout.',
		consequence: 'The caller promise rejects while the remote operation outcome remains unknown.',
		automaticRecovery: 'None; automatic replay could duplicate the operation.',
		nextAction: 'Reconcile state or retry only through an idempotent operation.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-session-timeout'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SEND_TARGET,
		event: 'registry.send.invalid-target',
		messagePrefix: 'registry.send: target must be a non-empty userId string',
		cause: 'send() received an empty or non-string user identifier.',
		consequence: 'Nothing is delivered locally or across the cluster; the call throws synchronously.',
		automaticRecovery: 'None; invalid input is not retried.',
		nextAction: 'Pass the canonical non-empty user ID recorded by the registry identify callback.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-send-target'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SEND_COALESCED_TARGET,
		event: 'registry.send-coalesced.invalid-target',
		messagePrefix: 'registry.sendCoalesced: target must be a non-empty userId string',
		cause: 'sendCoalesced() received an empty or non-string user identifier.',
		consequence: 'Nothing is coalesced locally or across the cluster; the call throws synchronously.',
		automaticRecovery: 'None; invalid input is not retried.',
		nextAction: 'Pass the canonical non-empty user ID recorded by the registry identify callback.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-send-coalesced-target'
	}),
	thrown({
		id: EXTENSIONS_ERROR_IDS.CIRCUIT_OPEN,
		event: 'breaker.circuit-open', messagePrefix: 'circuit breaker is open - backend unavailable',
		cause: 'The optional breaker crossed its failure threshold and is protecting an unhealthy backend.',
		consequence: 'Awaited operations fail fast and fire-and-forget operations are skipped.',
		automaticRecovery: 'After resetTimeout, bounded probes test the backend and the first success closes the circuit.',
		nextAction: 'Restore the backend and inspect breaker state transitions before increasing thresholds.',
		sources: ['src/shared/breaker.js'], anchor: 'extensions-err-circuit-open'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CAPABILITY_COOKIE_METRICS,
		event: 'capability-cookie.metrics-instrument.failed', component: 'runtime.capability-cookie', severity: 'error',
		problemPrefix: 'The capability-cookie metrics instrument threw; further instrument failures are not reported.',
		cause: 'The application-supplied metrics counter for capability-cookie misses threw when incremented.',
		consequence: 'Cookie validation continues unchanged. Every miss still calls the instrument, so the counter resumes if the fault clears, but later failures are silent on this instance.',
		automaticRecovery: 'None needed for validation; the instrument is called again on every miss.',
		nextAction: 'Fix the capability_cookie_misses_total counter of the registry passed as the metrics option; the error attribute carries what its inc() threw.',
		sources: ['src/capability-cookie.js'], anchor: 'extensions-err-capability-cookie-metrics'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_POOL_IDLE_CLIENT,
		event: 'postgres.pool.idle-client.failed', component: 'runtime.postgres', severity: 'error',
		problemPrefix: 'An idle Postgres pool client reported an error.',
		cause: 'A pooled connection failed while idle: the server closed it, the network dropped, authentication expired, or TLS renegotiation failed.',
		consequence: 'That connection is discarded by the pool. In-flight queries on other connections are unaffected; the next checkout opens a fresh connection.',
		automaticRecovery: 'pg.Pool reconnects on demand; nothing is retried by this package.',
		nextAction: 'Read the error attribute (connection strings are redacted) and check Postgres logs, network stability, and idle timeouts between the instance and the database.',
		sources: ['src/postgres/index.js'], anchor: 'extensions-err-postgres-pool-idle-client'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_TRANSACTION_CLIENT,
		event: 'postgres.transaction.client.failed', component: 'runtime.postgres', severity: 'error',
		problemPrefix: 'A Postgres connection checked out for a transaction reported an error.',
		cause: 'The server ended the connection, or the network dropped it, while a transaction held it: a failover or restart, idle_in_transaction_session_timeout, or pg_terminate_backend.',
		consequence: 'The statement in flight, or the next one, fails and the operation that ran the transaction rejects. The server rolls the transaction back unless the drop hit COMMIT, whose outcome is then unknown. The connection is discarded rather than returned to the pool. Reported once per connection.',
		automaticRecovery: 'pg.Pool opens a fresh connection on the next checkout. The failed operation is not retried by this package.',
		nextAction: 'The error attribute often says only that the connection terminated; the reason is in the Postgres log. Retry the failed operation if it is safe to, and after a drop during COMMIT check first whether it took effect.',
		sources: ['src/shared/pg-tx.js'], anchor: 'extensions-err-postgres-transaction-client'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_NOTIFY_PAYLOAD_NEAR_LIMIT,
		event: 'postgres.notify.payload-near-limit', component: 'runtime.postgres.notify', severity: 'warn',
		problemPrefix: 'A Postgres NOTIFY payload is approaching the 8000-byte transport limit.',
		cause: 'A notification longer than 7500 bytes arrived on the bridged channel; Postgres rejects payloads above roughly 8000 bytes at pg_notify time.',
		consequence: 'This notification is still processed. A slightly larger one makes pg_notify raise "payload string too long", which aborts the transaction that sent it: with a notify trigger, the application\'s own INSERT or UPDATE fails, and nothing reaches the bridge.',
		automaticRecovery: 'None; the limit is a server constant.',
		nextAction: 'Shrink the notified row (send an id and let clients fetch) or split the event; see the shortlink.',
		link: 'https://svti.me/pg-notify-limit',
		sources: ['src/postgres/notify.js'], anchor: 'extensions-err-postgres-notify-payload-near-limit'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_NOTIFY_PARSE,
		event: 'postgres.notify.parse.failed', component: 'runtime.postgres.notify', severity: 'warn',
		problemPrefix: 'The application-owned Postgres NOTIFY parser, or the publish of its result, threw.',
		cause: 'The custom parse() passed to createNotifyBridge threw for this payload, or publishing the result it returned threw. For a parse() failure the error attribute carries only the name and code, since the message can quote the payload; a publish failure carries its message.',
		consequence: 'The notification is dropped and counted in notify_parse_errors_total; delivery of later notifications continues.',
		automaticRecovery: 'None for the dropped notification.',
		nextAction: 'Make parse() return null for payloads it cannot handle instead of throwing, and fix the producer that emits the unexpected shape.',
		sources: ['src/postgres/notify.js'], anchor: 'extensions-err-postgres-notify-parse'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_TASKS_STATE_LISTENER,
		event: 'postgres.tasks.state-listener.failed', component: 'runtime.postgres.tasks', severity: 'warn',
		problemPrefix: 'The task state-change listener failed; task execution continues.',
		cause: 'The onStateChange callback threw synchronously or returned a promise that rejected.',
		consequence: 'The state transition itself was already committed; only the observer missed it. Later transitions still invoke the listener.',
		automaticRecovery: 'None; the listener is invoked again on the next transition.',
		nextAction: 'Fix the listener; it must not throw, and it should catch its own asynchronous failures.',
		sources: ['src/postgres/tasks.js'], anchor: 'extensions-err-postgres-tasks-state-listener'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CLOCK_SKEW_SAMPLE,
		event: 'redis.clock-skew.sample.failed', component: 'runtime.redis.clock-skew', severity: 'error',
		problemPrefix: 'A Redis clock-skew sample failed; the last good gauge value remains active.',
		cause: 'The TIME round trip to Redis threw, typically because the connection is down or the breaker is open.',
		consequence: 'The skew gauge keeps its previous value instead of reporting zero, and no trip callback fires from this round.',
		automaticRecovery: 'The next scheduled sample retries; the Redis client owns reconnection.',
		nextAction: 'Check Redis connectivity; if samples keep failing, the skew alert is blind and the clock should not be trusted for fencing decisions.',
		sources: ['src/redis/clock-skew.js'], anchor: 'extensions-err-clock-skew-sample'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CLOCK_SAMPLE,
		event: 'redis.clock.sample.failed', component: 'runtime.redis.clock', severity: 'error',
		problemPrefix: 'A Redis cluster-clock sample failed; the last good state remains active.',
		cause: 'The TIME round trip to Redis threw, typically because the connection is down or the breaker is open.',
		consequence: 'The clock keeps its previous offset and skew instead of fabricating zero; disagreement checks skip this round.',
		automaticRecovery: 'The next scheduled sample retries; the Redis client owns reconnection.',
		nextAction: 'Check Redis connectivity; repeated failures mean the cluster clock is running on a stale offset.',
		sources: ['src/redis/clock.js'], anchor: 'extensions-err-clock-sample'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CLOCK_NTP_SAMPLE,
		event: 'redis.clock.ntp-sample.failed', component: 'runtime.redis.clock', severity: 'warn',
		problemPrefix: 'The auxiliary NTP clock sample failed and was excluded from this round.',
		cause: 'The configured ntp() source threw or rejected.',
		consequence: 'This round votes with the local and Redis sources only; a stale NTP offset never keeps voting.',
		automaticRecovery: 'The next scheduled sample calls ntp() again.',
		nextAction: 'Check the NTP source the ntp option calls; the cluster clock stays healthy without it, but the third vote is missing.',
		sources: ['src/redis/clock.js'], anchor: 'extensions-err-clock-ntp-sample'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CLOCK_LEADER_OFFSET,
		event: 'redis.clock.leader-offset-read.failed', component: 'runtime.redis.clock', severity: 'warn',
		problemPrefix: 'The cluster-clock leader offset could not be published or read; the prior state remains active.',
		cause: 'On the leader, writing its offset key to Redis threw. On a follower, reading the key threw or its value did not parse.',
		consequence: 'A failing leader leaves followers on the last offset it published until the key expires. A failing follower keeps using the last offset it read for at most leaderTtlMs after that read, then stamps from its own consistent() clock until a read succeeds.',
		automaticRecovery: 'The next scheduled sample publishes or reads the key again.',
		nextAction: 'Check Redis connectivity and confirm the leader instance is alive and publishing.',
		sources: ['src/redis/clock.js'], anchor: 'extensions-err-clock-leader-offset'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CURSOR_SUBSCRIBER,
		event: 'redis.cursor.subscriber.failed', component: 'runtime.redis.cursor', severity: 'error',
		problemPrefix: 'The Redis cursor subscriber connection reported an error.',
		cause: 'The dedicated pub/sub connection that receives remote cursor updates emitted an error.',
		consequence: 'Remote cursor positions may be delayed or missed on this instance while the subscriber is unhealthy.',
		automaticRecovery: REDIS_RECONNECTS,
		nextAction: 'Inspect the Redis error and verify cursor propagation after reconnection.',
		sources: ['src/redis/cursor/redis-io.js'], anchor: 'extensions-err-cursor-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CURSOR_CLEANUP,
		event: 'redis.cursor.cleanup.failed', component: 'runtime.redis.cursor', severity: 'warn',
		problemPrefix: 'Cursor stale-entry cleanup failed; the next sweep retries.',
		cause: 'The cleanup Lua script threw for at least one active topic in a sweep. The failed attribute counts those topics out of the topics attribute, and the error attribute is the first failure.',
		consequence: 'Stale cursor entries on the failed topics linger until a later sweep succeeds; live updates are unaffected. Reported once per failure streak, and once more if the streak narrows from every topic failing to only some; a sweep with no failure re-arms it.',
		automaticRecovery: 'The periodic sweep runs the script again on every active topic.',
		nextAction: 'Check Redis health; failed equal to topics usually means Redis is unreachable. Topics are not logged, since they can carry user ids; if only some topics keep failing, scan the cursor hashes for one with a corrupt entry.',
		sources: ['src/redis/cursor/redis-io.js'], anchor: 'extensions-err-cursor-cleanup'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.GROUPS_META_INITIALIZE,
		event: 'redis.groups.meta-initialize.failed', component: 'runtime.redis.groups', severity: 'warn',
		problemPrefix: 'The initial Redis group metadata write failed.',
		cause: 'Writing the group meta hash at construction threw.',
		consequence: 'getMeta() on this instance returns the initial in-memory metadata until a later write succeeds; other instances do not see it yet.',
		automaticRecovery: 'The next getMeta() or setMeta() call retries the write.',
		nextAction: 'Check Redis connectivity; metadata converges once a write succeeds.',
		sources: ['src/redis/groups.js'], anchor: 'extensions-err-groups-meta-initialize'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.GROUPS_HEARTBEAT,
		event: 'redis.groups.heartbeat.failed', component: 'runtime.redis.groups', severity: 'warn',
		problemPrefix: 'A Redis group heartbeat pipeline failed.',
		cause: 'The periodic pipeline that refreshes member timestamps and key expiry was rejected, or Redis refused one of its commands (READONLY after a failover, OOM, NOPERM).',
		consequence: 'If heartbeats keep failing past the member TTL, other instances treat this instance\'s members as gone and the group keys may expire. Reported once per failure streak per group; a successful heartbeat re-arms it.',
		automaticRecovery: 'The next heartbeat tick retries; a single failure inside the TTL window has no visible effect.',
		nextAction: 'Check Redis connectivity; sustained failures on one instance mean its members are being evicted cluster-wide.',
		sources: ['src/redis/groups.js'], anchor: 'extensions-err-groups-heartbeat'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.GROUPS_SUBSCRIBER,
		event: 'redis.groups.subscriber.failed', component: 'runtime.redis.groups', severity: 'error',
		problemPrefix: 'The Redis group subscriber connection reported an error.',
		cause: 'The dedicated pub/sub connection that receives cross-instance group events emitted an error.',
		consequence: 'Cross-instance group events may be delayed or missed on this instance while the subscriber is unhealthy.',
		automaticRecovery: REDIS_RECONNECTS,
		nextAction: 'Inspect the Redis error and connection health, then verify group event delivery.',
		sources: ['src/redis/groups.js'], anchor: 'extensions-err-groups-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.LEADER_TICK,
		event: 'redis.leader.tick.failed', component: 'runtime.redis.leader', severity: 'error',
		problemPrefix: 'A Redis leader election tick failed; leadership is fail-closed.',
		cause: 'Acquiring or renewing the leader lease threw, typically because Redis is unreachable or the breaker is open.',
		consequence: 'A current leader steps down on the failed renewal being reported rather than assuming it still holds the lease; a candidate does not acquire. If the failure is local to this instance, another instance acquires the lease once it expires; if every instance fails, leader-gated work pauses until a tick succeeds.',
		automaticRecovery: 'The next tick retries; the Redis client owns reconnection.',
		nextAction: 'Check Redis connectivity. The operation attribute says whether the failing step was acquire or renew.',
		sources: ['src/redis/leader.js'], anchor: 'extensions-err-leader-tick'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.LEADER_READ_CURRENT,
		event: 'redis.leader.read-current.failed', component: 'runtime.redis.leader', severity: 'warn',
		problemPrefix: 'The current Redis leader could not be read.',
		cause: 'currentLeader() could not read the lease key.',
		consequence: 'The caller receives null for this call; election itself is unaffected.',
		automaticRecovery: 'None; the caller decides whether to retry.',
		nextAction: 'Check Redis connectivity if this repeats.',
		sources: ['src/redis/leader.js'], anchor: 'extensions-err-leader-read-current'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.LEADER_RELEASE,
		event: 'redis.leader.release.failed', component: 'runtime.redis.leader', severity: 'warn',
		problemPrefix: 'The Redis leader lease could not be released; it expires by TTL.',
		cause: 'The compare-and-delete release on stop() threw.',
		consequence: 'No other instance can become leader until the lease TTL elapses; the stopping instance already treats itself as a non-leader.',
		automaticRecovery: 'The lease expires on its own; no other instance can accidentally release someone else\'s lease.',
		nextAction: 'Nothing unless the failover delay is unacceptable; then check Redis connectivity during shutdown.',
		sources: ['src/redis/leader.js'], anchor: 'extensions-err-leader-release'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBER,
		event: 'redis.presence.keyspace-subscriber.failed', component: 'runtime.redis.presence', severity: 'error',
		problemPrefix: 'A presence keyspace-notification subscriber connection reported an error.',
		cause: 'A per-master connection subscribed to key-deletion events, which the presence cleanup of expired topics listens on, emitted an error after its subscription stood. An error before that is reported as EXTENSIONS-ERR-PRESENCE-KEYSPACE-SUBSCRIBE instead.',
		consequence: 'Presence expiry deletions from that master may be missed until reconciliation repairs the subscriber set.',
		automaticRecovery: 'A sweep of the masters runs at once. These connections do not reconnect: one that ended is replaced by a new connection to its master.',
		nextAction: 'Inspect the Redis error and confirm reconciliation restores expired presence state.',
		sources: ['src/redis/presence/subscriber.js'], anchor: 'extensions-err-presence-keyspace-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBE,
		event: 'redis.presence.keyspace-subscribe.failed', component: 'runtime.redis.presence', severity: 'warn',
		problemPrefix: 'The presence keyspace cleanup could not subscribe to key-deletion events.',
		cause: 'psubscribe to __keyevent@*__:del failed: an ACL denied the user PSUBSCRIBE or that channel pattern (NOPERM), or the connection failed or dropped during the call, in which case the error is the one the connection raised (a refused login, for example). Redis accepts the subscription even when keyspace notifications are disabled, so a disabled notify-keyspace-events setting never produces this event; the subscription then succeeds and receives nothing, and EXTENSIONS-ERR-PRESENCE-KEYSPACE-EVENTS-OFF reports the setting where the server lets it be read.',
		consequence: 'This instance is not told when a topic\'s presence hash is deleted because the last instance presenting it stopped heartbeating, so its sync-only observers keep showing the last roster until new presence traffic or a reload.',
		automaticRecovery: 'On a cluster the master is subscribed again with a backoff from 1 to 30 seconds, and at once whenever the topology changes, and each kind of failure is reported once per streak per master. On standalone Redis the subscription is attempted again only when the presence subscriber connection is set up again.',
		nextAction: 'For NOPERM, grant the Redis user +psubscribe and the pattern &__keyevent@*__:del; otherwise check connectivity to the server (on a cluster, the master attribute names it). On a cluster the next retry subscribes again; on standalone Redis restart the instance. For the cleanup to receive anything once subscribed, notify-keyspace-events must include E and g (for example Eg) on every master; see the shortlink.',
		link: 'https://svti.me/redis-keyspace',
		sources: ['src/redis/presence/subscriber.js'], anchor: 'extensions-err-presence-keyspace-subscribe'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_EVENTS_OFF,
		event: 'redis.presence.keyspace-events.off', component: 'runtime.redis.presence', severity: 'warn',
		problemPrefix: 'A Redis server\'s keyspace notification setting cannot deliver the key deletions the presence cleanup listens for.',
		cause: 'notify-keyspace-events on that server lacks E (keyevent notifications) or g (generic commands, also included in A); the flags attribute carries its value. Redis accepts the subscription regardless, so nothing else reports this.',
		consequence: 'The keyspace cleanup never fires for topics whose presence hash lives on that server: when the last instance presenting a topic dies, its sync-only observers keep showing the last roster until new presence traffic or a reload.',
		automaticRecovery: 'None; this is server configuration. The setting is read when the subscription is set up, so a corrected server is checked again the next time it is.',
		nextAction: 'Run CONFIG SET notify-keyspace-events Eg on every master (or add E and g to the flags it already has), and set it in redis.conf so a restart keeps it; see the shortlink.',
		link: 'https://svti.me/redis-keyspace',
		sources: ['src/redis/presence/subscriber.js'], anchor: 'extensions-err-presence-keyspace-events-off'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_RECONCILE,
		event: 'redis.presence.keyspace-reconcile.failed', component: 'runtime.redis.presence', severity: 'error',
		problemPrefix: 'Presence keyspace subscriber reconciliation failed; it is retried with a backoff.',
		cause: 'Re-reading the cluster master set or re-subscribing a master threw, or a master did not answer CLUSTER MYID.',
		consequence: 'Masters added by a failover may not have a keyspace subscriber yet, so expiry cleanup from them lags.',
		automaticRecovery: 'Reconciliation reruns after a backoff from 1 to 30 seconds, and at once on the next topology event or subscriber error; a master ioredis dropped from its node list is swept again once a refreshed slot map lists it. A failed sweep is reported once per failure streak, a master that does not answer once until it answers or the slot map drops it.',
		nextAction: 'Check cluster health; if it persists, inspect connectivity to individual masters, and that the Redis user may run CLUSTER MYID (+cluster|myid).',
		sources: ['src/redis/presence/subscriber.js'], anchor: 'extensions-err-presence-keyspace-reconcile'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PRESENCE_SUBSCRIBER,
		event: 'redis.presence.subscriber.failed', component: 'runtime.redis.presence', severity: 'error',
		problemPrefix: 'The Redis presence subscriber connection reported an error.',
		cause: 'The dedicated pub/sub connection that receives cross-instance presence events emitted an error.',
		consequence: 'Cross-instance presence diffs may be delayed or missed on this instance while the subscriber is unhealthy.',
		automaticRecovery: REDIS_RECONNECTS,
		nextAction: 'Inspect Redis connectivity and verify presence diffs resume.',
		sources: ['src/redis/presence/subscriber.js'], anchor: 'extensions-err-presence-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PUBLISH_RATE_SUBSCRIBER,
		event: 'redis.publish-rate.subscriber.failed', component: 'runtime.redis.publish-rate', severity: 'error',
		problemPrefix: 'The cluster publish-rate subscriber connection reported an error.',
		cause: 'The dedicated pub/sub connection that receives remote rate samples emitted an error.',
		consequence: 'Cluster-wide rate views go stale and admission decisions lose remote samples until the subscriber recovers.',
		automaticRecovery: 'The Redis client owns reconnection; stale remote samples age out.',
		nextAction: 'Inspect Redis connectivity and verify remote rate samples resume.',
		sources: ['src/redis/publish-rate.js'], anchor: 'extensions-err-publish-rate-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PUBLISH_RATE_LISTENER,
		event: 'redis.publish-rate.listener.failed', component: 'runtime.redis.publish-rate', severity: 'error',
		problemPrefix: 'A publish-rate threshold listener threw; aggregation continues.',
		cause: 'An onPublishRate callback threw while being told about crossings.',
		consequence: 'That listener missed this set of crossings; other listeners and the aggregation loop are unaffected.',
		automaticRecovery: 'None; the listener is invoked again on the next crossing.',
		nextAction: 'Fix the listener so it does not throw.',
		sources: ['src/redis/publish-rate.js'], anchor: 'extensions-err-publish-rate-listener'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.RATELIMIT_PROXY_COLLAPSE,
		event: 'redis.ratelimit.proxy-collapse-risk', component: 'runtime.redis.ratelimit', severity: 'warn',
		problemPrefix: 'A private or loopback rate-limit key was denied while no trusted address header is configured.',
		cause: 'keyBy is ip (the default), no ADDRESS_HEADER is set, and a denied bucket key was a private or loopback address, which is what every client looks like behind an unconfigured reverse proxy.',
		consequence: 'All clients share one bucket: one busy client rate-limits everyone. Reported once per limiter; the address itself is never printed.',
		automaticRecovery: 'None; this is configuration.',
		nextAction: 'Configure ADDRESS_HEADER and XFF_DEPTH on the adapter so the real client address is resolved, or pass an explicit keyBy resolver.',
		sources: ['src/redis/ratelimit.js'], anchor: 'extensions-err-ratelimit-proxy-collapse'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_SUBSCRIBER,
		event: 'redis.registry.subscriber.failed', component: 'runtime.redis.registry', severity: 'error',
		problemPrefix: 'The connection-registry subscriber connection reported an error.',
		cause: 'The dedicated pub/sub connection that receives cross-instance requests and replies emitted an error.',
		consequence: 'Cross-instance requests and replies may be delayed or lost while the subscriber is unhealthy; pending requests still time out normally.',
		automaticRecovery: 'The Redis client owns reconnection; pending requests remain bounded by timeout.',
		nextAction: 'Inspect Redis connectivity and verify cross-instance request/reply delivery.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER,
		event: 'redis.sharded-pubsub.subscriber.failed', component: 'runtime.redis.sharded-pubsub', severity: 'error',
		problemPrefix: 'A sharded pub/sub subscriber connection reported an error.',
		cause: 'A shard-channel subscriber emitted an error, commonly the server-initiated unsubscribe during a slot migration.',
		consequence: 'Delivery on the affected shard channels is interrupted until ownership is refreshed and the subscriptions are restored.',
		automaticRecovery: 'The bus marks the slot map stale, refreshes ownership, and re-subscribes wanted channels.',
		nextAction: 'Inspect the Redis error and verify the topology refresh and shard delivery complete.',
		sources: ['src/redis/sharded-pubsub.js'], anchor: 'extensions-err-sharded-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE,
		event: 'redis.sharded-pubsub.channel-reconcile.failed', component: 'runtime.redis.sharded-pubsub', severity: 'error',
		problemPrefix: 'A sharded pub/sub channel could not be re-subscribed on its new owner.',
		cause: 'SSUBSCRIBE on the channel\'s new slot owner threw or timed out after a reshard, or the slot had no owner in the cluster\'s slot map. The channel is not logged, since it is built from a topic; the slot attribute identifies the first failed one and the failed attribute counts them.',
		consequence: 'Messages on those channels are missed on this instance until a retry re-subscribes them; the other moved channels were handled. Reported once per failure streak; a reconciliation that completes re-arms it.',
		automaticRecovery: 'Yes: the reconciliation is retried after 1 s, then with the delay doubling up to 30 s, until every moved channel is subscribed on its owner. A subscriber error retries at once.',
		nextAction: 'Check the health of the node that owns the slot attribute (CLUSTER SHARDS or CLUSTER SLOTS), and for NOPERM the Redis user\'s channel permissions. Delivery resumes on its own once the node accepts the subscription.',
		sources: ['src/redis/sharded-pubsub.js'], anchor: 'extensions-err-sharded-channel-reconcile'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SHARDED_RECONCILE,
		event: 'redis.sharded-pubsub.reconcile.failed', component: 'runtime.redis.sharded-pubsub', severity: 'error',
		problemPrefix: 'Sharded pub/sub reconciliation could not refresh slot ownership.',
		cause: 'Reading CLUSTER SLOTS, or a master\'s CLUSTER MYID, threw while reconciling after a subscriber error or on a retry.',
		consequence: 'Channels that moved during the reshard stay unsubscribed on this instance until a reconciliation succeeds. Reported once per failure streak; a reconciliation that completes re-arms it.',
		automaticRecovery: 'Yes: the reconciliation is retried after 1 s, then with the delay doubling up to 30 s, until it completes. A subscriber error retries at once.',
		nextAction: 'Check cluster health and run CLUSTER SLOTS from the instance; delivery resumes on its own once the slot map can be read.',
		sources: ['src/redis/sharded-pubsub.js'], anchor: 'extensions-err-sharded-reconcile'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PUBSUB_TRACE_OMITTED,
		event: 'redis.pubsub.trace-context.omitted', component: 'runtime.redis.pubsub', severity: 'warn',
		problemPrefix: 'Trace context was omitted from a relay envelope that would otherwise exceed maxEnvelopeBytes.',
		cause: 'The application payload alone fits under maxEnvelopeBytes, but adding the traceparent and tracestate fields would push the envelope over it, and receiving instances drop oversized envelopes before parsing.',
		consequence: 'The message is relayed and delivered without tracing fields; the consumer operation on other instances starts a new trace instead of continuing this one. Reported once per bus.',
		automaticRecovery: 'None; each affected envelope is relayed untraced.',
		nextAction: 'Raise maxEnvelopeBytes on every instance of the bus, or shrink payloads that sit within a few hundred bytes of the cap.',
		sources: ['src/redis/pubsub.js'], anchor: 'extensions-err-pubsub-trace-omitted'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SHARDED_TRACE_OMITTED,
		event: 'redis.sharded-pubsub.trace-context.omitted', component: 'runtime.redis.sharded-pubsub', severity: 'warn',
		problemPrefix: 'Trace context was omitted from a shard relay envelope that would otherwise exceed maxEnvelopeBytes.',
		cause: 'The application payload alone fits under maxEnvelopeBytes, but adding the traceparent and tracestate fields would push the envelope over it, and receiving instances drop oversized envelopes before parsing.',
		consequence: 'The message is relayed and delivered without tracing fields; the consumer operation on other instances starts a new trace instead of continuing this one. Reported once per bus.',
		automaticRecovery: 'None; each affected envelope is relayed untraced.',
		nextAction: 'Raise maxEnvelopeBytes on every instance of the bus, or shrink payloads that sit within a few hundred bytes of the cap.',
		sources: ['src/redis/sharded-pubsub.js'], anchor: 'extensions-err-sharded-trace-omitted'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REDIS_REPLAY_STORAGE_FALLBACK,
		event: 'redis.replay.storage-fallback', component: 'runtime.redis.replay', severity: 'warn',
		problemPrefix: 'Replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.',
		cause: 'A replay publish could not write to Redis (unreachable, breaker open, or the publish script failed) and the buffer was created with localFanoutOnStorageFailure.',
		consequence: 'Subscribers connected now receive the message, but it is not in the replay buffer, so a client that reconnects and resumes cannot recover it. Only the first fallback is reported; replay_storage_fallbacks_total counts every one.',
		automaticRecovery: 'Yes for later publishes: each one tries the store first and is stored again once Redis answers. Messages that fell back are not backfilled.',
		nextAction: 'Check Redis connectivity and the breaker, then read replay_storage_fallbacks_total for how many publishes went unstored. The requestId attribute correlates the first one with its request.',
		sources: ['src/redis/replay.js'], anchor: 'extensions-err-redis-replay-storage-fallback'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REDIS_REPLAY_STREAM_STORAGE_FALLBACK,
		event: 'redis.replay-stream.storage-fallback', component: 'runtime.redis.replay-stream', severity: 'warn',
		problemPrefix: 'Stream replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.',
		cause: 'A stream replay publish could not write to Redis (unreachable, breaker open, or the publish script failed) and the buffer was created with localFanoutOnStorageFailure.',
		consequence: 'Subscribers connected now receive the message, but it is not in the stream, so a client that reconnects and resumes cannot recover it. Only the first fallback is reported; replay_storage_fallbacks_total counts every one.',
		automaticRecovery: 'Yes for later publishes: each one tries the store first and is stored again once Redis answers. Messages that fell back are not backfilled.',
		nextAction: 'Check Redis connectivity and the breaker, then read replay_storage_fallbacks_total for how many publishes went unstored. The requestId attribute correlates the first one with its request.',
		sources: ['src/redis/replay-stream.js'], anchor: 'extensions-err-redis-replay-stream-storage-fallback'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_REPLAY_STORAGE_FALLBACK,
		event: 'postgres.replay.storage-fallback', component: 'runtime.postgres.replay', severity: 'warn',
		problemPrefix: 'Replay storage failed; the publish went to live subscribers without a replay sequence, so durability is degraded.',
		cause: 'A replay publish could not write to Postgres (unreachable, breaker open, or the insert failed) and the buffer was created with localFanoutOnStorageFailure.',
		consequence: 'Subscribers connected now receive the message, but it is not in the replay table, so a client that reconnects and resumes cannot recover it. Only the first fallback is reported; replay_storage_fallbacks_total counts every one.',
		automaticRecovery: 'Yes for later publishes: each one tries the store first and is stored again once Postgres answers. Messages that fell back are not backfilled.',
		nextAction: 'Check Postgres connectivity and the breaker, then read replay_storage_fallbacks_total for how many publishes went unstored. The requestId attribute correlates the first one with its request.',
		sources: ['src/postgres/replay.js'], anchor: 'extensions-err-postgres-replay-storage-fallback'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.RATELIMIT_STORE_FLOOR,
		event: 'redis.ratelimit.store-unreachable', component: 'runtime.redis.ratelimit', severity: 'warn',
		problemPrefix: 'The rate-limit store is unreachable; verdicts are decided on the in-process floor.',
		cause: 'A rate-limit decision could not reach Redis, and the limiter was created with an in-process floor, so this instance decided on its own.',
		consequence: 'Limits hold per instance while degraded: with N instances a client can be allowed up to N times the floor budget. Reported once per limiter; ratelimit_storage_fallbacks_total counts every local verdict.',
		automaticRecovery: 'Yes: every decision tries Redis first, so cluster-wide limiting resumes as soon as the store answers.',
		nextAction: 'Check Redis connectivity and the breaker. If per-instance limits are not acceptable for this route, create the limiter without a floor so an unreachable store fails the call instead.',
		sources: ['src/redis/ratelimit.js'], anchor: 'extensions-err-ratelimit-store-floor'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.COMPOSITE_RATELIMIT_STORE_FLOOR,
		event: 'redis.composite-ratelimit.store-unreachable', component: 'runtime.redis.composite-ratelimit', severity: 'warn',
		problemPrefix: 'The composite rate-limit store is unreachable; verdicts are decided on the in-process floor.',
		cause: 'A composite rate-limit decision could not reach Redis, and the limiter was created with an in-process floor, so this instance decided on its own.',
		consequence: 'Limits hold per instance while degraded: with N instances a client can be allowed up to N times each floor budget. Reported once per limiter; ratelimit_composite_storage_fallbacks_total counts every local verdict.',
		automaticRecovery: 'Yes: every decision tries Redis first, so cluster-wide limiting resumes as soon as the store answers.',
		nextAction: 'Check Redis connectivity and the breaker. If per-instance limits are not acceptable for this route, create the limiter without a floor so an unreachable store fails the call instead.',
		sources: ['src/redis/composite-ratelimit.js'], anchor: 'extensions-err-composite-ratelimit-store-floor'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PRESENCE_ENVELOPE_OVERSIZED,
		event: 'redis.presence.envelope-oversized', component: 'runtime.redis.presence', severity: 'warn',
		problemPrefix: 'A presence event envelope exceeds maxEnvelopeBytes and was not relayed to other instances.',
		cause: 'A presence event (join, leave, or update) serialized larger than maxEnvelopeBytes, the bound every peer enforces before it parses an envelope. The event and size are in the attributes.',
		consequence: 'The event applied on this instance only. Other instances never received it, so their rosters are missing this change: a partial roster rather than an error anywhere else.',
		automaticRecovery: 'None for this event. A later event for the same entry that fits under the bound is relayed normally.',
		nextAction: 'Shrink the presence data - select fewer or smaller fields - or raise maxEnvelopeBytes on every instance together.',
		sources: ['src/redis/presence.js'], anchor: 'extensions-err-presence-envelope-oversized'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PUBLISH_RATE_REMOTE_CAP,
		event: 'redis.publish-rate.remote-instances-capped', component: 'runtime.redis.publish-rate', severity: 'warn',
		problemPrefix: 'The publish-rate aggregator reached its sibling-instance bound; stale entries are evicted and envelopes past the bound are dropped.',
		cause: 'The aggregator is tracking as many sibling instances as its bound allows (the maxRemoteInstances attribute).',
		consequence: 'Cluster-wide publish rates can under-count while the bound holds, because evicted and dropped instances are missing from the sum. Reported once per aggregator.',
		automaticRecovery: 'Partial: an evicted instance is counted again once it publishes, if a slot is free.',
		nextAction: 'A cluster this large usually means a deployment misconfiguration or instances that exit without deactivating. Read cluster_publish_rate_remote_evicted_total and cluster_publish_rate_remote_dropped_total.',
		sources: ['src/redis/publish-rate.js'], anchor: 'extensions-err-publish-rate-remote-cap', link: 'https://svti.me/publish-rate-aggregator'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_INDEX_BUCKET,
		event: 'redis.registry.index-bucket.large', component: 'runtime.redis.registry', severity: 'warn',
		problemPrefix: 'A registry secondary-index bucket is very large and keeps growing.',
		cause: 'One value of an indexed attribute (named in the attributes) is shared by more userIds than the per-bucket threshold.',
		consequence: 'Nothing is dropped - evicting from the index would make sendTo miss recipients - so the bucket and its memory keep growing. Reported once per attribute.',
		automaticRecovery: 'None; the bucket shrinks as those users disconnect.',
		nextAction: 'Index only the attributes sendTo filters on. A broad attribute that almost every user shares, such as a default role, makes one huge bucket without narrowing any sendTo.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-index-bucket', link: 'https://svti.me/registry-index'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REGISTRY_USER_INDEX,
		event: 'redis.registry.user-index.large', component: 'runtime.redis.registry', severity: 'warn',
		problemPrefix: 'The cluster-wide registry user index is very large and keeps growing.',
		cause: 'The user index, kept on every instance, reached its size threshold.',
		consequence: 'Nothing is evicted - eviction would mis-route sendTo - so memory keeps growing: roughly 32 bytes of map state per entry on every instance. Reported once.',
		automaticRecovery: 'None; the index shrinks as users disconnect.',
		nextAction: 'Stop registering identities that are unique per connection (anonymous or random ids), or split the cluster.',
		sources: ['src/redis/registry.js'], anchor: 'extensions-err-registry-user-index', link: 'https://svti.me/registry-index'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PUBSUB_RELAY_BURST,
		event: 'redis.pubsub.relay-batch.burst', component: 'runtime.redis.pubsub', severity: 'warn',
		problemPrefix: 'One event-loop iteration queued more relay publishes than the per-tick bound.',
		cause: 'Code published in a tight synchronous loop without yielding, so the whole burst landed in one tick.',
		consequence: 'Everything is still relayed, as one pipelined batch at the end of the iteration; memory and latency spike for that tick. Reported once per bus.',
		automaticRecovery: 'Yes; the batch drains every tick.',
		nextAction: 'Find the publish-in-loop and yield between chunks, or send the list as one envelope with publishBatched.',
		sources: ['src/redis/pubsub.js'], anchor: 'extensions-err-pubsub-relay-burst', link: 'https://svti.me/pubsub-burst'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SHARDED_RELAY_BURST,
		event: 'redis.sharded-pubsub.relay-batch.channel-burst', component: 'runtime.redis.sharded-pubsub', severity: 'warn',
		problemPrefix: 'One event-loop iteration queued publishes for more distinct shard channels than the per-tick bound.',
		cause: 'One synchronous burst published to that many distinct topics, which usually means a topic name built from a value that is unique per message.',
		consequence: 'Everything is still published; the tick batch grows with the number of channels. Reported once per bus.',
		automaticRecovery: 'Yes; the batch drains every tick.',
		nextAction: 'Look for a topic-cardinality leak: a topic name that embeds a per-message id instead of a per-room one.',
		sources: ['src/redis/sharded-pubsub.js'], anchor: 'extensions-err-sharded-relay-burst', link: 'https://svti.me/sharded-bus-burst'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REDIS_DUPLICATES,
		event: 'redis.client.duplicates.excessive', component: 'runtime.redis.client', severity: 'warn',
		problemPrefix: 'One Redis client has spawned more duplicate connections than a process should need.',
		cause: 'Modules that each take their own subscriber connection were constructed many times, usually per request or per connection instead of once at startup.',
		consequence: 'Every duplicate is a real socket to Redis, so a leak ends at the server connection limit. Reported once per client.',
		automaticRecovery: 'Duplicates are released as they end; the leak itself continues until the code changes.',
		nextAction: 'Find the module constructed repeatedly without a matching deactivate or destroy, and construct it once at startup.',
		sources: ['src/redis/index.js'], anchor: 'extensions-err-redis-duplicates', link: 'https://svti.me/redis-duplicates'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SMOOTH_OWNER_FALLBACK,
		event: 'redis.smooth.relay-owner.unresolved', component: 'runtime.redis.smooth', severity: 'warn',
		problemPrefix: 'The smooth relay channel owner could not be resolved; publishes are pinned to a fallback master.',
		cause: 'The cluster did not report which master owns the relay channel, typically while the topology is changing.',
		consequence: 'Publishes go through the first master so broadcast order is kept; delivery continues. Reported once per relay.',
		automaticRecovery: 'Delivery continues through the fallback master without intervention.',
		nextAction: 'None if the cluster was resharding. On a stable cluster, check that every master is reachable from this instance.',
		sources: ['src/redis/smooth-cluster.js'], anchor: 'extensions-err-smooth-owner-fallback'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.RESUME_TOPICS_TRUNCATED,
		event: 'replay.resume.topics-truncated', component: 'runtime.replay', severity: 'warn',
		problemPrefix: 'A resume request presented more topics than the per-request bound; the remainder were not resumed.',
		cause: 'A client asked to resume more topics in one request than the accepted attribute allows.',
		consequence: 'The first topics up to the bound were resumed; the rest were neither inspected nor resumed, so they start without replay. Reported at most once per interval (intervalMs attribute) per store.',
		automaticRecovery: 'None for that request.',
		nextAction: 'Give the client fewer topics per connection, or spread its subscriptions across connections.',
		sources: ['src/shared/replay-helpers.js'], anchor: 'extensions-err-resume-topics-truncated'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PROMETHEUS_SERIES_CAP,
		event: 'prometheus.series.cap-reached', component: 'runtime.prometheus', severity: 'warn',
		problemPrefix: 'A metric reached its maxSeries cap; new label sets for it are dropped.',
		cause: 'The metric named in the attributes has as many distinct label sets as its maxSeries allows.',
		consequence: 'Existing series keep updating; observations for any new label set are dropped. prometheus_series_dropped_total counts the drops. Reported once per metric.',
		automaticRecovery: 'None while the cap holds.',
		nextAction: 'Raise maxSeries if the cardinality is legitimate, or pass mapTopic so topic labels stay bounded.',
		sources: ['src/prometheus/index.js'], anchor: 'extensions-err-prometheus-series-cap'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_TASKS_SWEEP,
		event: 'postgres.tasks.sweep.failed', component: 'runtime.postgres.tasks', severity: 'error',
		problemPrefix: 'A background task sweep failed.',
		cause: 'The dispatch, recovery, or cleanup sweep (named in the sweep attribute) threw: Postgres unreachable, or a schema mismatch such as a table managed with autoMigrate: false that lacks a column this release reads.',
		consequence: 'That sweep did nothing: pending tasks are not dispatched, stuck ones are not reclaimed, or terminal rows are not deleted. Reported once per failure streak per sweep.',
		automaticRecovery: 'Yes: every interval retries, and the first success resumes normal work and re-arms this report.',
		nextAction: 'Read the error attribute. A missing-column error means the table needs the ALTER TABLE statements from the migration guide; otherwise check Postgres connectivity and the breaker.',
		sources: ['src/postgres/tasks.js'], anchor: 'extensions-err-postgres-tasks-sweep'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REDIS_CLIENT,
		event: 'redis.client.failed', component: 'runtime.redis.client', severity: 'error',
		problemPrefix: 'The Redis client created by createRedisClient reported an error.',
		cause: 'The main ioredis connection emitted an error: the server is unreachable, refused the connection, or dropped it.',
		consequence: 'Commands on this client fail or queue until the connection is restored; modules report their own failures on top of this one.',
		automaticRecovery: REDIS_RECONNECTS,
		nextAction: 'Check Redis reachability and credentials; the error attribute carries the redacted cause.',
		sources: ['src/redis/index.js'], anchor: 'extensions-err-redis-client'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.REDIS_RESUBSCRIBE,
		event: 'redis.subscriber.resubscribe.failed', component: 'runtime.redis.subscriber', severity: 'error',
		problemPrefix: 'Redis refused a subscription restored after a reconnect.',
		cause: 'A subscriber connection reconnected, or on a Redis Cluster its subscriptions moved to a new node connection after the previous one ended, and Redis refused the SUBSCRIBE, PSUBSCRIBE or SSUBSCRIBE that restores one of its channels, for example NOPERM after the Redis user lost access to the channel, which is itself what makes Redis close a subscribed connection.',
		consequence: 'Messages on the refused channels do not reach this instance, while the module that owns the connection still treats them as subscribed. The owner attribute names that module, kind the subscription type of the first refused channel and refused how many channels were refused; channel names are not logged. Reported once per failure streak per subscriber.',
		automaticRecovery: 'Refused channels are retried with a backoff from 1 to 30 seconds while the connection stays up, and again after every reconnect or move to a new cluster node connection, until Redis accepts them or the module releases them. Channels Redis accepts are restored at once.',
		nextAction: 'Grant the Redis user the channels this package subscribes to, or restore the server configuration that changed; the next retry restores the subscription.',
		sources: ['src/shared/redis-subscriber.js'], anchor: 'extensions-err-redis-resubscribe'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.PUBSUB_SUBSCRIBER,
		event: 'redis.pubsub.subscriber.failed', component: 'runtime.redis.pubsub', severity: 'error',
		problemPrefix: 'The pub/sub bus subscriber connection reported an error.',
		cause: 'The dedicated connection that receives relayed publishes from other instances emitted an error.',
		consequence: 'Publishes from other instances are not delivered locally while the subscriber is down; local publishes are unaffected.',
		automaticRecovery: REDIS_RECONNECTS,
		nextAction: 'Inspect Redis connectivity and verify cross-instance delivery resumes.',
		sources: ['src/redis/pubsub.js'], anchor: 'extensions-err-pubsub-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CRDT_SUBSCRIBER,
		event: 'redis.crdt.subscriber.failed', component: 'runtime.redis.crdt', severity: 'error',
		problemPrefix: 'The CRDT relay subscriber connection reported an error.',
		cause: 'The dedicated connection that receives CRDT updates from other instances emitted an error.',
		consequence: 'Updates from other instances are not applied locally while the subscriber is down, and they are not replayed when it reconnects: the relay keeps no backlog, and a replica syncs from its peers only when its document is loaded on this instance.',
		automaticRecovery: REDIS_RECONNECTS + ' Updates missed during the outage are not recovered automatically.',
		nextAction: 'Restore Redis connectivity. Documents that received remote updates during the outage stay diverged on this instance until they are loaded again, which syncs them from live peers: after every local editor of the document has left, or after a restart.',
		sources: ['src/redis/crdt.js'], anchor: 'extensions-err-crdt-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SMOOTH_SUBSCRIBER,
		event: 'redis.smooth.subscriber.failed', component: 'runtime.redis.smooth', severity: 'error',
		problemPrefix: 'The smooth relay subscriber connection reported an error.',
		cause: 'The dedicated connection that receives smooth relay commands, broadcasts and sync replies from other instances emitted an error.',
		consequence: 'Relay traffic from other instances is not received while the subscriber is down.',
		automaticRecovery: REDIS_RECONNECTS,
		nextAction: 'Inspect Redis connectivity and verify relay traffic resumes.',
		sources: ['src/redis/smooth-cluster.js'], anchor: 'extensions-err-smooth-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.TOPIC_BROADCAST_SUBSCRIBER,
		event: 'redis.topic-broadcast.subscriber.failed', component: 'runtime.redis.topic-broadcast', severity: 'error',
		problemPrefix: 'The topic-broadcast subscriber connection reported an error.',
		cause: 'The dedicated connection that receives broadcasts from other instances emitted an error.',
		consequence: 'Broadcasts from other instances are not delivered locally while the subscriber is down.',
		automaticRecovery: REDIS_RECONNECTS,
		nextAction: 'Inspect Redis connectivity and verify cross-instance broadcasts resume.',
		sources: ['src/redis/topic-broadcast.js'], anchor: 'extensions-err-topic-broadcast-subscriber'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CRDT_SUBSCRIBE,
		event: 'redis.crdt.subscribe.failed', component: 'runtime.redis.crdt', severity: 'error',
		problemPrefix: 'The CRDT relay could not subscribe to its channel.',
		cause: 'SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.',
		consequence: 'Updates and sync requests from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak; an onError option, when set, still sees every failure.',
		automaticRecovery: 'The connection is closed and the next relay subscribes on a fresh one; a subscribe that succeeds re-arms this report.',
		nextAction: 'Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.',
		sources: ['src/redis/crdt.js'], anchor: 'extensions-err-crdt-subscribe'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SMOOTH_SUBSCRIBE,
		event: 'redis.smooth.subscribe.failed', component: 'runtime.redis.smooth', severity: 'error',
		problemPrefix: 'The smooth relay could not subscribe to its channel.',
		cause: 'SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.',
		consequence: 'Relay commands, broadcasts and sync replies from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak; an onError option, when set, still sees every failure.',
		automaticRecovery: 'The connection is closed and the next relay subscribes on a fresh one; a subscribe that succeeds re-arms this report.',
		nextAction: 'Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.',
		sources: ['src/redis/smooth-cluster.js'], anchor: 'extensions-err-smooth-subscribe'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.TOPIC_BROADCAST_SUBSCRIBE,
		event: 'redis.topic-broadcast.subscribe.failed', component: 'runtime.redis.topic-broadcast', severity: 'error',
		problemPrefix: 'The topic broadcast could not subscribe to its channel.',
		cause: 'SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.',
		consequence: 'Broadcasts and requests from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak; an onError option, when set, still sees every failure.',
		automaticRecovery: 'The connection is closed and the next broadcast subscribes on a fresh one; a subscribe that succeeds re-arms this report.',
		nextAction: 'Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.',
		sources: ['src/redis/topic-broadcast.js'], anchor: 'extensions-err-topic-broadcast-subscribe'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.CURSOR_SUBSCRIBE,
		event: 'redis.cursor.subscribe.failed', component: 'runtime.redis.cursor', severity: 'error',
		problemPrefix: 'The cursor relay could not subscribe to its channel.',
		cause: 'SUBSCRIBE on the dedicated connection was rejected: the Redis user may not subscribe to the channel (NOPERM), or the connection failed while subscribing.',
		consequence: 'Cursor positions from other instances do not reach this instance until a subscribe succeeds. Reported once per failure streak.',
		automaticRecovery: 'The connection is closed and the next cursor update subscribes on a fresh one; a subscribe that succeeds re-arms this report.',
		nextAction: 'Check Redis connectivity and that the Redis user may SUBSCRIBE to the channel.',
		sources: ['src/redis/cursor/redis-io.js'], anchor: 'extensions-err-cursor-subscribe'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.POSTGRES_NOTIFY_CONNECTION,
		event: 'postgres.notify.connection.failed', component: 'runtime.postgres.notify', severity: 'error',
		problemPrefix: 'The Postgres LISTEN connection of a NOTIFY bridge reported an error and was closed.',
		cause: 'The dedicated LISTEN connection emitted an error: the server went away, restarted, or dropped the connection.',
		consequence: 'Notifications on the channel attribute are missed until the bridge listens again; Postgres does not queue them for a closed listener.',
		automaticRecovery: 'With autoReconnect the bridge reconnects after reconnectInterval; in advisory mode the next poll re-establishes the listener.',
		nextAction: 'Check Postgres availability. Notifications sent while the listener was down are lost, so a consumer that needs them must resynchronise from the table.',
		sources: ['src/postgres/notify.js'], anchor: 'extensions-err-postgres-notify-connection'
	}),
	direct({
		id: EXTENSIONS_ERROR_IDS.SINK_FAILED,
		event: 'operational.sink.failed', component: 'runtime.observability', severity: 'error',
		problemPrefix: 'The configured operational event sink failed; console fallback was restored for this event.',
		cause: 'The sink installed through setOperationalEventSink threw or returned a rejected promise for one record.',
		consequence: 'That record was written to console instead; the sink stays installed and receives the next record.',
		automaticRecovery: 'Per record: console fallback. The sink is not removed.',
		nextAction: 'Fix the sink named by the originalSource and originalEvent attributes; a sink should never throw.',
		sources: ['src/shared/diagnostic.js'], anchor: 'extensions-err-sink-failed'
	}),
	definition({
		id: EXTENSIONS_ERROR_IDS.INVARIANT,
		event: 'invariant.violated', component: 'runtime.assertion', emission: 'head',
		messagePrefix: HEAD + 'runtime.assertion event=invariant.violated severity=',
		cause: 'A framework-internal assertion failed. For assert and fatal the message is the assertion category; for devAssert it is the text the call site passed. The severity is chosen by the call site, so both vary.',
		consequence: 'Depends on the tier. A soft violation is counted and logged; at the fatal tier the worker is scheduled to exit with status 78 and the supervisor replaces it. A development-only assertion is logged and never counted.',
		automaticRecovery: 'None for the condition itself. A fatal-tier violation exits the worker, which is replacement rather than recovery.',
		nextAction: 'Read the severity first, because it selects the tier, then the category and context attributes. These are library-internal invariants, so a violation is an extensions defect rather than an application misconfiguration; report it with both attributes.',
		sources: ['src/shared/assert.js'], anchor: 'extensions-err-invariant'
	}),
	consoleLine({
		id: EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RECORD_SHAPE,
		event: 'observability.diagnostic.record-shape', severity: 'error',
		messagePrefix: '[extensions] operational event dropped, invalid record shape',
		cause: 'Something emitted an operational diagnostic the runtime could not build a record from: a malformed event name, an unknown severity or data class, or a bad timestamp. An unserializable attribute is a quieter failure handled by the renderer, which prints the envelope with the attributes stripped.',
		consequence: 'That diagnostic is dropped: it reaches neither the configured sink nor the log. Everything else keeps emitting normally.',
		automaticRecovery: 'None for the dropped record. Telemetry never throws, so the emitting path continued as if it had been reported.',
		nextAction: 'Read the event name printed with this line and fix the emitter; if it is application code calling the diagnostic surface, check the record against the documented shape.',
		sources: ['src/shared/diagnostic.js'], anchor: 'extensions-err-diagnostic-record-shape'
	}),
	consoleLine({
		id: EXTENSIONS_ERROR_IDS.DIAGNOSTIC_RENDER_COLLAPSE,
		event: 'observability.diagnostic.render-collapse', severity: 'error',
		messagePrefix: '[extensions] diagnostic render failed: ',
		cause: 'Formatting the record failed twice: once with its attributes and once with them stripped, which means the process JSON serializer itself is failing.',
		consequence: 'The event is lost; only this line and the event name reach the console.',
		automaticRecovery: 'None.',
		nextAction: 'Treat the process as unhealthy; a serializer that cannot render an envelope of strings is a runtime fault, not an application one.',
		sources: ['src/shared/diagnostic.js'], anchor: 'extensions-err-diagnostic-render-collapse'
	}),
	consoleLine({
		id: EXTENSIONS_ERROR_IDS.DIAGNOSTIC_CONSOLE_WRITE,
		event: 'observability.diagnostic.console-write', severity: 'error',
		messagePrefix: '[extensions] diagnostic rendered but the console refused it: ',
		cause: 'The record rendered, but the console method chosen for its severity threw: a host replaced the console or the write end of its transport is gone.',
		consequence: 'The rendered line was not written; this notice goes through console.error because that is a different method in every case but one.',
		automaticRecovery: 'None.',
		nextAction: 'Check what replaced or wrapped the console in this process.',
		sources: ['src/shared/diagnostic.js'], anchor: 'extensions-err-diagnostic-console-write'
	}),
	consoleLine({
		id: EXTENSIONS_ERROR_IDS.DIAGNOSTIC_SINK_NOTICE,
		event: 'observability.diagnostic.sink-notice', severity: 'error',
		messagePrefix: '[extensions] operational sink failed and its failure notice could not be built: ',
		cause: 'A sink failed, the original record was printed to console, and then building the sink-failure notice itself threw, usually because the injected clock is broken.',
		consequence: 'The original event was already written; only the notice about the sink is missing.',
		automaticRecovery: 'None.',
		nextAction: 'Fix the sink, then check the runtime clock injection if this line keeps appearing.',
		sources: ['src/shared/diagnostic.js'], anchor: 'extensions-err-diagnostic-sink-notice'
	})
]);

const ERROR_BY_ID = new Map(EXTENSIONS_ERROR_REGISTRY.map((entry) => [entry.id, entry]));

export function extensionsErrorDefinition(id) {
	const entry = ERROR_BY_ID.get(id);
	if (!entry) throw new TypeError('Unknown extensions error id: ' + id);
	return entry;
}

export function extensionsErrorHelpSuffix(id) {
	const entry = extensionsErrorDefinition(id);
	// A console cannot resolve a repo-relative path; entries that carry an
	// absolute link render it instead of the packaged doc route.
	return ' [' + entry.id + '] See: ' + (entry.link ?? entry.help);
}

/**
 * The message of a thrown failure: the registry prefix, the call-site detail,
 * the stable ID tag, and the repo-relative help route.
 */
export function extensionsError(id, detail = '') {
	const entry = extensionsErrorDefinition(id);
	if (entry.emission !== 'thrown') throw new TypeError('Extensions error id is not a thrown failure: ' + id);
	return entry.messagePrefix + detail + extensionsErrorHelpSuffix(id);
}

/**
 * The full text of a console-emitted failure line: the registry prefix, the
 * call-site detail, and the stable ID tag. Printing through the registry keeps
 * the emitted line and the indexed prefix the same bytes. No help route is
 * appended: a console cannot resolve one, and the stable ID finds the entry.
 */
export function extensionsConsoleLine(id, detail = '') {
	const entry = extensionsErrorDefinition(id);
	if (entry.emission !== 'console') throw new TypeError('Extensions error id is not console-emitted: ' + id);
	return entry.messagePrefix + detail + ' [' + entry.id + ']' + (entry.link ? ' See: ' + entry.link : '');
}
