# Circuit breaker metrics

`wireBreakerMetrics(breaker, metrics)` is exported from
`svelte-adapter-uws-extensions/prometheus`. Pass the same registry used by the
extensions, and retain its returned cleanup function. Call cleanup when the
binding is no longer needed, including application shutdown before discarding
the breaker. `breaker.destroy()` stops timers; it does not remove metrics wiring.

| Metric | Meaning |
|---|---|
| `circuit_breaker_states{state}` | Current number of registered partitions in `healthy`, `broken` or `probing`. |
| `circuit_breaker_transitions_total{from,to}` | Observed transitions between those fixed states since wiring. |

The state gauge is collected at scrape time. Each registered breaker contributes
its default partition and all currently retained keyed partitions. Evicted keys
stop contributing immediately; aggregate transitions remain cumulative. Keys,
tenant identifiers and backend errors never become labels. Custom structural
breakers expose their default state only because their partitions are private.

Multiple breakers can share a registry. Wiring the same breaker and registry
again is reference-counted: it neither doubles state counts nor installs another
transition listener. Each returned cleanup function is idempotent; the final one
removes that binding. Different registries remain independent. A registry does
not retain an otherwise unreachable breaker. All three state samples are present immediately. All six transition samples
are initialized to zero before their first event, so an
idle healthy backend is distinguishable from absent instrumentation.

Wiring reads state and subscribes to transitions; it does not wrap `guard`,
`success` or `failure`. Scrapes aggregate current state on demand. Pair this with
[PostgreSQL pool metrics](postgres-support.md#pool-metrics) and
[extension operation metrics](../README.md#prometheus-metrics) to distinguish dependency health from
operation outcomes and local delivery without a confirmed relay.
