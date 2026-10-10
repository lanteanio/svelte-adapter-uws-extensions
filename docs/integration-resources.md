# Integration resources

[Package overview](../README.md) | [Public entry points](./public-entries.md)

The shared [translation contract](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/translating.md)
defines the English source locale, application-owned catalogs, stable
machine-field boundary, placeholder rules, and bidirectional-text handling
across all three ecosystem packages.

The canonical [privacy integration contract](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/privacy-integration.md)
records the exact no-TTL replay/dead-letter defaults, durable idempotency and
task lifetimes, alarm lifecycle, identity-extractor requirements, and
application-owned deletion boundaries. Complete its generated
[host worksheet](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/privacy/v1/ropa-dpa-transfer-checklist.md)
for every Redis/Postgres provider, replica, backup, region, and transfer.

When an operation fails, search the generated [error reference](./errors.md)
with the stable ID, event, or beginning of the emitted message.

During an incident, start from the ecosystem's versioned
[operations pack v1](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/operations/v1/README.md).
Its failure map and Redis/durable-work runbooks state ownership, loss semantics,
abort criteria, recovery checks, and the evidence required at handoff.
Use its [coordinated release matrix](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/operations/v1/coordinated-release.md)
before changing the adapter/realtime/extensions tuple or renaming a durable
alarm path; rollback restores the complete recorded tuple.

Before launch or a topology/autoscaling change, use the ecosystem's versioned
[capacity kit v1](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/capacity/v1/README.md).
Its open-arrival proof includes Redis/Postgres and relay telemetry in the same
artifact as the exact deployment shape, peak p50/p95/p99 and errors, the first
resource to saturate, and recovery under continuing traffic. Per-primitive
microbenchmarks do not prove distributed production capacity.
