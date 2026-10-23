# Contributing

Use the [issue forms](https://github.com/lanteanio/svelte-adapter-uws-extensions/issues/new/choose)
for bugs, feature requests and usage questions. Vulnerabilities use the private
route in [SECURITY.md](SECURITY.md). This is a small MIT project; the maintainer
accepts changes and there is no promised response time.

## Clone to a working demonstration

Install Node 22 or newer, Git, and Docker with Compose v2. Start Docker before
running a backend lane. A fresh clone needs one dependency installation:

```sh
git clone --branch dev --single-branch https://github.com/lanteanio/svelte-adapter-uws-extensions.git
cd svelte-adapter-uws-extensions
npm ci
npm run preflight
npm run smoke
npm run verify:fast
```

Stop if the preflight fails. It checks the installed adapter's Node, operating
system, architecture, Linux libc and native addon requirements. The
[first-success guide](docs/getting-started.md) includes expected output and
prerequisite fixes.

There is no build step: the package ships the JavaScript and adjacent declarations
under `src/`. `npm run smoke` exercises the same cursor join, move, read and remove
flow using the in-memory Redis double and a real Redis container. Two tracker
instances observe shared state; WebSocket connections use the public testing
helpers. It prints resolved package/backend versions and removes its own container
on success or failure. Docker allocates a free loopback port. The first run may
pull the Redis image. Docker absence fails the command rather than silently
substituting the mock. `npm run smoke -- --mock-only` is an explicit local-only
check, not evidence for backend behavior.

## Change-to-test map

Always run `npm run verify:fast`. Then choose the row covering the changed
behavior. A focused unit command is useful while editing; it does not replace
its corresponding real-backend lane before review.

| Change | Required evidence beyond the fast lane |
|---|---|
| Prose, issue forms, contribution guide | `npm run verify:docs`; execute changed examples or commands. |
| Pure utilities, Prometheus, breaker or admission policy | `npm run verify:unit`; focused tests for every affected caller. |
| Redis commands, scripts, expiry, pub/sub or reconnect | `npm run verify:unit`, `npm run verify:redis`, `npm run verify:valkey`, `npm run verify:cluster`, `npm run verify:valkey-cluster`. |
| Redis keys, hash tags, routing or a shared multi-slot helper | Both cluster lanes above plus the standalone Redis and Valkey lanes. Each cluster lane includes its dedicated tests and the standalone suite mirrored onto a cluster. |
| Postgres SQL, transactions, notifications, tasks or jobs | `npm run verify:unit`, `npm run verify:postgres` and `npm run verify:pgbouncer`. |
| Dependency or lockfile changes | `npm run verify:dependencies`, a clean `npm ci`, full unit tests, and a full SBOM using the command below. |
| Public exports, declarations or package metadata | `npm run verify:unit` (includes a packed strict TypeScript consumer), the affected backend lanes, and `npm run gate:cross-repo -- --quick` when sibling contracts change. |
| Cursor/presence wire behavior or platform wrappers | Unit tests, smoke, Redis/Valkey/cluster lanes and matching lead-package wire fixtures. Coordinate sibling changes through their owning repository. |
| What a closed connection or a removed subscription must release: relay, presence, groups, replay | The affected correctness lanes plus `npm run verify:leak` (see the leak lane below). |
| Hot-path changes | Relevant correctness lanes plus a repeatable before/after benchmark from `bench/` under the same load and environment. |
| CI or verification tooling | `npm run verify:docs`, run the changed lane, and check failure as well as success behavior. |

`npm run verify:pr` combines the fast, unit and smoke lanes. It does not cover the
backend tiers. `npm run test:integration` runs every backend tier in sequence:
standalone Redis, Postgres, Postgres behind PgBouncer, standalone Valkey, Redis
Cluster and Valkey Cluster, each cluster both natively and with the standalone
suite mirrored onto it. `npm run verify:full` is `verify:pr` plus that whole
matrix, and the release workflow runs the same matrix through `verify:release`.
A reviewer may accept a named exception when a lane is unrelated
or unavailable; record the missing claim and who accepted it in the PR. A skipped
lane is not a passing lane.

## Verification inventory

The table below is generated from `package.json` and `.github/workflows/`.
`npm run check:contributor-map` refuses stale inventory. After changing scripts or
the workflow, run `node scripts/check-contributor-map.js --write` and inspect the diff.

<!-- verification-inventory:start -->
Hosted jobs: `dependencies`, `external-links`, `backend`, `verify`, `publish`, `suite`, `onboarding`.

| Command | Script expansion | Coverage |
|---|---|---|
| `npm run check:contributor-map` | `node scripts/check-contributor-map.js` | Hosted |
| `npm run check:format` | `node scripts/check-formatting.js` | Hosted |
| `npm run gate:cross-repo` | `node scripts/cross-repo-gate.js` | Local |
| `npm run oracle:real-cluster` | `node scripts/real-cluster-oracle.js` | Local |
| `npm run smoke` | `node scripts/smoke.js` | Hosted |
| `npm run verify:cluster` | `test:integration:cluster`, `test:integration:cluster-mirror` | Hosted |
| `npm run verify:dependencies` | `check:lockfile` | Hosted |
| `npm run verify:docs` | `check:documentation`, `check:links`, `check:format`, `check:contributor-map` | Local |
| `npm run verify:fast` | `check`, `check:documentation`, `check:links`, `check:format`, `check:contributor-map`, `check:publish` | Hosted |
| `npm run verify:full` | `verify:pr`, `preflight`, `verify:fast`, `check`, `check:documentation`, `check:links`, `check:format`, `check:contributor-map`, `check:publish`, `verify:unit`, `test`, `smoke`, `test:integration`, `test:integration:redis`, `test:integration:postgres`, `test:integration:pgbouncer`, `test:integration:valkey`, `test:integration:cluster`, `test:integration:cluster-mirror`, `test:integration:valkey-cluster`, `test:integration:valkey-cluster-mirror` | Local |
| `npm run verify:leak` | `test:leak`, `test:leak:cluster` | Local |
| `npm run verify:pgbouncer` | `test:integration:pgbouncer` | Hosted |
| `npm run verify:postgres` | `test:integration:postgres` | Hosted |
| `npm run verify:pr` | `preflight`, `verify:fast`, `check`, `check:documentation`, `check:links`, `check:format`, `check:contributor-map`, `check:publish`, `verify:unit`, `test`, `smoke` | Hosted |
| `npm run verify:redis` | `test:integration:redis` | Hosted |
| `npm run verify:release` | `verify:dependencies`, `check:lockfile`, `verify:pr`, `preflight`, `verify:fast`, `check`, `check:documentation`, `check:links`, `check:format`, `check:contributor-map`, `check:publish`, `verify:unit`, `test`, `smoke`, `test:integration`, `test:integration:redis`, `test:integration:postgres`, `test:integration:pgbouncer`, `test:integration:valkey`, `test:integration:cluster`, `test:integration:cluster-mirror`, `test:integration:valkey-cluster`, `test:integration:valkey-cluster-mirror` | Hosted |
| `npm run verify:unit` | `test` | Hosted |
| `npm run verify:valkey` | `test:integration:valkey` | Hosted |
| `npm run verify:valkey-cluster` | `test:integration:valkey-cluster`, `test:integration:valkey-cluster-mirror` | Hosted |
<!-- verification-inventory:end -->

Hosted coverage means the checked-in workflow invokes that command, directly or
through an aggregate. It does not assert branch protection or that an unpushed
commit has run on GitHub. Lanes marked local are not hosted. The `Verify` workflow
runs the static/unit/smoke gate on Linux with Node 22; the `Integration` workflow
runs each backend tier in its own job on the Node floor and on Node 24, and records
the Node version and server image digests in the job summary. Cross-repository
lanes remain contributor-run requirements for changes selected by the map, and a
backend change still runs its lanes locally before review.

Plan for seconds for static checks, roughly 1-3 minutes for the unit suite, and
10-60 seconds for smoke after the image is cached. Standalone backend suites
usually take 1-5 minutes each, and each cluster lane 3-10 minutes. These are planning
ranges, not deadlines or pass criteria; record actual elapsed time with results.
A cold image pull, slow Docker storage or a large stress case can take longer.

## Leak lane

`npm run test:leak` asks whether a Redis-backed deployment grows under a
workload it should sustain indefinitely. It brings up the standalone Redis stack
(`test:leak:cluster` the Redis Cluster stack, `verify:leak` both), forks three
server instances that reach each other only through Redis, and churns real
WebSocket clients against every instance at a fixed rate: each one connects as a
new user, subscribes, publishes through the replay buffer, waits for its own
event and closes. Held clients on every instance must receive the publishes made
on the others, so the cross-instance relay is under load the whole time.

After a one-minute warmup, a forced collection, an unsampled resettle and a wait
until every instance holds still, a window (`--minutes N`, default 3) samples
each instance's resident set, active handles and Redis connection listeners and
the deployment's Redis key count every two seconds. The verdict uses the trend
kernel from `svelte-adapter-uws/sim` with the adapter lane's rules: a slope
counts only when the fit explains the samples (r-squared at least 0.5), more than
0.5% failed churns or a p95 over 50% above the warmed baseline fail the run, and
settled heap plus external is compared at both ends. Just before the window
every client closes and a cooldown passes; the keys without an expiry are counted
at that quiet point, and the held clients reconnect. After the window every
client closes again and, after the same cooldown, no connection may still be
registered, no presence or group roster entry may remain in Redis (the
`presence:user` keys and the fields of the `presence:topic`, `presence:pending`
and group `members` hashes), and the keys without an expiry may not exceed the
quiet-point count by more than 8.

One instance is a self-check: once the window opens it leaks, per closed
connection, a 64 KiB buffer, a timer, a Redis listener, a key without an expiry
and a roster field, the keys under a prefix of their own. The run fails unless
each gate catches the leak planted for it: the resident-set and retained gates
the buffers, the handle gate the timers, the listener gate the listeners, the
key-count and expiry gates the keys, and the membership gate the roster fields.
Exit code 1 is a failed verdict and 2 a run that could not reach one it trusts.
An interrupt (SIGINT, SIGTERM or SIGHUP) stops the instances, tears the backend
stack down and exits 130, 143 or 129. Each run writes a JSON record with every
sample to the gitignored `.leak-lane/` directory (`--out <dir>` to write
elsewhere). The isolation variables above apply; a run takes about 6-10 minutes
per backend, so it is its own command rather than part of `verify:full`.

## Dependency graph

The dependency workflow uses Node 24.13.1 and npm 11.8.0 for lockfile generation
and verification. This is the dependency toolchain; the supported runtime minimum
remains Node 22. A dependency change uses that toolchain and commits the resulting
`package-lock.json` together with any intentional manifest change.

```sh
npm ci
npm run verify:dependencies
npm sbom --sbom-format cyclonedx --package-lock-only
```

`check:lockfile` copies the manifest, lockfile and project npm settings to a fresh
temporary directory, runs `npm install --package-lock-only --ignore-scripts`, and
requires an identical lockfile. It never changes the checkout or its installed
dependencies. `verify:dependencies` also requires a valid installed dependency tree
and a clean npm audit; registry access is required. The full lockfile SBOM includes
development, peer and optional packages for all platforms. The hosted dependency
job retains that CycloneDX file for 30 days and can be called by a release workflow
through `workflow_call`. CI workflow presence does not configure branch protection.

### Database client maintenance

Keep ioredis and pg on supported compatible releases and commit their resolved
versions in the lockfile. Check upstream releases and advisories before each
release and at least quarterly. ioredis uses best-effort maintenance; retain it
for the existing Redis API while its supported server versions, reliability and
security meet this package's requirements.

Start a client maintenance review when a supported Redis, Valkey or PostgreSQL
version cannot be used with the client, a reachable high or critical advisory has
no fixed compatible release, or six months pass without upstream activity while
a reproducible compatibility or security issue remains unresolved. Record the
upstream evidence and affected operations in the issue. Review remediation,
a maintained fork or client replacement according to compatibility and measured
performance; inactivity alone does not justify replacing a stable dependency.

For a client update, run the dependency checks above, the unit suite and affected
real-backend lanes. Redis client changes also exercise Cluster reconnects,
subscription recovery, bounded queues and command deadlines. Recheck any private
client integration seam and retain hot-path benchmark evidence before release.

## Backend setup and isolation

Integration commands create and tear down their own Compose stacks. Run tiers
sequentially on one machine. The Redis and Postgres defaults use ports 56379 and
55432; Valkey uses 56381. Redis Cluster uses 57000-57005 and subnet 10.249.0.0/24;
Valkey Cluster uses 57100-57105 and subnet 10.249.1.0/24. Both subnets sit
outside Docker's default address pools. The PgBouncer session and transaction
pools use 55433 and 55434 in front of their own Postgres on the Postgres port.
Check for conflicts with other stacks before starting cluster tests. Do not stop
another project's stack.

For an isolated standalone run, choose both a free port and a unique project:

```powershell
$env:INTEGRATION_REDIS_HOST_PORT = '56390'
$env:INTEGRATION_COMPOSE_PROJECT = 'extensions-local-check'
npm run verify:redis
```

The Valkey, Redis Cluster, Valkey Cluster and PgBouncer stacks append `-valkey`,
`-cluster`, `-valkey-cluster` and `-pgbouncer` to that project name. The matching
variables are `INTEGRATION_POSTGRES_HOST_PORT`,
`INTEGRATION_PGBOUNCER_SESSION_PORT`, `INTEGRATION_PGBOUNCER_TRANSACTION_PORT`,
`INTEGRATION_VALKEY_HOST_PORT`, `INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE`, and
`INTEGRATION_VALKEY_CLUSTER_HOST_PORT_BASE`. Changing cluster host ports does not
change their bridge subnets; when a subnet overlaps another network, set
`INTEGRATION_REDIS_CLUSTER_SUBNET` or `INTEGRATION_VALKEY_CLUSTER_SUBNET` to a free
/24 prefix such as `10.250.0`. The setup files under `test/integration/`
and their Compose files are the source of truth. Every image there is pinned by
digest, with the exact release in the tag beside it; move a pin in every Compose
file and in `scripts/smoke.js` together, then run the whole matrix. If a run is
interrupted, remove only its named Compose project, using that setup's exact
Compose file.

`gate:cross-repo` requires local adapter and realtime checkouts and package-manager
network access. It reports which checks used installed tarballs and which ran in
each repository's own checkout. Use `--strict` when missing prerequisites must fail.
The `--quick` gate packs and installs the three working trees, checks their peers
and declarations, then runs a native WebSocket contract from that clean consumer.
The adapter's pinned canonical uWebSockets.js binding is a required dependency
for this gate. Realtime's message hook routes through two extension buses sharing
the Redis test helper, and both local and remote clients must receive the exact
ordered values. This contract requires no backend service; native backend lanes
remain separate. Use `--rungs contracts` to run only this contract and its packing
and installation prerequisites.
Use `--require-clean --strict` when recording exact committed source identities;
the default also permits uncommitted working-tree changes.

`oracle:real-cluster` is the same delivery contract on a real Redis Cluster: it
packs this checkout, installs it with the adapter and a named realtime tarball or
version, starts the cluster lane's Compose stack (same isolation variables), and
writes a JSON verdict with package integrities and topology. Pass
`--realtime <tarball>` when checking a realtime candidate. Inputs, cases and the
result format are in the [real-cluster oracle guide](./docs/real-cluster-oracle.md).
Never edit a sibling repository as part of this checkout's change; route the
required change to its owner with the expected contract and verification evidence.

## Source and generated-file ownership

| Area | Owner and corresponding tests |
|---|---|
| `src/redis/` | Redis modules; `test/redis/`, standalone and cluster integration. |
| `src/postgres/` | Postgres modules; `test/postgres/` and Postgres integration. |
| `src/shared/` | Shared primitives; `test/shared/` and every affected backend. |
| `src/prometheus/` | Metric registry; `test/prometheus/`. |
| `src/testing/`, `src/sim.*` | Public test helpers and deterministic simulation; `test/testing/` and `test/sim/`. |
| Adjacent `.d.ts` files | Public contract; export checks and the packed consumer test. |
| `docs/errors.md` | Generated from `src/error-registry.js`; regenerate with `node scripts/generate-error-reference.js --write`. |

The adapter is the lead for mirrored APIs and wire contracts. Read its actual
implementation and tests before proposing a different option, default or field.
A sibling-only extension must first be agreed and implemented in the lead.

## Contribution conventions

- Follow `.editorconfig`. `npm run check:format` parses it, checks UTF-8, committed
  line endings, final newlines, indentation and trailing whitespace, and never
  rewrites files. Literal string content is preserved. `.gitattributes` commits
  text as LF even on Windows.
- Keep commits small, with an imperative subject describing behavior, such as
  `fix(cursor): preserve the latest position`. Explain a non-obvious reason in
  a short body. No required signoff, DCO trailer, CLA or commit-signing policy
  exists; submitting a contribution licenses it under the repository's MIT license.
- Keep code and documentation typography to straight quotes and ASCII hyphens.
  Preserve legitimate accented letters in names and human-language content.
- Change source and declarations together. Public API changes update README.
  Put user-visible changes under Unreleased in the Keep-a-Changelog format.
  Explain compatibility impact; the maintainer chooses and stamps the release
  version and npm channel. Do not change `package.json`'s version in a PR.
- Commit `package-lock.json` with dependency changes; use `npm ci` for verification.
  Do not hand-edit the lockfile or bundle unrelated dependency upgrades. Generated
  documents must be regenerated from their owner and reviewed as diffs.
- Source uses `src/shared/runtime.js` for clocks, RNG and timers. A new public
  export needs its declaration and an `exports` entry. Keep secrets and personal
  deployment data out of tests, logs, metrics and issue attachments.

## Work lifecycle

Coordinate ownership on the public GitHub issue before starting a change. Describe
its observable outcome, owning repository, reproduction or supporting evidence,
acceptance criteria, required checks, dependencies and compatibility risks. Resolve
product decisions and missing evidence before implementation. Send security evidence
through the private reporting route in [SECURITY.md](./SECURITY.md).

Prioritize security and data loss, release-blocking regressions, confirmed correctness
problems, compatibility and operations, then convenience and documentation. Explain
priority changes on the issue. Within the same class, prefer the smallest complete
change that removes the most risk.

Keep no more than three independent changes under implementation per repository
unless the maintainer agrees to a larger group with distinct file ownership and
explicit dependencies. Each contributor owns one change or one coordinated group;
review is separate. Describe progress on the issue and say when work pauses so
another contributor can continue without duplicating it.

When a dependency prevents progress, link it from the issue and name its owner,
what is already complete and when to check it again. Difficulty alone is not a
dependency. Report changes needed in another package as an issue in its repository.

Maintainers review open issues every 30 days to revalidate evidence, ownership and
priority, merge duplicates and revisit dependencies. An issue unchanged across two
reviews may be closed with an explicit condition for reopening it. Record the
review date on the issue; this is a maintenance checklist, not a response-time promise.

Request pull-request review after implementation and the relevant checks are complete.
Acceptance requires the stated behavior, regression evidence, synchronized docs and
types, reviewed generated files and lockfile changes, relevant test results or an
accepted named exception, independent review and maintainer acceptance. Merging and
publishing are separate: link the release once publication happens.

The `good first issue` label is curated: a complete reproduction, named files,
a runnable test command, and bounded acceptance criteria must be present. Exclude
security, wire or concurrency design work. Remove the label when scope expands.
Use the [filtered issue list](https://github.com/lanteanio/svelte-adapter-uws-extensions/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22)
to find candidates. If none are available, propose a small documentation example
or regression test through an issue form; there is no promise of a standing queue.
