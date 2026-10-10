# Contributing

Use the [issue forms](https://github.com/lanteanio/svelte-adapter-uws-extensions/issues/new/choose)
for bugs, feature requests and usage questions. Vulnerabilities use the private
route in [SECURITY.md](SECURITY.md). This is a small MIT project; the maintainer
accepts changes and there is no promised response time.

## Clone to a working demonstration

Install Node 22 or newer, Git, and Docker with Compose v2. Start Docker before
running a backend lane. A fresh clone needs one dependency installation:

```sh
git clone https://github.com/lanteanio/svelte-adapter-uws-extensions.git
cd svelte-adapter-uws-extensions
npm ci
npm run smoke
npm run verify:fast
```

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
| Postgres SQL, transactions, notifications, tasks or jobs | `npm run verify:unit` and `npm run verify:postgres`. |
| Public exports, declarations or package metadata | `npm run verify:unit` (includes a packed strict TypeScript consumer), the affected backend lanes, and `npm run gate:cross-repo -- --quick` when sibling contracts change. |
| Cursor/presence wire behavior or platform wrappers | Unit tests, smoke, Redis/Valkey/cluster lanes and matching lead-package wire fixtures. Coordinate sibling changes through their owning repository. |
| Hot-path changes | Relevant correctness lanes plus a repeatable before/after benchmark from `bench/` under the same load and environment. |
| CI or verification tooling | `npm run verify:docs`, run the changed lane, and check failure as well as success behavior. |

`npm run verify:pr` combines the fast, unit and smoke lanes. It does not cover all
backend tiers. `npm run verify:full` adds every backend tier. `npm run test:integration`
by itself runs only standalone Redis and Postgres; it excludes Valkey and all
cluster tiers. A reviewer may accept a named exception when a lane is unrelated
or unavailable; record the missing claim and who accepted it in the PR. A skipped
lane is not a passing lane.

## Verification inventory

The table below is generated from `package.json` and `.github/workflows/`.
`npm run check:contributor-map` refuses stale inventory. After changing scripts or
the workflow, run `node scripts/check-contributor-map.js --write` and inspect the diff.

<!-- verification-inventory:start -->
Hosted jobs: `suite`.

| Command | Script expansion | Coverage |
|---|---|---|
| `npm run check:contributor-map` | `node scripts/check-contributor-map.js` | Hosted |
| `npm run check:format` | `node scripts/check-formatting.js` | Hosted |
| `npm run gate:cross-repo` | `node scripts/cross-repo-gate.js` | Local |
| `npm run smoke` | `node scripts/smoke.js` | Hosted |
| `npm run verify:cluster` | `test:integration:cluster`, `test:integration:cluster-mirror` | Local |
| `npm run verify:docs` | `check:format`, `check:contributor-map` | Local |
| `npm run verify:fast` | `check`, `check:format`, `check:contributor-map` | Hosted |
| `npm run verify:full` | `verify:pr`, `verify:fast`, `check`, `check:format`, `check:contributor-map`, `verify:unit`, `test`, `smoke`, `verify:redis`, `test:integration:redis`, `verify:postgres`, `test:integration:postgres`, `verify:valkey`, `test:integration:valkey`, `verify:cluster`, `test:integration:cluster`, `test:integration:cluster-mirror`, `verify:valkey-cluster`, `test:integration:valkey-cluster`, `test:integration:valkey-cluster-mirror` | Local |
| `npm run verify:postgres` | `test:integration:postgres` | Local |
| `npm run verify:pr` | `verify:fast`, `check`, `check:format`, `check:contributor-map`, `verify:unit`, `test`, `smoke` | Hosted |
| `npm run verify:redis` | `test:integration:redis` | Local |
| `npm run verify:unit` | `test` | Hosted |
| `npm run verify:valkey` | `test:integration:valkey` | Local |
| `npm run verify:valkey-cluster` | `test:integration:valkey-cluster`, `test:integration:valkey-cluster-mirror` | Local |
<!-- verification-inventory:end -->

Hosted coverage means the checked-in workflow invokes that command, directly or
through an aggregate. It does not assert branch protection or that an unpushed
commit has run on GitHub. Lanes marked local are not hosted. The workflow runs the
static/unit/smoke gate on Linux with Node 22; backend and cross-repository lanes
remain contributor-run requirements for changes selected by the map.

Plan for seconds for static checks, roughly 1-3 minutes for the unit suite, and
10-60 seconds for smoke after the image is cached. Standalone backend suites
usually take 1-5 minutes each, and each cluster lane 3-10 minutes. These are planning
ranges, not deadlines or pass criteria; record actual elapsed time with results.
A cold image pull, slow Docker storage or a large stress case can take longer.

## Backend setup and isolation

Integration commands create and tear down their own Compose stacks. Run tiers
sequentially on one machine. The Redis and Postgres defaults use ports 56379 and
55432; Valkey uses 56381. Redis Cluster uses 57000-57005 and subnet 172.30.0.0/24;
Valkey Cluster uses 57100-57105 and subnet 172.31.0.0/24. Check for conflicts with
other stacks before starting cluster tests. Do not stop another project's stack.

For an isolated standalone run, choose both a free port and a unique project:

```powershell
$env:INTEGRATION_REDIS_HOST_PORT = '56390'
$env:INTEGRATION_COMPOSE_PROJECT = 'extensions-local-check'
npm run verify:redis
```

The matching variables are `INTEGRATION_POSTGRES_HOST_PORT`,
`INTEGRATION_VALKEY_HOST_PORT`, `INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE`, and
`INTEGRATION_VALKEY_CLUSTER_HOST_PORT_BASE`. Changing cluster host ports does not
change their fixed bridge subnets. If a subnet overlaps another stack, use an
isolated Docker host for that lane. The setup files under `test/integration/`
and their Compose files are the source of truth. If a run is interrupted, remove
only its named Compose project, using that setup's exact Compose file.

`gate:cross-repo` requires local adapter and realtime checkouts and package-manager
network access. It reports which checks used installed tarballs and which ran in
each repository's own checkout. Use `--strict` when missing prerequisites must fail.
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

The local Relay board is the workspace source of truth. Public GitHub issues and
PRs are intake and review surfaces; maintainers link them to the corresponding work item.
External contributors coordinate ownership on the public issue; they do not need
access to the maintainer's local board.
The states are backlog, todo, in_progress, in_review and done. A label on GitHub
may mirror a state, but is not a second independent queue.

An item is Ready for todo when it states the observable outcome, owning repository,
reproduction or supporting evidence, acceptance criteria, test lanes, dependencies,
compatibility risk and cost of delay. Product decisions or missing evidence are
resolved before implementation starts. Security evidence belongs in the private route.

Order ready work by risk and cost of delay: security/data loss, release-blocking
regressions, confirmed correctness problems, compatibility/operations, then
convenience and documentation. Record why an item changes priority. Prefer the
smallest complete change that removes the most risk within the same class.

Use a provisional limit of three independent implementation items in progress
per repository. A maintainer may approve a larger group with distinct file ownership
and linked dependencies. Each contributor owns one item or one explicitly coordinated
cluster at a time; review work is separate. Claim the Relay task before work, move
it to in_progress, record progress there, and release its lease when work pauses
or finishes. Do not take another contributor's live claim.

A blocked item names its dependency, the owner who can resolve it, completed useful
work, and a next review date. Keep it out of the ready queue while the blocker holds;
blocked is a reason recorded on the item, not an invented Relay status. Difficulty
alone is not a blocker. Route sibling work as a task in that repository's workstream.

At each 30-day backlog review, revalidate evidence, ownership and priority; merge
duplicates; revisit blocked items; and promote only Ready work. An unchanged item
across two reviews may be closed with an explicit reopening condition. The maintainer
records the review date; this is a maintenance checklist, not a response-time promise.

Move to in_review only after the implementation and its relevant checks are complete.
Done requires all acceptance criteria, regression evidence, synchronized docs/types,
reviewed generated and lockfile changes, relevant lane results or an accepted named
exception, independent review, and maintainer acceptance. Done is distinct from
published: attach a release link when publication happens.

The `good first issue` label is curated: a complete reproduction, named files,
a runnable test command, and bounded acceptance criteria must be present. Exclude
security, wire or concurrency design work. Remove the label when scope expands.
Use the [filtered issue list](https://github.com/lanteanio/svelte-adapter-uws-extensions/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22)
to find candidates. If none are available, propose a small documentation example
or regression test through an issue form; there is no promise of a standing queue.
