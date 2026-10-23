# Real-cluster oracle

[Package overview](../README.md) | [Release verification](./releasing.md)

The real-cluster oracle is a repository script that proves cross-instance
delivery through packed or published packages and a real Redis Cluster. It
never substitutes an in-memory store or the simulator. It is not part of the
published package API: run it from a checkout of this repository.

```sh
npm ci
npm run oracle:real-cluster -- --realtime ../svelte-realtime-candidate.tgz
```

The script:

1. packs this checkout with `npm pack`, unless `--extensions` names a tarball
   or a published version;
2. installs extensions, `svelte-adapter-uws`, `svelte-realtime`, their peers
   and the adapter's pinned native `uWebSockets.js` into a fresh consumer;
3. starts the Redis Cluster of the cluster integration lane
   (`test/integration/docker-compose.cluster.yml`: three masters, three
   replicas, Redis 7.4) unless an external cluster is named;
4. runs the scenario `scripts/fixtures/real-cluster-oracle.mjs` from the
   consumer; every module any package imports or requires must resolve inside
   the consumer, and starting a worker thread or a child process, which that
   check cannot follow, fails the run;
5. removes the Compose stack, checks that no container or network of the
   project is left, and writes the result.

## Inputs

| Option | Default | Meaning |
|---|---|---|
| `--extensions <tgz or spec>` | pack this checkout | `svelte-adapter-uws-extensions` to install: a tarball path or a published version, tag or `svelte-adapter-uws-extensions@<spec>`. |
| `--realtime <tgz or spec>` | `next` | `svelte-realtime` to install, in the same forms. |
| `--adapter <tgz or spec>` | the installed version | `svelte-adapter-uws` to install. The default installs the exact version in `package-lock.json` and fails unless the consumer receives the same integrity. |
| `--work-dir <path>` | `.real-cluster-oracle/` | Tarballs, consumer, result and scenario log. |
| `--out <file>` | `<work-dir>/result.json` | Result path. |

The oracle needs Node 22.15 or later (`module.registerHooks`), above the
package's own engine floor of 22.0. On an older Node it stops at the `inputs`
step with exit 2, before anything is packed or installed.

A path input must be a gzip tarball. A spec must be a version, dist-tag or
range of the named package; `file:`, `link:`, git, URL and `npm:` alias specs,
directories and other files are refused before anything is packed or
installed, and so is an option given without a value. A tarball is identified
by its SHA-256 and sha512 integrity, which must match the `integrity` the
consumer lock records for it; a registry input by the `resolved` URL and
`integrity` the consumer lock records.

Docker is required unless `ORACLE_CLUSTER_NODES` names an external cluster as a
JSON array of `{"host", "port"}` seeds. `ORACLE_CLUSTER_NAT_MAP` translates
announced node addresses, and `ORACLE_REDIS_USERNAME` / `ORACLE_REDIS_PASSWORD`
authenticate. Credentials are never written to the result. An external cluster
must be dedicated, healthy, serve all 16384 slots with at least three masters
and be reachable at every announced address. The scenario uses one random
pub/sub channel per run, writes no keys, and does not reconfigure the cluster.

The Docker stack honors the integration isolation variables
`INTEGRATION_COMPOSE_PROJECT`, `INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE` and
`INTEGRATION_REDIS_CLUSTER_SUBNET`. Unset, they default to project
`svelte-adapter-uws-extensions-oracle`, host ports 57200-57205 and subnet
`10.249.2.0/24`, apart from the integration lanes. Run one oracle per project
name at a time. Ctrl-C, SIGTERM or SIGHUP (a closed terminal) removes the
stack, records the interrupt and the step it hit in the result file and exits
with 130, also when a child process such as `npm install` died of the same
Ctrl-C first. Windows delivers no SIGTERM; a closed console window gives the
process only a few seconds there.

## Attesting published packages

A documentation host that records evidence for published versions runs the
oracle from a checkout of this repository and names every package by its
registry version:

```sh
git clone https://github.com/lanteanio/svelte-adapter-uws-extensions.git
cd svelte-adapter-uws-extensions
git checkout dev
npm ci
INTEGRATION_COMPOSE_PROJECT=docs-oracle \
INTEGRATION_REDIS_CLUSTER_HOST_PORT_BASE=58600 \
INTEGRATION_REDIS_CLUSTER_SUBNET=10.249.60 \
npm run oracle:real-cluster -- \
  --extensions 0.6.0-next.63 --adapter 0.6.0-next.96 --realtime 0.6.0-next.91 \
  --work-dir ../oracle-run --out ../oracle-run/result.json
```

Required services: Docker with Compose v2 able to pull `redis:7.4-alpine`,
six free host ports from the chosen base, and a free /24 bridge subnet; or an
external cluster through `ORACLE_CLUSTER_NODES` instead of Docker. Network
access to the npm registry and to GitHub (the native `uWebSockets.js` tarball
the adapter pins) is required for the install. Keep `result.json` and
`scenario-stderr.log` together with the host's own claim identifier. The
scenario itself comes from the checkout, so `source.commit` in the result
names the scenario revision while `packages` names the attested artifacts.

## Scenario

The source server's message hook is realtime's `createMessage`, with
realtime's bus set to an extensions `createPubSubBus` on the cluster; the
target server activates a second bus on its own cluster client. Both are
native uWebSockets.js test servers. Each server holds two WebSocket clients,
one per client pair, and each pair has its own topic:

- the plain pair sends no `hello`, so `publishBatched` reaches it through the
  adapter's per-member fallback;
- the batch-capable pair announces the `batch` capability, so the adapter
  sends it shared `{"type":"batch"}` frames. The case fails unless both
  batch-capable clients, local and remote, receive at least one batch frame,
  and both plain clients receive none.

Every frame that carries an event counts: one for another topic or event fails
the case instead of being filtered out.

The cases run in order:

| Case | Passes when |
|---|---|
| `setup` | The packages load from the consumer's `node_modules`, both cluster clients connect, both servers and all four sockets are up and subscribed. |
| `cluster-topology` | `CLUSTER INFO` reports `cluster_state:ok`, all 16384 slots are served and there are at least three masters. |
| `single-fanout` | Six ordered `platform.publish` calls per topic arrive at all four sockets exactly once, in order. |
| `batch-fanout` | One six-member `platform.publishBatched` and one ordered barrier publish per topic arrive at all four sockets exactly once, in order, and the totals still hold after a 500 ms drain window. On the remote sockets the barrier travels the same relay path as every earlier message, so a duplicate there arrives before it; the drain window, which starts once the barrier reached the remote server, covers a duplicate the bus could echo to the local sockets after the barrier's direct local delivery. |
| `cleanup` | Sockets, servers, buses and connections close. It runs even after a failure. |

A pass means each of the four sockets received exactly thirteen values.

## Result

The result file is JSON with `schemaVersion: 1`. Paths it names are relative
to the work directory or bare file names. In every other string of it, also in
captured npm, Docker and scenario output, and in `scenario-stderr.log`, the
work directory, the checkout and the home directory are replaced by
`<work-dir>`, `<repo>` and `~`. A Windows directory is matched in any letter
case and in its native, forward-slash and JSON-escaped (`C:\\Users\\...`)
spellings, a drive path also in its MSYS (`/c/Users/...`) spelling, a UNC path
(`\\server\share\...`) also as `//server/share/...`, and each of them also by
its 8.3 short name (`C:\Users\LONGNA~1`) where that differs. Only whole path
prefixes are replaced: never a filesystem or share root such as `HOME=/`,
never part of a longer name, and never a path inside a URL other than
`file://`. Digest, `integrity` and `resolved`
fields are written as they are.

| Field | Content |
|---|---|
| `status` | `passed`, `failed` (a case failed or did not run) or `error` (the oracle could not run, was interrupted, or could not confirm its stack was removed). |
| `command` | The invocation. |
| `source` | This repository's commit and whether the working tree had uncommitted changes. |
| `packages` | Per package: `kind` (`tarball` or `registry`), installed `version`, and the tarball name, `sha256` and `integrity`, or the registry `spec`, `resolved` URL and `integrity`; the native binding's pin and version. |
| `services` | `docker-compose` with project, Compose file, host ports, subnet, seeds and `teardown` (`removed`, `leaked` or `unverified`, with any teardown or fallback error), or `external` with seeds. |
| `scenario` | The scenario report: `cases` with status, duration and error, `counts` (planned, executed, passed, failed, notRun), `deliveries` per client pair (values and batch frames per socket), `backend` (masters, replicas, engine and version per master) and `provenance` (Node version, platform, scenario SHA-256, and version, consumer-relative root, entry-file SHA-256 and `package.json` SHA-256 of every loaded package, and `resolution`: whether module resolution was confined to the consumer, which specifiers were refused, and which worker or child-process starts were blocked). |
| `error` | For `error`: the step (`inputs`, `pack`, `install`, `cluster`, `scenario`, `teardown`, `interrupted`) and its output. |

Exit codes: `0` every case passed and the stack is gone, `1` the scenario ran
and did not pass, `2` the oracle could not run or could not confirm its stack
was removed, `130` interrupted. The scenario's standard error is kept next to
the result as `scenario-stderr.log`.

`seed` is `null` and `faultMode` is `none`: a run is a real-service execution,
not a seeded simulation, and injects no failure. It establishes the five
named delivery cases for the recorded package tuple on the recorded topology.
Failover, partitions, throughput and latency are covered by the integration
lanes and the simulator, not by this oracle. Each run produces fresh
observations; a result applies only to the tuple it records.
