# Real-cluster oracle

[Package overview](../README.md) | [Release verification](./releasing.md)

The real-cluster oracle is a repository script that proves cross-instance
delivery through packed packages and a real Redis Cluster. It never substitutes
an in-memory store or the simulator. It is not part of the published package
API: run it from a checkout of this repository at the commit you want to
attest.

```sh
npm ci
npm run oracle:real-cluster -- --realtime ../svelte-realtime-candidate.tgz
```

The script:

1. packs this checkout with `npm pack`;
2. installs that tarball, `svelte-adapter-uws`, `svelte-realtime`, their peers
   and the adapter's pinned native `uWebSockets.js` into a fresh consumer;
3. starts the Redis Cluster of the cluster integration lane
   (`test/integration/docker-compose.cluster.yml`: three masters, three
   replicas, Redis 7.4) unless an external cluster is named;
4. runs the scenario `scripts/fixtures/real-cluster-oracle.mjs` from the
   consumer, so every import resolves to the installed packages;
5. removes the Compose stack and writes the result.

## Inputs

| Option | Default | Meaning |
|---|---|---|
| `--realtime <tgz or spec>` | `next` | `svelte-realtime` to install: a tarball path or an npm version, tag or `svelte-realtime@<spec>`. |
| `--adapter <tgz or spec>` | the installed version | `svelte-adapter-uws` to install. The default installs the exact version in `package-lock.json` and fails unless the consumer receives the same integrity. |
| `--work-dir <path>` | `.real-cluster-oracle/` | Tarballs, consumer, result and scenario log. |
| `--out <file>` | `<work-dir>/result.json` | Result path. |

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
name at a time.

## Scenario

Two native uWebSockets.js test servers each hold one WebSocket client
subscribed to one topic. The source server's message hook is realtime's
`createMessage`, with realtime's bus set to an extensions `createPubSubBus` on
the cluster; the target server activates a second bus on its own cluster
client. The cases run in order:

| Case | Passes when |
|---|---|
| `setup` | The packages load from the consumer, both cluster clients connect, both servers and sockets are up and subscribed. |
| `cluster-topology` | `CLUSTER INFO` reports `cluster_state:ok`, all 16384 slots are served and there are at least three masters. |
| `single-fanout` | Six ordered `platform.publish` calls arrive at both sockets exactly once, in order. |
| `batch-fanout` | One six-member `platform.publishBatched` and one ordered barrier publish arrive at both sockets exactly once, in order. The barrier proves no duplicate is still in flight without a sleep. |
| `cleanup` | Sockets, servers, buses and connections close. It runs even after a failure. |

A pass means each socket received exactly thirteen values. The clients
announce no batch capability, so `publishBatched` takes the adapter's
per-member fallback path.

## Result

The result file is JSON with `schemaVersion: 1`:

| Field | Content |
|---|---|
| `status` | `passed`, `failed` (a case failed or did not run) or `error` (the oracle could not run). |
| `command` | The invocation. |
| `source` | This repository's commit and whether the working tree had uncommitted changes. |
| `packages` | Per package: `kind` (`tarball` or `registry`), installed `version`, and `sha256` plus `integrity` of a tarball, or the registry `resolved` URL and `integrity`; the native binding's pin and version. |
| `services` | `docker-compose` with project, Compose file, host ports, subnet and seeds, or `external` with seeds. |
| `scenario` | The scenario report: `cases` with status, duration and error, `counts` (planned, executed, passed, failed, notRun), `deliveries` per socket, `backend` (masters, replicas, engine and version per master) and `provenance` (Node version, platform, scenario SHA-256, and version, entry-file SHA-256 and `package.json` SHA-256 of every loaded package). |
| `error` | For `error`: the step (`inputs`, `pack`, `install`, `cluster`, `scenario`) and its output. |

Exit codes: `0` every case passed, `1` the scenario ran and did not pass, `2`
the oracle could not run. The scenario's standard error is kept next to the
result as `scenario-stderr.log`.

`seed` is `null` and `faultMode` is `none`: a run is a real-service execution,
not a seeded simulation, and injects no failure. It establishes the five
named delivery cases for the recorded package tuple on the recorded topology.
Failover, partitions, throughput and latency are covered by the integration
lanes and the simulator, not by this oracle. Each run produces fresh
observations; a result applies only to the tuple it records.
