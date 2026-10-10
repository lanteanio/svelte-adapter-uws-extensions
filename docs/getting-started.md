# First success

Run a shared cursor from join through move, stored snapshot and removal. Start
with the in-memory Redis double, then repeat the same assertions through two
independent connections to a real Redis server. Both routes use the public
extensions factories and testing helpers; the sockets are test connections.
This demonstrates shared state and relay delivery without requiring a Svelte
application or opening a browser.

## Prerequisites

Use Node 22 or newer and Git. The checked-out adapter selects the supported
Node ABI, operating system, architecture and Linux libc combination. Run its
preflight below and continue only after `preflight OK`. A native addon that is
missing, incompatible or from the wrong adapter build is a failure, even for
the mock route. The minimum CI route uses Node 22.0.0 on Ubuntu 24.04 with glibc.
The preflight is authoritative for other machines.

The mock route needs no Redis, Postgres or Docker server. The real route also
needs a running Docker daemon that can start Linux containers and pull
`redis:7.4-alpine`. On Windows, run the commands in PowerShell or Command Prompt;
if PowerShell refuses `npm.ps1`, use `npm.cmd` in place of `npm`.

## Clone and run without a backend

These commands exercise the current development checkout. Its committed lock
selects the dependencies; it may contain fixes beyond the published package.
For installation into an existing application, use the version tuple in the
[README installation section](../README.md#installation).

```sh
git clone --branch dev --single-branch https://github.com/lanteanio/svelte-adapter-uws-extensions.git
cd svelte-adapter-uws-extensions
npm ci
npm run preflight
npm run smoke -- --mock-only
```

Stop at the first failed command. `npm ci` installs the locked dependencies;
there is no build step. The preflight prints the Node version and ABI,
OS/architecture, libc on Linux, and the loaded native addon version. Success
includes `preflight OK`. The smoke prints resolved versions followed by:

```text
PASS mock Redis: two cursor instances shared join, move, snapshot and remove
smoke: mock-only complete; real Redis was not exercised
```

A writer moves to `{ x: 12, y: 34 }` in `canvas`. A second tracker receives the
join and movement, reads the same stored cursor, then observes its removal and
an empty snapshot. Failed assertions exit nonzero. The script closes both
trackers and their clients; this route creates no external state.

## Repeat against real Redis

Start Docker, then run from the same checkout:

```sh
npm run smoke
```

The command runs the mock assertions, checks the Docker daemon, starts a Redis
container on a Docker-assigned loopback port, and repeats the flow using two
independent Redis connections. It prints the server version and finishes with:

```text
PASS real Redis: independent connections shared join, move, snapshot and remove
smoke: all checks passed; clients and container closed
```

The first run may take longer while Docker pulls the image. The script removes
its uniquely named `extensions-smoke-...` container on success, assertion failure
or Ctrl+C. It uses no persistent volume and does not connect to your existing
Redis server. If the process is forcibly killed, list leftovers with
`docker ps -a --filter name=extensions-smoke-`; remove the specific container
shown with `docker rm --force <container-name>`.

## When a prerequisite fails

| Failure | Next action |
|---|---|
| `npm` is unavailable or Node is below the declared floor | Install a supported Node release, reopen the terminal, then restart at `npm ci`. |
| `npm ci` cannot fetch dependencies | Fix the reported network, registry or GitHub access error; repeat `npm ci` without rewriting the lock. |
| Preflight reports unsupported OS, architecture or libc | Use a supported native environment. For Linux, use the glibc environment named in the preflight; Alpine/musl has no supported prebuilt addon. |
| Preflight reports a missing or mismatched native addon | Follow the exact archive install command printed by the installed adapter, then repeat the preflight. Do not continue after an install that omitted a failed optional addon. |
| `docker info failed` | Start Docker, enable Linux containers and confirm the current user can reach its daemon; rerun the real route. |
| An assertion fails after prerequisites pass | Keep the complete command output, including versions, and open a [bug report](https://github.com/lanteanio/svelte-adapter-uws-extensions/issues/new?template=bug_report.yml). |

The minimum CI job runs the same post-clone commands in the same order: clean
installation, preflight, mock smoke, real smoke. The PR verification lane also
runs the preflight before tests and smoke. A successful mock route proves only
the in-memory demonstration; real Redis success is reported separately.

## Use the primitives in your application

Inspect the runnable [smoke source](https://github.com/lanteanio/svelte-adapter-uws-extensions/blob/dev/scripts/smoke.js)
for the public calls: `createCursor`, `attach`, `update`, `list` and `remove`.
Replace the testing platform and sockets with your adapter's platform and real
connections, and configure topic authorization before exposing them to users.
Continue with [Redis client setup](../README.md#redis-client) and
[cursor tracking](../README.md#cursor). To contribute a change,
follow the [change-to-test map](https://github.com/lanteanio/svelte-adapter-uws-extensions/blob/dev/CONTRIBUTING.md#change-to-test-map).
