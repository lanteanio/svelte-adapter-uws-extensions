# Releasing

Publication requires this package version and Node engine to match the installed adapter's compatibility manifest. `prepublishOnly` runs `node scripts/check-compatibility.js --release` before the ordinary checks; development checks permit an unpublished local version stamp, but still enforce the Node prerequisite. Coordinate tuple changes in the lead manifest before publishing.

This is the publication contract for `svelte-adapter-uws-extensions`. Release preparation does not authorize pushing tags or publishing a version. The checked-in path verifies a candidate; an authorized maintainer controls promotion.

## Release scope and acceptance

The 0.6 line provides shared Redis and Postgres state for applications whose WebSocket clients can reach different workers or instances: fan-out and cursor/presence state for connected users, replay for reconnecting clients, and durable jobs/tasks, idempotency, sessions and locks for application handlers. Its acceptance criteria are the behavior of the exact packaged code and the operator's recovery path. This package does not supply application authorization, exactly-once delivery, a managed database, or deployment-specific backup, region and retention policy. A local mock pass does not establish cross-instance or failure-recovery behavior.

The ecosystem's current promotion requirements live in the adapter's [stable-promotion contract](https://github.com/lanteanio/svelte-adapter-uws/blob/dev/docs/releasing.md#stable-promotion). Before extensions promotes to `latest`, retain evidence for the exact candidate:

- Application consumers can install all declared exports and types from the retained tarball: `verify:pr`, the packed declaration/consumer suites, `publint` and the packed type-resolution gate pass on every gated platform. The shipped examples and preflight run with the declared adapter/native tuple.
- Operators can run the affected stores against real services: `verify:release` passes both standalone Redis and Postgres lanes. For a release claiming sharded Redis or Valkey behavior, the matching cluster/failover or Valkey lane also passes; retain the server versions and configuration with those results. Relevant disconnect, retry, erasure, lease-loss and recovery regressions must pass rather than relying on a throughput result alone.
- Operators have a measured capacity envelope for the changed hot paths: retain the baseline/candidate runs, workload shape, environment, throughput and latency distribution, errors and recovery under load. The affected benchmark must show no unexplained regression. Results from another workload or a shared noisy host are not proof of a speedup or fleet-wide capacity.
- Consumers receive a qualified ecosystem tuple: the lead compatibility row names exact adapter, extensions and realtime artifacts and matching qualification heads. Its cross-repository gate is green at those heads, and wire/persistence revisions remain compatible. A `workspace` compatibility row is a development declaration, not publication or qualification evidence.
- Maintainers can account for failures and changes: operational error references and diagnostics match the shipped code; README, MIGRATION and changelog describe the exact artifact; documented migration/rollback procedures cover its storage and wire changes. No known reachable defect is left without a documented workaround and an explicit acceptance decision.
- Publication and routing identities are reconciled: all published versions have their immutable registry row and changelog heading, the candidate has retained-artifact/provenance evidence, and the required consumer checks pass before `next` or `latest` moves. A missing evidence item blocks promotion; do not weaken a gate in the change claiming readiness.

These are release acceptance conditions, not claims that the current development checkout is ready for stable publication.

## Identity and branch roles

- `dev` contains development candidates; prerelease versions use `publishConfig.tag: next`.
- `main` contains stable releases; stable versions use `publishConfig.tag: latest`.
- Prepare an annotated immutable tag `svelte-adapter-uws-extensions@<version>` on the reviewed release commit. The tag must match package name/version exactly and its commit must belong to the corresponding remote branch.
- Record the source commit and tag, retained tarball SHA-256, npm integrity/shasum, publication time and workflow run URL with the release evidence. A dist-tag is a movable routing pointer, not artifact identity.

Never reuse a version or move its Git tag. Upgrade the coordinated adapter/realtime/extensions tuple before promotion; use the [compatibility table](../README.md#version-compatibility) and each package's migration guide. A package passing its own suite does not certify a sibling's behavior.

## Verify, retain, then publish

The tag-only [release.yml](https://github.com/lanteanio/svelte-adapter-uws-extensions/blob/dev/.github/workflows/release.yml) has two jobs:

1. `verify` has only `contents: read`. Node 24.13.1 and npm 11.8.0 install the committed lock. The identity guard checks the event, repository, annotated tag, checkout and branch lineage. `verify:release` checks isolated lock regeneration, the complete dependency graph and audit, preflight, the unit/check chain, mock smoke, and standalone real Redis and Postgres integration suites. `prepublishOnly` independently invokes the check chain. Finally one publication artifact is packed, hashed and retained for 90 days; the filename and digest are separate job outputs.
2. `publish` runs in `npm-release` with `id-token: write`. It installs no repository dependency tree and executes no repository script. Fully pinned actions download the retained artifact; a closed PowerShell body compares its SHA-256 with the verify job output. npm publishes those exact bytes with provenance and scripts disabled to the quarantine `candidate` dist-tag. It does not move `next` or `latest`.

`check-release-workflow.js` checks closed workflow/job/step key inventories, exact commands and action commits, the artifact/digest bodies, and the complete package-script inventory. Script changes must update the explicit reviewed inventory in the same change. Plain local `npm publish` invokes `prepublishOnly`; overriding npm lifecycle execution deliberately bypasses that local guard and is not a supported publication route.

The standalone integration suites start and remove their own Docker Compose stacks. Cluster/Valkey variants remain separate lanes listed in [CONTRIBUTING](https://github.com/lanteanio/svelte-adapter-uws-extensions/blob/dev/CONTRIBUTING.md#verification-inventory); a release needing those surfaces must additionally run their lanes and retain the evidence. Hosted execution and an actual registry publication cannot be inferred from a passing local workflow test.

## External setup

Before the first authorized release, configure the npm trusted publisher for package `svelte-adapter-uws-extensions`, GitHub owner `lanteanio`, repository `svelte-adapter-uws-extensions`, workflow filename `release.yml`, and environment `npm-release`. Configure that GitHub environment with required reviewers, prevent self-review, and restrict deployment to the package's version tags. Protect `dev`, `main`, and version tags against unreviewed updates/deletion. This repository cannot prove these account-level settings from local files.

Follow the [npm trusted-publisher setup](https://docs.npmjs.com/trusted-publishers/) for the account-side identity. Use public GitHub-hosted runners and npm trusted publishing/OIDC; there is no token fallback. The workflow fails closed if trust, environment approval, identity, tests, artifact transfer, or provenance publication fails. Configure trust and review the concrete candidate before pushing a release tag.

After verifying trusted publication, revoke existing automation tokens with publish access and disable token publication for this package in npm. Remove registry tokens from repository and environment secrets. Keep the OIDC workflow as the sole publication identity; a local lifecycle check does not prevent someone holding a valid token from publishing unrelated bytes.

## Candidate promotion and recovery

After quarantine publication, install the exact `package@version` in a clean consumer, run preflight and the affected real-backend route, inspect npm provenance/integrity against the retained evidence, and verify the coordinated ecosystem tuple. Only then may an authorized maintainer move `next` or `latest` to that immutable version. A failed candidate stays out of those channels; fix it under a new version.

For an incident, capture the currently routed version and integrity before changing anything. Restore the intended dist-tag to the previously verified immutable version, reinstall that exact artifact, and follow [MIGRATION](../MIGRATION.md) for storage/wire compatibility. Moving a dist-tag does not roll back persistent data or already installed consumers. A stable hotfix starts from the recorded stable release tag, receives a new version and the same verification, and its source change is merged back to `dev` through review.

## Preflight and freeze

Choose a canonical npm SemVer without build metadata. Freeze the reviewed source SHA and run the full release verification contract above. Append a `proposed` candidate event with its SHA, planned exact tag, target channel, UTC time and evidence. The development package version may still equal a previously published baseline; its value alone never proves publication or candidate readiness. Change version, lockfile and publication channel together before preparing an actual new candidate.

Once tagged, source changes require a new version and candidate. Append `tagged` only after the annotated tag actually exists. Never delete, reuse, or retarget a release tag. Do not invent future events, timestamps, digests or successful runs in order to prepare the manifest.

## Prerelease publication

For a prerelease, freeze on `dev` and target `next`. The verified workflow publishes the retained tarball to `candidate` first. Only after actual registry metadata exists, append the immutable published row and `published` candidate event. Preserve its npm `gitHead`, integrity, shasum and publication time from the primary registry record; link the exact workflow run and retained digest in Notes. Append the corresponding `candidate` routing event. Every non-legacy published identity needs the complete `proposed -> tagged -> published` lifecycle and candidate quarantine route.

After exact-artifact consumer verification and ecosystem review, append a forward routing event for moving `next`. Record the observed old and new versions and the operation's UTC time. Normal promotion must strictly increase npm SemVer precedence. Perform coordinated family promotion in dependency order: adapter, extensions, then realtime. Each owner retains its own verification evidence.

## Stable promotion

Freeze the reviewed stable source on `main`, select a fresh stable version and set `publishConfig.tag` to `latest`. Run the same verification and quarantine lifecycle. Stable promotion publishes a new immutable version; it does not rename a prerelease or infer sibling compatibility from one package's suite. Move `latest` only after exact-artifact acceptance and the reviewed compatibility tuple. Record the previous and new immutable identities in the routing event.

## Abort

Before publication, append `aborted` to a proposed or tagged candidate, record the failure evidence and leave existing tags intact. After registry publication, retain the published identity even if acceptance fails. Record `routing-aborted` for the quarantined artifact, remove or restore the candidate pointer as recorded, and prepare fixes under a new version. Never use `npm unpublish` as an abort or rollback procedure.

## Stable hotfix

Fork from the recorded immutable stable release tag, apply the smallest reviewed repair, and choose a new stable version. Verify, quarantine and promote through the same gates. Merge the repair back to `dev` through review before resuming ordinary development promotion; retain links to both the stable change and merge-back. An unreviewed development head is not a substitute for the stable incident baseline.

## Dist-tag rollback

Record the actual current channel pointer and both registry identities before a rollback. Append a `rollback` event whose From version matches that pointer and whose To version is an already published, verified artifact. Restore the channel to that exact version, then reinstall and test it in the affected deployment. This routing operation does not revert persistent storage, wire changes or consumers which already installed another version; apply the migration/restore procedure separately when needed. Never rewrite an old published row to describe the new route.

## Manifest maintenance

[release-manifest.md](./release-manifest.md) is append-only. Candidate decisions, immutable published identities and routing events are separate tables. Historical `legacy-none` rows are checked against the captured npm baseline; no historical Git tags or routing movements are fabricated. Preserve every published row and append future identities only after registry confirmation. To correct an event, append a complete `correction` event with its own ID/time, the original event ID, corrected event/time and complete corrected payload; never edit the event in place. The state validator applies corrections and then checks route continuity and candidate identity.

Every published version has exactly one dated heading in CHANGELOG. An unpublished candidate or superseded version carries its explicit not-published marker immediately under the heading. When registry publication is confirmed, remove that marker and append the published identity together. `node scripts/check-release-ledger.js` validates this offline, including lifecycle transitions, immutable identities, quarantine, monotonic promotion and rollback continuity; it runs in `npm run check`. The ledger is evidence recorded after actions, not authorization to publish or move a channel.
