# Redis input-drop diagnostics

[Metrics overview](../README.md#prometheus-metrics) | [Error reference](errors.md)

Replay and cursor failures use the existing operational event sink, or the default console when no sink is installed. They emit diagnostics even when metrics are disabled. Each component instance emits once per fixed reason/stage pair; counters continue incrementing. Records contain only these finite labels, never topics, connection or user IDs, payloads, or caught error fields.

| Counter | Labels | Counts |
| --- | --- | --- |
| `replay_input_dropped_total` | `reason`, `stage` | Corrupt Redis replay decode observations and failed erasure operations |
| `cursor_input_dropped_total` | `reason`, `stage` | Rejected cursor input and failed background operations |

Counts represent observations or failed operations, not unique lost messages. Reading the same corrupt value again counts again. The sorted-set replay oldest-entry probe counts separately from the delivered-range scan; the existing `replay_corruptions_total` keeps its delivered-range counting behavior. A failed cursor command batch counts once even when several commands fail. These counters aggregate across instances sharing a metrics registry.

Replay reports `corrupt_entry` at `decode`. Both replay backends and cursor report erasure observations at `erase`: `scan_failed`, `read_failed`, `corrupt_entry`, and `delete_failed`; replay also reports `extractor_failed` when `forgetUserId` throws. Erasure remains best effort: its removed count is partial after a reported failure, and zero does not establish that every candidate was inspected. Cursor deletion failures also retain the existing `remove` diagnostic. A valid entry belonging to another user is normal absence.

`migrateReplayToStream` emits the same replay diagnostic for `corrupt_entry` and `non_monotonic` at `migration`, once per pair per migration call. Its existing result counts still describe entries copied, and it has no metrics option.

Cursor uses the following additional finite labels:

| Stage | Reasons |
| --- | --- |
| `relay_receive` | `invalid_raw`, `invalid_json`, `invalid_envelope`, `delivery_failed` |
| `relay_send` | `serialization_failed`, `breaker_open`, `redis_failure` |
| `snapshot_read` | `corrupt_entry`, `breaker_open`, `redis_failure` |
| `snapshot_write` | `serialization_failed`, `breaker_open`, `redis_failure` |
| `catch_up` | `breaker_open`, `redis_failure`, `delivery_failed` |
| `subscribe` | `breaker_open` |
| `remove` | `breaker_open`, `redis_failure` |
| `update` | `invalid_envelope`, `data_limit`, `serialization_failed` |

Expiry, own-instance relay echoes, and topics with no local subscribers are normal absence and do not count. Existing subscribe, subscriber, cleanup and scheduler diagnostics retain their own failure reporting.

For corruption, check that all writers use compatible formats. A skipped stored value is not repaired by diagnostics. For backend failures, inspect connectivity, permissions and breaker health. Later valid input and successful operations continue normally; the counter distinguishes an incomplete result from a correctly empty result after the instance's first warning has already been emitted.
