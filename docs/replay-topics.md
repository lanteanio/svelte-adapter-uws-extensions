# Replay topic limit and generations

[Redis replay](../README.md#replay-buffer-redis) | [Postgres replay](../README.md#replay-buffer-postgres) | [Migration guide](../MIGRATION.md)

The Redis (sorted-set and stream) and Postgres replay buffers bound the topics that hold replay state with `maxTopics`, and give every seq space an epoch that is never handed out twice. This page describes both, what they cost and what they delete.

## The topic limit

`maxTopics` has the core replay plugin's name, default (100) and least-recently-used eviction: creating a topic past the cap evicts the least recently used topic. Eviction deletes that topic's buffer, sequence counter and epoch (Postgres: its events and its `svti_replay_seq` row), so its history is gone. A client resuming an evicted topic with the epoch it was given rehydrates; a client that only holds the baseline epoch 0 gets `truncated`, because a nonzero position on a topic with no seq space names a numbering the store no longer holds. The topic's next publish starts at seq 1 under a new generation.

The core plugin's buffer lives in one process, so its cap is per process. These buffers are one store shared by every instance using the same Redis key prefix or Postgres table, so the cap is per store: 100 topics across the whole cluster, not per instance. A per-instance cap would let one instance evict a topic another instance is serving, because no instance sees the others' use. Size `maxTopics` for the number of topics the whole deployment keeps replay history for.

A topic's use is its last publish or `clearTopic`. Reads - `seq`, `since`, `gap`, `replay` and resume - do not refresh it, so a resume storm never turns into writes; a topic that is only read is evicted before one that is written.

Topics stored before the limit existed count. On Redis the first topic created after the upgrade registers every existing topic as least recently used, and each later creation evicts the oldest past the cap; a topic published since the upgrade has its use recorded and is kept over one that was not. On Postgres every existing sequence row counts from schema version 3 on, with the migration time as its last use, and the bounded cleanup also drains a surplus when no topic is created.

## Cost

A publish to an existing topic is unchanged in round trips: one `EVAL` on the topic's own Redis slot, which also stamps `replay:used:{topic}`, or one Postgres statement, whose row update also stamps `last_used_at`. Reads issue no writes.

Creating a topic on Redis costs two more round trips: one to the `{replay-topics}` slot, which holds the generation counter and the topic registry, and the write retried with the reserved generation. When the registry is over the cap an eviction pass follows: the least recently registered entries are checked against their own use stamps in one pipelined round trip, an entry used since it was registered is moved up instead of evicted, and the registry is updated once. The registry is never written by a steady publish, so it does not concentrate publish traffic on one slot.

Creating a topic on Postgres costs one more statement, which deletes up to four topics past the cap with their events; the cleanup pass deletes eight per statement. Both read the sequence table, which the cap bounds; it carries no index on `last_used_at`, so a publish's row update stays a heap-only update.

## Generations

Every epoch comes from one counter per store: the Redis key `replay:generation:{replay-topics}`, or the Postgres sequence `svti_replay_seq_epoch_seq`. A topic takes a new value whenever its seq space restarts: when it is created, when it is created again after expiry, eviction, `clear()` or a lossy reshard, and on `clearTopic`. Because no value is handed out twice, a topic's metadata can be deleted without a recreated topic coming back under an epoch a client holds for its predecessor:

- With a `ttl`, every Redis key of a topic, the epoch and use stamp included, expires after `ttl` seconds without a publish. The Postgres cleanup deletes the sequence row of a topic not published for longer than the `ttl`, whose events have expired anyway.
- `clear()` deletes every topic's keys or rows, epochs and the Redis registry included. Only the counter survives.
- `clearTopic(topic)` drops the topic's events, resets its counter to 0 and sets a new epoch; the next publish continues under that epoch, so a client that subscribed after the clear resumes by gap-fill.

Epochs stored before the counter existed are raised into it once. On Redis, the first reservation that finds the counter missing scans the `replay:*` keys once per deployment, sets the counter to the highest stored epoch and registers the existing topics; reservations wait for that pass. Postgres schema version 3 starts the sequence above the highest stored epoch when it creates it. Writers of an earlier release take epochs outside the counter (a new topic gets epoch 1), so stop them before writers of this release start.

Back up and restore the counter with the topics it covers, and never restore it below a stored epoch: a lower counter hands an epoch out again. For the same reason, empty the Postgres tables with `clear()` or `DELETE`, not `TRUNCATE ... RESTART IDENTITY`, which restarts the owned epoch sequence. The recovery verifier checks the Postgres sequence against the highest stored epoch (`serial_sequence_authority` on `replay-sequence`).
