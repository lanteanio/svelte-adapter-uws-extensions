# Replay topic limit and generations

[Redis replay](../README.md#replay-buffer-redis) | [Postgres replay](../README.md#replay-buffer-postgres) | [Migration guide](../MIGRATION.md)

The Redis (sorted-set and stream) and Postgres replay buffers bound the topics that hold replay state with `maxTopics`, and give every seq space an epoch that is never handed out twice. This page describes both, what they cost and what they delete.

## The topic limit

`maxTopics` has the core replay plugin's name, default (100), validation and least-recently-used eviction: creating a topic past the cap evicts the least recently used topic. Eviction deletes that topic's buffer, sequence counter and epoch (Postgres: its events and its `svti_replay_seq` row), so its history is gone. A client resuming an evicted topic with the epoch it was given rehydrates; a client that only holds the baseline epoch 0 gets `truncated`, because a nonzero position on a topic with no seq space names a numbering the store no longer holds. The topic's next publish starts at seq 1 under a new generation.

The cap is one per store: every instance using the same Redis key prefix or Postgres table shares it. The core plugin refuses to run in a multi-worker runtime, so an application on it has exactly one buffer of `maxTopics` topics; a store shared by every instance is the one buffer here, and one cap for it is the same contract. Size `maxTopics` for the number of topics the whole deployment keeps replay history for, with room for growth. A sustained rate on `replay_topic_evictions_total` (label-free, counted where the eviction happens) means the cap is too small for the topics in use.

A topic's use is its last publish, its `clearTopic`, and its reads: `seq`, `since`, `gap` and `replay` (and so resume) count as use, as in the core plugin. A read stamps the use at most once a minute per topic per process, inside the round trip or statement it already sends, so a resume storm does not add round trips.

## Cost

A publish to an existing topic is unchanged in round trips: one `EVAL` on the topic's own Redis slot, which also stamps `replay:used:{topic}`, or one Postgres statement, whose row update also stamps `last_used_at`. A read is unchanged in round trips too; at most once a minute per topic it carries the stamp (Redis: one more script in the read's pipeline, on the same slot; Postgres: an `UPDATE` in the read statement's `WITH`, which waits for the row lock of a publish to the same topic in flight).

Creating a topic on Redis costs two more round trips: one to the `{replay-topics}` slot, which holds the generation counter and the topic registry, and the write retried with the reserved generation. When the registry is over the cap an eviction pass follows: the least recently registered entries are checked against their own use stamps in one pipelined round trip, an entry used since it was registered is moved up instead of evicted, and the registry is updated once. The registry is never written by a steady publish or a read, so it does not concentrate their traffic on one slot.

Creating a topic on Postgres costs one more statement, which deletes up to four topics past the cap with their events. It finds the cutoff, the newest row past the cap, without locks, and then locks only the rows at or below it, skipping rows another session holds and re-checking `last_used_at` under the lock, so a publish to a hot topic is never blocked by an eviction. The bounded cleanup reads the cutoff once per pass and then removes eight topics per statement in short autocommit statements, so draining a large surplus does not sort the table per batch and holds each row lock only for its statement. The sequence table carries no index on `last_used_at`, so a publish's row update stays a heap-only update.

## Generations

Every epoch comes from one counter per store: the `next` field of the Redis hash `replay:generation:{replay-topics}`, or the Postgres sequence `svti_replay_seq_epoch_seq`. A topic takes a new value whenever its seq space restarts: when it is created, when it is created again after expiry, eviction, `clear()` or a lossy reshard, and on `clearTopic`. Because no value is handed out twice, a topic's metadata can be deleted without a recreated topic coming back under an epoch a client holds for its predecessor:

- With a `ttl`, every Redis key of a topic, the epoch and use stamp included, expires after `ttl` seconds without a publish. The Postgres cleanup deletes the sequence row of a topic not used for longer than the `ttl`, whose events have expired anyway.
- `clear()` deletes every topic's keys or rows, epochs and the Redis registry included. Only the counter survives.
- `clearTopic(topic)` drops the topic's events, resets its counter to 0 and sets a new epoch; the next publish continues under that epoch, so a client that subscribed after the clear resumes by gap-fill.

The Redis counter never falls below the Redis server's clock in milliseconds: each value is the larger of the previous one plus one and the clock. If the counter itself is lost, to a FLUSH or to memory eviction, the next value therefore starts above every value issued before at no more than one per millisecond, and a seed scan raises it above every epoch still stored. All times the Redis scripts record, the use stamps and registry scores included, come from the server's `TIME`, so instance clock skew does not reorder topics.

## Topics stored before the limit

Topics stored before the upgrade count toward the cap.

- **Postgres.** Schema version 3 starts each existing topic's `last_used_at` at its newest event's `created_at` (1970 for a topic with no event left), so eviction keeps the most recently published topics. The bounded cleanup drains a surplus even when no topic is created.
- **Redis.** Neither buffer stores a time per entry (sorted-set members carry the seq, stream ids are `<seq>-0`), so the best available signal is the seq key's last access as Redis tracks it, `OBJECT IDLETIME`, which a read or a write refreshes. Under an LFU `maxmemory-policy` Redis does not track idle time; existing topics then all rank as oldest, and the first creations evict them in registry order. A topic used after the upgrade has its use stamped and is kept over one that was not.

The Redis scan of the replay keys runs once per deployment, in one instance: it holds a lease, keeps its cursor and the highest epoch seen in the counter hash so a failed pass resumes where it stopped, and marks the deployment seeded when done. A topic created while the scan runs waits for it at most two seconds and then takes a generation from the clock floor. The scan needs `SCAN`, and the scripts need `OBJECT IDLETIME` and `TIME`, in the Redis user's ACL.

When the topics found exceed `maxTopics`, the store reports it once per process (`EXTENSIONS-ERR-REDIS-REPLAY-TOPICS-OVER-CAP`, `EXTENSIONS-ERR-POSTGRES-REPLAY-TOPICS-OVER-CAP`) with the count and the cap.

Writers of an earlier release take epochs outside the counter (a new topic gets epoch 1), so stop them before writers of this release start. An earlier Postgres writer that starts after version 3 is in the ledger refuses to run, but one already running keeps writing; an earlier Redis writer cannot be detected.

Back up and restore the counter with the topics it covers, and never restore it below a stored epoch: a lower counter hands an epoch out again. For the same reason, empty the Postgres tables with `clear()` or `DELETE`, not `TRUNCATE ... RESTART IDENTITY`, which restarts the owned epoch sequence. The recovery verifier checks the Postgres sequence against the highest stored epoch (`serial_sequence_authority` on `replay-sequence`).
