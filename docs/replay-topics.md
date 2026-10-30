# Replay topic limit and epochs

[Redis replay](../README.md#replay-buffer-redis) | [Postgres replay](../README.md#replay-buffer-postgres) | [Migration guide](../MIGRATION.md)

The Redis (sorted-set and stream) and Postgres replay buffers bound the topics that hold replay state with `maxTopics`, and give every seq space an opaque random epoch. This page describes both, what they cost and what they delete.

## The topic limit

`maxTopics` has the core replay plugin's name, default (100), validation and least-recently-used eviction: creating a topic past the cap evicts the least recently used topic. Eviction deletes that topic's buffer, sequence counter and epoch (Postgres: its events and its `svti_replay_seq` row), so its history is gone. A client resuming an evicted topic with the epoch it was given rehydrates; a client that only holds the baseline epoch 0 gets `truncated`, because a nonzero position on a topic with no seq space names a numbering the store no longer holds. The topic's next publish starts at seq 1 under a newly drawn epoch.

The cap is one per store: every instance using the same Redis key prefix or Postgres table shares it. The core plugin refuses to run in a multi-worker runtime, so an application on it has exactly one buffer of `maxTopics` topics; a store shared by every instance is the one buffer here, and one cap for it is the same contract. Size `maxTopics` for the number of topics the whole deployment keeps replay history for, with room for growth. A sustained rate on `replay_topic_evictions_total` (label-free, counted where the eviction happens) means the cap is too small for the topics in use.

A topic's use is its last publish, its `clearTopic`, and its reads: `seq`, `since`, `gap` and `replay` (and so resume) count as use, as in the core plugin. A read stamps the use at most once a minute per topic per process, inside the round trip or statement it already sends, so a resume storm does not add round trips.

## Cost

A publish to an existing topic is unchanged in round trips: one `EVAL` on the topic's own Redis slot, which also stamps `replay:used:{topic}`, or one Postgres statement, whose row update also stamps `last_used_at`. A read is unchanged in round trips too; at most once a minute per topic it carries the stamp (Redis: one more script in the read's pipeline, on the same slot; Postgres: an `UPDATE` in the read statement's `WITH`, which waits for the row lock of a publish to the same topic in flight).

Creating a topic on Redis costs two more round trips: one to the `{replay-topics}` slot, which holds the topic registry, and the write retried with a drawn epoch. When the registry is over the cap an eviction pass follows: the least recently registered entries are checked against their own use stamps in one pipelined round trip, an entry used since it was registered is moved up instead of evicted, and the registry is updated once. The registry is never written by a steady publish or a read, so it does not concentrate their traffic on one slot.

Creating a topic on Postgres costs one more statement, which deletes up to four topics past the cap with their events. It finds the cutoff, the newest row past the cap, without locks, and then locks only the rows at or below it, skipping rows another session holds and re-checking `last_used_at` under the lock, so a publish to a hot topic is never blocked by an eviction. The bounded cleanup reads the cutoff once per pass and then removes topics in short autocommit statements of about 2,000 event rows each (2,000 / `size` topics, between 8 and 256), up to 63 statements within the pass's one-second deadline; while a backlog remains the next pass follows after 100 ms. Draining a large surplus therefore does not sort the table per batch, holds each row lock only for its statement, and removes up to about 500 topics a second at the default `size` of 1000 (more for a smaller `size`), so 100,000 leaked topics drain in a few minutes, bounded by how fast the database deletes their events. The sequence table carries no index on `last_used_at`, so a publish's row update stays a heap-only update.

## Waits

Two operations can wait, and nothing else does:

- **A Postgres read that carries the use stamp** (at most once a minute per topic per process) waits for the row lock of a publish to the same topic in flight, for as long as that publish's statement runs. A read that does not carry the stamp, and every Redis read, never waits for a lock.
- **Creating a Redis topic before the deployment is seeded** waits up to two seconds for the seed scan, so the cap counts the existing topics, and then goes on without it. This applies only until the one-time scan completes; once the deployment is seeded no creation waits.

## Epochs

As in the adapter, an epoch is an opaque random 32-bit integer (1 .. 2^32 - 1; 0 means no seq space), compared only for equality. It is drawn from the runtime RNG when a topic's seq space is created or recreated - when the topic is created, created again after expiry, eviction, `clear()` or a lossy reshard, and on `clearTopic` - and stored with it. It carries no time and no count, so a client learns nothing from it about when or how often topics are created. The creating script never keeps the epoch still stored for the topic: a draw equal to it is replaced by the next value. The accepted failure is a 2^-32 collision with an epoch the topic had before and no longer stores, which costs one missed reset, never corruption.

Because a recreated topic draws anew, its metadata can be deleted:

- With a `ttl`, every Redis key of a topic, the epoch and use stamp included, expires after `ttl` seconds without a publish. The Postgres cleanup deletes the sequence row of a topic not used for longer than the `ttl`, whose events have expired anyway.
- `clear()` deletes every topic's keys or rows, epochs and the Redis registry included.
- `clearTopic(topic)` drops the topic's events, resets its counter to 0 and sets a new epoch; the next publish continues under that epoch, so a client that subscribed after the clear resumes by gap-fill.

All times the Redis scripts record, the use stamps and registry scores included, come from the server's `TIME`, so instance clock skew does not reorder topics.

## Topics stored before the limit

Topics stored before the upgrade count toward the cap.

- **Postgres.** Schema version 3 starts each existing topic's `last_used_at` at its newest event's `created_at` (1970 for a topic with no event left), so eviction keeps the most recently published topics. The bounded cleanup drains a surplus even when no topic is created.
- **Redis.** Neither buffer stores a time per entry (sorted-set members carry the seq, stream ids are `<seq>-0`). With a `ttl`, a publish resets the topic's expiry, so the scan ranks each existing topic by its seq key's remaining TTL: the most recently published are kept. Without a `ttl` existing topics enroll as least recently used, all equal, and the first creations past the cap evict them in topic-name order. Either way a topic published or read after the upgrade has its use stamped and is kept over one that was not. A topic left with an epoch or use stamp but no seq key has no seq space; the scan deletes those leftovers rather than enrolling them.

The Redis scan of the replay keys runs once per deployment, in one instance: it holds a lease, keeps its cursor in `replay:seed:{replay-topics}` so a failed pass resumes where it stopped, and marks the deployment seeded when done. The scan needs `SCAN`, and the scripts need `TIME`, in the Redis user's ACL.

When the topics found exceed `maxTopics`, the store reports it once per process (`EXTENSIONS-ERR-REDIS-REPLAY-TOPICS-OVER-CAP`, `EXTENSIONS-ERR-POSTGRES-REPLAY-TOPICS-OVER-CAP`) with the count and the cap.

Writers of an earlier release number epochs 1, 2, ... per topic instead of drawing them, so stop them before writers of this release start. An earlier Postgres writer that starts after version 3 is in the ledger refuses to run, but one already running keeps writing; an earlier Redis writer cannot be detected.

Back up and restore a topic's epoch with its sequence and buffer.
