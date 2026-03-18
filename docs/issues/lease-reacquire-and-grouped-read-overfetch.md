# Lease Reacquire And Grouped-Read Overfetch

## Summary

Two open issues remain in the current write-authority and query hot paths:

1. a restarted primary on the same node cannot reacquire ownership until the old lease expires
2. grouped same-block reads materialize an entire contiguous run before exact filtering or page-limit short-circuiting

Relevant current-state references are:

- `docs/write-authority.md`
- `docs/query-execution.md`

## Same-Node Restart Reacquire Gap

The active write-authority doc says same-owner acquisition should be allowed through an epoch bump, but the current lease code only permits in-place renewal for the exact same `(owner_id, session_id)` while the lease is still valid.

Current behavior in [`crates/finalized-history-query/src/ingest/authority/lease.rs`](/Users/jh/work/roaring-monad/crates/finalized-history-query/src/ingest/authority/lease.rs#L102):

1. if `owner_id` and `session_id` both match and the lease is still valid, the session renews in place
2. otherwise, if the lease is still valid, acquisition returns `LeaseStillFresh`
3. takeover with `epoch + 1` only becomes possible after expiry

That means a primary process crash followed by a restart on the same node gets stuck behind its own still-fresh lease instead of reclaiming ownership immediately with a new session and epoch.

## Why This Matters

This creates avoidable downtime for the normal restart path:

- the old process is gone, but the new process cannot resume publication
- failover behavior is better than restart behavior on the same node
- the code no longer matches the current write-authority contract in `docs/write-authority.md`

## End-State Property

Same-node restart should be treated as a fresh acquisition:

- same `owner_id`
- new `session_id`
- `epoch + 1`
- immediate fence advancement

That path should not wait for lease expiry.

## How To Fix It

Adjust acquisition so "same owner, different session" is its own pre-expiry branch:

1. if `owner_id` and `session_id` both match and the lease is still valid, keep the current in-place renewal path
2. if `owner_id` matches but `session_id` differs, attempt a CAS that writes the new `session_id`, bumps `epoch`, preserves `indexed_finalized_head`, and extends `lease_valid_through_block`
3. after a successful CAS, advance the fence for the new epoch before returning the new write token
4. only return `LeaseStillFresh` when the lease is still valid and belongs to a different `owner_id`

That keeps ordinary renewals cheap while making restart reacquire match the documented ownership model.

## Grouped-Read Overfetch

The query executor now coalesces contiguous same-block candidates, but it loads the full run before exact filtering and before honoring `take`.

Current behavior in [`crates/finalized-history-query/src/logs/query.rs`](/Users/jh/work/roaring-monad/crates/finalized-history-query/src/logs/query.rs#L289):

1. build a contiguous run of same-block candidates
2. call `load_contiguous_run(...)` for the whole run
3. only afterwards apply `exact_match`
4. only afterwards stop once `take` is satisfied

The materializer then range-reads the entire run span in [`crates/finalized-history-query/src/logs/materialize.rs`](/Users/jh/work/roaring-monad/crates/finalized-history-query/src/logs/materialize.rs#L178).

## Why This Matters

This is a bad trade in false-positive-heavy queries:

- a broad indexed clause can produce a long same-block run
- exact matching may keep only a small fraction of it
- first-page queries may hit `take` quickly once true matches appear
- the current path still pays the range-read and decode cost for the entire run

That increases cold-read work and can hurt first-page latency without improving correctness.

## End-State Property

Grouped reads should preserve the benefit of coalescing without forcing full-run eager work ahead of exact filtering.

At minimum, the read path should avoid decoding and fetching more of a run than is needed to:

- prove the next exact match
- satisfy `take`
- preserve exact pagination semantics

## How To Fix It

Do not make "full contiguous run" the materialization unit.

A better shape is:

1. keep contiguous-run discovery as a locality hint
2. materialize candidates incrementally within the run, stopping as soon as `take` is satisfied
3. range-read only a bounded prefix of the run at a time, then exact-match those items before reading more
4. carry forward any unread suffix as the next chunk rather than forcing one large eager decode

The important design point is that the coalescing unit should be bounded by useful work, not by the longest same-block candidate streak. That preserves the cold-read benefits of range locality without turning false positives into mandatory full-run reads.
