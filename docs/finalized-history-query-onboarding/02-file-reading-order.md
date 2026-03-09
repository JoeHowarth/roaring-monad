# Finalized History Query File Reading Order

This is the recommended reading order for understanding the new finalized-history-query plan and mapping it back to the current codebase.

Unlike the existing finalized-log-index onboarding set, this guide is target-oriented:

- it starts from the planned architecture
- it then maps planned modules to today’s files
- it highlights where the current code will be split

## Pass 1: Read The Target Architecture First

1. `docs/finalized-history-query-migration-plan.md`
   Purpose:
   the target architecture, service boundary, pagination model, and migration phases.

   Look for:
   - shared substrate vs family vs method adapters
   - transport-free `query_logs`
   - `QueryPageMeta`
   - resume-token semantics
   - deferred work

2. `docs/finalized-history-query-onboarding/01-architecture-roadmap.md`
   Purpose:
   the conceptual model and why the boundaries are chosen the way they are.

3. `docs/finalized-history-query-onboarding/03-high-level-pseudocode.md`
   Purpose:
   the target control flow without the current file-layout noise.

Checkpoint:

At this point you should know what the target service contract is and why it is not centered on `eth_getLogs`.

## Pass 2: Map The Current Public Surface To The Target One

4. `crates/finalized-log-index/src/api.rs`
   Future mapping:
   - `api/service.rs`
   - `api/query_logs.rs`
   - `core/runtime.rs`

   Look for:
   - `FinalizedLogIndex`
   - `FinalizedIndexService`
   - runtime degraded / throttled behavior
   - maintenance / GC / pruning methods that stay on the concrete service

5. `crates/finalized-log-index/src/domain/filter.rs`
   Future mapping:
   - `core/clause.rs`
   - `logs/filter.rs`
   - method-layer request / budget types

   Look for:
   - `Clause<T>`
   - `QueryOptions`
   - `LogFilter`

6. `crates/finalized-log-index/src/domain/types.rs`
   Future mapping:
   - shared finalized-history view types
   - `logs/types.rs`

   Look for:
   - `Log`
   - `Block`
   - `MetaState`
   - `BlockMeta`

Checkpoint:

You should now be able to name the current types that will become:

- transport-free method-layer types
- shared view types
- log-family types

## Pass 3: Read The Shared Substrate Candidates

7. `crates/finalized-log-index/src/domain/keys.rs`
   Future mapping:
   - `streams/keys.rs`

   Look for:
   - shard math
   - `stream_id`
   - local vs global log ID helpers
   - locator page keys

8. `crates/finalized-log-index/src/codec/manifest.rs`
   Future mapping:
   - `streams/manifest.rs`

   Look for:
   - `Manifest`
   - `ChunkRef`
   - tail encoding

9. `crates/finalized-log-index/src/codec/chunk.rs`
   Future mapping:
   - `streams/chunk.rs`

   Look for:
   - roaring bitmap chunk encoding
   - checksum / count invariants

10. `crates/finalized-log-index/src/query/planner.rs`
    Future mapping:
    - `streams/planner.rs`
    - `logs/index_spec.rs`

    Look for:
    - overlap estimation
    - clause ordering
    - what is truly generic vs what is log-specific

11. `crates/finalized-log-index/src/ingest/engine.rs`
    Future mapping:
    - `streams/writer.rs`
    - `logs/ingest.rs`

    Read this file in this internal order:
    - `collect_stream_appends`
    - `apply_stream_appends`
    - `should_seal`
    - `ingest_finalized_block`

Checkpoint:

You should now be able to explain what belongs in the shared roaring stream substrate and what must stay in the logs family.

## Pass 4: Read The Target Query Path In Today’s Files

12. `crates/finalized-log-index/src/query/engine.rs`
    Future mapping:
    - `api/query_logs.rs`
    - `core/range.rs`
    - `logs/window.rs`

    Look for:
    - block-range clipping
    - current `block_hash` mode
    - current block-window to log-ID-window mapping

    Important:
    `block_hash` mode exists today but is intentionally out of scope for the new wave-1 service surface.

13. `crates/finalized-log-index/src/query/executor.rs`
    Future mapping:
    - shared candidate execution
    - `logs/materialize.rs`
    - `logs/block_scan.rs`

    Read this file in this internal order:
    - `execute_plan`
    - `fetch_union_log_level`
    - `load_stream_entries`
    - `intersect_sets`
    - `load_log_by_id`
    - `exact_match`
    - `execute_block_scan`

    Look for:
    - where the current executor loses the primary ID
    - where `max_results` stops execution today
    - why pagination metadata requires a lookahead change
    - why block scan remains family-specific in wave 1

Checkpoint:

You should now be able to describe the target wave-1 `query_logs` path as:

1. resolve block window
2. resolve log-ID window in the logs family
3. run indexed query or log-specific block scan
4. preserve IDs long enough to emit page metadata
5. return `QueryPage<Log>`

## Pass 5: Read The Shared-State Codec Boundary

14. `crates/finalized-log-index/src/codec/log.rs`
    Future mapping:
    - `codec/finalized_state.rs`
    - log-only payload / locator codecs

    Look for:
    - `encode/decode_meta_state`
    - `encode/decode_block_meta`
    - `encode/decode_log`
    - locator-page codecs

Checkpoint:

You should now understand why wave 1 uses shared view types over the existing persisted bytes instead of redesigning those bytes immediately.

## Pass 6: Read The Tests With The New Boundary In Mind

15. `crates/finalized-log-index/tests/finalized_index.rs`
    Read for:
    - ingest/query lifecycle
    - broad-query policy
    - current limit behavior
    - backend degradation behavior

16. `crates/finalized-log-index/tests/differential_and_gc.rs`
    Read for:
    - naive-vs-indexed equivalence
    - GC / recovery interaction

While reading, ask:

- which tests will stay end-to-end?
- which ones should move into shared substrate tests?
- which new tests are needed for:
  - resume tokens
  - `has_more`
  - empty-page cases
  - invalid request rejection for `limit = 0`
  - invalid request rejection for `resume_log_id` outside the resolved block window
  - indexed-path vs block-scan pagination

## Fastest Practical Reading Path

If you only have 45 minutes, read these:

1. `docs/finalized-history-query-migration-plan.md`
2. `docs/finalized-history-query-onboarding/01-architecture-roadmap.md`
3. `crates/finalized-log-index/src/api.rs`
4. `crates/finalized-log-index/src/domain/filter.rs`
5. `crates/finalized-log-index/src/domain/types.rs`
6. `crates/finalized-log-index/src/query/engine.rs`
7. `crates/finalized-log-index/src/query/executor.rs`
8. `crates/finalized-log-index/src/query/planner.rs`
9. `crates/finalized-log-index/src/ingest/engine.rs`

That set is enough to understand the target method boundary, the current implementation, and the main split points in the migration plan.
