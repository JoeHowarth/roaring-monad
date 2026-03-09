# Finalized Log Index File Reading Order

This is the recommended file order for reading the indexing and query core. The order is optimized for understanding, not for implementation chronology.

## Pass 1: Build the top-level frame

1. `crates/finalized-log-index/src/lib.rs`
   Purpose: module map and public exports.

2. `crates/finalized-log-index/src/api.rs`
   Look for:
   - `FinalizedLogIndex`
   - `FinalizedIndexService`
   - where ingest/query/maintenance/GC are delegated
   - runtime degraded vs throttled state

3. `crates/finalized-log-index/src/config.rs`
   Look for:
   - `BroadQueryPolicy`
   - `IngestMode`
   - chunk/tail/planner thresholds

## Pass 2: Learn the domain and persisted keyspace

4. `crates/finalized-log-index/src/domain/types.rs`
   Look for:
   - `Log`
   - `Block`
   - `BlockMeta`
   - `MetaState`

5. `crates/finalized-log-index/src/domain/filter.rs`
   Look for:
   - `Clause`
   - `LogFilter`
   - `QueryOptions`
   - `max_or_terms`

6. `crates/finalized-log-index/src/domain/keys.rs`
   Look for:
   - key naming conventions
   - `stream_id`
   - locator-page and chunk key helpers
   - how `index_kind`, hex-encoded value, and shard are combined into one physical stream key

Checkpoint:

At this point you should be able to name every major logical record and the key prefix where it lives.

## Pass 3: Learn the physical record formats

7. `crates/finalized-log-index/src/codec/log.rs`
   Look for:
   - `encode/decode_meta_state`
   - `encode/decode_block_meta`
   - `encode/decode_log`
   - `encode/decode_log_locator_page`

8. `crates/finalized-log-index/src/codec/manifest.rs`
   Look for:
   - `Manifest`
   - `ChunkRef`
   - `encode/decode_manifest`
   - `encode/decode_tail`

9. `crates/finalized-log-index/src/codec/chunk.rs`
   Look for:
   - `ChunkBlob`
   - checksum behavior
   - roaring bitmap serialization

Checkpoint:

You should now understand what the ingest path writes and what the query path later reads.

## Pass 4: Read ingest in the right order

10. `crates/finalized-log-index/src/ingest/planner.rs`
    Purpose: tiny file, but it tells you what kinds of stream fanout to expect.

11. `crates/finalized-log-index/src/ingest/engine.rs`
    Read this file in this internal order:
    - `ingest_finalized_block`
    - `collect_stream_appends`
    - `apply_stream_appends`
    - `should_seal`
    - `load_state` / `store_state`
    - `load_manifest` / `store_manifest`

    Questions to hold while reading:
    - How are global log IDs assigned?
    - Why are locator pages written before stream appends complete?
    - When does a tail become a chunk?
    - Which streams are appended for each log?

12. `crates/finalized-log-index/src/ingest/chunk_manager.rs`
    Purpose: older/smaller extraction of the seal logic. Useful as a simplified conceptual version, even though `engine.rs` carries the full active path.

13. `crates/finalized-log-index/src/ingest/tail_manager.rs`
    Purpose: simplified load/store/append model for tails.

Checkpoint:

You should now be able to explain how one block mutates:

- `meta/state`
- `block_meta/*`
- `block_hash_to_num/*`
- `log_locator_pages/*`
- `manifests/*`
- `tails/*`
- `chunks/*`
- `log_packs/*`

## Pass 5: Read query in the right order

14. `crates/finalized-log-index/src/query/engine.rs`
    Look for:
    - range clipping against finalized head
    - `block_hash` special case
    - transition from filter to `QueryPlan`

15. `crates/finalized-log-index/src/query/planner.rs`
    Read for:
    - `QueryPlan`
    - `ClauseKind`
    - broad-query policy
    - overlap estimation from manifests and tails
    - the shared chunk-range helper used by both planner and executor

16. `crates/finalized-log-index/src/query/executor.rs`
    Read this file in this internal order:
    - `execute_plan`
    - `fetch_union_log_level`
    - `load_stream_entries`
    - `maybe_merge_chunk`
    - `intersect_sets`
    - `load_log_by_id`
    - `exact_match`
    - `execute_block_scan`

Why this order:

- first understand how candidate sets are built
- then see how shard-local ranges are pushed down so binary search can isolate the overlapping chunk slice
- then note the full-shard fast path, where the planner uses manifest counts directly and the executor skips pointless range checks
- then notice that `topic0` uses the same indexed path as the other topic clauses
- then understand how actual logs are materialized
- then look at the fallback scan path

Checkpoint:

You should now be able to describe the difference between:

- planner estimation
- candidate generation
- exact predicate validation
- final materialization of logs

## Pass 6: Read the backend contracts

17. `crates/finalized-log-index/src/store/traits.rs`
    Look for:
    - minimal APIs required by ingest/query
    - CAS conditions
    - fencing

18. `crates/finalized-log-index/src/store/meta.rs`
    Purpose: simplest reference implementation of the meta store contract.

19. `crates/finalized-log-index/src/store/blob.rs`
    Purpose: simplest reference implementation of the blob store contract.

20. `crates/finalized-log-index/src/store/fs.rs`
    Purpose: file-backed version if you want a more concrete persistence model.

## Pass 7: Validate with tests

21. `crates/finalized-log-index/tests/finalized_index.rs`
    Read for:
    - basic ingest/query lifecycle
    - broad query policy
    - block-hash mode
    - backend degradation behavior
    - narrow-range query behavior that only loads overlapping chunks

22. `crates/finalized-log-index/tests/differential_and_gc.rs`
    Read for:
    - query equivalence against a naive implementation
    - how the persisted structures interact with GC/recovery

## Optional Follow-On Files

23. `crates/finalized-log-index/src/recovery/startup.rs`
    Read when you want startup semantics.

24. `crates/finalized-log-index/src/gc/worker.rs`
    Read when you want lifecycle cleanup semantics.

25. `crates/finalized-log-index/src/store/scylla.rs`
26. `crates/finalized-log-index/src/store/minio.rs`
    Read when you need production/distributed backend details.

## Fastest Practical Reading Path

If you only have 45 minutes, read these and ignore the rest initially:

1. `src/api.rs`
2. `src/domain/types.rs`
3. `src/domain/filter.rs`
4. `src/domain/keys.rs`
5. `src/codec/log.rs`
6. `src/codec/manifest.rs`
7. `src/ingest/engine.rs`
8. `src/query/engine.rs`
9. `src/query/planner.rs`
10. `src/query/executor.rs`
11. `tests/finalized_index.rs`

That set covers almost all of the architecture that matters for core indexing and query behavior.
