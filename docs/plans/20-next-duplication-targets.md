Yes. The next best targets are:

1. `logs/query/stream_bitmap.rs` vs `traces/query/stream_bitmap.rs`
They still have the same loader shape: page-meta load, page-blob load, fragment load, same `StreamBitmapLoader` implementation, same page-walk logic.
The diff is mostly:
- family meta type
- page span constant
- `tables.log_streams()` vs `tables.trace_streams()`
- module-local test block on the logs side

This is the strongest remaining duplicate in the runtime path.

2. `logs/query/clause.rs` vs `traces/query/clause.rs`
These still share the same planner skeleton:
- clause-spec building
- clause ranking
- `StreamPlanner` impl
- same page-overlap and fragment-loading plumbing

The domain-specific part is just:
- clause kinds
- stream-id mapping
- filter field extraction

This should probably become one shared planner helper plus family-owned clause mapping.

3. Thin logs-side stream wrappers still exist for tests
In [`logs/ingest/stream.rs`](../../crates/finalized-history-query/src/logs/ingest/stream.rs):
- `persist_stream_fragments` is still a very small wrapper over shared `bitmap_pages::persist_stream_fragments`
- `compact_sealed_stream_pages` is now test-only and also just forwards into shared page compaction

Not urgent, but still shimmy.

4. `logs/ingest/artifact.rs` and `traces/ingest/artifact.rs`
Much better than before, but still parallel in shape:
- persist payload blob/header
- persist block record
- persist primary-dir fragment

The dir part is shared now. The remaining duplication is basically “family payload artifact writer”, which may be okay unless you want to push to a trait/config-driven ingest block writer.

5. `logs/table_specs.rs` vs `traces/table_specs.rs`
There is still a lot of mechanical duplication:
- bucket/sub-bucket specs
- stream page specs
- blob/meta/by-block specs
- shard/local/page helpers

That’s lower value than query/ingest duplication because it’s mostly declarative, but it’s still duplicated.

6. `logs/codec.rs` vs `traces/codec.rs`
Not all of it, but both define:
- stream bitmap meta codec
- block-level record codec
- some parallel fixed-codec scaffolding

This is more “same pattern” than “same algorithm”, so I’d rank it below query/clause.

If I were doing the next pass, I’d go in this order:
1. unify `query/stream_bitmap`
2. unify `query/clause`
3. clean the remaining logs ingest shims
4. only then consider spec/codec dedup

The main remaining duplication is now in query planning/loading, not ingest.
