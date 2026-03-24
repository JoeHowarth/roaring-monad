# Metadata Artifact Renaming

## Summary

The current persisted-artifact vocabulary mixes several naming schemes:

- canonical block artifacts use `block_*`
- dir contribution artifacts use `frag`
- dir summaries use `sub` and `bucket`
- bitmap contribution artifacts use `frag`
- bitmap summaries use `page`
- bitmap blob encoding uses `chunk`

This plan normalizes the naming across the whole artifact set.

## Naming Principles

- use `block` for canonical per-block artifacts
- use `by_block` for per-block contribution artifacts inside a larger
  container
- use container names for merged summaries:
  `bitmap_page`, `dir_sub_bucket`, `dir_bucket`
- use `blob`, `header`, and `meta` only when they represent distinct
  artifact kinds
- remove `frag` from the top-level vocabulary
- de-emphasize `chunk` as an internal codec term rather than a
  top-level storage concept
- keep the naming pattern reusable across families, while preserving
  family-qualified keyspaces where the mapping is family-specific

## Renaming Plan

### Canonical Block Artifacts

- `block_meta` -> `block_record`
- `block_hash_to_num` -> `block_hash_index`
- `block_log_headers` -> `block_log_header`
- `block_logs` -> `block_log_blob`

### Dir Artifacts

Dir artifacts are family-specific because they map a family's primary ID
space to blocks. The naming scheme below is the suffix vocabulary; the
persisted keyspace remains family-qualified.

Logs examples:

- `log_dir_by_block`
- `log_dir_sub_bucket`
- `log_dir_bucket`

- `log_dir_frag` -> `dir_by_block`
- `log_dir_sub` -> `dir_sub_bucket`
- `log_dir` -> `dir_bucket`

### Bitmap Artifacts

- `open_stream_page` -> `open_bitmap_page`
- `stream_frag_meta` + `stream_frag_blob` -> `bitmap_by_block`
- `stream_page_meta` -> `bitmap_page_meta`
- `stream_page_blob` -> `bitmap_page_blob`

If per-block bitmap metadata and blob remain split temporarily, keep the
same conceptual renaming and apply it to both halves until they are
merged.

## Type Renames

- `BlockMeta` -> `BlockRecord`
- `LogDirFragment` -> `DirByBlock`
- `LogDirectoryBucket` -> `DirBucket`

If per-block bitmap metadata/blob are merged, introduce a dedicated
combined type for that object rather than continuing to reuse
`StreamBitmapMeta`.

## Vocabulary End State

The intended top-level vocabulary is:

- publication state
- block record
- block hash index
- block log header
- block log blob
- dir by-block
- dir sub-bucket
- dir bucket
- open bitmap page
- bitmap page
- bitmap page start
- bitmap by-block
- bitmap page meta
- bitmap page blob

This vocabulary is intended as a reusable naming pattern across
families. Where an artifact maps family-specific primary IDs to blocks,
the persisted keyspace should remain family-qualified rather than
collapsing into one shared global `dir_*` namespace.

## Sequencing

1. merge per-block bitmap metadata/blob if we decide to collapse them
2. rename persisted key families
3. rename Rust types
4. update docs to remove `frag` and de-emphasize `chunk`
