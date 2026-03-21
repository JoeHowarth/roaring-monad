# Family-Scoped Storage Boundary

## Goal

Replace the current crate-local `MetaStore`/`BlobStore` abstraction with a
backend-neutral storage boundary that:

- can live in separate crates with no knowledge of finalized-history types
- exposes family-scoped metadata tables instead of one ambient global metadata
  store
- keeps key encoding, value encoding, and domain semantics in
  `finalized-history-query`
- preserves the ability for backends to store multiple families in one physical
  table when that remains the best implementation

The main objective is to make storage backends generic infrastructure rather
than partially domain-aware code.

## Problem

The current layering is only partially generic.

Today:

- `finalized-history-query` owns the domain key layout and typed artifact tables
- storage backends are implemented inside the crate
- metadata backends receive whole raw keys and derive a group from the first
  path segment
- some backend logic still knows about domain group names and prefix-listing
  behavior across those groups

This creates three problems:

1. the storage layer is harder to extract and reuse because it contains
   finalized-history-specific assumptions
2. typed artifact tables still receive an ambient `MetaStore` with access to the
   full keyspace rather than a family-scoped handle
3. generic `list_prefix` remains a more visible abstraction than the actual
   domain operation being performed

## Intended End State

The storage boundary is split into generic crates and domain crates.

Representative shape:

```rust
pub struct FamilyId(&'static str);

pub trait MetaStore {
    fn family(&self, id: FamilyId) -> Arc<dyn KvTable>;
}

#[allow(async_fn_in_trait)]
pub trait KvTable: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Record>>;
    async fn put(&self, key: &[u8], value: Bytes, cond: PutCond) -> Result<PutResult>;
    async fn delete(&self, key: &[u8], cond: DelCond) -> Result<()>;
    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}
```

In this model:

- the generic storage crate only knows about opaque family identifiers and byte
  keys/values
- `finalized-history-query` defines stable family constants such as
  `BLOCK_RECORD`, `BITMAP_BY_BLOCK`, and `OPEN_BITMAP_PAGE`
- typed artifact readers are built on top of a specific `KvTable`
- backend implementations may still map many families onto one physical table,
  but that is an internal backend choice rather than a crate-wide abstraction

## Scope

This plan covers:

- extracting the metadata/blob storage traits into generic crates
- replacing ambient metadata-store access with family-scoped handles
- moving backend-specific family/prefix knowledge out of backend
  implementations
- narrowing metadata enumeration from generic prefix scans to per-family scans
  wherever possible

This plan does not cover:

- changing the persisted artifact key suffix layout
- changing blob object naming
- redesigning typed artifact caches
- changing publication, ingest, or query semantics

## Design Principles

### 1. Keep The Storage Layer Type-Free

The generic storage crate should not depend on domain structs, domain key
layouts, or domain terminology beyond an opaque family identifier.

It should traffic only in:

- family identifiers
- opaque byte keys
- opaque byte values
- conditional write/delete semantics
- pagination tokens

### 2. Make Family Scope Explicit

Metadata operations should happen through a family-scoped handle rather than a
global store plus full path keys.

This means:

- `block_record/<block_num>` becomes family = `block_record`, key = encoded
  `<block_num>`
- `bitmap_by_block/<stream_id>/<page_start>/<block_num>` becomes family =
  `bitmap_by_block`, key = encoded suffix only

The family boundary becomes part of the type and constructor structure rather
than an informal prefix convention.

### 3. Prefer A Small Value Type Over A Trait For Family Identity

Family identity should be represented as a lightweight value such as:

```rust
pub struct FamilyId(&'static str);
```

rather than a trait.

The problem is stable identity, not behavioral polymorphism. A value type keeps
the API simple while still avoiding raw stringly-typed plumbing everywhere.

### 4. Preserve Backend Freedom

The logical family boundary should not force one physical table per family.

Backends remain free to implement:

- one physical metadata table shared by many families
- one physical table per family
- family-specific optimizations for only selected families

So long as the generic interface is honored, the physical mapping remains an
implementation detail.

### 5. Replace Generic Prefix Scans With Family-Local Enumeration

The current generic `list_prefix` abstraction is broader than most call sites
need.

For metadata, the intended direction is:

- enumerate within one family table
- let domain code decide how to encode sortable suffix keys
- keep backend implementations unaware of higher-level path semantics

Blob stores may still need prefix listing because the object-store model is
naturally prefix-oriented.

## Target Architecture

### Storage Crates

Introduce backend-neutral crates along these lines:

- `storage-api` or similar: traits and common types
- `storage-fs`: filesystem-backed implementation
- `storage-scylla`: Scylla-backed metadata implementation
- `storage-s3` or `storage-minio`: blob-store implementation

These crates should not depend on `finalized-history-query`.

### Domain Crate Responsibilities

`finalized-history-query` remains responsible for:

- family constants
- suffix-key encoding and decoding
- typed artifact readers and caches
- persisted-value encoding and decoding
- domain-specific enumeration helpers

### Typed Table Construction

The typed `Tables` facade should be built from family-scoped handles rather than
the full metadata store.

Each typed table constructor should accept only the handle(s) it actually
needs.

## Work Packages

### 1. Introduce Generic Storage Traits In Extractable Modules

Create a minimal generic API for:

- metadata records
- metadata table handles
- blob store access
- conditional puts/deletes
- pagination

As an intermediate step, these modules may remain inside the repo before being
moved into separate crates.

### 2. Introduce `FamilyId`

Add a small family-identity value type and define finalized-history family
constants in the domain crate.

Examples:

- `BLOCK_RECORD`
- `BLOCK_LOG_HEADER`
- `LOG_DIR_BY_BLOCK`
- `LOG_DIR_SUB_BUCKET`
- `BITMAP_BY_BLOCK`
- `BITMAP_PAGE_META`
- `OPEN_BITMAP_PAGE`

### 3. Add Family-Scoped Metadata Handles

Replace direct whole-store access for metadata with a structure like:

- `meta_store.family(BLOCK_RECORD)`
- `meta_store.family(BITMAP_BY_BLOCK)`

Each returned handle should operate on suffix keys only.

### 4. Refactor Typed Artifact Tables Onto Family Handles

Update typed readers so they depend on the specific family handles they need
rather than an ambient `MetaStore`.

### 5. Replace Metadata `list_prefix` Call Sites

Migrate metadata enumeration call sites from generic cross-family prefix scans to
explicit family-local iteration.

Priority targets:

- directory-fragment compaction
- bitmap-by-block page compaction
- frontier fallback reads for directory fragments
- frontier fallback reads for bitmap-by-block fragments
- open bitmap page repair and sealing discovery

### 6. Simplify Backend Implementations

Once family-scoped handles exist, simplify backends accordingly.

For Scylla specifically:

- remove domain-specific known-group lists
- remove group extraction from raw full keys at the public trait boundary
- keep any shared physical `meta_kv` table strictly as an internal mapping from
  `(family, bucket, suffix_key)` to bytes

### 7. Extract Backends Into Separate Crates

After the interfaces stabilize:

- move generic storage traits and common types into a separate crate
- move backend implementations into separate crates
- keep finalized-history-specific test adapters in this repo only where they are
  truly domain-specific

## Exit Criteria

- storage traits and backend implementations can live in separate crates with no
  dependency on finalized-history domain types
- metadata access in finalized-history-query is family-scoped rather than based
  on ambient full-path keys
- typed artifact tables depend only on the family handles they actually need
- metadata backends no longer expose generic cross-family prefix scans as their
  primary abstraction
- current behavior for ingest, compaction, repair, and query execution is
  preserved
