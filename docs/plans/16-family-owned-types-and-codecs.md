# Family-Owned Types And Codecs

## Goal

Move family-specific schema, codecs, keys, and query/ingest semantics out of
shared crate-level modules and into the owning family modules.

The target architecture is:

- the outer engine knows only about family-agnostic execution concepts
- `logs`, `txs`, and `traces` each own their own types, codecs, keys,
  table specs, filters, query planning, materialization, and ingest behavior
- shared modules keep only truly cross-family concepts such as pagination,
  finalized-head/publication state, backend access, and generic execution flow

This plan is about the domain/model boundary. It complements
`13-family-scoped-storage-boundary.md`, which is about the storage boundary.

## Problem

The current layering is still logs-shaped in shared places.

Today:

- `domain/types.rs` contains many logs-specific structs
- `domain/keys.rs` and `domain/table_specs.rs` mix shared and logs-specific
  storage vocabulary
- `codec/log.rs` and `codec/log_ref.rs` live outside the `logs` family module
- the public service and generic execution layers still depend directly on
  logs-family types and engines

That creates three problems:

1. adding `txs` and `traces` would either bloat shared modules or force a
   second round of refactors later
2. the generic engine boundary is not honest about what is family-specific
3. family-local concerns are harder to evolve because their code is scattered
   across shared and family modules

## Intended End State

The outer system is family-agnostic.

It owns:

- query/request pagination and resume metadata
- finalized-head and publication-state reads
- generic block-range validation
- generic execution flow on opaque family-defined primary/materialized items
- backend and cache infrastructure

Each family owns:

- request filter semantics
- family item structs
- family-local persisted metadata structs
- family codecs
- family key suffix encoding helpers
- family table specs/constants
- block-window to family-primary-window resolution
- primary materialization
- family ingest artifact writing

Representative layout:

- `src/core/`
- `src/store/`
- `src/api.rs`
- `src/logs/`
- `src/txs/`
- `src/traces/`

Representative family shape:

- `logs/types.rs`
- `logs/codec.rs` or `logs/codec/*`
- `logs/keys.rs`
- `logs/table_specs.rs`
- `logs/filter.rs`
- `logs/query/*`
- `logs/materialize/*`
- `logs/ingest/*`

Shared modules should not define logs/txs/traces item structs or codecs.

## Design Principles

### 1. Keep Shared Modules Small And Honest

If a type only exists for one family, it belongs to that family even if its
name sounds generic.

Examples that should be treated skeptically:

- block-record structs that exist only for one family
- family-local page metadata
- family-local directory/sub-bucket summaries
- codecs for only one family's artifacts

### 2. Do Not Introduce A Giant Family Trait

The generic boundary should stay narrow and capability-based.

Prefer a small set of focused traits or family components over one large trait
that owns planning, ingest, codec, materialization, and filter semantics all at
once.

### 3. Move `logs` First

`logs` should become the proving-ground family implementation.

Do not design the abstraction around hypothetical `txs`/`traces` needs that are
not yet concrete. Build the smallest boundary that cleanly separates `logs`
from the outer engine, then add `txs` and `traces` once that boundary feels
stable.

### 4. Keep Family Storage Vocabulary With The Family

Once family-scoped storage handles exist, family-local keys and table specs
should live next to the family code that uses them.

Shared storage code should only know about opaque family/table handles and byte
keys.

### 5. Rename Shared Modules Only After The Move

`domain` may become misleading once family-local schema moves out of it, but the
rename should happen after the ownership boundary is already clear.

Do not combine the semantic move and the naming cleanup into one large step.

## Work Packages

### 1. Introduce A Minimal Family Boundary

Add a new shared boundary module such as `family.rs` or `families/mod.rs`.

The outer engine should receive only what it actually needs:

- family filter/request type
- family query planner hook
- family block-window to primary-window resolver
- family primary materializer
- family ingest writer
- family codec hooks for family-local stored values

Associated types should remain opaque to the outer engine.

### 2. Split Shared And Logs-Owned Types

Move logs-owned types out of `domain/types.rs` and into `logs/types.rs`.

Likely logs-owned examples:

- `Log`
- `Block`
- `BlockLogHeader`
- `DirBucket`
- `DirByBlock`
- `StreamBitmapMeta`
- `BlockRecord` if it remains logs-family-specific

Keep only truly shared state in shared modules:

- `PublicationState`
- finalized-head/publication projections
- generic page/query metadata
- any block-ref type that is genuinely shared across families

### 3. Move Logs Codecs Under `logs`

Move logs-family codecs out of the shared `codec` area and under `logs`.

Priority targets:

- `codec/log.rs`
- `codec/log_ref.rs`

Keep only genuinely shared codec helpers/macros in the shared codec module.

### 4. Move Logs Keys And Table Specs Under `logs`

Split shared and logs-local pieces of:

- `domain/keys.rs`
- `domain/table_specs.rs`

Logs-specific key suffix helpers and table specs should live in `logs`.
Shared publication/common helpers can stay shared.

### 5. Refactor Generic Execution To Depend On The Family Boundary

Update the service and generic execution layers so they no longer hardcode the
logs family.

Priority targets:

- `api.rs`
- generic query execution code in `core/*`
- startup/recovery code that currently assumes logs-specific next-position logic

The goal is that the outer engine can orchestrate a family implementation
without importing the family's concrete structs directly.

### 6. Make `logs` The First Family Implementation

Adapt the current logs stack to the new family boundary while preserving
behavior.

This includes:

- query planning
- primary-window resolution
- materialization
- ingest artifact writes
- family-local tests and fixtures

No behavior change should be coupled to this refactor.

### 7. Add `txs` And `traces` Scaffolds

After `logs` cleanly fits the new boundary, add empty or thin scaffolds for:

- `txs/`
- `traces/`

These should establish the target layout, but they do not need full query/ingest
implementations in the same change.

### 8. Remove Or Rename `domain`

Once family-local schema, keys, and codecs are gone from shared modules:

- delete `domain` if it no longer adds value, or
- rename it to `shared` if shared persisted/state vocabulary still remains

Do this only after the family ownership boundary is already reflected in code.

## Suggested Execution Order

1. add the minimal family boundary
2. move logs-owned types
3. move logs codecs
4. move logs keys and table specs
5. refactor generic execution onto the family boundary
6. make `logs` the first implementation
7. add `txs`/`traces` scaffolds
8. rename or remove `domain`

## Verification

- existing logs query, ingest, startup, and publication tests still pass
- no shared module should import logs-family item structs or logs codecs after
  the migration is complete
- family-local keys, table specs, and codecs should be defined under their
  owning family modules
- current-state docs should describe the new shared/family split once the move
  lands
