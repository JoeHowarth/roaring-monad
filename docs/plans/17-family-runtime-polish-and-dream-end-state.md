# Family Runtime Polish And Dream End State

## Goal

Take the current multi-family ingest coordinator from "correct and usable" to a
clean final architecture.

The intended end state is:

- one shared finalized block envelope
- one concrete family registry for `logs`, `txs`, and `traces`
- one shared ingest coordinator that validates once and publishes once
- one service-owned runtime and one service-owned family set used everywhere
- family-local schema, codecs, query logic, and ingest behavior living inside
  each family module
- no leftover migration aliases or family-boundary shims that obscure ownership

This plan is a polish pass on top of the family-owned type and shared ingest
refactors already in place.

## Problem

The crate now has the right ingest cardinality, but the shape is still more
ceremonious than it needs to be.

Today:

- `FinalizedBlock` is shared, but it still imports a primitive alias from
  `logs`
- the family boundary mixes a genuinely useful `Family` trait with a generic
  `Families<L, T, R>` tuple shape that is concretely hardcoded to three
  families anyway
- the service owns one family set for ingest, while reader startup creates a
  fresh `Families::default()`
- there are still transitional aliases such as `Block = FinalizedBlock`
  and `logs::types` re-exporting the shared block type

That leaves three kinds of complexity:

1. duplicated or indirect sources of truth
2. generic ceremony around a fixed concrete family set
3. migration residue that makes the current shape harder to read

## Intended End State

The architecture should read directly from the module graph.

- `api.rs`
  - concrete service boundary
  - concrete query and ingest methods
  - service wiring only
- `runtime.rs`
  - shared store handles and typed tables
  - no per-call table rebuilding
- `block.rs`
  - shared finalized block envelope
  - no dependence on family-local primitive aliases
- `family.rs`
  - concrete family registry
  - minimal family-facing traits or hooks
  - startup-state and write-count aggregation
- `ingest/engine.rs`
  - shared publication and block-sequence coordinator
- `logs/`
  - full logs family implementation
- `txs/`
  - full tx family implementation
- `traces/`
  - full trace family implementation

The family set should be explicit and concrete:

```rust
pub struct Families {
    pub logs: LogsFamily,
    pub txs: TxsFamily,
    pub traces: TracesFamily,
}
```

The aggregated startup/write bookkeeping should also be concrete:

```rust
pub struct FamilyStates {
    pub logs: LogSequencingState,
    pub txs: TxStartupState,
    pub traces: TraceStartupState,
}

pub struct FamilyBlockWrites {
    pub logs: usize,
    pub txs: usize,
    pub traces: usize,
}
```

The service should own exactly one canonical instance of that family registry
and pass it to both startup and ingest paths.

## Design Principles

### 1. Prefer Concrete Structure Over Generic Tuple Ceremony

We already know the participating family set.

Do not hide a fixed `logs/txs/traces` registry behind generic type parameters
unless that genericity enables a real alternative assembly path we intend to
support.

### 2. Keep Shared Primitives Shared

Shared types should not import basic aliases from a family module.

If multiple families need `Hash32`, it belongs in a small shared module such as
`primitives.rs`, not in `logs::types`.

### 3. One Source Of Truth Per Runtime Concern

The service should own:

- one runtime
- one family registry
- one ingest coordinator

Reader startup, writer startup, and ingest should all flow through those same
owned objects.

### 4. Remove Transitional Compatibility Layers Quickly

Once callers have moved, remove aliases and re-exports that preserve old names
without adding clarity.

The codebase is pre-production and does not need to optimize for temporary
backward-compatibility shims.

### 5. Make Families Own Their Whole World

The dream state only really lands once `logs`, `txs`, and `traces` each own:

- family item types
- persisted metadata types
- codecs
- key helpers
- table specs
- startup state
- per-block ingest behavior
- query or materialization behavior where applicable

## Work Packages

### 1. Introduce Shared Primitive Types

Add a tiny shared module for truly cross-family primitive aliases:

- `Hash32`
- any other primitive aliases that are genuinely shared

Then update:

- `block.rs`
- `logs/types.rs`
- `txs/types.rs`
- `traces/types.rs`

to import from that shared module instead of from `logs`.

### 2. Make The Family Registry Concrete

Replace:

- `Families<L, T, R>`
- `FamilyStates<L, T, R>`
- `StartupState<L, T, R>`

with concrete structs for the known family set.

Keep the `Family` trait only if it still provides useful family-local structure.
If the trait starts fighting the concrete family registry, simplify it further.

### 3. Unify Startup Around The Service-Owned Family Set

Change startup helpers so they take `&Families` rather than creating a fresh
default family registry internally.

The reader-only and writer startup paths should agree on:

- runtime
- publication store
- family registry

with no duplicate assembly logic.

### 4. Delete Transitional Block Aliases

Remove:

- `pub type Block = FinalizedBlock`
- block re-exports from `logs::types`

The concrete ingest boundary should speak in terms of `FinalizedBlock`
everywhere.

### 5. Shrink `api.rs` To Wiring

After startup ownership is unified, trim `api.rs` so it mostly:

- constructs runtime
- constructs families
- constructs ingest coordinator
- exposes concrete methods

Push shared startup aggregation and family bookkeeping down into the appropriate
shared modules.

### 6. Make `txs` And `traces` Real Families

Replace the current scaffold behavior with real family implementations that own
their own:

- types
- codecs
- storage vocabulary
- startup state
- per-block ingest logic

This step should happen family by family. `logs` remains the reference
implementation until the new families are real.

## Suggested Execution Order

1. Add shared primitive aliases and remove `logs` as the source of `Hash32`.
2. Make `Families`, `FamilyStates`, and startup aggregation concrete.
3. Change startup to use the service-owned family registry everywhere.
4. Remove `Block` aliases and `logs` block re-exports.
5. Simplify `api.rs` after the ownership boundaries settle.
6. Implement real `txs` family behavior.
7. Implement real `traces` family behavior.
8. Update current-state docs after each architectural step.

## Success Criteria

This plan is complete when:

- `block.rs` does not depend on `logs::types`
- there is one canonical family registry owned by the service
- startup and ingest use that same registry
- the public ingest API accepts only `FinalizedBlock`
- there are no transitional `Block` aliases or compatibility re-exports
- the family boundary is easier to read than the current generic-tuple form
- `txs` and `traces` can grow into full implementations without another
  architectural reset
