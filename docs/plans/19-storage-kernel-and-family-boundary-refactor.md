# Storage Kernel And Family Boundary Refactor

## Goal

Refactor `crates/finalized-history-query` from its current improved-but-still-mixed
state into a cleaner architecture with:

- a small shared storage kernel built around principled primitives
- explicit family-owned schema and execution code for logs and traces
- smaller, single-responsibility modules instead of a monolithic `src/tables.rs`
- storage codecs defined once through traits, not ad hoc wrappers or duplicate
  inherent methods
- no transport or RPC logic inside this crate

This plan is not about adding new product behavior. It is about reaching a more
elegant, principled, and maintainable internal structure while preserving the
current logs and traces behavior.

## Problem

Recent refactors improved the code materially:

- storage codecs are now trait-based
- cached point-table boilerplate was collapsed
- scannable fragment and bitmap-page blob wrappers share more logic
- the trace family is fully implemented inside the crate boundary

But the architecture is still not at the desired end state.

Today:

- `src/tables.rs` is still a large mixed-responsibility file
- storage primitives and family-specific table wrappers live together
- logs and traces still mirror one another across storage-facing code in ways
  that are partly structural and partly accidental
- the shared storage abstractions exist, but they are not yet first-class
  modules that read as the core substrate of the crate
- some family-local code still reaches for low-level storage helpers directly
  rather than through a clearly layered storage boundary

That leaves the crate in a better state than before, but not yet in a state that
feels elegant or inevitable.

## Refactoring Principles

### 1. Favor Architectural Clarity Over Line Count

The point of this refactor is not to maximize generic code.

We should only introduce shared abstractions when they make the code read more
directly and more truthfully. Fewer lines alone is not success.

### 2. Separate Storage Shape From Family Semantics

The shared layer should know how to:

- encode and decode stored values
- cache point lookups
- iterate scannable partitions
- cache blob reads
- expose common publication and runtime primitives

The shared layer should not know logs-vs-traces semantics.

Family code should own:

- key shape
- schema and persisted value types
- ingest rules
- query semantics
- materialization and exact-match behavior

### 3. Keep Families Explicit

Do not build a generic "family engine".

Logs and traces have enough semantic differences that forcing them through a
single behavioral abstraction would make the code less clear. The right shared
boundary is storage shape, not query or ingest semantics.

### 4. Prefer Small, Named Modules Over One Clever File

If a concept is real, it should have a real home.

The target state should make it obvious where to look for:

- codec traits and macros
- cached point table logic
- scannable fragment iteration logic
- cached blob logic
- cache metrics and cache internals

### 5. Every Step Must End In A Better State

Each package in this plan must leave the crate:

- compiling
- verified with `scripts/verify.sh finalized-history-query`
- easier to understand than before the package started

No package should rely on a later cleanup to make an intermediate shortcut
acceptable.

### 6. Avoid Mechanical Genericity That Obscures Meaning

Use traits and helpers for:

- storage codecs
- cache-backed point tables
- scannable partition iteration
- cached blob access

Do not use traits to hide:

- query planning
- materialization behavior
- family ingest semantics
- filter semantics

## Target End State

The crate should read in layers.

### Layer 1: Method boundary

- `src/api.rs`
- service entrypoints and request/response types

### Layer 2: Shared finalized-history substrate

- family coordinator
- startup/runtime/authority/publication logic
- range and execution helpers
- storage kernel modules

### Layer 3: Family-owned schema and execution

- logs schema, ingest, query, and materialization
- traces schema, ingest, query, and materialization
- txs remains scaffolded until it grows real behavior

The shared kernel itself should become explicit. To avoid confusion with the
existing backend-facing `src/store/` tree, the shared refactor target should use
`src/kernel/`, not `src/storage/`.

A likely target structure is:

- `src/kernel/codec.rs`
- `src/kernel/cache.rs`
- `src/kernel/point_table.rs`
- `src/kernel/scannable_table.rs`
- `src/kernel/blob_table.rs`
- `src/kernel/mod.rs`

The exact filenames may differ, but the concepts should be split this way.

## Work Packages

### Preconditions

This refactor should start from a clean working tree.

The current refactor stack that introduced trait-based codecs and the latest
table deduplication work should be committed first. Do not start this module
split on top of a large dirty tree.

### 1. Freeze Current Behavior With Focused Structural Guardrails

Before moving files around, add or expand tests where the storage refactor could
accidentally regress behavior.

Target coverage:

- table-cache metrics remain stable for cached point and blob lookups
- directory fragment loading preserves sort order by block number
- bitmap fragment loading preserves full partition coverage
- publication-state codec round-trips still work through the trait-based API
- logs and traces continue to read from compacted artifacts when present and
  frontier fragments otherwise

This package should be intentionally small. It exists to make the larger module
movement safer.

Execution rule for this package:

- prefer extending existing test files over creating new ones
- only add tests when a later package would otherwise move ownership or helper
  logic without a direct guardrail

Likely existing homes:

- `crates/finalized-history-query/tests/cache_behavior.rs`
- `crates/finalized-history-query/tests/ingest_compaction.rs`
- `crates/finalized-history-query/tests/publication_authority.rs`
- existing unit tests under `src/logs/*`, `src/traces/*`, and `src/streams.rs`

### 2. Move Storage Codecs Into A Real Shared Kernel Namespace

Create a dedicated `src/kernel/` module tree and move the current shared codec
surface into it first.

Minimum extraction targets:

- the storage codec trait and fixed-codec macro
- any codec-local helpers that exist only to support the storage codec layer

Likely result:

- `src/kernel/codec.rs` becomes the canonical home of `StorageCodec` and the
  fixed-codec macro
- `src/codec.rs` temporarily becomes a narrow compatibility shim that re-exports
  the moved codec surface
- all code continues to compile through the shim before any later module moves

This package should not change higher-level family behavior. It is a module
ownership move.

### 3. Move Cache And Generic Table Helpers Into The Shared Kernel

After the codec move, extract the current shared storage helpers into dedicated
kernel modules.

Minimum extraction targets:

- bytes cache implementation and cache metrics types
- cached point-table helper
- scannable fragment helper
- cached blob helper

Likely target modules:

- `src/kernel/cache.rs`
- `src/kernel/point_table.rs`
- `src/kernel/scannable_table.rs`
- `src/kernel/blob_table.rs`

Temporary compatibility shims are allowed only if they keep the tree compiling
for the duration of this package and are removed in the next package.

This package is still not about changing family behavior. It is about making the
shared storage kernel real and explicit.

### 4. Reduce `src/tables.rs` To Family-Facing Table Assembly

After the storage helpers move, rewrite `src/tables.rs` so that it becomes a
small assembly layer rather than a kitchen sink.

Its responsibilities should shrink to:

- the `Tables<M, B>` aggregate
- family-facing typed wrapper structs where they still add readability
- constructor wiring for those wrappers
- lightweight accessor methods

It should stop owning:

- cache internals
- generic point/scannable/blob helper implementations
- low-level storage codec definitions
- generic partition iteration loops
- generic blob-cache read-through logic

The resulting file should read as a catalog of the crate's typed storage access
surface, not as the implementation of every storage primitive.

Allowed responsibilities in the target `src/tables.rs`:

- the `Tables<M, B>` aggregate
- typed wrapper definitions that still encode domain meaning
- family-facing constructor wiring
- lightweight wrapper methods that mostly encode keys and delegate to shared
  storage primitives

Forbidden responsibilities in the target `src/tables.rs`:

- cache implementation details
- generic helper type definitions
- storage codec macro definitions
- raw partition iteration loops
- family-agnostic blob cache plumbing

### 5. Make Family Schema Modules Fully Own Persisted Types And Specs

Review `src/logs/*` and `src/traces/*` and tighten the boundary so each family
owns its persisted vocabulary completely.

For each family, the family module should own:

- persisted value types
- key helpers
- table specs
- schema-local storage codecs

The shared storage kernel should provide the mechanism, but not own any
family-specific naming or wire-shape policy.

Concretely, verify and tighten ownership around:

- `logs::types`, `logs::table_specs`, `logs::keys`, `logs::codec`
- `traces::types`, `traces::table_specs`, `traces::keys`, `traces::codec`

This package must explicitly keep the following out of the shared storage
kernel:

- logs stream-id naming and partition naming conventions
- traces stream-id naming and partition naming conventions
- logs block-header wire format
- traces block-header wire format
- family-specific directory fragment and bitmap metadata semantics

If any family-specific type or key helper is still in the wrong layer after the
recent refactors, move it now.

### 6. Replace Repetitive Wrapper Structs With More Declarative Shapes Where It Improves Readability

Now that the storage kernel is real, revisit the remaining typed wrappers.

Goal:

- keep a typed family-facing API
- remove wrapper structs that exist only to forward directly to a generic helper
  without adding meaning

Possible end-state patterns:

- thin wrapper structs for tables with family-specific key helpers
- type aliases when the wrapper adds no meaning
- small extension impls when a helper needs only one or two family-local methods

This package applies only to specific wrapper categories:

- point tables
- scannable fragment tables
- bitmap-page blob tables

This package must be disciplined. If an alias or helper makes call sites less
clear, keep the wrapper struct.

This package is explicitly skippable. If packages 2-5 already leave the wrapper
layer in its clearest form, do not force further churn just to replace structs
with aliases or extension impls.

The standard is not "fewest structs." The standard is "most direct expression of
the storage model."

### 7. Tighten The Runtime/Publication/Storage Boundary

Review the boundary between:

- `src/runtime.rs`
- `src/startup.rs`
- `src/family.rs`
- `src/store/publication.rs`
- the new storage kernel

The target is a clean layering where:

- runtime owns store handles and typed storage access
- publication owns publication-state semantics only
- family/startup logic derives sequencing state from published head and typed
  family metadata
- no module reaches around its typed boundary for convenience

Expected leak patterns to review in this package:

- modules outside the kernel importing kernel helper internals directly when a
  typed table wrapper should be the boundary instead
- publication-state logic depending on compatibility shim paths that should be
  removed after the codec move settles
- family/startup code reaching for raw store or scannable-table details instead
  of typed runtime/table access

This package should remove any lingering cross-layer leaks that remain after the
earlier packages.

### 8. Clean Up Test Infrastructure To Match The New Layering

The tests should reflect the architecture we want.

Update helpers and fixtures so that:

- storage codec tests use the shared trait cleanly
- table-oriented tests target the storage kernel where appropriate
- family tests stay focused on family behavior instead of incidental storage
  plumbing
- repetitive fixture encoding helpers are reduced where doing so improves
  readability

The goal is not to hide how storage works. The goal is to align tests with the
same boundaries as the production code.

### 9. Update The Docs At Architectural Boundaries, Then Align Final Current-State Docs

Docs should be updated incrementally at meaningful architectural milestones, not
only at the very end.

Minimum expectations:

- after the storage kernel becomes real, the overview and any layering docs stop
- after the shared kernel becomes real, the overview and any layering docs stop
  describing `src/tables.rs` or `src/codec.rs` as all-in-one homes for these
  concerns
- after family schema ownership is tightened, docs reflect that ownership in
  current-state terms
- after the full refactor, `docs/overview.md` and any affected topic docs read
  as if authored for the final layering

Minimum final targets:

- `docs/overview.md`
- any topic docs that mention `src/codec.rs`, `src/tables.rs`, or the current
  storage layering

### 10. Final Mirroring Review Gate

Once the storage kernel and family boundaries are cleaner, re-review logs and
traces side by side.

Look for the remaining mirrored code and sort it into two buckets:

1. semantic duplication that should remain explicit
2. structural duplication that should move into shared shape-based helpers

Candidates for additional shared helpers:

- family-local table assembly helpers
- repeated range-to-partition iteration patterns
- repeated small blob helper utilities

Candidates that should likely remain explicit:

- query engines
- materializers
- ingest fanout
- exact-match semantics

This is not an implementation package. It is the final adversarial review gate
before declaring the refactor complete.

The exit criteria for this gate are:

- every remaining logs/traces mirror is explicitly classified as either
  necessary semantic duplication or shared structural shape
- no remaining duplication is left unexamined merely because the code now passes
- any further abstraction that would reduce readability is explicitly rejected

## Execution Rules

These rules apply to every package in this plan.

### 1. No Shortcut Abstractions

Do not introduce a generic family engine.

Do not introduce traits whose only job is to make logs and traces look more
similar than they really are.

### 2. Split By Responsibility, Not By File Count

A smaller file count is not a goal.

If two concepts are genuinely distinct, they should live in distinct modules
even if that creates more files.

### 3. Preserve Typed APIs At The Boundaries

Call sites should continue to read in domain terms:

- block records
- directory buckets
- bitmap page metadata
- trace block headers

Do not replace a clear typed table call with raw key/value plumbing in the name
of generic reuse.

### 4. Prefer Mechanical, Verifiable Migrations

Where possible:

- move one concept at a time
- keep names stable during moves
- let trait-based storage codec calls remain `.encode()` and `Type::decode(...)`
  once the trait is in scope
- use targeted mechanical replacement when it keeps the diff honest

### 5. Verify After Every Package

Run `scripts/verify.sh finalized-history-query` after each package or tightly
related small batch of packages.

Do not stack multiple large structural moves without verification.

### 6. Commit At Architectural Boundaries

Make commits when the code reaches a cleaner stable state, not only at the very
end.

Good commit boundaries here include:

- codec layer moved into the storage namespace with a temporary shim if needed
- generic storage helpers moved out of `src/tables.rs`
- `src/tables.rs` reduced to family-facing assembly
- family schema ownership tightened
- runtime/publication boundary tightened
- docs aligned to final state

### 7. Temporary Shims Must Be Deliberate And Short-Lived

Compatibility shims are allowed only when they satisfy all of the following:

- they keep the tree compiling during a single package boundary
- they do not introduce new semantic behavior
- they are deleted in the next package unless explicitly justified as permanent
  public surface

Examples:

- a temporary `src/codec.rs` re-export shim after moving the canonical codec
  implementation to `src/kernel/codec.rs`
- a temporary re-export module during a storage-helper move

## Success Criteria

This plan is complete when all of the following are true:

- the shared storage kernel exists as explicit modules
- the shared kernel exists as explicit modules
- `src/tables.rs` is materially smaller and focused on typed table assembly
- family-owned schema/codecs/keys/specs are clearly owned by logs and traces
- runtime/publication/startup/storage boundaries are clean and typed
- logs and traces share shape-based storage helpers but keep semantic execution
  logic explicit
- the crate remains transport-free
- `scripts/verify.sh finalized-history-query` passes throughout

At that point the codebase should not merely be less repetitive. It should read
as a principled system with a small number of honest concepts.
