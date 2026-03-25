# Upstream Review Stack Refresh

## Goal

Define a concrete upstreaming stack for `finalized-history-query` that matches
the current codebase more closely than
[`21-upstream-review-stack.md`](21-upstream-review-stack.md).

The main adjustment is to treat family-owned passive definitions as early
infrastructure rather than as the final layer. Today the storage, status, and
public API layers already depend on family request types, refs, state structs,
table specs, and some family-owned payload shapes.

## Why Refresh The Stack

The earlier stack assumed a cleaner horizontal split:

1. public/core vocabulary
2. storage/runtime substrate
3. shared read path
4. shared write path
5. family adapters

That is no longer true in the current tree:

- `api.rs` and `family.rs` already mention concrete family types
- `tables.rs` already depends on family table specs and family-owned payload
  shapes
- `status.rs` already depends on family sequencing state
- parts of shared ingest already depend on family stream-page constants and
  bitmap metadata types

The refreshed stack keeps the same review goal, but moves passive family
definitions earlier so the remaining commits can stay honest and compile with
minimal stubbing.

The main further refinements from commit-one prototyping are:

- the public API facade needs its own commit
- the shared read substrate and shared write substrate deserve separate commits

If `api.rs` lands too early in a reduced shape, it creates a review-only
constructor story and service surface that later commits must replace. If the
shared read and write substrate land together, review load spikes again right
where the crate becomes most subtle. The stack below separates both of those
boundaries.

## Principles

- Keep the final architecture visible from the first commit.
- Use stubs for behavior, not for ownership boundaries.
- Introduce real type names early if later layers already depend on them.
- Avoid temporary generic abstractions created only for the review stack.
- Keep each commit reviewable on its own, with a clear question for reviewers.
- Intermediate commits must compile, but they do not need full end-to-end
  ingest or query behavior.
- Prefer landing a file in its final module path early and filling it in later
  rather than moving it between commits.
- If a public facade lands early, it should already match the long-term public
  shape closely enough that later commits mostly fill in bodies rather than
  redesign signatures.

## Stub Policy

Allowed:

- method bodies that return a clear "not yet implemented" error
- placeholder materializers or family hooks whose signatures match the final
  design
- empty or trivially default implementations where the later commit will fill
  in the behavior

Not allowed:

- temporary public types that will be renamed or deleted later
- temporary module moves whose only purpose is to satisfy the stack
- fake generic interfaces that hide concrete family ownership already present
  in the design
- speculative public method signatures that later commits are likely to replace
- panics where a typed error or empty result can preserve the final API shape
- public methods that silently report success while skipping authoritative work
- owned stand-ins for types that are intended to become zero-copy family view
  types later

## Target Stack

Aim for eight commits.

### 1. Core vocabulary and error surface

Goal:

- establish the crate shell and shared vocabulary without committing to a
  temporary service facade

Include:

- workspace wiring
- `crates/finalized-history-query/Cargo.toml`
- `src/lib.rs`
- `src/error.rs`
- `src/core/*`

What should be real in this commit:

- error/result types
- core ID/range/page/header/clause vocabulary
- stable module paths and crate-root re-exports for the core types that belong at
  the crate root

What should explicitly not land yet:

- `src/api.rs`
- storage/runtime wiring
- family storage definitions
- materializers or ingest/query behavior

Review question:

- is the shared vocabulary the right long-term substrate language?

### 2. Final public API boundary

Goal:

- land the real service facade early enough for reviewers to judge the public
  surface before substrate details arrive

Include:

- `src/api.rs`
- `src/config.rs`
- `src/family.rs`
- minimal family shells needed for the public boundary to compile

What "minimal family" means here:

- request filter types
- public ref/view type declarations
- ingest input types
- sequencing-state structs
- family marker structs where the public API already references them

What should be real in this commit:

- the end-state-shaped `FinalizedHistoryService` type
- the real constructor names and public method list
- request/result structs
- `FinalizedBlock`
- any public response helper types such as receipts or status items that belong
  to the final API surface

What may be stubbed:

- all service method bodies
- any constructor internals
- any family behavior behind the public API

Important constraint:

- do not land a reduced or review-only service facade here; if `api.rs` lands
  in this commit, its public shape should already be close to the final crate

Review question:

- is the public method boundary the right long-term surface?

### 3. Passive family definitions

Goal:

- land family-owned passive types without pulling in behavior-heavy logic

Include:

- passive `src/logs/*`
- passive `src/txs/*`
- passive `src/traces/*`

What should be real in this commit:

- family-owned data types
- filters
- sequencing-state structs if not already present
- opaque or thin shell ref/view types

What should explicitly not land yet:

- family table specs
- codecs used only for storage artifacts
- materializers
- ingest implementations

Important constraint:

- placeholder ref/view types should be opaque shells or declarations, not owned
  mini-implementations that pre-commit to the wrong design before the real
  zero-copy forms land

Review question:

- do the family-owned public and passive types have the right boundaries?

### 4. Generic storage and kernel contracts

Goal:

- land the generic storage abstractions separately from this crate's concrete
  table wiring

Include:

- storage traits and backend-neutral helpers under `src/store/*`
- generic kernel code under `src/kernel/*`

What should be real in this commit:

- store traits
- backend-neutral table contracts
- generic blob/point/scannable wrappers
- generic codec helpers that are not specific to a family storage layout

What should explicitly not land yet:

- `src/runtime.rs`
- `src/tables.rs`
- `src/streams.rs`
- family `table_specs`

Review question:

- are the generic storage contracts and kernel layers clean on their own?

### 5. Runtime, tables, streams, and concrete storage layout

Goal:

- land the concrete storage wiring once the generic contracts are already
  reviewed

Include:

- `src/runtime.rs`
- `src/tables.rs`
- `src/streams.rs`
- family `table_specs`
- family-owned storage payload/header structs and storage codecs

What should be real in this commit:

- `Runtime`
- `Tables`
- concrete table wiring
- family table specs
- bytes-cache policy and metrics

What may be stubbed:

- behavior-heavy helpers that require the later query or ingest path
- exact-match materialization

Review question:

- does the concrete artifact layout match the intended storage model?

### 6. Shared read path, publication, blocks, and status

Goal:

- land the shared finalized read machinery before write-path complexity arrives

Include:

- `src/query/*`
- `src/blocks.rs`
- `src/core/directory.rs`
- `src/core/directory_resolver.rs`
- `src/store/publication.rs`
- `src/status.rs`

What should be real in this commit:

- publication-head reads
- block-bound resolution
- block-range clipping and page planning
- status loading
- shared query execution scaffolding
- direct block query support

What may be stubbed:

- family materializers
- family-specific filter planning details

Preferred stub shape:

- keep the shared query planner/runner signatures real
- prefer typed unimplemented errors over misleading empty success

Review question:

- are the shared query, publication, and block/status substrates correct on
  their own?

### 7. Shared ingest substrate, authority, and recovery

Goal:

- land the shared finalized write machinery after the read substrate is already
  reviewed

Include:

- `src/ingest/*`
- the shared coordinating portions of `src/family.rs`

What should be real in this commit:

- authority/session interfaces
- lease and read-only/write authority implementations
- recovery and compaction helpers
- publication-after-write discipline
- ingest coordinator shape
- finalized-sequence validation

What may be stubbed:

- per-family `ingest_block` bodies
- family-specific stream fanout
- family-specific artifact persistence

Preferred stub shape:

- keep the shared coordinator signatures real
- prefer typed unimplemented errors over fake writes or misleadingly successful
  ingest

Review question:

- are authority, recovery, and shared publication discipline correct before the
  family-specific persistence logic fills in?

### 8. Full family behavior, tests, and support crates

Goal:

- replace the remaining stubs with the real family implementations and land the
  end-to-end support surface

Include:

- full `src/logs/*`
- full `src/txs/*`
- full `src/traces/*`
- crate tests under `crates/finalized-history-query/tests/*`
- `crates/log-workload-gen/*`
- `crates/benchmarking/*`

What becomes real here:

- family ingest behavior
- family filter semantics
- family materialization
- directory writes and resolution details
- bitmap fanout and compaction semantics
- point lookups and block hydration that depend on family-owned artifacts
- end-to-end tests and support tooling

Review question:

- do the concrete family implementations correctly realize the reviewed shared
  architecture?

## File-Shaping Guidance

When a file mixes shared and family concerns, prefer the layer that owns most of
the concepts in the file today.

The stack is commit-scoped, not file-exclusive. A file may land as a skeletal
version in an earlier commit and then gain its real behavior later, but it
should stay in its final location and keep its final public names.

Applied to the current tree:

- keep `blocks.rs` with the shared read substrate rather than inventing a block
  family
- keep `tables.rs` with runtime/concrete storage wiring, even though it imports
  family specs
- keep family `table_specs` with the concrete storage-layout commit, not with
  the final behavior commit
- keep family materializers and ingest implementations for the last commit
- keep tests with the family behavior commit unless a smaller shared test adds
  clear review value earlier
- if a family ref/view type lands before its real behavior, prefer an opaque
  shell plus docs over a partial owned implementation

## Commit Hygiene

- Each commit should compile on its own.
- Each commit should keep the public API and module paths stable for later
  commits.
- Each commit should verify the crates it changes using `scripts/verify.sh`.
- Support crates should join the verification set in the commit that introduces
  them.
- Commit messages should explain the review boundary, especially where the
  commit intentionally introduces stubs that a later commit will fill in.
- Early-commit tests should primarily validate shape, invariants, and explicit
  non-support rather than toy behavior that the real implementation will later
  replace.

## What This Stack Optimizes For

This stack optimizes for four things simultaneously:

- minimal churn against the current architecture
- honest review boundaries
- a low volume of temporary stub code
- early review of the real public API facade without forcing a temporary
  constructor or service story

It is intentionally less pure than the older horizontal split, but more
accurate to the current codebase. The cost is that some family-owned modules
arrive earlier and the stack is longer than the original five-commit goal. The
benefit is that later commits can focus on real behavior instead of spending
review budget on scaffolding, relearning a replaced public facade, or reviewing
query and ingest substrate complexity in one large middle commit.

## Exit Criteria

This plan is successful if:

- each intermediate commit compiles
- the early commits expose the real long-term API and ownership boundaries
- the public service facade is reviewed in a close-to-final shape before
  substrate details arrive
- the storage/kernel contracts are reviewed before the concrete table map lands
- the concrete storage commit does not pretend family-owned table layout is
  generic
- the shared read substrate and shared write substrate are each reviewable
  without being fused into one large middle-layer commit
- the final commit is mostly behavior fill-in, tests, and support tooling rather
  than another architecture reshuffle
