# Cleanup Taxonomy And Execution

## Purpose

This document defines the implementation-level cleanup model behind
startup recovery, periodic maintenance, and GC.

It is the implementation companion to
`docs/plans/recovery-gc-and-maintenance.md`.

## Goal

Define:

- what cleanup classes exist
- which phase owns each class
- what is automatic GC versus operator pruning
- what is safe to delete and when

## Cleanup Phases

### Startup recovery

Runs under acquired write authority before new ingest proceeds.

Owns:

- unpublished suffix cleanup
- unpublished summary cleanup
- stale sealed open-page marker repair

### Periodic maintenance

Runs during healthy steady-state writer operation.

Owns:

- bounded inventory and frontier housekeeping that is safe to perform
  online
- work that should not wait for the next restart but is not GC debt

### GC

Runs as a background debt-measurement and cleanup pass.

Owns:

- debris that should not remain indefinitely
- backlog measurement for guardrails
- cleanup classes that are safe to enumerate and delete in background

### Operator pruning

Runs only when a caller supplies an explicit boundary or policy.

Owns:

- policy-driven data removal such as pruning old block-hash index entries
- any future retention-based cleanup that is not correctness debt

## Artifact Classes

### Authoritative published artifacts

Examples:

- `block_meta/*`
- `block_logs/*`
- `block_log_headers/*`
- `block_hash_to_num/*` while retained
- `log_dir_frag/*`
- `stream_frag_meta/*`
- `stream_frag_blob/*`

Rule:

- never delete automatically merely because they are old
- only delete if they are known to be unpublished, orphaned by explicit
  correctness rules, or selected by operator pruning policy

### Published acceleration artifacts

Examples:

- `log_dir_sub/*`
- `log_dir/*`
- `stream_page_meta/*`
- `stream_page_blob/*`

Rule:

- may be deleted if they are proven unpublished or provably rebuildable
  debris
- must not be treated as authoritative sources of truth

### Inventory and control-plane state

Examples:

- `open_stream_page/*`
- fence state
- publication state

Rule:

- `publication_state` is never GC-managed
- `open_stream_page/*` may be repaired or deleted when provably stale

## Ownership Matrix

### Startup recovery owns

- blocks above visible head
- summaries for unpublished ranges above visible head
- stale open-page markers that now refer to sealed pages

### Periodic maintenance owns

- bounded open-page housekeeping that does not require full restart logic
- any later online sealing or summary maintenance adopted by the service

### GC owns

- debris classes that should never persist indefinitely and can be
  safely audited in background
- guardrail measurement for those classes

### Operator pruning owns

- explicit boundary-based deletion such as
  `prune_block_hash_index_below(min_block_num)`

## Safe Deletion Rules

### Unpublished authoritative artifacts

Delete when:

- block number is above `publication_state.indexed_finalized_head`
- cleanup is running under valid write authority

Delete order:

1. derived summaries for the unpublished range
2. unpublished blocks in reverse order

### Published acceleration artifacts

Delete when:

- they are provably outside published authoritative state and are
  rebuildable
- or they are explicitly classified as debris

Do not delete merely because they are old.

### Open-page markers

Delete when:

- the referenced page is sealed at the current frontier
- the related compaction/repair action has completed

## Guardrail Model

Guardrails should measure cleanup debt classes that the system can act
on meaningfully.

The current config names should be reconciled to current artifact
classes. A likely future shape is:

- unpublished or orphaned acceleration artifacts
- stale inventory markers
- operator-prunable metadata backlog

If existing names such as "orphan chunk bytes" or "stale tail keys" do
not correspond to current artifact classes, they should be renamed
before implementation hardens around them.

## Operator Pruning

Some cleanup should remain explicit rather than automatic.

The current example is:

- prune block-hash index entries below an operator-chosen block number

This should be modeled as operator pruning, not as GC debt, unless the
project later decides to add a formal retention policy for that index.

## Execution Rules

### Fence awareness

Any metadata mutation or deletion must use the current fence epoch.

### Visibility awareness

Cleanup must never infer visibility from anything other than
`publication_state.indexed_finalized_head`.

### Bounded online work

Periodic maintenance must be bounded so it cannot become an unbounded
startup substitute.

### Background cost awareness

GC listing and deletion work must be designed with large keyspaces in
mind. Where a class is too expensive for naïve scanning, that should be
made explicit rather than hidden behind a placeholder implementation.

## Open Questions

### 1. Should old block-hash index entries be automatic or operator-pruned?

Options:

- A. always operator-pruned by explicit block boundary
- B. automatic retention window
- C. hybrid: explicit for now, automatic later if needed

Recommendation:

- C, with current implementation starting at A

Reason:

- the code already exposes explicit boundary pruning
- automatic retention can be added later if there is a real operating
  need

### 2. Should published acceleration artifacts ever be GC’d automatically?

Options:

- A. no, only unpublished or clearly orphaned ones
- B. yes, if rebuildable and older than a retention window

Recommendation:

- A

Reason:

- the current plans are about correctness and debt cleanup, not storage
  minimization through aggressive rebuild-on-demand

### 3. Does periodic maintenance need its own persistent checkpoints?

Options:

- A. no, keep it stateless and bounded
- B. yes, track progress across runs for expensive scans

Recommendation:

- start with A

Reason:

- startup recovery already handles correctness-critical repair
- checkpoints add another state machine before the cleanup model is even
  settled
