# Write Authority

This document describes the write-authority model: leases, session-based ownership, service roles, and startup behavior.

## Service Roles

Two roles are available at the `FinalizedHistoryService` constructor layer:


| Role          | Constructor              | Writes | Lease                 | Upstream observation required |
| ------------- | ------------------------ | ------ | --------------------- | ----------------------------- |
| Reader-only   | `new_reader_only(...)`   | No     | No                    | No                            |
| Reader+writer | `new_reader_writer(...)` | Yes    | LeaseAuthority        | Yes                           |


Reader-only nodes load `startup_plan(...)` state observationally and never attempt ownership.

## Write Authority Boundary

Leadership is behind `WriteAuthority`. That boundary owns:

- acquisition — obtaining write ownership
- renewal — extending lease validity
- publish / head advancement — advancing `indexed_finalized_head`
- cached lease/session tracking for the active writer process

The ingest engine owns:

- finalized-sequence validation
- artifact persistence
- compaction and recovery orchestration

### WriteAuthority

```rust
trait WriteAuthority {
    type Session<'a>: WriteSession
    where
        Self: 'a;

    async fn begin_write(
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<Self::Session<'_>>;
}

trait WriteSession {
    fn state(&self) -> AuthorityState;
    async fn publish(
        self,
        new_head: u64,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()>;
}
```

`begin_write(...)` returns a writer-scoped session that holds the current
published head and owns publish/invalidation behavior for that operation.

## PublicationState

```rust
PublicationState {
    owner_id: u64,          // stable node identity
    session_id: [u8; 16],   // process instance identity
    indexed_finalized_head: u64,  // only reader-visible publication head
    lease_valid_through_block: u64,  // inclusive external-block lease validity bound
}
```

`indexed_finalized_head` is the only reader-visible publication watermark. Readers clip queries against this value.

## Lease Clock Model

### External clock source

The lease clock is the latest observed finalized block number from the upstream source — not wall-clock time, not `indexed_finalized_head`, not locally-inferred "blocks we have ingested."

All contenders make lease decisions against the same external finalized-block observation.

### Validity rule

A lease is valid while `observed_upstream_finalized_block` `<=` `lease_valid_through_block`.

Takeover is allowed once `observed_upstream_finalized_block` `>` `lease_valid_through_block`.

### Config

See [config.md](config.md) for field defaults. The key fields are `publication_lease_blocks` and `publication_lease_renew_threshold_blocks`.

Required invariant: `publication_lease_renew_threshold_blocks < publication_lease_blocks`.

Renewal rule: renew when the observed upstream finalized block enters the renew window; otherwise reuse the current lease.

## Begin / Publish Lifecycle

### `begin_write(...)`

When a write-scoped session starts:

1. load current `publication_state`
2. if no cached lease exists, acquire ownership from store
3. if a cached lease exists, re-check and renew it against the current upstream observation
4. if cached state is stale but same-writer reacquire is possible, drop the cache and reacquire internally
5. return a session holding `AuthorityState { indexed_finalized_head }`

### Renewal

Renewal happens during ingest when the observed upstream block enters the renew window. It extends `lease_valid_through_block` via CAS without changing `session_id`.

### Publish (head advance)

After all artifacts for a block batch are durable:

1. the write session re-checks and renews the lease if needed
2. it CASes `publication_state` with `indexed_finalized_head = new_head`
3. invalidating errors clear the cached lease inside the authority

## Hard Expiry

Once a lease has expired, the current cached lease is no longer valid.
The authority must reacquire ownership from `publication_state` before it can
begin another write-scoped session. For the same live process, that reacquire
can reuse the same `session_id`.

This ensures:

- any gap in liveness forces the writer to re-prove ownership from `publication_state`

If a cached lease becomes unusable, the authority drops it internally before the
next write-scoped session attempt.

## Write Entry Flow

### Lease-backed reader+writer ingest

```python
async def ingest_finalized_blocks(owner_id, observed_upstream_finalized_block, blocks):
    session = begin_write(
        observed_upstream_finalized_block
    )
    family_states = load_family_startup_state(session.state().indexed_finalized_head)
    if session.state().needs_recovery:
        repair_stale_open_page_markers(family_states)
    validate_sequence(blocks, session.state().indexed_finalized_head)
    ingest(blocks, family_states)
    publish(blocks[-1].block_num)
```

The writer has no separate explicit startup call. Recovery that is required for correctness runs inside the write-scoped ingest entry after ownership transitions that may leave stale mutable state behind. Continuous lease renewals skip that repair path. Recovery does not delete unpublished suffix artifacts.

### Reader-only

```python
async def reader_only_status():
    return service.status()  # observational only, never mutates ownership
```

### `startup_plan(...)`

`startup_plan(...)` is observational only:

- loads `publication_state`
- derives `next_log_id`
- never mutates ownership

## Takeover Flow

1. standby observes the upstream finalized block number continuously
2. standby loads `publication_state`
3. standby compares `observed_upstream_finalized_block` to `lease_valid_through_block`
4. if the observed block is still within the validity window, standby remains passive
5. once the observed block is past the validity bound, standby attempts takeover by CAS
6. the winning CAS writes: standby's `owner_id`, fresh `session_id`, unchanged `indexed_finalized_head`, new `lease_valid_through_block`
7. after acquisition, the new primary derives next sequencing state from the published head, repairs stale sealed open-page markers, and resumes ingest

If the takeover CAS fails, the standby reloads `publication_state` and re-evaluates rather than retrying blindly.

## Fail-Closed Rule

If a node does not have a current observed upstream finalized block number, it fails closed. It must not:

- acquire ownership
- renew ownership
- publish a new head
- continue owner-only work

Using the last observed block number after observation has been lost is not allowed.

## Same-Node Restart

Reacquiring with the same node identity must still use a fresh `session_id`. Node identity is not process identity — a restarted process generates a new `session_id`, so it always takes the foreign-session takeover path.

## Ownership Rules

- a healthy primary keeps ownership
- standby writers may observe and remain ready but must not seize ownership while the current lease is valid
- to force replacement: stop the writer process and wait for the lease to expire (at most `publication_lease_blocks` finalized blocks); a standby takes over once expiry is observed
