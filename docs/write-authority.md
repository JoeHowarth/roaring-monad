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
    async fn ensure_writer(
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<AuthorityState>;
    async fn publish(new_head: u64) -> Result<()>;
    async fn clear();
}
```

`ensure_writer(...)` returns the current `indexed_finalized_head` in a small
named state object. The authority keeps the active writer session internally
rather than exposing a separate token type.

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

## Ensure / Publish Lifecycle

### `ensure_writer(...)`

If the authority does not have a cached lease:

1. load current `publication_state`
2. if absent, `create_if_absent` with the initial state
3. if present and lease expired (or same owner), CAS to claim ownership with a fresh `session_id`
4. cache the acquired lease internally and return the current `indexed_finalized_head`

If the authority already has a cached lease:

1. use the cached `session_id` and head
2. check them against the current `publication_state`
3. verify the lease is still valid against the current upstream observation
4. if valid, return the current `indexed_finalized_head`

### Renewal

Renewal happens during ingest when the observed upstream block enters the renew window. It extends `lease_valid_through_block` via CAS without changing `session_id`.

### Publish (head advance)

After all artifacts for a block batch are durable:

1. CAS `publication_state` with `indexed_finalized_head = new_head`
2. renewal piggybacks on publish when needed

## Hard Expiry

Once a lease has expired, ownership must be reacquired through a fresh acquisition attempt. There is no silent same-session renewal after expiry.

This ensures:

- any gap in liveness forces the writer to re-prove ownership from `publication_state`

The `authorize`/`renew_if_needed` path returns `LeaseLost` after expiry, forcing re-acquisition through the service layer.

## Startup Flow

### Lease-backed reader+writer

```python
async def startup_reader_writer(owner_id, observed_upstream_finalized_block):
    authority_state = ensure_writer(
        observed_upstream_finalized_block
    )
    return recovery_plan_from(authority_state.indexed_finalized_head)
```

Startup derives the next write position from the published head. It does not delete unpublished suffix artifacts.

### Reader-only

```python
async def startup_reader_only():
    return startup_plan()  # observational only, never mutates ownership
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
7. after acquisition, the new primary derives next sequencing state from the published head and resumes ingest

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
