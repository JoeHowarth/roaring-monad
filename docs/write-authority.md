# Write Authority

This document describes the write-authority model: leases, fencing, epochs, service roles, and startup/recovery behavior.

## Service Roles

Three roles are available at the `FinalizedHistoryService` constructor layer:

| Role | Constructor | Writes | Lease | Upstream observation required |
|------|-----------|--------|-------|------------------------------|
| Reader-only | `new_reader_only(...)` | No | No | No |
| Reader+writer | `new_reader_writer(...)` | Yes | LeaseAuthority | Yes |
| Single-writer | `new_single_writer(...)` | Yes | SingleWriterAuthority | No |

Reader-only nodes load `startup_plan(...)` state observationally and never attempt ownership.

## Write Authority Boundary

Leadership is behind `WriteAuthority`. That boundary owns:

- acquisition — obtaining write ownership
- renewal — extending lease validity
- publish / head advancement — advancing `indexed_finalized_head`
- fence-token derivation — producing `FenceToken` from `WriteToken`

The ingest engine owns:

- finalized-sequence validation
- artifact persistence
- compaction and recovery orchestration

### WriteToken

```rust
WriteToken {
    epoch: u64,
    indexed_finalized_head: u64,
}
```

The `epoch` is the fencing clock for write-side artifact mutation and cleanup.

## PublicationState

```rust
PublicationState {
    owner_id: u64,          // stable node identity
    session_id: [u8; 16],   // process instance identity
    epoch: u64,             // fencing and ownership version
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

## Acquire / Authorize / Publish / Fence Lifecycle

### Acquisition

1. load current `publication_state`
2. if absent, `create_if_absent` with the initial state
3. if present and lease expired (or same owner), CAS to claim ownership with `epoch + 1`
4. advance backend fence to the new epoch
5. return `WriteToken`

If the ownership CAS succeeds but fence advancement fails, retrying the same session re-arms the backend fence before acquisition succeeds.

### Authorization (cached writer re-auth)

When a writer token is cached from a previous startup:

1. check the cached token's epoch against the current `publication_state`
2. verify the lease is still valid against the current upstream observation
3. if valid, reuse the token without an epoch bump

### Renewal

Renewal happens during ingest when the observed upstream block enters the renew window. It extends `lease_valid_through_block` via CAS without bumping epoch.

### Publish (head advance)

After all artifacts for a block batch are durable:

1. CAS `publication_state` with `indexed_finalized_head = new_head`
2. renewal piggybacks on publish when needed

### Fencing

Every `MetaStore.put` and `MetaStore.delete` call carries a `FenceToken(epoch)`. The store rejects operations where the token's epoch is below the stored minimum epoch. This prevents stale-epoch writers from mutating artifacts after a takeover.

## Hard Expiry

Once a lease has expired, ownership must be reacquired with an epoch bump — even if the same session attempts to renew. There is no silent same-session renewal after expiry.

This ensures:

- any gap in liveness is visible in the epoch sequence
- `advance_fence` invalidates stale-epoch writes from before the gap

The `authorize`/`renew_if_needed` path returns `LeaseLost` after expiry, forcing re-acquisition through the service layer.

## Startup Flow

### Lease-backed reader+writer

```python
async def startup_reader_writer(owner_id, observed_upstream_finalized_block, cached_lease=None):
    if cached_lease is not None:
        lease = authorize_cached_publication(cached_lease, observed_upstream_finalized_block)
        # still runs cleanup and repair before returning
        return recovery_plan_from(lease)

    lease = acquire_publication(owner_id, observed_upstream_finalized_block)
    cleanup_unpublished_suffix(lease.indexed_finalized_head)
    repair_open_stream_page_markers(derive_next_log_id(lease.indexed_finalized_head))
    return recovery_plan_from(lease)
```

Cleanup, marker repair, and next-position derivation run on both the cached and fresh paths. A cached writer re-authorization still cleans partial artifacts left by a failed ingest.

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
6. the winning CAS writes: standby's `owner_id`, fresh `session_id`, `epoch = current.epoch + 1`, unchanged `indexed_finalized_head`, new `lease_valid_through_block`
7. after acquisition, the new primary: advances backend fence, cleans unpublished suffix, repairs stale markers, derives next sequencing state

If the takeover CAS fails, the standby reloads `publication_state` and re-evaluates rather than retrying blindly.

## Fail-Closed Rule

If a node does not have a current observed upstream finalized block number, it fails closed. It must not:

- acquire ownership
- renew ownership
- publish a new head
- continue owner-only work

Using the last observed block number after observation has been lost is not allowed.

## Same-Node Restart

Reacquiring with the same node identity must still bump `epoch`. Node identity is not process identity — a restarted process generates a new `session_id`, so it always takes the foreign-session takeover path.

## Single-Writer Mode

Use `SingleWriterAuthority` (`new_single_writer`) when:

- exactly one writer process exists with no standby
- the deployment guarantees exclusive access (single-node or process-level lock)
- lease overhead and observation dependency are not needed

### Safety properties deliberately absent

- no lease-based expiry or takeover — a second writer is not fenced by lease freshness
- `publish` uses `PutCond::Any` rather than `compare_and_set` — concurrent writers would silently overwrite each other's heads
- the backend fence is the only protection against stale-epoch writes; there is no lease-level guard against split-brain

Single-writer mode relies entirely on the deployment preventing concurrent writers.

## Ownership Rules

- a healthy primary keeps ownership
- standby writers may observe and remain ready but must not seize ownership while the current lease is valid
- to force replacement: stop the writer process and wait for the lease to expire (at most `publication_lease_blocks` finalized blocks); a standby takes over once expiry is observed
