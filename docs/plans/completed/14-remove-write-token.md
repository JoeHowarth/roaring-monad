# Remove WriteToken

## Summary

This plan removes `WriteToken` from the write-authority boundary now
that publication ownership is session-based rather than epoch-based.

The target model is:

- `LeaseAuthority` owns the current publication session internally
- ingest and service code no longer thread a separate token struct
- publish/authorize validate against cached lease state plus
  `publication_state`
- the public API no longer exposes a token-shaped write capability

## Work Packages

### 1. Remove Token From The Authority Trait

- replace `authorize(current, observed)` with a stateful re-authorization
  call that does not take or return `WriteToken`
- replace `publish(current, new_head)` with `publish(new_head)`
- update the service and ingest layers to stop passing tokens around

### 2. Keep Lease Validation Semantics

- preserve the current stale-writer checks based on cached
  `session_id` and `indexed_finalized_head`
- continue returning `LeaseLost` or `PublicationConflict` in the same
  cases as today
- keep acquire as the only operation that establishes a new writer
  session

### 3. Remove Dead Token Plumbing

- delete `WriteToken`
- remove now-redundant helpers and debug assertions that only exist to
  move tokens across layers
- update tests, benches, and docs to describe session-based authority
  without token threading

## Verification

- authority unit tests still cover takeover, expiry, renewal, and
  publish conflict paths
- service/startup/ingest tests still cover cached-writer reuse and
  stale-writer rejection
- routine crate verification remains clean
