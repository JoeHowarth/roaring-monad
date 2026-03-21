# Authority-Owned Write Session

## Summary

This plan moves the remaining write-operation lifecycle out of the
service layer and fully into `WriteAuthority`.

The target model is:

- the service does not manage write-session invalidation
- the service does not hold a separate write-operation mutex
- the authority owns acquire, renew, invalidate, and publish for one
  guarded writer operation
- the public authority boundary exposes a writer-scoped session/guard
  instead of separate `ensure_writer()` and `clear()` calls

## Work Packages

### 1. Introduce A Writer-Scoped Authority Operation

- replace the current `ensure_writer()` / `publish()` / `clear()` split
  with a single writer-scoped API
- choose either:
  - `with_writer(...) -> Result<T>`
  - or `begin_write(...) -> Result<WriteGuard>`
- make the authority return the current published head through that
  scoped API

### 2. Move Serialization And Invalidation Into The Authority

- move the writer-operation mutex out of `FinalizedHistoryService`
- let `LeaseAuthority` serialize write operations internally
- let the authority decide when cached lease state must be dropped after
  `LeaseLost`, `PublicationConflict`, or observation failure

### 3. Simplify Service And Ingest Callers

- remove service-side calls to `clear()`
- make startup and ingest depend only on the scoped writer operation
- keep query paths observational and unchanged
- update tests to assert the same takeover, expiry, and publish-conflict
  behavior under the new API shape

## Verification

- authority tests still cover bootstrap, renewal, expiry, takeover, and
  publish-conflict paths
- service/startup/ingest tests still cover cached-writer reuse and
  stale-writer rejection
- routine crate verification remains clean
