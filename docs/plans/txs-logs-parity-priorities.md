# Txs vs Logs Parity Priorities

This document captures follow-up work from a direct code comparison of:

- `crates/finalized-history-query/src/txs/`
- `crates/finalized-history-query/src/logs/`

The goal is not to force both families into identical shapes. The goal is to:

- fix correctness and maintenance risks,
- align behavior where asymmetry is accidental,
- preserve deliberate design tradeoffs where they are paying for themselves.

## Actionable

### 1. Add log-side ingest validation

Problem:

- `txs` ingest validates transaction shape and ordering.
- `logs` ingest currently serializes logs without equivalent validation.
- This is the highest-value parity gap because it affects correctness at write time.

Work:

- Validate each log before encoding it.
- Enforce `topics.len() <= 4`.
- Enforce block-level consistency for fields that are expected to agree with the enclosing block.
- Decide and enforce the expected ordering rules for `tx_idx` and `log_idx`.

Suggested tests:

- Reject a log with more than 4 topics.
- Reject a log whose `block_num` disagrees with the enclosing block.
- Reject out-of-order or duplicated `log_idx` values if ordering is meant to be canonical.

Exit criteria:

- Invalid logs fail ingest before any artifacts are persisted.
- Negative tests exist first, then the implementation is tightened.

### 2. Remove or fix stale `BlockLogHeaderRef`

Problem:

- `logs/log_ref.rs` contains a `BlockLogHeaderRef` parser that appears to assume an older wire format than the current `BlockLogHeader` codec.
- This is dangerous dead code because it looks authoritative while not matching the actual encoding path.

Work:

- Confirm it is unused.
- If unused, delete it.
- If it is meant to survive, rewrite it to parse the current `BucketedOffsets`-backed layout and add direct tests against real encoded headers.

Exit criteria:

- No parser remains that disagrees with the live header encoding.

### 3. Add tx-side tests in the places that currently have none

Problem:

- `logs` has much broader local coverage than `txs`.
- The weakest tx-side parity is in `ingest.rs` and `filter.rs`.

Work:

- Add tx ingest tests for valid and invalid signed tx payloads, `tx_idx` ordering, and stream append generation.
- Add tx filter tests for exact-match behavior, especially create transactions, missing selectors, and malformed signed tx payloads.

Exit criteria:

- `txs/ingest.rs` and `txs/filter.rs` each have focused unit coverage for their main edge cases.

### 4. Materializer style parity

Problem:

- `logs` overrides `load_by_id`.
- `txs` relies on the trait default.
- The behavior is effectively the same, but the implementation shape diverges.

Work:

- remove the log override

Exit criteria:

- The two materializers express single-item hydration the same way.

### 5. Normalize table naming

Problem:

- `txs` table names are family-prefixed.
- `logs` still owns several generic table names.

Work:

- rename the log-side table specs and wire up the corresponding migration/update work.

Exit criteria:

- Naming is either consistent or explicitly justified.

### 6. Standardize empty-block blob behavior

Problem:

- Empty tx blocks and empty log blocks do not persist the same way.
- That is not clearly intentional and creates avoidable storage and behavioral drift.

Work:

- persist empty block artifacts for both

Exit criteria:

- `txs` and `logs` behave identically for zero-item blocks.

### 7. Unify arithmetic behavior in stream append collection

Problem:

- `txs` and `logs` do not handle primary-id arithmetic the same way.
- This is a low-probability edge case, but the inconsistency is unnecessary.

Work:

- Replace both ad hoc approaches with a shared checked helper.
- Do not use checked arithmetic, prefer clarity since it is impossible to get past u64 in production

Exit criteria:

- Both families compute stream append IDs through the same overflow policy.
