# Remove Single-Writer Mode

## Goal

Remove both:

- `SingleWriterAuthority`
- `IngestMode::SingleWriterFast`

and leave the crate with one writer model:

- lease-backed `reader_writer`

plus:

- `reader_only`

This reduces concept count, removes a parallel startup/publish path, and makes the write-authority model uniform across deployments.

## Scope

Delete the single-writer path from:

- authority implementations
- service constructors / role handling
- config and ingest-mode surface
- tests
- current-state docs

## Intended End State

The crate exposes only two service roles:

- `new_reader_only(...)`
- `new_reader_writer(...)`

All write-capable services:

- require upstream finalized-block observation
- acquire authority through lease-backed publication ownership
- publish head advancement through the lease-backed authority path

There is no exclusive-writer special case in the runtime model.

## Planned Changes

### 1. Remove `SingleWriterAuthority`

Delete:

- `src/ingest/authority/single_writer.rs`
- re-exports and module wiring for `SingleWriterAuthority`
- `FinalizedHistoryService::new_single_writer(...)`
- `ServiceRole::SingleWriter`

Update call sites, tests, and public exports accordingly.

### 2. Remove `IngestMode::SingleWriterFast`

Delete:

- `config::IngestMode::SingleWriterFast`
- any branching that treats single-writer publication specially

Keep only the lease-backed publication mode on the write path.

### 3. Collapse Docs and Vocabulary

Update current-state docs so the write-authority model describes only:

- reader-only
- lease-backed reader+writer

Remove single-writer terminology from:

- `docs/overview.md`
- `docs/write-authority.md`
- `docs/config.md`

## Verification

Verify `finalized-history-query` after the change.

