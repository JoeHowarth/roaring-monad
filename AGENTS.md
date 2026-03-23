Start by reading ./docs/overview.md for orientation, then topic docs as needed. Do not read ./docs/historical or ./docs/plans unless relevant to the task.

Use `scripts/verify.sh <crate> [<crate> ...]` for routine repo verification. It runs mechanical auto-fixes (`cargo fix`, `cargo clippy --fix`) before fmt, tests, and clippy so you do not waste cycles on import cleanup, redundant closures, or other machine-fixable warnings.

NOTE: "keep in sync" references in `~/.claude/CLAUDE.md` and `~/.codex/AGENTS.md` refer to keeping those two global files in sync with each other. They do NOT refer to this repo-local file. Do not copy repo-specific instructions (verify.sh, backward-compat policy, autofix, etc.) into the global files.

This project is not deployed in production yet. Backward compatibility is not a constraint here; prefer the best end-state design and implementation over preserving transitional shapes. This note is repo-local and should not be copied into home-directory instruction files.

If you need to run the underlying commands directly:

- format with `cargo +nightly-2025-12-09 fmt --all`
- lint with `cargo clippy --all-targets --all-features -- -D clippy::suspicious -D clippy::style -D clippy::clone_on_copy -D clippy::redundant_clone -D clippy::iter_kv_map -D clippy::iter_nth -D clippy::unnecessary_cast -D clippy::filter_next -D clippy::needless_lifetimes -D clippy::useless_conversion -D clippy::useless_vec -D clippy::needless_question_mark -D clippy::bool_comparison -D unused_imports -D unused_parens -D deprecated -A clippy::type_complexity -A clippy::int_plus_one -A clippy::uninlined-format-args -A clippy::enum-variant-names -A clippy::mutable_key_type -A clippy::large_enum_variant -A clippy::doc-overindented-list-items`

Make sure to verify after every commit for each crate you changed.

Commit after each major code change so the working tree is clean. You can break work into smaller commits, but unless we are actively discussing or you are mid-implementation, leave the tree clean.

When reporting verification in user-facing updates or final responses, do not write out the full fmt or clippy command lines unless explicitly asked. Summarize them concisely (for example: "fmt passed", "clippy passed", "tests passed").

Optimization and Profiling Log Discipline:

- Keep a running log in the repo root at `OPTIMIZATION_LOG.md`.
- After each optimization attempt, append:
  timestamp, change summary, hypothesis, exact commands, and before/after metrics.
- Do not include routine verification commands such as `fmt` or `clippy` in optimization-log command lists; only include commands that materially measure, profile, or exercise the optimization itself.
- After each profiling run, append:
  workload range/shape, environment settings, tooling used, bottleneck evidence, and interpretation.
- Continuously capture methodology learnings:
  what produced reliable measurements, what was noisy/misleading, and how to avoid false conclusions.
- For performance-related commits, include the profile/benchmark deltas in the commit message body.

Onboarding Docs Discipline:

- Keep the topic-based docs in `docs/` (overview.md, storage-model.md, write-authority.md, query-execution.md, ingest-pipeline.md, caching.md, backend-stores.md, config.md) updated when architecture, terminology, storage layout, or core query/indexing behavior changes.
- Treat those docs as clean current-state documentation, not as a changelog or historical narrative.
- Write them as they should read if authored at that moment in time; do not add retrospective notes such as "this was changed from X" unless the comparison is part of the architecture itself.

Bugs

- When a bug is found, write a test that exposes the bug and validate that the test fails.
- Only after there are 1 or more failing tests can you fix the bug in code
