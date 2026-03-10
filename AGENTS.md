NOTE: Keep this file in sync with ~/.claude/CLAUDE.md when making changes.

Whenever you format inside monad-bft repo use:

cargo +nightly-2025-12-09 fmt --all

When linting in monad-bft repo use:

cargo clippy --all-targets --all-features -- -D clippy::suspicious -D clippy::style -D clippy::clone_on_copy -D clippy::redundant_clone -D clippy::iter_kv_map -D clippy::iter_nth -D clippy::unnecessary_cast -D clippy::filter_next -D clippy::needless_lifetimes -D clippy::useless_conversion -D clippy::useless_vec -D clippy::needless_question_mark -D clippy::bool_comparison -D unused_imports -D unused_parens -D deprecated -A clippy::type_complexity -A clippy::int_plus_one -A clippy::uninlined-format-args -A clippy::enum-variant-names -A clippy::mutable_key_type -A clippy::large_enum_variant -A clippy::doc-overindented-list-items

Make sure to lint and format after every commit and also run cargo test -p <crate being changed>

Commit after each major code change so the working tree is clean. You can break work into smaller commits, but unless we are actively discussing or you are mid-implementation, leave the tree clean.

When reporting verification in user-facing updates or final responses, do not write out the full fmt or clippy command lines unless explicitly asked. Summarize them concisely (for example: "fmt passed", "clippy passed", "tests passed").

When amending commits, use `git am` (alias for `git commit --amend --no-edit`).

When force pushing, use `git fp` (alias for `git push --force-with-lease`).

When asked to review pr comments, fetch them using:

curl -s -L \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/category-labs/monad-bft/pulls/<pr_number>/comments \
| jq -r '.[] | "File: \(.path)\n\nComment: \(.body)\n\nHunk:\n\(.diff_hunk)\n--------------------------------"'

get the pr number via:

branch=$(git rev-parse --abbrev-ref HEAD)
repo_owner="category-labs"
repo_name="monad-bft"

curl -s -L \
-H "Accept: application/vnd.github+json" \
-H "X-GitHub-Api-Version: 2022-11-28" \
"https://api.github.com/repos/${repo_owner}/${repo_name}/pulls?head=${repo_owner}:${branch}&state=all" \
| jq -r '.[0].number'

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

- Keep the onboarding docs in `docs/finalized-log-index-onboarding/` updated when architecture, terminology, storage layout, or core query/indexing behavior changes.
- Treat those docs as clean current-state documentation, not as a changelog or historical narrative.
- Write them as they should read if authored at that moment in time; do not add retrospective notes such as "this was changed from X" unless the comparison is part of the architecture itself.
