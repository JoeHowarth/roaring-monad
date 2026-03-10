#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: scripts/verify.sh <crate> [<crate> ...]

Runs the repo-standard formatter once, then `cargo test -p <crate>` and
`cargo clippy -p <crate> --all-targets --all-features` for each listed crate.

Example:
  scripts/verify.sh finalized-history-query benchmarking
EOF
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 1
fi

cd "$ROOT"

echo "==> fmt"
cargo +nightly-2025-12-09 fmt --all

for crate in "$@"; do
  echo "==> test: $crate"
  cargo test -p "$crate"

  echo "==> clippy: $crate"
  cargo clippy -p "$crate" --all-targets --all-features -- \
    -D clippy::suspicious \
    -D clippy::style \
    -D clippy::clone_on_copy \
    -D clippy::redundant_clone \
    -D clippy::iter_kv_map \
    -D clippy::iter_nth \
    -D clippy::unnecessary_cast \
    -D clippy::filter_next \
    -D clippy::needless_lifetimes \
    -D clippy::useless_conversion \
    -D clippy::useless_vec \
    -D clippy::needless_question_mark \
    -D clippy::bool_comparison \
    -D unused_imports \
    -D unused_parens \
    -D deprecated \
    -A clippy::type_complexity \
    -A clippy::int_plus_one \
    -A clippy::uninlined-format-args \
    -A clippy::enum-variant-names \
    -A clippy::mutable_key_type \
    -A clippy::large_enum_variant \
    -A clippy::doc-overindented-list-items
done
