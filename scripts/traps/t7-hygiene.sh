#!/usr/bin/env bash
# T7 — hygiene trap. The gate set:
#   1. mix compile --warnings-as-errors exits 0 (no deprecation noise)
#   2. mix credo --strict exits 0 (documented config only)
#   3. no dead CI files or badges (amendment 3: CI waits for the fork going public)
#   4. no debug leftovers (IO.inspect) in lib/
#   5. the legacy espec suite is migrated: no espec dep, no spec/ dir,
#      and the full ExUnit suite is green.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE/../.."

FAILED=0
say() { printf '[t7] %s\n' "$*"; }
pin() { if [ "$1" -ne 0 ]; then say "FAIL: $2"; FAILED=1; else say "ok: $2"; fi; }

MIX_ENV=dev mix compile --warnings-as-errors >/tmp/fb_t7_compile.txt 2>&1
pin $? "mix compile --warnings-as-errors (see /tmp/fb_t7_compile.txt)"

mix credo --strict >/tmp/fb_t7_credo.txt 2>&1
pin $? "mix credo --strict (see /tmp/fb_t7_credo.txt)"

[ ! -f .travis.yml ]; pin $? "no dead .travis.yml"
! grep -qi "travis" README.md; pin $? "no dead Travis badge in README"

if grep -rn "IO.inspect" lib/ >/dev/null 2>&1; then
  grep -rn "IO.inspect" lib/ | head -3
  pin 1 "no IO.inspect in lib/"
else
  pin 0 "no IO.inspect in lib/"
fi

grep -q "espec" mix.exs
if [ $? -eq 0 ]; then pin 1 "espec leaves the deps"; else pin 0 "espec leaves the deps"; fi
[ ! -d spec ]; pin $? "no spec/ dir (legacy suite migrated)"

MIX_ENV=test mix test >/tmp/fb_t7_test.txt 2>&1
pin $? "full ExUnit suite green (see /tmp/fb_t7_test.txt)"

[ "$FAILED" -eq 0 ] && say "PASS: all hygiene pins hold" || say "RED: hygiene pins violated"
exit "$FAILED"
