#!/usr/bin/env bash
# T6 — types/dialyzer trap. The gate: `mix dialyzer` exits 0 on lib/ with
# zero warnings. Evidence: /tmp/fb_t6_dialyzer.txt
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE/../.."

log() { printf '[t6] %s\n' "$*"; }

MIX_ENV=dev mix dialyzer > /tmp/fb_t6_dialyzer.txt 2>&1
STATUS=$?

if [ "$STATUS" -ne 0 ]; then
  log "FAIL: dialyzer emitted warnings (exit $STATUS) — see /tmp/fb_t6_dialyzer.txt"
  grep -E "^warning:|type warning|callback_type_mismatch|_mismatch" /tmp/fb_t6_dialyzer.txt | sort | uniq -c | sed 's/^/  /'
  exit 1
fi

log "PASS: dialyzer clean"
