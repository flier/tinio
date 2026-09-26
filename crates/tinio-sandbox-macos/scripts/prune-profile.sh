#!/usr/bin/env bash
# Establish which baseline allowances the profile actually needs.
#
# For each rule id, render the profile without that one rule and run the startup
# probe. A rule whose removal breaks the probe is load-bearing and must carry the
# observation in its `evidence` field; a rule whose removal changes nothing must
# be deleted from the source; an id that is not in the rendered profile at all
# gets no verdict from this run, and the harness says so. Run on a Mac, once per
# P3 landing.
#
# **This probe is not the product, and its verdicts are a floor rather than the
# answer.** It never binds a control socket and never calls
# `prepare_home_derived`, so the allowance audit re-measured every id against the
# real binary as well and the two disagreed on two rules; the product path is the
# authority, and `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5's audit
# paragraph holds the two-column table. Do not read this script's output as
# authority on its own.
#
# **No ripgrep.** The id scan below is `grep -oE` + `sed`, both shipped in macOS's
# own `/usr/bin`, because the machine this runs on is the verification guest: it
# has no `rg`, and it cannot get one — the guest cannot reach github.com, which is
# the only place ripgrep comes from. The workspace's other scripts may assume `rg`;
# this one may not, because this is where it runs.
#
# Usage: bash crates/tinio-sandbox-macos/scripts/prune-profile.sh
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "prune-profile.sh needs macOS (Seatbelt is a macOS facility)"; exit 1
fi

cd "$(dirname "$0")/../../.."
# **The id scan is `grep -oE` + `sed` rather than `rg -o --no-filename` + `cut`,
# and the two are the same extraction**: `grep -o` prints each `id: "…"` match on
# its own line, the first `sed` expression drops the `id: "` prefix and the second
# the closing quote, and neither needs a tool the Mac does not have.
ids=$(grep -oE 'id: "[a-z0-9-]+"' crates/tinio-sandbox-macos/src/profile.rs \
  | sed -E 's/^id: "//; s/"$//' | sort -u)

echo "rule id            verdict"
echo "------------------ ---------------------------------------------"
for id in $ids; do
  # The output is kept rather than discarded: the third verdict — an id whose
  # rule is not in the rendered profile — is visible only in it, and reading that
  # run as "required" would document a rule as load-bearing on the strength of a
  # run that never removed anything.
  out=$(TINIO_SANDBOX_MACOS_PRUNE="$id" cargo test -q -p tinio-sandbox-macos --test startup \
      -- --nocapture a_confined_process_starts 2>&1) && status=0 || status=$?
  if grep -q "rule-not-present id=$id" <<<"$out"; then
    printf '%-18s NOT PRESENT — the id is in the source but not in the rendered profile;\n' "$id"
    printf '%-18s check that the probe asks for it before reading this as a verdict\n' ""
  elif [[ $status -eq 0 ]]; then
    printf '%-18s NOT NEEDED — delete the rule from the source\n' "$id"
  else
    printf "%-18s required — keep it, with the observation in \`evidence\`\n" "$id"
  fi
done
echo
echo "Paste this table into the design's evidence table (Task 11) and rewrite each"
echo "surviving entry's \`evidence\` field with what the run showed — and with the"
echo "product-path measurement beside it, which this probe cannot make."
