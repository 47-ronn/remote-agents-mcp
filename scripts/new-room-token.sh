#!/usr/bin/env bash
# new-room-token.sh -- generate a relay room token that passes the relay's
# default --min-token-len=32 (token-addressed rooms: the token is the room's
# ONLY gate, so it must be long random). Printed to stdout and copied to the
# clipboard when xclip is available.
#
# Usage: new-room-token [length]        (default 32)
#
# IMPORTANT: after rotating a room token update ALL clients that carry it
# (opencode config, agent --token args on fleet hosts, the panel's saved
# credentials, the win host) and only then restart the relay with the new
# --min-token-len — otherwise peers are silently isolated in an empty room.
set -euo pipefail

len="${1:-32}"
if [[ "$len" -lt 24 ]]; then
  echo "error: length must be >= 24 (relay min-token-len)" >&2
  exit 1
fi

# Token-safe alphabet: no whitespace, no quotes, no shell metacharacters —
# safe to paste into URLs, JSON, systemd units and PowerShell.
# (`|| true`: under pipefail `tr` dies of SIGPIPE once `head` is satisfied —
# that's the normal termination of this pipeline, not an error.)
token="$(tr -dc 'A-Za-z0-9' < /dev/urandom 2>/dev/null | head -c "$len" || true)"
[[ ${#token} -eq "$len" ]] || { echo "error: entropy source failed" >&2; exit 1; }

printf '%s\n' "$token"
if command -v xclip >/dev/null 2>&1; then
  printf '%s' "$token" | xclip -selection clipboard
  echo "(copied to clipboard)" >&2
fi
