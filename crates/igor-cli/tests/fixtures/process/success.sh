#!/bin/sh
set -eu

test "${SAFE_VALUE:-}" = "visible"
test -z "${HOME+x}"
test -z "${TELEGRAM_BOT_TOKEN+x}"
test -z "${OPENCODE_TEST_SECRET+x}"
printf 'fixture stdout\n'
printf 'identity %s %s\n' "$$" "$(ps -o pgid= -p "$$" | tr -d ' ')"
printf 'fixture stderr\n' >&2
sleep 0.3
