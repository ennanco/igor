#!/bin/sh
set -eu

trap 'printf "graceful-cancellation\n"; exit 0' TERM

# Block until SIGTERM arrives; sleep in a loop so the trap fires promptly.
while true; do
  sleep 1
done
