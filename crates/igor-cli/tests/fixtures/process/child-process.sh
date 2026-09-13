#!/bin/sh
set -eu

PARENT_PID=$$

# The child ignores SIGTERM so cancellation must escalate for the whole group.
trap '' TERM
set +e
sleep 300 &
CHILD_PID=$!
set -e
trap 'exit 0' TERM

# Retrieve the PGID (process-group ID) of the current process.
PGID=$(ps -o pgid= -p "$PARENT_PID" | tr -d ' ')

# Deterministic parseable line: parent=<pid> child=<pid> pgid=<pgid>
printf 'parent=%d child=%d pgid=%d\n' "$PARENT_PID" "$CHILD_PID" "$PGID"

# Wait indefinitely so the caller can inspect the running child.
wait
