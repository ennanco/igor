#!/bin/sh
set -eu

# Emit 256 KiB per stream, well above typical pipe buffers.
dd if=/dev/zero bs=262144 count=1 2>/dev/null | tr '\000' 'A'
dd if=/dev/zero bs=262144 count=1 2>/dev/null | tr '\000' 'B' >&2

exit 0
