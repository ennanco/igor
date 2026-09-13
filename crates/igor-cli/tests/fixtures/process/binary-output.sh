#!/bin/sh
set -eu

# Emit deterministic binary data containing NUL and invalid-UTF-8 bytes.
# The byte sequence is: 0x00 0x01 0x80 0xFF repeated 256 times.
# POSIX printf %b accepts backslash-zero followed by three octal digits.
HEX=""
i=0
while [ "$i" -lt 256 ]; do
  HEX="${HEX}\0000\0001\0200\0377"
  i=$((i + 1))
done

printf '%b' "$HEX"
printf '%b' "$HEX" >&2

exit 0
