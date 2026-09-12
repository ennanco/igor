#!/bin/sh

printf 'failure stdout\n'
printf 'failure stderr\n' >&2
exit 7
