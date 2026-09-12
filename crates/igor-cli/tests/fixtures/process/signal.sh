#!/bin/sh

printf 'terminating by signal\n'
kill -TERM "$$"
