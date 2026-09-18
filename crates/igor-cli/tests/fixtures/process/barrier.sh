#!/bin/sh
set -eu

barrier_dir=$1
printf '%s\n' "$$" > "$barrier_dir/pid"
printf '%s\n' "${CUDA_VISIBLE_DEVICES-}" > "$barrier_dir/cuda-visible-devices"
: > "$barrier_dir/started"
while [ ! -e "$barrier_dir/release" ]; do
    sleep 0.02
done
