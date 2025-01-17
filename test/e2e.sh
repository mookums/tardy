#!/usr/bin/env bash

set -e
count=$1

for ((i = 1; i <= count; i++))
do
    rand64=$((RANDOM))
    rand64=$((rand64 | RANDOM << 15))
    rand64=$((rand64 | RANDOM << 30))
    rand64=$((rand64 | RANDOM << 45))

    echo "running e2e with argument $rand64"
    zig build run_e2e -- "$rand64" 
    echo "$rand64 passed"
done
