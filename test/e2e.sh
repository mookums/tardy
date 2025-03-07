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
    zig build -Dasync=io_uring test_e2e -- "$rand64"
    zig build -Dasync=epoll test_e2e -- "$rand64"
    zig build -Dasync=poll test_e2e -- "$rand64"
    # echo "$rand64 passed"
done
