#!/usr/bin/env fish

for i in (seq 1 $argv[1])
    set code (xxd -l 1 -p /dev/urandom | head -c 1)
    set at (date +'%Y-%m-%dT%H:%M:%S.%N%:z')
    set event (jq -nc --arg code "$code" --arg at "$at" '{"short_code": $code, "at": $at}')
    redis-cli xadd Events '*' event "$event" > /dev/null &
end
