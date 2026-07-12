#!/usr/bin/env fish

for i in (seq 1 $argv[1])
    redis-cli xadd Events '*' event "{\"short_code\":\"$(xxd -l 1 -p /dev/urandom | head -c 1)\",\"at\":\"$(date -Iseconds)\"}" > /dev/null &
end
