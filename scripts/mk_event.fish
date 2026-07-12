#!/usr/bin/env fish

for i in (seq 1 $argv[1])
    redis-cli xadd Events '*' event "{\"short_code\":\"$(xxd -l 4 -p /dev/urandom)\",\"time\":\"$(date -Iseconds)\"}"
end
