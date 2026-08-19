#!/usr/bin/env fish

for i in (seq 1 $argv[1])
    set code (xxd -l 1 -p /dev/urandom | head -c 1)
    curl --silent -X GET "http://localhost:3000/s/$code" > /dev/null &
end
