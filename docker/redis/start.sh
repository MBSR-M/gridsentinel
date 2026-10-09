#!/bin/sh
set -eu
password=$(cat /run/secrets/redis_password)
umask 077
printf 'bind 0.0.0.0\nprotected-mode yes\nappendonly no\nmaxmemory 256mb\nmaxmemory-policy allkeys-lru\nrequirepass %s\n' "$password" > /tmp/gridsentinel-redis.conf
exec redis-server /tmp/gridsentinel-redis.conf
