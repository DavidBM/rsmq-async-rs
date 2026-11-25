-- Delete a queue and all its messages atomically.
-- KEYS[1]: ns:qname        (sorted set key)
-- KEYS[2]: ns:QUEUES       (queue index set)
-- KEYS[3]: ns:qname:cfg
-- KEYS[4]: ns:qname:msg
-- ARGV[1]: qname
-- Returns 1 if the queue existed, 0 if not.

local cfg = KEYS[3]
local msg = KEYS[4]

local deleted = redis.call("DEL", cfg, msg, KEYS[1])
redis.call("SREM", KEYS[2], ARGV[1])

if deleted == 0 then
    return 0
end
return 1
