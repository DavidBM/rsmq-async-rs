-- Atomically delete a single message: remove from the sorted set and from the :msg hash.
-- KEYS[1]: ns:qname     (sorted set key)
-- ARGV[1]: message id
-- Returns 1 if the message existed (and was deleted), 0 otherwise.

local msg = KEYS[1] .. ":msg"

local zrem = redis.call("ZREM", KEYS[1], ARGV[1])
local hdel = redis.call("HDEL", msg, ARGV[1])

if zrem == 1 and hdel == 1 then
    return 1
end
return 0
