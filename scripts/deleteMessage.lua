-- Atomically delete a single message: remove from the sorted set and from the :msg hash.
-- KEYS[1]: ns:qname     (sorted set key)
-- KEYS[2]: ns:qname:msg
-- ARGV[1]: message id
-- ARGV[2]: optional receive count to guard a worker acknowledgment
-- Returns 1 if the message existed (and was deleted), 0 otherwise.

local msg = KEYS[2]

if ARGV[2] then
    local packed = redis.call("HGET", msg, ARGV[1])
    if not packed or string.match(packed, "^([^\n]+)") ~= ARGV[2] then
        return 0
    end
end

local zrem = redis.call("ZREM", KEYS[1], ARGV[1])
local hdel = redis.call("HDEL", msg, ARGV[1])

if zrem == 1 and hdel == 1 then
    return 1
end
return 0
