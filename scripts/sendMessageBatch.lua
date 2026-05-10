-- Atomically insert a batch of messages into the queue.
-- KEYS[1]: ns:qname               (sorted set key)
-- ARGV[1]: realtime flag ("1" or "0") — when "1", returns the new ZCARD so the
--          caller can do a single PUBLISH outside the script.
-- ARGV[2..]: triplets (id, score, body), one per message. Length = 1 + 3*N.
-- Returns: new ZCARD if realtime, else 0.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"
local n = (#ARGV - 1) / 3

for i = 0, n - 1 do
    local id    = ARGV[2 + i * 3]
    local score = ARGV[3 + i * 3]
    local body  = ARGV[4 + i * 3]
    redis.call("ZADD", KEYS[1], score, id)
    redis.call("HSET", msg, id, body)
end

redis.call("HINCRBY", cfg, "totalsent", n)

if ARGV[1] == "1" then
    return redis.call("ZCARD", KEYS[1])
end
return 0
