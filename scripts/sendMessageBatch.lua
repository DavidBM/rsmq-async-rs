-- Atomically inserts a batch of messages into the queue.
-- KEYS[1]: namespace:queuename (the sorted set key for the queue)
-- ARGV[1]: realtime flag ("1" or "0") — when "1", returns the new ZCARD so the
--          caller can do a single PUBLISH outside the script.
-- ARGV[2..]: triplets of (id, score, body), one per message. Total length must be
--            1 + 3*N for N messages.
-- Returns: new ZCARD if realtime, else 0.

local n = (#ARGV - 1) / 3
local queue_hash = KEYS[1] .. ":Q"

for i = 0, n - 1 do
    local id    = ARGV[2 + i * 3]
    local score = ARGV[3 + i * 3]
    local body  = ARGV[4 + i * 3]
    redis.call("ZADD", KEYS[1], score, id)
    redis.call("HSET", queue_hash, id, body)
end

redis.call("HINCRBY", queue_hash, "totalsent", n)

if ARGV[1] == "1" then
    return redis.call("ZCARD", KEYS[1])
end
return 0
