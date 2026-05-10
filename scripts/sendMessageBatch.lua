-- Atomically insert a batch of messages into the queue.
-- KEYS[1]: ns:qname               (sorted set key)
-- ARGV[1]: realtime flag ("1" or "0")
-- ARGV[2]: sent timestamp (decimal string, used as the `sent` slot for every msg)
-- ARGV[3..]: triplets (id, score, body), one per message. Length = 2 + 3*N.
-- Returns: new ZCARD if realtime, else 0.
--
-- Per-message hash value layout: "<rc>\n<fr>\n<sent>\n<body>". rc and fr start at 0.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"
local sent = ARGV[2]
local n = (#ARGV - 2) / 3

for i = 0, n - 1 do
    local id    = ARGV[3 + i * 3]
    local score = ARGV[4 + i * 3]
    local body  = ARGV[5 + i * 3]
    redis.call("ZADD", KEYS[1], score, id)
    redis.call("HSET", msg, id, "0\n0\n" .. sent .. "\n" .. body)
end

redis.call("HINCRBY", cfg, "totalsent", n)

if ARGV[1] == "1" then
    return redis.call("ZCARD", KEYS[1])
end
return 0
