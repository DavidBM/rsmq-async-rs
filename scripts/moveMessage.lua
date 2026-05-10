-- Atomically move a single message from one queue to another, preserving body+rc+fr+sent.
-- KEYS[1]: ns:src     (source sorted set)
-- KEYS[2]: ns:dst     (destination sorted set)
-- ARGV[1]: message id
-- ARGV[2]: "1" for microsecond scores, "0" for millisecond
-- Returns 1 if moved, 0 if the message did not exist in the source.

local src_msg = KEYS[1] .. ":msg"
local dst_msg = KEYS[2] .. ":msg"
local dst_cfg = KEYS[2] .. ":cfg"

local packed = redis.call("HGET", src_msg, ARGV[1])
if not packed then
    return 0
end

local time = redis.call("TIME")
local score
if ARGV[2] == "1" then
    score = tonumber(time[1]) * 1000000 + tonumber(time[2])
else
    score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

-- If src had rc>0 but fr=0 (message moved before its first delivery), patch fr=score
-- so a subsequent receive on dst is consistent.
local p1 = string.find(packed, "\n", 1, true)
local p2 = string.find(packed, "\n", p1 + 1, true)
local p3 = string.find(packed, "\n", p2 + 1, true)
local rc = tonumber(string.sub(packed, 1, p1 - 1))
local fr_str = string.sub(packed, p1 + 1, p2 - 1)
if rc > 0 and tonumber(fr_str) == 0 then
    local sent_str = string.sub(packed, p2 + 1, p3 - 1)
    local body = string.sub(packed, p3 + 1)
    packed = rc .. "\n" .. score .. "\n" .. sent_str .. "\n" .. body
end

redis.call("ZADD", KEYS[2], score, ARGV[1])
redis.call("HSET", dst_msg, ARGV[1], packed)
redis.call("HINCRBY", dst_cfg, "totalsent", 1)

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", src_msg, ARGV[1])

return 1
