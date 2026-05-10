-- Atomically move a single message from one queue to another, preserving its body
-- and the receive-count / first-received metadata. Score in the destination = current time.
--
-- KEYS[1]: ns:src     (source sorted set)
-- KEYS[2]: ns:dst     (destination sorted set)
-- ARGV[1]: message id
-- ARGV[2]: "1" for microsecond scores, "0" for millisecond
-- Returns 1 if moved, 0 if the message did not exist in the source.

local src_msg = KEYS[1] .. ":msg"
local dst_msg = KEYS[2] .. ":msg"
local dst_cfg = KEYS[2] .. ":cfg"

local body = redis.call("HGET", src_msg, ARGV[1])
if not body then
    return 0
end

local rc = redis.call("HGET", src_msg, ARGV[1] .. ":rc")
local fr = redis.call("HGET", src_msg, ARGV[1] .. ":fr")

local time = redis.call("TIME")
local score
if ARGV[2] == "1" then
    score = tonumber(time[1]) * 1000000 + tonumber(time[2])
else
    score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

redis.call("ZADD", KEYS[2], score, ARGV[1])
redis.call("HSET", dst_msg, ARGV[1], body)
-- Preserve metadata. Default :fr to current score if rc was set without one.
if rc then
    redis.call("HSET", dst_msg, ARGV[1] .. ":rc", rc)
    redis.call("HSET", dst_msg, ARGV[1] .. ":fr", fr or score)
end
redis.call("HINCRBY", dst_cfg, "totalsent", 1)

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", src_msg, ARGV[1], ARGV[1] .. ":rc", ARGV[1] .. ":fr")

return 1
