-- Atomically moves a single message from one queue to another, preserving its body
-- and the receive-count / first-received timestamps. The message becomes visible in
-- the destination queue immediately (score = current time).
--
-- KEYS[1]: ns:src     (source sorted set key)
-- KEYS[2]: ns:dst     (destination sorted set key)
-- ARGV[1]: message id
-- ARGV[2]: "1" for microsecond scores (break-js-comp), "0" for millisecond scores.
--
-- Returns 1 if the message was found and moved, 0 if it didn't exist in the source.

local src_hash = KEYS[1] .. ":Q"
local dst_hash = KEYS[2] .. ":Q"

local body = redis.call("HGET", src_hash, ARGV[1])
if not body then
    return 0
end

local rc = redis.call("HGET", src_hash, ARGV[1] .. ":rc")
local fr = redis.call("HGET", src_hash, ARGV[1] .. ":fr")

local time = redis.call("TIME")
local score
if ARGV[2] == "1" then
    score = tonumber(time[1]) * 1000000 + tonumber(time[2])
else
    score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

redis.call("ZADD", KEYS[2], score, ARGV[1])
redis.call("HSET", dst_hash, ARGV[1], body)
if rc then redis.call("HSET", dst_hash, ARGV[1] .. ":rc", rc) end
if fr then redis.call("HSET", dst_hash, ARGV[1] .. ":fr", fr) end
redis.call("HINCRBY", dst_hash, "totalsent", 1)

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", src_hash, ARGV[1], ARGV[1] .. ":rc", ARGV[1] .. ":fr")

return 1
