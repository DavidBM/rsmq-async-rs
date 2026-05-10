-- Update the visibility timestamp (score) of an in-flight message.
-- KEYS[1]: ns:qname     (sorted set key)
-- ARGV[1]: message id
-- ARGV[2]: hidden duration in ms ("-1" to use the queue's default `vt`)
-- Returns 1 if the message existed and was updated, 0 otherwise.
-- Errors: "QueueNotFound" if the queue doesn't exist.

local cfg = KEYS[1] .. ":cfg"

if redis.call("EXISTS", cfg) == 0 then
    return redis.error_reply("QueueNotFound")
end

if redis.call("ZSCORE", KEYS[1], ARGV[1]) == false then
    return 0
end

local hidden_ms = tonumber(ARGV[2])
if hidden_ms < 0 then
    hidden_ms = tonumber(redis.call("HGET", cfg, "vt"))
end

local time = redis.call("TIME")
local now_us = tonumber(time[1]) * 1000000 + tonumber(time[2])
redis.call("ZADD", KEYS[1], now_us + hidden_ms * 1000, ARGV[1])
return 1
