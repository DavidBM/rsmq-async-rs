-- Atomically send a batch of messages.
-- KEYS[1]: ns:qname           (sorted set key)
-- KEYS[2]: ns:rt:qname        (realtime channel name)
-- KEYS[3]: ns:qname:cfg
-- KEYS[4]: ns:qname:msg
-- ARGV[1]: delay (ms) or "-1" to use the queue's default `delay`
-- ARGV[2]: realtime ("1" or "0")
-- ARGV[3..]: pairs of (random 16-hex-character ID suffix, body), one per message. Length = 2 + 2*N.
-- Returns the assigned IDs in input order.
-- Errors: "QueueNotFound" if the queue doesn't exist; "MessageTooLong" if any body > maxsize.

local cfg = KEYS[3]
local msg = KEYS[4]

if redis.call("EXISTS", cfg) == 0 then
    return redis.error_reply("QueueNotFound")
end

local cfg_vals = redis.call("HMGET", cfg, "delay", "maxsize")
local delay_ms = tonumber(ARGV[1])
if delay_ms < 0 then
    delay_ms = tonumber(cfg_vals[1])
end
local maxsize = tonumber(cfg_vals[2])

local n = (#ARGV - 2) / 2

if maxsize ~= -1 then
    for i = 0, n - 1 do
        local body = ARGV[4 + i * 2]
        if #body > maxsize then
            return redis.error_reply("MessageTooLong")
        end
    end
end

local time = redis.call("TIME")
local sec_str = time[1]
local usec_str = time[2]
local time_us_str = sec_str .. string.rep("0", 6 - #usec_str) .. usec_str
local now_us = tonumber(time[1]) * 1000000 + tonumber(time[2])
local score = now_us + delay_ms * 1000

-- Equal scores are sorted by ID. Reserve one counter value per message.
local first_sequence = redis.call("HINCRBY", cfg, "totalsent", n) - n + 1
local ids = {}
for i = 0, n - 1 do
    local id = string.format("%016x", first_sequence + i) .. ARGV[3 + i * 2]
    local body = ARGV[4 + i * 2]
    redis.call("ZADD", KEYS[1], score, id)
    redis.call("HSET", msg, id, "0\n0\n" .. time_us_str .. "\n" .. body)
    ids[i + 1] = id
end

if ARGV[2] == "1" then
    local size = redis.call("ZCARD", KEYS[1])
    redis.call("PUBLISH", KEYS[2], size)
end

return ids
