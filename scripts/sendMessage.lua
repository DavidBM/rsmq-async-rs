-- Atomically send a single message: ZADD + HSET (packed body) + HINCRBY totalsent
-- + optional PUBLISH. Reads queue config (delay default, maxsize) from :cfg.
--
-- KEYS[1]: ns:qname           (sorted set key)
-- KEYS[2]: ns:rt:qname        (realtime channel name)
-- ARGV[1]: id
-- ARGV[2]: delay (ms) or "-1" to use the queue's default `delay`
-- ARGV[3]: realtime ("1" or "0")
-- ARGV[4]: "1" for microsecond scores, "0" for millisecond
-- ARGV[5]: body
-- Returns the assigned id (echoed for convenience).
-- Errors: "QueueNotFound" if the queue doesn't exist; "MessageTooLong" if body > maxsize.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"

if redis.call("EXISTS", cfg) == 0 then
    return redis.error_reply("QueueNotFound")
end

local cfg_vals = redis.call("HMGET", cfg, "delay", "maxsize")

local delay_ms = tonumber(ARGV[2])
if delay_ms < 0 then
    delay_ms = tonumber(cfg_vals[1])
end

local maxsize = tonumber(cfg_vals[2])
if maxsize ~= -1 and #ARGV[5] > maxsize then
    return redis.error_reply("MessageTooLong")
end

local time = redis.call("TIME")
local sec_str = time[1]
local usec_str = time[2]
local time_us_str = sec_str .. string.rep("0", 6 - #usec_str) .. usec_str

local now_score
local scaled_delay
if ARGV[4] == "1" then
    now_score = tonumber(time[1]) * 1000000 + tonumber(time[2])
    scaled_delay = delay_ms * 1000
else
    now_score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    scaled_delay = delay_ms
end

local score = now_score + scaled_delay

redis.call("ZADD", KEYS[1], score, ARGV[1])
redis.call("HSET", msg, ARGV[1], "0\n0\n" .. time_us_str .. "\n" .. ARGV[5])
redis.call("HINCRBY", cfg, "totalsent", 1)

if ARGV[3] == "1" then
    local size = redis.call("ZCARD", KEYS[1])
    redis.call("PUBLISH", KEYS[2], size)
end

return ARGV[1]
