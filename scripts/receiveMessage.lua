-- Receive (or pop) the next visible message from a queue. Reads vt from :cfg if
-- hidden_ms is -1; calls TIME internally.
-- KEYS[1]: ns:qname           (sorted set: id -> score)
-- ARGV[1]: hidden ms ("-1" => use queue default `vt`; ignored if deleting)
-- ARGV[2]: "true" or "false"  (delete after receive)
-- ARGV[3]: "1" microsecond scores, "0" millisecond
-- Returns { found, id, body, rc, fr, sent } or { false, "", "", 0, "0", "0" }.
-- Errors: "QueueNotFound" if the queue doesn't exist.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"

if redis.call("EXISTS", cfg) == 0 then
    return redis.error_reply("QueueNotFound")
end

local should_delete = ARGV[2] == "true"
local hidden_ms = tonumber(ARGV[1])
if hidden_ms < 0 and not should_delete then
    hidden_ms = tonumber(redis.call("HGET", cfg, "vt"))
end

local time = redis.call("TIME")
-- Build time_us as a string (sec .. zero-padded usec) to avoid Lua 5.1 double
-- precision loss on 16-digit microsecond timestamps.
local sec_str = time[1]
local usec_str = time[2]
local time_us_str = sec_str .. string.rep("0", 6 - #usec_str) .. usec_str

-- Score arithmetic (uses Lua doubles; precision adequate for ZADD).
local now_score
local scaled_hidden
if ARGV[3] == "1" then
    now_score = tonumber(time[1]) * 1000000 + tonumber(time[2])
    scaled_hidden = (hidden_ms or 0) * 1000
else
    now_score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    scaled_hidden = hidden_ms or 0
end

-- For score-unit timestamps (used as fr when rc==1), choose ms or us based on mode.
local fr_now_str
if ARGV[3] == "1" then
    fr_now_str = time_us_str
else
    -- ms: sec * 1000 + floor(usec/1000), built as string to avoid precision loss.
    -- usec_str is up to 6 digits; we need its first 3 (the ms portion).
    local usec_padded = string.rep("0", 6 - #usec_str) .. usec_str
    local ms_part = string.sub(usec_padded, 1, 3)
    fr_now_str = sec_str .. ms_part
end

local ids = redis.call("ZRANGE", KEYS[1], "-inf", now_score, "BYSCORE", "LIMIT", 0, 1)
if #ids == 0 then
    return { false, "", "", 0, "0", "0" }
end

local id = ids[1]
local packed = redis.call("HGET", msg, id)
if not packed then
    return { false, "", "", 0, "0", "0" }
end

local p1 = string.find(packed, "\n", 1, true)
local p2 = string.find(packed, "\n", p1 + 1, true)
local p3 = string.find(packed, "\n", p2 + 1, true)
local rc = tonumber(string.sub(packed, 1, p1 - 1))
local fr_str = string.sub(packed, p1 + 1, p2 - 1)
local sent_str = string.sub(packed, p2 + 1, p3 - 1)
local body = string.sub(packed, p3 + 1)

redis.call("HINCRBY", cfg, "totalrecv", 1)
rc = rc + 1

local fr_out_str
if rc == 1 then
    fr_str = fr_now_str
    fr_out_str = fr_now_str
else
    fr_out_str = fr_str
end

if should_delete then
    redis.call("ZREM", KEYS[1], id)
    redis.call("HDEL", msg, id)
else
    redis.call("HSET", msg, id, rc .. "\n" .. fr_str .. "\n" .. sent_str .. "\n" .. body)
    redis.call("ZADD", KEYS[1], now_score + scaled_hidden, id)
end

return { true, id, body, rc, fr_out_str, sent_str }
