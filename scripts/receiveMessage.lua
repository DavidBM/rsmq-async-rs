-- Receive (or pop) the next visible message from a queue.
-- KEYS[1]: ns:qname           (sorted set: id -> score)
-- KEYS[2]: ns:qname:cfg
-- KEYS[3]: ns:qname:msg
-- ARGV[1]: hidden ms ("-1" => use queue default `vt`; ignored if deleting)
-- ARGV[2]: "true" or "false"  (delete after receive)
-- ARGV[3]: optional minimum visibility in ms for a worker lease
-- Returns { found, id, body, rc, fr, sent } or { false, "", "", 0, "0", "0" }.
-- Errors: "QueueNotFound" if the queue doesn't exist.

local cfg = KEYS[2]
local msg = KEYS[3]

if redis.call("EXISTS", cfg) == 0 then
    return redis.error_reply("QueueNotFound")
end

local should_delete = ARGV[2] == "true"
local hidden_ms = tonumber(ARGV[1])
if hidden_ms < 0 and not should_delete then
    hidden_ms = tonumber(redis.call("HGET", cfg, "vt"))
end

if ARGV[3] and not should_delete then
    hidden_ms = math.max(hidden_ms or 0, tonumber(ARGV[3]))
end

local time = redis.call("TIME")
local sec_str = time[1]
local usec_str = time[2]
local time_us_str = sec_str .. string.rep("0", 6 - #usec_str) .. usec_str
local now_us = tonumber(time[1]) * 1000000 + tonumber(time[2])

local ids = redis.call("ZRANGE", KEYS[1], "-inf", now_us, "BYSCORE", "LIMIT", 0, 1)
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
    fr_str = time_us_str
    fr_out_str = time_us_str
else
    fr_out_str = fr_str
end

if should_delete then
    redis.call("ZREM", KEYS[1], id)
    redis.call("HDEL", msg, id)
else
    redis.call("HSET", msg, id, rc .. "\n" .. fr_str .. "\n" .. sent_str .. "\n" .. body)
    redis.call("ZADD", KEYS[1], now_us + (hidden_ms or 0) * 1000, id)
end

return { true, id, body, rc, fr_out_str, sent_str }
