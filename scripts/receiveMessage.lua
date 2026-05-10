-- Receive (or pop) the next visible message from a queue.
-- KEYS[1]: ns:qname           (sorted set: id -> score)
-- KEYS[2]: current timestamp  (upper bound for ZRANGE BYSCORE; also :fr init value)
-- KEYS[3]: new visibility ts  (new score; ignored if deleting)
-- ARGV[1]: "true" or "false"  (whether to delete after receive)
-- Returns { found, id, body, rc, fr, sent } or { false, "", "", 0, 0, 0 }.
--
-- Per-message hash value layout: "<rc>\n<fr>\n<sent>\n<body>".

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"

local ids = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, 1)
if #ids == 0 then
    return { false, "", "", 0, 0, 0 }
end

local id = ids[1]
local packed = redis.call("HGET", msg, id)
if not packed then
    return { false, "", "", 0, 0, 0 }
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

local fr_out
if rc == 1 then
    fr_str = KEYS[2]
    fr_out = KEYS[2]
else
    fr_out = fr_str
end

if ARGV[1] == "true" then
    redis.call("ZREM", KEYS[1], id)
    redis.call("HDEL", msg, id)
else
    redis.call("HSET", msg, id, rc .. "\n" .. fr_str .. "\n" .. sent_str .. "\n" .. body)
    redis.call("ZADD", KEYS[1], KEYS[3], id)
end

return { true, id, body, rc, tonumber(fr_out), tonumber(sent_str) }
