-- Receive (or pop) the next visible message from a queue.
-- KEYS[1]: ns:qname           (sorted set: id -> score)
-- KEYS[2]: current timestamp  (upper bound for ZRANGE BYSCORE; also :fr init value)
-- KEYS[3]: new visibility ts  (new score to push the message to, ignored if deleting)
-- ARGV[1]: "true" or "false"  (whether to delete after receive)
-- Returns { found, id, body, rc, fr } or { false, "", "", 0, 0 }.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"

local ids = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, 1)
if #ids == 0 then
    return { false, "", "", 0, 0 }
end

local id = ids[1]
local body = redis.call("HGET", msg, id)
if not body then
    -- Phantom (sorted-set entry without a body); leave it for the caller to retry.
    return { false, "", "", 0, 0 }
end

redis.call("HINCRBY", cfg, "totalrecv", 1)
local rc = redis.call("HINCRBY", msg, id .. ":rc", 1)

local fr
if rc == 1 then
    redis.call("HSET", msg, id .. ":fr", KEYS[2])
    fr = KEYS[2]
else
    fr = redis.call("HGET", msg, id .. ":fr")
end

if ARGV[1] == "true" then
    redis.call("ZREM", KEYS[1], id)
    redis.call("HDEL", msg, id, id .. ":rc", id .. ":fr")
else
    redis.call("ZADD", KEYS[1], KEYS[3], id)
end

return { true, id, body, rc, fr }
