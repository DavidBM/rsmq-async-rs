-- Atomically receive up to N visible messages, sharing the same new visibility timestamp.
-- Phantom entries (sorted-set members with no body) are skipped silently.
-- KEYS[1]: ns:qname           (sorted set key)
-- KEYS[2]: current timestamp  (upper bound for ZRANGE BYSCORE; also :fr init)
-- KEYS[3]: new visibility ts  (new score for each non-deleted message)
-- ARGV[1]: "true" or "false"  (delete after receive — pop semantics)
-- ARGV[2]: max_count          (positive integer as string)
-- Returns an array of { id, body, rc, fr } tuples.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"
local should_delete = ARGV[1] == "true"
local max_count = tonumber(ARGV[2])

local ids = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, max_count)
local results = {}

for i = 1, #ids do
    local id = ids[i]
    local body = redis.call("HGET", msg, id)
    if body then
        redis.call("HINCRBY", cfg, "totalrecv", 1)
        local rc = redis.call("HINCRBY", msg, id .. ":rc", 1)
        local fr
        if rc == 1 then
            redis.call("HSET", msg, id .. ":fr", KEYS[2])
            fr = KEYS[2]
        else
            fr = redis.call("HGET", msg, id .. ":fr")
        end
        if should_delete then
            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", msg, id, id .. ":rc", id .. ":fr")
        else
            redis.call("ZADD", KEYS[1], KEYS[3], id)
        end
        table.insert(results, { id, body, rc, fr })
    end
end

return results
