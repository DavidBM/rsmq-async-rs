-- Atomically receive up to N visible messages, sharing the same new visibility timestamp.
-- KEYS[1]: ns:qname           (sorted set key)
-- KEYS[2]: current timestamp  (upper bound for ZRANGE BYSCORE; :fr init)
-- KEYS[3]: new visibility ts  (new score for each non-deleted message)
-- ARGV[1]: "true" or "false"  (delete after receive — pop semantics)
-- ARGV[2]: max_count          (positive integer as string)
-- Returns an array of { id, body, rc, fr, sent } tuples.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"
local should_delete = ARGV[1] == "true"
local max_count = tonumber(ARGV[2])

local ids = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, max_count)
local results = {}

for i = 1, #ids do
    local id = ids[i]
    local packed = redis.call("HGET", msg, id)
    if packed then
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

        if should_delete then
            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", msg, id)
        else
            redis.call("HSET", msg, id, rc .. "\n" .. fr_str .. "\n" .. sent_str .. "\n" .. body)
            redis.call("ZADD", KEYS[1], KEYS[3], id)
        end

        table.insert(results, { id, body, rc, tonumber(fr_out), tonumber(sent_str) })
    end
end

return results
