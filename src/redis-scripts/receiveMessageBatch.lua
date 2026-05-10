-- Atomically receives up to N visible messages, updating each one's visibility timestamp
-- (or removing them if requested), and incrementing the per-queue and per-message counters.
-- KEYS[1]: namespace:queuename (the sorted set key)
-- KEYS[2]: current timestamp (used for score upper bound and `:fr` initialization)
-- KEYS[3]: new visibility timestamp (applied to each non-deleted received message)
-- ARGV[1]: "true" or "false" — whether to delete the messages after receiving (pop semantics)
-- ARGV[2]: maximum number of messages to receive (positive integer as string)
-- Returns: a Lua array of [id, body, rc, fr] tuples (one per actually-returned message).
--          Phantom entries (sorted-set members with no body in the hash) are skipped silently
--          to mirror the single-message receive logic.

local max_count = tonumber(ARGV[2])
local ids = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, max_count)
local should_delete = ARGV[1] == "true"
local queue_hash = KEYS[1] .. ":Q"
local results = {}

for i = 1, #ids do
    local id = ids[i]
    local body = redis.call("HGET", queue_hash, id)
    if body then
        redis.call("HINCRBY", queue_hash, "totalrecv", 1)
        local rc = redis.call("HINCRBY", queue_hash, id .. ":rc", 1)
        local fr
        if rc == 1 then
            redis.call("HSET", queue_hash, id .. ":fr", KEYS[2])
            fr = KEYS[2]
        else
            fr = redis.call("HGET", queue_hash, id .. ":fr")
        end
        if should_delete then
            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", queue_hash, id, id .. ":rc", id .. ":fr")
        else
            redis.call("ZADD", KEYS[1], KEYS[3], id)
        end
        table.insert(results, { id, body, rc, fr })
    end
end

return results
