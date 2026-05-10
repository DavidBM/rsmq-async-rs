-- Atomic receive with consumer-side dead-letter routing. For each visible message,
-- bumps rc; if rc > max_receives the message is atomically moved to the DLQ (preserving
-- :fr and the bumped :rc) and the script tries the next visible message. Returns the
-- first deliverable message, or empty if none qualified within the per-call cap.
--
-- KEYS[1]: ns:src      (source sorted set)
-- KEYS[2]: now ts      (upper bound for ZRANGE BYSCORE; :fr init)
-- KEYS[3]: new vt ts   (new visibility for the delivered message)
-- KEYS[4]: ns:dlq      (destination DLQ sorted set)
-- ARGV[1]: max_receives  (rc > max_receives routes to DLQ)
-- ARGV[2]: "1" for microsecond DLQ scores, "0" for millisecond
-- Returns { found, id, body, rc, fr }.

local max_receives = tonumber(ARGV[1])
local src_cfg = KEYS[1] .. ":cfg"
local src_msg = KEYS[1] .. ":msg"
local dlq_cfg = KEYS[4] .. ":cfg"
local dlq_msg = KEYS[4] .. ":msg"
local max_iter = 100  -- safety cap per invocation

for _ = 1, max_iter do
    local ids = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, 1)
    if #ids == 0 then
        return { false, "", "", 0, 0 }
    end

    local id = ids[1]
    local body = redis.call("HGET", src_msg, id)

    if not body then
        redis.call("ZREM", KEYS[1], id)
    else
        redis.call("HINCRBY", src_cfg, "totalrecv", 1)
        local rc = redis.call("HINCRBY", src_msg, id .. ":rc", 1)

        if rc > max_receives then
            local fr = redis.call("HGET", src_msg, id .. ":fr") or KEYS[2]
            local time = redis.call("TIME")
            local dlq_score
            if ARGV[2] == "1" then
                dlq_score = tonumber(time[1]) * 1000000 + tonumber(time[2])
            else
                dlq_score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            end

            redis.call("ZADD", KEYS[4], dlq_score, id)
            redis.call("HSET", dlq_msg, id, body)
            redis.call("HSET", dlq_msg, id .. ":rc", rc)
            redis.call("HSET", dlq_msg, id .. ":fr", fr)
            redis.call("HINCRBY", dlq_cfg, "totalsent", 1)

            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", src_msg, id, id .. ":rc", id .. ":fr")
        else
            local fr
            if rc == 1 then
                redis.call("HSET", src_msg, id .. ":fr", KEYS[2])
                fr = KEYS[2]
            else
                fr = redis.call("HGET", src_msg, id .. ":fr")
            end
            redis.call("ZADD", KEYS[1], KEYS[3], id)
            return { true, id, body, rc, fr }
        end
    end
end

return { false, "", "", 0, 0 }
