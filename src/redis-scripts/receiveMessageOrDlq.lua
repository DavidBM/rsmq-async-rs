-- Atomic receive with consumer-side dead-letter routing. For each visible message in
-- the source queue, increments the receive count; if the new `rc` exceeds `max_receives`
-- the message is atomically moved to the DLQ (preserving `:fr` and the bumped `:rc`)
-- and the script tries the next visible message. Returns the first message whose `rc`
-- is still within budget, or empty if none qualify within the per-call iteration limit.
--
-- KEYS[1]: ns:src      (source sorted set key)
-- KEYS[2]: now ts      (current timestamp; upper bound for ZRANGE BYSCORE and :fr init)
-- KEYS[3]: new vt ts   (new visibility timestamp for the delivered message)
-- KEYS[4]: ns:dlq      (destination DLQ sorted set key)
-- ARGV[1]: max_receives (messages with rc > max_receives are routed to the DLQ)
-- ARGV[2]: "1" for microsecond DLQ scores (break-js-comp), "0" for millisecond
--
-- Returns { found, id, body, rc, fr } where `found` is true if a message was returned.

local max_receives = tonumber(ARGV[1])
local src_hash = KEYS[1] .. ":Q"
local dlq_hash = KEYS[4] .. ":Q"
local max_iter = 100  -- safety cap to keep individual script invocations bounded

for _ = 1, max_iter do
    local message = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, 1)
    if #message == 0 then
        return { false, "", "", 0, 0 }
    end

    local id = message[1]
    local body = redis.call("HGET", src_hash, id)

    if not body then
        -- Phantom (sorted-set entry without a body); clean it up and try the next one.
        redis.call("ZREM", KEYS[1], id)
    else
        redis.call("HINCRBY", src_hash, "totalrecv", 1)
        local rc = redis.call("HINCRBY", src_hash, id .. ":rc", 1)

        if rc > max_receives then
            -- Route to DLQ. If the message was DLQ'd before any normal delivery, `:fr`
            -- was never set on src — fall back to KEYS[2] so the DLQ entry is consistent
            -- with what receiveMessage.lua expects.
            local fr = redis.call("HGET", src_hash, id .. ":fr") or KEYS[2]
            local time = redis.call("TIME")
            local dlq_score
            if ARGV[2] == "1" then
                dlq_score = tonumber(time[1]) * 1000000 + tonumber(time[2])
            else
                dlq_score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            end

            redis.call("ZADD", KEYS[4], dlq_score, id)
            redis.call("HSET", dlq_hash, id, body)
            redis.call("HSET", dlq_hash, id .. ":rc", rc)
            redis.call("HSET", dlq_hash, id .. ":fr", fr)
            redis.call("HINCRBY", dlq_hash, "totalsent", 1)

            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", src_hash, id, id .. ":rc", id .. ":fr")
        else
            -- Deliver normally: set/extend visibility, return the message.
            local fr
            if rc == 1 then
                redis.call("HSET", src_hash, id .. ":fr", KEYS[2])
                fr = KEYS[2]
            else
                fr = redis.call("HGET", src_hash, id .. ":fr")
            end
            redis.call("ZADD", KEYS[1], KEYS[3], id)
            return { true, id, body, rc, fr }
        end
    end
end

-- Skipped `max_iter` phantom/DLQ messages without finding a deliverable one;
-- caller can invoke again to drain further.
return { false, "", "", 0, 0 }
