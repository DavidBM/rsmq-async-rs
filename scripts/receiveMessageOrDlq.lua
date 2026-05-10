-- Atomic receive with consumer-side DLQ routing. For each visible message, bumps rc;
-- if rc > max_receives the message is atomically moved to the DLQ and the script tries
-- the next visible message. Returns the first deliverable message, or empty if none
-- qualified within the per-call iteration cap.
--
-- KEYS[1]: ns:src       (source sorted set)
-- KEYS[2]: now ts       (upper bound for ZRANGE BYSCORE; :fr init)
-- KEYS[3]: new vt ts    (new visibility for the delivered message)
-- KEYS[4]: ns:dlq       (destination DLQ sorted set)
-- ARGV[1]: max_receives
-- ARGV[2]: "1" for microsecond scores, "0" for millisecond
-- Returns { found, id, body, rc, fr, sent } or { false, "", "", 0, 0, 0 }.

local max_receives = tonumber(ARGV[1])
local src_cfg = KEYS[1] .. ":cfg"
local src_msg = KEYS[1] .. ":msg"
local dlq_cfg = KEYS[4] .. ":cfg"
local dlq_msg = KEYS[4] .. ":msg"
local max_iter = 100

for _ = 1, max_iter do
    local ids = redis.call("ZRANGE", KEYS[1], "-inf", KEYS[2], "BYSCORE", "LIMIT", 0, 1)
    if #ids == 0 then
        return { false, "", "", 0, 0, 0 }
    end

    local id = ids[1]
    local packed = redis.call("HGET", src_msg, id)

    if not packed then
        redis.call("ZREM", KEYS[1], id)
    else
        local p1 = string.find(packed, "\n", 1, true)
        local p2 = string.find(packed, "\n", p1 + 1, true)
        local p3 = string.find(packed, "\n", p2 + 1, true)
        local rc = tonumber(string.sub(packed, 1, p1 - 1))
        local fr_str = string.sub(packed, p1 + 1, p2 - 1)
        local sent_str = string.sub(packed, p2 + 1, p3 - 1)
        local body = string.sub(packed, p3 + 1)

        redis.call("HINCRBY", src_cfg, "totalrecv", 1)
        rc = rc + 1

        if rc > max_receives then
            -- Patch fr to KEYS[2] if it was never set on a normal delivery.
            local fr_for_dlq
            if tonumber(fr_str) == 0 then
                fr_for_dlq = KEYS[2]
            else
                fr_for_dlq = fr_str
            end
            local time = redis.call("TIME")
            local dlq_score
            if ARGV[2] == "1" then
                dlq_score = tonumber(time[1]) * 1000000 + tonumber(time[2])
            else
                dlq_score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            end

            local repacked = rc .. "\n" .. fr_for_dlq .. "\n" .. sent_str .. "\n" .. body
            redis.call("ZADD", KEYS[4], dlq_score, id)
            redis.call("HSET", dlq_msg, id, repacked)
            redis.call("HINCRBY", dlq_cfg, "totalsent", 1)

            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", src_msg, id)
        else
            local fr_out
            if rc == 1 then
                fr_str = KEYS[2]
                fr_out = KEYS[2]
            else
                fr_out = fr_str
            end
            redis.call("HSET", src_msg, id, rc .. "\n" .. fr_str .. "\n" .. sent_str .. "\n" .. body)
            redis.call("ZADD", KEYS[1], KEYS[3], id)
            return { true, id, body, rc, tonumber(fr_out), tonumber(sent_str) }
        end
    end
end

return { false, "", "", 0, 0, 0 }
