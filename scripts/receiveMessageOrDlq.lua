-- Atomic receive with consumer-side DLQ routing.
-- KEYS[1]: ns:src       (source sorted set)
-- KEYS[2]: ns:dlq       (destination DLQ sorted set)
-- ARGV[1]: hidden ms ("-1" => use queue default `vt`)
-- ARGV[2]: max_receives
-- Returns { found, id, body, rc, fr, sent }.
-- Errors: "QueueNotFound" if the source queue doesn't exist.

local src_cfg = KEYS[1] .. ":cfg"
local src_msg = KEYS[1] .. ":msg"
local dlq_cfg = KEYS[2] .. ":cfg"
local dlq_msg = KEYS[2] .. ":msg"

if redis.call("EXISTS", src_cfg) == 0 then
    return redis.error_reply("QueueNotFound")
end

local hidden_ms = tonumber(ARGV[1])
if hidden_ms < 0 then
    hidden_ms = tonumber(redis.call("HGET", src_cfg, "vt"))
end
local max_receives = tonumber(ARGV[2])

local time = redis.call("TIME")
local sec_str = time[1]
local usec_str = time[2]
local time_us_str = sec_str .. string.rep("0", 6 - #usec_str) .. usec_str
local now_us = tonumber(time[1]) * 1000000 + tonumber(time[2])
local new_score = now_us + (hidden_ms or 0) * 1000

local max_iter = 100

for _ = 1, max_iter do
    local ids = redis.call("ZRANGE", KEYS[1], "-inf", now_us, "BYSCORE", "LIMIT", 0, 1)
    if #ids == 0 then
        return { false, "", "", 0, "0", "0" }
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
            local fr_for_dlq
            if fr_str == "0" then
                fr_for_dlq = time_us_str
            else
                fr_for_dlq = fr_str
            end

            local repacked = rc .. "\n" .. fr_for_dlq .. "\n" .. sent_str .. "\n" .. body
            redis.call("ZADD", KEYS[2], now_us, id)
            redis.call("HSET", dlq_msg, id, repacked)
            redis.call("HINCRBY", dlq_cfg, "totalsent", 1)

            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", src_msg, id)
        else
            local fr_out_str
            if rc == 1 then
                fr_str = time_us_str
                fr_out_str = time_us_str
            else
                fr_out_str = fr_str
            end
            redis.call("HSET", src_msg, id, rc .. "\n" .. fr_str .. "\n" .. sent_str .. "\n" .. body)
            redis.call("ZADD", KEYS[1], new_score, id)
            return { true, id, body, rc, fr_out_str, sent_str }
        end
    end
end

return { false, "", "", 0, "0", "0" }
