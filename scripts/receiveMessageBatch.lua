-- Atomically receive up to N visible messages.
-- KEYS[1]: ns:qname           (sorted set key)
-- ARGV[1]: hidden ms ("-1" => use queue default `vt`)
-- ARGV[2]: "true" or "false"  (delete after receive)
-- ARGV[3]: max_count (positive integer as string)
-- ARGV[4]: "1" microsecond scores, "0" millisecond
-- Returns an array of { id, body, rc, fr, sent } tuples.
-- Errors: "QueueNotFound" if the queue doesn't exist.

local cfg = KEYS[1] .. ":cfg"
local msg = KEYS[1] .. ":msg"

if redis.call("EXISTS", cfg) == 0 then
    return redis.error_reply("QueueNotFound")
end

local should_delete = ARGV[2] == "true"
local max_count = tonumber(ARGV[3])
local hidden_ms = tonumber(ARGV[1])
if hidden_ms < 0 and not should_delete then
    hidden_ms = tonumber(redis.call("HGET", cfg, "vt"))
end

local time = redis.call("TIME")
local sec_str = time[1]
local usec_str = time[2]
local time_us_str = sec_str .. string.rep("0", 6 - #usec_str) .. usec_str

local now_score
local scaled_hidden
if ARGV[4] == "1" then
    now_score = tonumber(time[1]) * 1000000 + tonumber(time[2])
    scaled_hidden = (hidden_ms or 0) * 1000
else
    now_score = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    scaled_hidden = hidden_ms or 0
end

local fr_now_str
if ARGV[4] == "1" then
    fr_now_str = time_us_str
else
    local usec_padded = string.rep("0", 6 - #usec_str) .. usec_str
    fr_now_str = sec_str .. string.sub(usec_padded, 1, 3)
end

local ids = redis.call("ZRANGE", KEYS[1], "-inf", now_score, "BYSCORE", "LIMIT", 0, max_count)
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

        local fr_out_str
        if rc == 1 then
            fr_str = fr_now_str
            fr_out_str = fr_now_str
        else
            fr_out_str = fr_str
        end

        if should_delete then
            redis.call("ZREM", KEYS[1], id)
            redis.call("HDEL", msg, id)
        else
            redis.call("HSET", msg, id, rc .. "\n" .. fr_str .. "\n" .. sent_str .. "\n" .. body)
            redis.call("ZADD", KEYS[1], now_score + scaled_hidden, id)
        end

        table.insert(results, { id, body, rc, fr_out_str, sent_str })
    end
end

return results
