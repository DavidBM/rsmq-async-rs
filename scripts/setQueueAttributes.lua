-- Update queue attributes. Pass empty string "" for fields you don't want to change.
-- KEYS[1]: ns:qname:cfg   (queue config hash)
-- ARGV[1]: vt (ms) or ""
-- ARGV[2]: delay (ms) or ""
-- ARGV[3]: maxsize (bytes; -1 = unlimited) or ""
-- Returns 1 if the queue existed, 0 otherwise.

if redis.call("EXISTS", KEYS[1]) == 0 then
    return 0
end

local time = redis.call("TIME")
redis.call("HSET", KEYS[1], "modified", tonumber(time[1]))

if ARGV[1] ~= "" then
    redis.call("HSET", KEYS[1], "vt", ARGV[1])
end
if ARGV[2] ~= "" then
    redis.call("HSET", KEYS[1], "delay", ARGV[2])
end
if ARGV[3] ~= "" then
    redis.call("HSET", KEYS[1], "maxsize", ARGV[3])
end

return 1
