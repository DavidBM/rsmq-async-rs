-- Atomically retrieves queue attributes and statistics in a single script execution,
-- eliminating the race between the TIME call and the ZCOUNT threshold.
-- KEYS[1]: ns:qname:Q (the queue hash key)
-- KEYS[2]: ns:qname (the sorted set key)
-- ARGV[1]: time multiplier for the hidden-messages ZCOUNT threshold

local time = redis.call("TIME")
local threshold = tonumber(time[1]) * tonumber(ARGV[1])

local attrs = redis.call("HMGET", KEYS[1], "vt", "delay", "maxsize", "totalrecv", "totalsent", "created", "modified")
local msgs = redis.call("ZCARD", KEYS[2])
local hiddenmsgs = redis.call("ZCOUNT", KEYS[2], threshold, "+inf")

-- Returns a flat array:
-- [1..2]  time (seconds, microseconds)
-- [3..9]  queue attributes (vt, delay, maxsize, totalrecv, totalsent, created, modified)
-- [10]    total messages (ZCARD)
-- [11]    hidden messages (ZCOUNT)
return {
    time[1], time[2],
    attrs[1], attrs[2], attrs[3], attrs[4], attrs[5], attrs[6], attrs[7],
    msgs, hiddenmsgs
}
