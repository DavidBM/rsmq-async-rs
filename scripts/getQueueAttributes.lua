-- Atomically read queue attributes + stats in one round trip.
-- KEYS[1]: ns:qname:cfg  (queue config hash)
-- KEYS[2]: ns:qname      (sorted set key)

local time = redis.call("TIME")
local now_us = tonumber(time[1]) * 1000000 + tonumber(time[2])

local attrs = redis.call("HMGET", KEYS[1], "vt", "delay", "maxsize", "totalrecv", "totalsent", "created", "modified")
local msgs = redis.call("ZCARD", KEYS[2])
local hiddenmsgs = redis.call("ZCOUNT", KEYS[2], now_us, "+inf")

-- Flat array:
-- [1..2]  time (seconds, microseconds)
-- [3..9]  vt, delay, maxsize, totalrecv, totalsent, created, modified
-- [10]    total messages (ZCARD)
-- [11]    hidden messages (ZCOUNT)
return {
    time[1], time[2],
    attrs[1], attrs[2], attrs[3], attrs[4], attrs[5], attrs[6], attrs[7],
    msgs, hiddenmsgs
}
