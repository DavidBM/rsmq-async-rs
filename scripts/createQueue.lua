-- Atomically create a queue. Initializes the :cfg hash with default-able fields
-- and adds the queue name to the {ns}:QUEUES set.
-- KEYS[1]: ns:qname:cfg
-- KEYS[2]: ns:QUEUES
-- ARGV[1]: qname
-- ARGV[2]: vt (ms)
-- ARGV[3]: delay (ms)
-- ARGV[4]: maxsize (bytes; -1 = unlimited)
-- Returns 1 if the queue was created, 0 if it already existed.

local time = redis.call("TIME")
local now = tonumber(time[1])

local created = redis.call("HSETNX", KEYS[1], "vt", ARGV[2])
if created == 0 then
    return 0
end

redis.call("HSETNX", KEYS[1], "delay", ARGV[3])
redis.call("HSETNX", KEYS[1], "maxsize", ARGV[4])
redis.call("HSETNX", KEYS[1], "created", now)
redis.call("HSETNX", KEYS[1], "modified", now)
redis.call("HSETNX", KEYS[1], "totalrecv", 0)
redis.call("HSETNX", KEYS[1], "totalsent", 0)

redis.call("SADD", KEYS[2], ARGV[1])

return 1
