local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")
if #msg == 0 then
	return {false, "", "", 0, 0}
end
redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)
local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
local o = {true, msg[1], mbody, rc}
if rc==1 then
	table.insert(o, KEYS[2])
else
	local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")
	table.insert(o, fr)
end
redis.call("ZREM", KEYS[1], msg[1])
redis.call("HDEL", KEYS[1] .. ":Q", msg[1], msg[1] .. ":rc", msg[1] .. ":fr")
return o
