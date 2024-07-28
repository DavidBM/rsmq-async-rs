local msg = redis.call("ZSCORE", KEYS[1], KEYS[2])
if not msg then
    return false
end
redis.call("ZADD", KEYS[1], KEYS[3], KEYS[2])
return true
