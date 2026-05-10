-- changeMessageVisibility.lua
-- This script changes the visibility timestamp of a message in a Redis sorted set.
-- KEYS[1]: The Redis key for the sorted set representing the message queue.
-- KEYS[2]: The message ID whose visibility is to be updated.
-- KEYS[3]: The new visibility timestamp to be set for the message.

-- Retrieve the current score (visibility timestamp) of the message
local currentScore = redis.call("ZSCORE", KEYS[1], KEYS[2])

-- If the message does not exist in the sorted set, return false
if not currentScore then
    return false
end

-- Update the message's visibility timestamp (score) to the new value provided
redis.call("ZADD", KEYS[1], KEYS[3], KEYS[2])

-- Return true indicating that the visibility has been successfully updated
return true
