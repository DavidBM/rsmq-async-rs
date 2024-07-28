-- This function either retrieves a message from the Redis queue, updates its visibility timeout,
-- increments counters, and returns message details, or removes the message if specified.
-- KEYS[1]: The Redis key for the sorted set representing the message queue.
-- KEYS[2]: The current time or a specific timestamp used for score comparisons.
-- KEYS[3]: The new visibility timestamp used to update the message score.
-- ARGV[1]: A string "true" or "false" indicating whether to delete the message after processing.

-- Find the next message due to be visible based on the current time (KEYS[2])
local message = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")

-- If no message is found, return a default empty response
if #message == 0 then
    return { false, "", "", 0, 0 }
end

-- Check if the message should be deleted
local should_delete = ARGV[1] == "true"

-- Increment the total received count for the queue
redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)

-- Get the message body from the hash
local messageBody = redis.call("HGET", KEYS[1] .. ":Q", message[1])

-- Increment the receive count for this message
local receiveCount = redis.call("HINCRBY", KEYS[1] .. ":Q", message[1] .. ":rc", 1)

-- Prepare the response table with message details
local response = { true, message[1], messageBody, receiveCount }

-- If the message is received for the first time, set and add the current time to the response
if receiveCount == 1 then
    redis.call("HSET", KEYS[1] .. ":Q", message[1] .. ":fr", KEYS[2])
    table.insert(response, KEYS[2])
else
    -- Otherwise, get the first received time and add it to the response
    local firstReceived = redis.call("HGET", KEYS[1] .. ":Q", message[1] .. ":fr")
    table.insert(response, firstReceived)
end

-- Update or remove the message based on the should_delete flag
if should_delete then
    -- Remove the message from the sorted set
    redis.call("ZREM", KEYS[1], message[1])
    -- Delete the message details from the hash
    redis.call("HDEL", KEYS[1] .. ":Q", message[1], message[1] .. ":rc", message[1] .. ":fr")
else
    -- Update the message's score to the new visibility timestamp (KEYS[3])
    redis.call("ZADD", KEYS[1], KEYS[3], message[1])
end

-- Return the response containing:
-- [1] boolean indicating if a message was found,
-- [2] message ID,
-- [3] message body,
-- [4] receive count,
-- [5] first received timestamp (either current time or previously set time)
return response
