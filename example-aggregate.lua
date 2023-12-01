explain this code '-- Define the keys of the two hash tables 
local hash1Key = KEYS[1] 
local hash2Key = KEYS[2] 
-- Get the contents of the two hash tables 
local hash1Contents = redis.call("HGETALL", hash1Key) 
local hash2Contents = redis.call("HGETALL", hash2Key) 
-- Create a new hash table for the result 
local resultHashKey = KEYS[3] 
for i = 1, #hash1Contents, 2 do 
    local field = hash1Contents[i] 
    local value = hash1Contents[i + 1] 
    local correspondingValue = redis.call("HGET", hash2Key, field) 
    if correspondingValue then 
        -- If the field exists in both hash tables, create a key-value pair in the result hash 
        redis.call("HSET", resultHashKey, field, value .. ":" .. correspondingValue) 
    end 
end 
return "OK" 