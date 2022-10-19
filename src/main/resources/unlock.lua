---
--- Created by Zihao Shen.
---

-- key of lock: key[1]
-- current thread id: argv[1]
-- get the value of key in redis: id
-- check if threadId = id
if (ARGV[1] == redis.call('get', KEYS[1])) then
    -- unlock
    return redis.call('del', KEYS[1])
end
return 0