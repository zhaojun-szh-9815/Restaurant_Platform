---
--- Created by Zihao Shen
---

-- voucher id
local voucherId = ARGV[1]
-- user id
local userId = ARGV[2]
-- order id
local orderId = ARGV[3]
-- key of stock
local stockKey = 'seckill:stock:' .. voucherId
-- key of order
local orderKey = 'seckill:order:' .. voucherId

-- check if the stock > 0
if (tonumber(redis.call('get', stockKey)) <= 0) then
    -- out of stock
    return 1
end
-- check if the user order more than once: SISMEMBER orderKey userId
if (redis.call('sismember', orderKey, userId) == 1) then
    -- if exist, duplicate order, reject
    return 2
end
-- stock subtract 1: incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- create order: sadd orderKey userId
redis.call('sadd', orderKey, userId)

-- send message to MessageQueue
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0