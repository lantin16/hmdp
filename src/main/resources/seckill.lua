-- 1. 参数列表
-- 优惠券id
local voucherId = ARGV[1]
-- 用户id
local userId = ARGV[2]
-- 订单id
local orderId = ARGV[3]

-- 2. 数据key
-- 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 下单key
local orderKey = 'seckill:order:' .. voucherId

-- 3. 脚本业务
-- 判断库存是否充足 get stockKey
-- 注意redis取出的值是字符串，因此要转成数字再比较
if (tonumber(redis.call('get', stockKey)) <= 0) then
    -- 库存不足
    return 1
end

-- 判断用户之前是否下过单 sismember orderKey userId
if (redis.call('sismember', orderKey, userId) == 1) then
    -- 存在，说明是重复下单
    return 2
end

-- 扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)

-- 下单（保存用户） sadd orderKey userId
redis.call('sadd', orderKey, userId)

-- 发送消息到队列中 xadd stream.orders * k1 v1 k2 v2 ...
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

return 0