package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 缓存工具封装类
 */

@Slf4j
@Component
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    /**
     * 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
     * @param key
     * @param value 要存储的Java对象
     * @param time
     * @param unit
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }


    /**
     * 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
     * @param key
     * @param value 要存储的Java对象
     * @param time
     * @param unit
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 封装成RedisData
        RedisData redisData = new RedisData();
        redisData.setData(value);
        // 设置逻辑过期，当前时间加上逻辑过期时间
        redisData.setExpireTime(LocalDateTime.now().plus(Duration.of(time, unit.toChronoUnit())));

        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));  // 逻辑过期本质是永久有效，因此不能设置TTL过期时间
    }




    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     *
     * 得先告诉是什么类型才能知道该返回什么类型，因此要传过来一个Class（泛型推断）
     * 返回值不确定，要用泛型
     * id的类型也不确定，也要用泛型
     * 由于不知道应该往数据库哪个表查，因此也需要调用者提供数据库查询的逻辑（函数式编程），有参有返回值的函数用Function
     *
     * @param keyPrefix key的前缀
     * @param id
     * @param type 反序列化的目标类型
     * @param dbFallback 从数据库查询的逻辑
     * @param time 缓存的TTL
     * @param unit 缓存的TTL单位
     * @return
     * @param <R> 反序列化的目标类型
     * @param <ID> id的类型
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
            Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. 从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (StrUtil.isNotBlank(json)) { // redis中查出空值""这里也是false
            // 3. 存在，反序列化为指定类型后返回
            return JSONUtil.toBean(json, type); // R的类型就是type
        }

        // 判断命中的是否是空值
        if ("".equals(json)) {
            return null;
        }

        // 4. 不存在/未命中。
        // 如果不是空值则根据id查询数据库
        R r = dbFallback.apply(id);

        // 5. 数据库中也不存在，返回错误
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);    // 空值的有效期要设置的短一些
            // 返回null
            return null;
        }

        // 6. 存在，写入redis
        this.set(key, r, time, unit);

        // 7. 返回
        return r;
    }





    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
     * @param keyPrefix
     * @param id
     * @param type
     * @param lockKeyPrefix
     * @param dbFallback
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <ID>
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
                                            String lockKeyPrefix, Function<ID, R> dbFallback,
                                            Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. 从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否命中
        if (StrUtil.isBlank(json)) {
            // 3. 如果未命中直接返回空
            return null;
        }

        // 4. 命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5. 判断是否过期
        if (LocalDateTime.now().isBefore(expireTime)) {
            // 5.1 未过期，直接返回对象
            return r;
        }

        // 6. 已过期，需要缓存重建
        // 6.1 获取互斥锁
        String lockKey = lockKeyPrefix + id;
        boolean getLock = tryLock(lockKey);

        // 6.2 判断是否获取锁成功
        if (getLock) {
            // 6.3 获取成功，需要再次检测redis缓存是否过期，做DoubleCheck，如果存在则无需重建缓存
            json = stringRedisTemplate.opsForValue().get(key);

            if (StrUtil.isBlank(json)) {
                return null;
            }

            redisData = JSONUtil.toBean(json, RedisData.class);
            r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            expireTime = redisData.getExpireTime();

            if (LocalDateTime.now().isBefore(expireTime)) {
                return r;
            }

            // 6.4 DoubleCheck后如果redis缓存仍是过期的，则开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally { // 释放锁要放在finally里面确保一定会执行
                    // 释放互斥锁
                    unLock(lockKey);
                }

            });
        }

        // 7. 返回过期的信息
        return r;
    }


    /**
     * 尝试获取锁
     * @param key 这里的锁其实就是redis中的一个key
     * @return
     */
    private boolean tryLock(String key) {
        // 锁也要设有效期，防止迟迟得不到释放也能通过有效期来释放锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);    // 这里最好不要直接返回flag，防止自动拆箱时出现空指针异常
    }


    /**
     * 释放锁
     * @param key
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }



}
