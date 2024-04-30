package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.MessageConstants;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询商铺详情信息
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {
        // 缓存空对象解决缓存穿透
        // Shop shop = queryWithPassThrough(id);
        // 封装了缓存工具后可以这么写
        // Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class,
        //         this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 在解决缓存穿透的基础上用互斥锁解决缓存击穿
        // Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        // Shop shop = queryWithLogicalExpire(id);
        // 封装了缓存工具后可以这么写
        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class,
                RedisConstants.LOCK_SHOP_KEY, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);


        if (shop == null) {
            return Result.fail(MessageConstants.SHOP_NOT_EXIST);
        }

        return Result.ok(shop);
    }


    /**
     * 互斥锁解决缓存击穿
     * 在解决缓存穿透的代码基础上增加解决缓存击穿
     * @param id
     * @return
     */
    private Shop queryWithMutex(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)) { // redis中查出空值""这里也是false
            // 3. 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        // 判断命中的是否是空值
        if ("".equals(shopJson)) {
            return null;
        }

        // 4. 不存在/未命中，实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id; // 每个店铺有自己的锁
        Shop shop = null;
        try {
            boolean getLock = tryLock(lockKey); // 拿到锁则为true
            // 4.2 判断是否获取成功
            if (!getLock) {
                // 4.3 如果失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);  // 递归实现重试
            }

            // 4.4 如果成功
            // 获取锁成功后应该再次检测redis缓存是否存在，做DoubleCheck。
            // 防止出现第一次检测未命中，然后另一个线程恰好完成了缓存重建释放了锁，然后前一个线程决定开始缓存重建并能获取到锁又开始重建
            shopJson = stringRedisTemplate.opsForValue().get(key);

            if (StrUtil.isNotBlank(shopJson)) {
                // 3. 如果redis中这次检测存在该key，则无需重建缓存，直接返回
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }

            if ("".equals(shopJson)) {
                return null;
            }


            // 4.5 如果第二次检测redis中仍然没有，则根据id查询数据库
            shop = getById(id);
            // 模拟重建的延时
            Thread.sleep(200);

            // 5. 数据库中也不存在，写入空值后返回null
            if (shop == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);    // 空值的有效期要设置的短一些
                // 返回null
                return null;
            }

            // 6. 存在，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally { // 即使在try中return了，finally中的代码总会执行
            // 7. 释放互斥锁
            unLock(lockKey);
        }

        // 8. 返回
        return shop;
    }


    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    /**
     * 逻辑过期解决缓存击穿
     * 由于热点key在缓存预热时已经被加入到redis，且没有设置redis过期时间，因此理论上在缓存中一定存在
     * 因此不用考虑缓存穿透的问题
     * @param id
     * @return
     */
    private Shop queryWithLogicalExpire(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否命中
        if (StrUtil.isBlank(shopJson)) {
            // 3. 如果未命中直接返回空
            return null;
        }

        // 4. 命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        // 由于RedisData的data属性是Object类型，不知道该反序列化成什么，所以实际上结果是JSONObject类型
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);  // 再反序列化一次才拿到Shop对象
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5. 判断是否过期
        if (LocalDateTime.now().isBefore(expireTime)) {
            // 5.1 未过期，直接返回店铺信息
            return shop;
        }

        // 6. 已过期，需要缓存重建
        // 6.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean getLock = tryLock(lockKey);

        // 6.2 判断是否获取锁成功
        if (getLock) {
            // 6.3 获取成功，需要再次检测redis缓存是否过期，做DoubleCheck，如果存在则无需重建缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);

            if (StrUtil.isBlank(shopJson)) {
                return null;
            }

            redisData = JSONUtil.toBean(shopJson, RedisData.class);
            // 由于RedisData的data属性是Object类型，不知道该反序列化成什么，所以实际上结果是JSONObject类型
            data = (JSONObject) redisData.getData();
            shop = JSONUtil.toBean(data, Shop.class);  // 再反序列化一次才拿到Shop对象
            expireTime = redisData.getExpireTime();

            if (LocalDateTime.now().isBefore(expireTime)) {
                return shop;
            }

            // 6.4 DoubleCheck后如果redis缓存仍是过期的，则开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 缓存重建
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally { // 释放锁要放在finally里面确保一定会执行
                    // 释放互斥锁
                    unLock(lockKey);
                }

            });
        }

        // 7. 无论是否获取锁成功，这里都返回过期的商铺信息
        return shop;
    }



    /**
     * 缓存空对象解决缓存穿透
     * 只解决了缓存穿透
     * @param id
     * @return
     */
    private Shop queryWithPassThrough(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)) { // redis中查出空值""这里也是false
            // 3. 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        // 判断命中的是否是空值
        if ("".equals(shopJson)) {
            return null;
        }

        // 4. 不存在/未命中。
        // 如果不是空值则根据id查询数据库
        Shop shop = getById(id);

        // 5. 数据库中也不存在，返回错误
        if (shop == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);    // 空值的有效期要设置的短一些
            // 返回null
            return null;
        }

        // 6. 存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 7. 返回
        return shop;
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


    /**
     * 更新商铺信息
     * @param shop
     * @return
     */
    @Override
    @Transactional  // 保证数据库和redis双写操作的一致性
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail(MessageConstants.SHOP_NOT_EXIST);
        }

        // 1. 先更新数据库
        updateById(shop);

        // 2. 再删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return Result.ok();
    }


    /**
     * 将店铺信息添加到redis
     * 逻辑过期解决缓存击穿
     * 缓存预热
     * @param id
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 1. 查询店铺数据
        Shop shop = getById(id);

        // 模拟缓存重建的延迟
        Thread.sleep(200);

        // 2. 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));    // 当前时间 + 多少秒

        // 3. 写入redis
        // 注意，这里就不要添加redis的过期时间了，实际可认为长期存在
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
