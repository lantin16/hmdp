package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    public void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);

        cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.MINUTES);
    }


    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        // 每个线程生成100个id
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();  // 每个线程执行完就减一
        };

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();  // 等待300次全部执行完
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }


    /**
     * 导入店铺信息到redis
     */
    @Test
    void loadShopData() {
        // 1. 查询店铺信息
        List<Shop> list = shopService.list();

        // 2. 把店铺按照类型分，类型一致的放在一个集合中
        // key：typeId，value：该类型的商户
        // Collectors.groupingBy能够自动按照某个依据分组然后收集到对应的集合中
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        // 3. 分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1 获取商户类型id
            Long typeId = entry.getKey();
            // 3.2 获取该类型的商户信息
            List<Shop> shops = entry.getValue();

            // 3.3 写入redis GEOADD key 经度 纬度 member
            String key = SHOP_GEO_KEY + typeId; // shop:geo:1

            // 一个shop发一次redis请求效率比较低
            // shops.forEach(shop ->
            //         stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString())
            // );

            // 一次添加一个GeoLocation的集合，GeoLocation的泛型是member的类型，效率更高
            List<RedisGeoCommands.GeoLocation<String>> locations = shops.stream()
                    .map(shop -> new RedisGeoCommands.GeoLocation<String>(shop.getId().toString(), new Point(shop.getX(), shop.getY())))
                    .collect(Collectors.toList());
            stringRedisTemplate.opsForGeo().add(key, locations);

        }

    }
}
