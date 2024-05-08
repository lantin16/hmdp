package com.hmdp.utils;


import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * redis全局ID生成器
 */

@Component
public class RedisIdWorker {

    // 基础时间（秒，2022年1月1日0时0分0秒）
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    // 序列号的位数
    private static final int COUNT_BITS = 32;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     *
     * @param keyPrefix 用于区分业务
     * @return
     */
    public long nextId(String keyPrefix) {
        // 1. 生成时间戳（当前时间 - 基础时间 得到的秒数）
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        // 2. 生成序列号（利用redis的自增长）
        // 注意：即使是同一个业务，也不能都使用同一个key来自增id，因为自增是有可能超过32位序列号的表示范围的
        // 因此这里采用在后面再拼接上一个日期，如20240506，代表那一天的业务自增id，这样一天之内自增超过2^32的可能性就很小了
        // 也就是说，不同日期下的单用不同的key，这样之后统计每天的下单数也比较方便
        // 2.1 获取当前日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.2 自增长
        long count = stringRedisTemplate.opsForValue().increment(RedisConstants.INCREMENT_ID_KEY + keyPrefix + date);

        // 3. 拼接并返回
        // 由于最后要返回long类型的id，因此这里用字符串的拼接并不好，可以采用位运算来拼接
        return timestamp << COUNT_BITS | count;
    }


    // public static void main(String[] args) {
    //     LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
    //     long second = time.toEpochSecond(ZoneOffset.UTC);
    //     System.out.println(second);
    // }

}
