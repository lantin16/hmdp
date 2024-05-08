package com.hmdp;

import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
public class RedissonTest {

    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RedissonClient redissonClient2;
    @Resource
    private RedissonClient redissonClient3;

    private RLock multiLock;

    @BeforeEach
    void setUp() {
        RLock lock1 = redissonClient.getLock("lock:order");
        RLock lock2 = redissonClient2.getLock("lock:order");
        RLock lock3 = redissonClient3.getLock("lock:order");

        // 创建联锁 multiLock
        multiLock = redissonClient.getMultiLock(lock1, lock2, lock3);   // 这里用哪个client调都一样
    }


    @Test
    void method1() throws InterruptedException {
        boolean isLock = multiLock.tryLock(1L, TimeUnit.SECONDS);
        if (!isLock) {
            log.error("获取锁失败...1");
            return;
        }
        try {
            log.info("获取锁成功...1");
            method2();
            log.info("开始执行业务...1");
        } finally {
            log.info("准备释放锁...1");
            multiLock.unlock();
        }

    }

    private void method2() throws InterruptedException {
        boolean isLock = multiLock.tryLock();
        if (!isLock) {
            log.error("获取锁失败...2");
            return;
        }
        try {
            log.info("获取锁成功...2");
            log.info("开始执行业务...2");
        } finally {
            log.info("准备释放锁...2");
            multiLock.unlock();
        }

    }
}
