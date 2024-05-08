package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Redis实现分布式锁
 * 初级版本
 */

public class SimpleRedisLock implements ILock{

    // 注意这里不能用@Resource注解自动注入，因为SimpleRedisLock并没有交给Spring的IOC容器管理，因此这里的StringRedisTemplate只能通过构造方法传入
    private StringRedisTemplate stringRedisTemplate;
    private String name;    // 锁的名称
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;    // 释放锁的lua脚本，泛型是返回值类型


    // 脚本的初始化和加载放在静态代码块中，这样这个类一加载就会初始化脚本
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));    // 设置脚本位置，自动去classPath下找
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    /**
     * 锁的名称应该是与业务挂钩的，不能所有业务都用一个锁
     * 因此需要用户传递过来，通过构造函数接收
     * @param stringRedisTemplate
     * @param name
     */
    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取当前线程的线程标识（UUID + 线程id）
        // 同一进程内的线程由线程ID区分，不同进程间的线程有UUID区分，拼接起来后保证了所有线程的线程标识唯一
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 尝试获取锁
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(RedisConstants.LOCK_KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);

        // 注意这里不要直接返回success，因为自动拆箱可能出现空指针异常
        return Boolean.TRUE.equals(success);    // 用这个即使success为nil也会返回false
    }

    /*@Override
    public void unlock() {
        String key = RedisConstants.LOCK_KEY_PREFIX + name;
        // 获取当前线程的线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取redis锁中的线程标识
        String lockThreadId = stringRedisTemplate.opsForValue().get(key);

        // 判断线程标识是否一致
        if (threadId.equals(lockThreadId)) {
            // 一致，是自己持有锁，可以释放锁
            stringRedisTemplate.delete(key);
        }

        // 不一致，不能删别人的锁
    }*/


    /**
     * 这样只用一行代码（调用lua脚本执行的代码）即可
     * 在lua脚本内部完成判断线程标识是否一致和释放锁的操作
     * 这样就能满足原子性
     */
    @Override
    public void unlock() {
        // 调用lua脚本
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(RedisConstants.LOCK_KEY_PREFIX + name),   // 单元素集合
                ID_PREFIX + Thread.currentThread().getId());
        // 这里并不关心返回值
    }
}
