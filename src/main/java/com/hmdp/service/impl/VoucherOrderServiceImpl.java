package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    // 秒杀业务lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 利用静态代码块加载lua脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 阻塞队列，当一个线程尝试从阻塞队列中获取元素时，如果队列中没有元素，则该线程就会阻塞，直到队列中有元素才会将该线程唤醒
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    // 线程池，将秒杀订单写入数据库，不用太快，异步写着就行，所以只给了一个线程
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();


    @PostConstruct  // 在当前类初始化完毕后就会执行这个方法
    private void init() {
        // 这个任务应该在项目一启动就开始，因为随时有可能有用户秒杀然后需要从阻塞队列中取订单
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    // 线程任务
    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            // TODO 这个线程一直尝试从消息队列中获取，导致结束项目运行后会发生线程池仍有线程未释放的异常
            // 不断从消息队列中获取订单信息
            while (true) {
                try {
                    // 1. 获取Stream消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),  // 指定消费组和消费者
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),    // 读取的选项，每次读多少个，是否阻塞...
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())   // 读取的队列名，以及从哪里开始读
                    );

                    // 2. 判断是否获取到了消息
                    if (list == null || list.isEmpty()) {
                        // 2.1 如果没获取到消息，则继续下一次循环尝试获取
                        continue;
                    }

                    // 2.2 如果获取到了消息，可以下单（具体业务逻辑）
                    // 解析取出的消息中的订单信息，在这里知道每次只取一个，因此list里实际只有一个元素
                    MapRecord<String, Object, Object> record = list.get(0); // String是消息的id
                    Map<Object, Object> values = record.getValue();  // 获取该消息中的键值对
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true); // 将map中的键值对封装到对象中

                    // 可以下单
                    handleVoucherOrder(voucherOrder);

                    // 3. ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 如果有异常则从 pending-list 中取消息再次处理，保证每个消息都至少执行一次
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),  // 指定消费组和消费者
                            StreamReadOptions.empty().count(1),    // 从pending-list中读不需要阻塞
                            StreamOffset.create(queueName, ReadOffset.from("0"))   // 每次都读pending-list的第一个消息（当处理完收到回复后则下一个已消费但未回复的消息就变成第一个了）
                    );

                    // 2. 判断是否获取到了消息
                    if (list == null || list.isEmpty()) {
                        // 2.1 如果没获取到消息，说明pending-list没有异常消息了，则结束循环
                        break;
                    }

                    // 2.2 如果获取到了消息，可以下单（具体业务逻辑）
                    // 解析取出的消息中的订单信息，在这里知道每次只取一个，因此list里实际只有一个元素
                    MapRecord<String, Object, Object> record = list.get(0); // String是消息的id
                    Map<Object, Object> values = record.getValue();  // 获取该消息中的键值对
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true); // 将map中的键值对封装到对象中

                    // 可以下单
                    handleVoucherOrder(voucherOrder);

                    // 3. ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    // 如果处理pending-list又出现了异常，这里不需要递归，而是等到下一次循环再从pending-list中取即可
                    // 确保每一个订单都会得到处理
                    try {
                        Thread.sleep(20);   // 为了防止过于频繁的尝试，可以加一点休眠
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * 处理从阻塞队列中取出的订单信息
     * @param voucherOrder
     */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 获取用户，注意这里不能才从UserHolder中取，因为这是独立于主线程之外的单独的线程
        Long userId = voucherOrder.getUserId();

        // 利用redisson中的分布式锁实现
        // 2. 创建锁对象
        RLock lock = redissonClient.getLock(RedisConstants.LOCK_KEY_PREFIX + RedisConstants.ORDER_PREFIX + userId);
        // 3. 尝试获取锁
        boolean isLock = lock.tryLock();    // 无参默认失败不等待不重试
        // 4. 判断是否获取成功
        if (!isLock) {
            // 获取锁失败，这里因为是异步处理，也不用返回给前端，记录下错误即可
            log.error("不允许重复下单");
            return;
        }

        try {
            // 注意，下面这样获取代理对象在这里就不行了，因为这是在子线程，是无法从ThreadLocal中取出想要的东西的
            // IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            // 可以在主线程中获取代理对象，并保存到成员变量，这里就可以用成员变量中的
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    // 记录主线程的代理对象，以便处理订单的子线程能够调用带有事务管理的createVoucherOrder()方法
    private IVoucherOrderService proxy;


    /**
     * 抢购特价券
     * 在lua脚本中向Stream消息队列发送消息，代替了阻塞队列
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取当前用户id
        Long userId = UserHolder.getUser().getId();
        // 生成订单id（是否真正生成订单由lua脚本判断）
        long orderId = redisIdWorker.nextId(RedisConstants.ORDER_PREFIX);    // 生成全局唯一且递增的订单id

        // 获取代理对象并保存到成员变量，以便处理订单的子线程能够调用带有事务管理的createVoucherOrder()方法
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 1. 执行lua脚本（判断购买资格，发送订单信息到消息队列）
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, // 脚本
                Collections.emptyList(),    // key类型的参数，没有就传空集合
                voucherId.toString(), userId.toString(), String.valueOf(orderId));// 其他类型的参数

        // 2. 根据lua脚本的执行结果判断是否秒杀成功（返回值是否是0）
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为0，代表没有购买资格，秒杀失败
            return Result.fail(r == 1 ? MessageConstants.SECKILL_VOUCHER_STOCK_NOT_ENOUGH : MessageConstants.DUPLICATE_ORDERS_NOT_ALLOWED);
        }

        // 3，返回订单id
        return Result.ok(orderId);
    }



    /**
     * 抢购特价券
     * 利用redis判断库存和一人一单，修改数据库异步执行，效率更高
     * 向阻塞队列发送消息
     * @param voucherId
     * @return
     */
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 获取当前用户id
        Long userId = UserHolder.getUser().getId();

        // 1. 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, // 脚本
                Collections.emptyList(),    // key类型的参数，没有就传空集合
                voucherId.toString(), userId.toString());// 其他类型的参数

        // 2. 根据lua脚本的执行结果判断是否秒杀成功（返回值是否是0）
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为0，代表没有购买资格，秒杀失败
            return Result.fail(r == 1 ? MessageConstants.SECKILL_VOUCHER_STOCK_NOT_ENOUGH : MessageConstants.DUPLICATE_ORDERS_NOT_ALLOWED);
        }

        // 2.2 为0，有购买资格，把下单信息保存到阻塞队列
        // 获取代理对象并保存到成员变量，以便处理订单的子线程能够调用带有事务管理的createVoucherOrder()方法
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 将订单id，用户id，优惠券id都封装进voucherOrder中
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId(RedisConstants.ORDER_PREFIX);    // 生成全局唯一且递增的订单id
        voucherOrder.setId(orderId);    // 订单id
        voucherOrder.setUserId(userId); // 当前用户id
        voucherOrder.setVoucherId(voucherId);   // 代金券id

        // 放入阻塞队列
        orderTasks.add(voucherOrder);

        // 3，返回订单id
        return Result.ok(orderId);
    }*/


    /**
     * 直接将要添加到数据库的订单传入，本方法中就不需要额外创建voucherOrder对象了
     * 理论上redis已经做过了库存和一人一单的判断，这里并不需要再判断一次，但是为了兜底，还是做一下判断
     * @param voucherOrder
     */
    @Transactional  // 涉及到多张表的修改，保证原子性
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 不能再从UserHolder中拿到userId，因为是独立的子线程在调用这个方法
        // Long userId = UserHolder.getUser().getId();
        Long userId = voucherOrder.getUserId();

        Long voucherId = voucherOrder.getVoucherId();

        // 理论上redis已经做过了库存和一人一单的判断，这里并不需要再判断一次，但是为了兜底，还是做一下判断

        // 查询订单
        int count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        // 判断该用户是否已经抢到了该优惠券
        if (count > 0) {
            // 用户已经购买过了，不能再购买
            // 不用返回给前端，记录下日志即可
            log.error("不允许重复下单");
            return;
        }

        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")    // set stock = stock - 1
                .eq("voucher_id", voucherId)
                .gt("stock", 0) // where id = ? and stock > 0
                .update();

        if (!success) {
            // 扣减失败
            log.error("秒杀券库存不足");
            return;
        }

        // 将传入的订单写入数据库
        save(voucherOrder);
    }



    /**
     * 抢购特价券
     * 判断库存、一人一单、修改数据库等操作串行执行，效率低
     * @param voucherId
     * @return
     */
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 查询优惠券
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);

        // 2，判断秒杀是否开始
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 秒杀未开始
            return Result.fail(MessageConstants.SECKILL_NOT_BEGIN);
        }

        // 3，判断秒杀是否结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 秒杀已结束
            return Result.fail(MessageConstants.SECKILL_HAVE_END);
        }

        // 4. 判断库存是否充足
        if (seckillVoucher.getStock() < 1) {
            // 库存不足
            return Result.fail(MessageConstants.SECKILL_VOUCHER_STOCK_NOT_ENOUGH);
        }

        Long userId = UserHolder.getUser().getId();

        // 对于每个用户id加一把锁，不同的用户之间不影响
        // 要保证每个用户是同一把锁，因此不能直接用userId，因为同一个用户多次获取的userId引用值是不同的，还是认为是不同的锁
        // 因此转为字符串toString，但是toString方法的底层是new了一个String，因此还是会出现引用值不同
        // 因此用intern()手动将userId字符串放入常量池，这样就能保证同一个用户的多个线程都是用的一把锁

        // 注意createVoucherOrder()方法是进行事务管理的，因此只有当它执行完后才会提交事务，数据库的修改才会生效
        // 因此这整个方法都应该被锁包裹，确保事务提交后才释放锁，否则如果先释放锁再提交事务仍然可能出现一人购买多单的情况
        // synchronized (userId.toString().intern()) {
        //     // 不能直接这么调，因为这样其实是使用this也就是当前的VoucherOrderServiceImpl对象调的而不是它的代理对象
        //     // 而事务要想生效，其实是因为Spring对VoucherOrderServiceImpl这个类作了动态代理，拿到了它的代理对象，用代理对象做的事务处理
        //     // 如果直接用目标对象/非代理对象是会出现事务失效的
        //     // return createVoucherOrder(voucherId);
        //
        //     // 正确写法：
        //     // 获取当前对象的代理对象
        //     IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
        //     return proxy.createVoucherOrder(voucherId);
        // }


        // // 创建分布式锁对象
        // // 为每个用户单独创建一个锁（拼上userId），这样锁定的范围较小，不同的用户之间不会受这个锁的影响，提高了效率
        // SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, RedisConstants.ORDER_PREFIX + userId);
        // // 获取锁
        // boolean getLock = lock.tryLock(1200L);  // 超时时间确保万一没有手动释放锁也能超时释放
        // // 判断是否获取锁成功
        // if (!getLock) {
        //     // 获取锁失败，返回错误或重试
        //     // 这里直接返回错误
        //     return Result.fail(MessageConstants.DUPLICATE_ORDERS_NOT_ALLOWED);
        // }
        //
        // try {
        //     // 获取锁成功
        //     // 获取代理对象
        //     IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
        //     return proxy.createVoucherOrder(voucherId);
        // } finally { // 放在finally里确保一定会释放锁
        //     // 释放锁
        //     lock.unlock();
        // }




        // 利用redisson中的分布式锁实现
        // 创建锁对象
        RLock lock = redissonClient.getLock(RedisConstants.LOCK_KEY_PREFIX + RedisConstants.ORDER_PREFIX + userId);
        // 尝试获取锁
        boolean isLock = lock.tryLock();    // 无参默认失败不等待不重试
        // 判断是否获取成功
        if (!isLock) {
            // 获取锁失败，直接返回错误
            return Result.fail(MessageConstants.DUPLICATE_ORDERS_NOT_ALLOWED);
        }

        try {
            // 获取锁成功
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }*/

    /*@Transactional  // 涉及到多张表的修改，保证原子性
    public Result createVoucherOrder(Long voucherId) {
        // 5. 一人一单
        Long userId = UserHolder.getUser().getId();

        // 5.1 查询订单
        int count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        // 5.2 判断该用户是否已经抢到了该优惠券
        if (count > 0) {
            // 用户已经购买过了，不能再购买
            return Result.fail(MessageConstants.DUPLICATE_ORDERS_NOT_ALLOWED);
        }

        // 6. 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")    // set stock = stock - 1
                .eq("voucher_id", voucherId)
                .gt("stock", 0) // where id = ? and stock > 0
                .update();

        if (!success) {
            // 扣减失败
            return Result.fail(MessageConstants.SECKILL_VOUCHER_STOCK_NOT_ENOUGH);
        }


        // 7. 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId(RedisConstants.ORDER_PREFIX);    // 生成全局唯一且递增的订单id
        voucherOrder.setId(orderId);    // 订单id
        voucherOrder.setUserId(userId); // 当前用户id
        voucherOrder.setVoucherId(voucherId);   // 代金券id
        save(voucherOrder); // 将订单写入数据库

        // 8. 返回订单id
        return Result.ok(orderId);
    }*/
}
