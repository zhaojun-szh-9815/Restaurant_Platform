package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥, Zihao Shen
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

    private static final String ORDER_ID_PREFIX = "order:";

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    // 1. get order information from MessageQueue
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2. check if get order
                    if (list == null || list.isEmpty()) {
                        // 2.1 if not get order, continue
                        continue;
                    }

                    // 2.2 if get order, extract order information
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3. create order in sql
                    handleVoucherOrder(voucherOrder);

                    // 4. ACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", entries.getId());
                } catch (Exception e) {
                    log.error("create order fail: ", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1. get order information from pendingList
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2. check if get order
                    if (list == null || list.isEmpty()) {
                        // 2.1 if not get order, pendingList have no message
                        break;
                    }

                    // 2.2 if get order, extract order information
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3. create order in sql
                    handleVoucherOrder(voucherOrder);

                    // 4. ACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", entries.getId());
                } catch (Exception e) {
                    log.error("pendingList create order fail: ", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }



    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1. get userId
        Long userId = voucherOrder.getUserId();
        // 2. create lock object
        RLock lock = redissonClient.getLock(RedisConstants.LOCK_KEY_PREFIX + ORDER_ID_PREFIX + userId);
        // 3. get lock
        boolean getLock = lock.tryLock();
        // 4. check if get lock
        if (!getLock) {
            // impossible because it has been check in redis
            log.error("one user can only order once.");
            return;
        }
        try {
            // unlock after transaction submitted
            proxy.CreateVoucherOrder(voucherOrder);
        } finally {
            // unlock
            lock.unlock();
        }
    }

    @Override
    public Result secKillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId(ORDER_ID_PREFIX);
        // 1. execute lua script
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(), voucherId.toString(), userId.toString(), String.valueOf(orderId));
        // 2. check if can order
        int r = result.intValue();
        if (r != 0){
            // 3. result != 0, reject
            return Result.fail(r == 1?"out of stock":"can only order once");
        }

        // get proxy object, because the child thread cannot get proxy
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 5. return order id
        return Result.ok(orderId);
    }

    @Transactional
    public void CreateVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            // impossible because check in redis
            log.error("the user has ordered once.");
            return;
        }

        // 4. decrease stock
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1").eq("voucher_id", voucherOrder.getVoucherId())
                // add optimal lock, stock > 0
                .gt("stock", 0).update();
        if (!success) {
            // impossible because check in redis
            log.error("out of stock.");
            return;
        }

        // save order
        save(voucherOrder);
    }

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);@PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1. get order information from blockingQueue
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2. create order
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("create order fail: ", e);
                }
            }
        }
    }

    @Override
    public Result secKillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1. execute lua script
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(), voucherId.toString(), userId.toString());
        // 2. check if can order
        int r = result.intValue();
        if (r != 0){
            // 3. result != 0, reject
            return Result.fail(r == 1?"out of stock":"can only order once");
        }
        // 4. result = 0, create order
        long orderId = redisIdWorker.nextId(ORDER_ID_PREFIX);

        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        // store the order information to blockingQueue
        orderTasks.add(order);

        // get proxy object, because the child thread cannot get proxy
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 5. return order id
        return Result.ok(orderId);
    }*/

    /*
        secKill v1: based on sql
     */
    /*@Transactional
    public Result CreateVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("You have ordered before.");
        }

        // 4. decrease stock
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1").eq("voucher_id", voucherId)
                // add optimal lock, stock > 0
                .gt("stock", 0).update();
        if (!success) {
            return Result.fail("Out of stock");
        }

        // 5. create order
        VoucherOrder order = new VoucherOrder();
        // 5.1 order id
        long orderId = redisIdWorker.nextId(ORDER_ID_PREFIX);
        order.setId(orderId);
        // 5.2 user id
        order.setUserId(userId);
        // 5.3 voucher id
        order.setVoucherId(voucherId);
        // save order
        save(order);

        return Result.ok(orderId);
    }

    @Override
    public Result secKillVoucher(Long voucherId) {
        // 1. query voucher
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2. check if voucher is in selling time
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("it is not start");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("It is already end");
        }
        // 3. check if out of stock
        if (voucher.getStock() < 1) {
            return Result.fail("Out of stock");
        }
        // check if the user had ordered the voucher before
        Long userId = UserHolder.getUser().getId();

        // get lock
        // SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, ORDER_ID_PREFIX + userId);
        RLock lock = redissonClient.getLock(RedisConstants.LOCK_KEY_PREFIX + ORDER_ID_PREFIX + userId);

        // boolean getLock = lock.tryLock(RedisConstants.LOCK_TTL);
        boolean getLock = lock.tryLock();

        if (!getLock) {
            // return fail or re-try
            return Result.fail("Limit one person order once!");
        }

        try {
            // get current proxy to make transaction usable (避免事务失效)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            // unlock after transaction submitted
            Result r = proxy.CreateVoucherOrder(voucherId);
            return r;
        } finally {
            // unlock
            lock.unlock();
        }
    }*/
}
