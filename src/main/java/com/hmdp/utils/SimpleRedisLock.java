package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Implement of ILock
 * </p>
 *
 * @author Zihao Shen
 */
public class SimpleRedisLock implements ILock{

    private StringRedisTemplate stringRedisTemplate;
    private String name;

    // solve multi-jvm problem
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        // get current thread id and lock value
        String lockValue = ID_PREFIX + Thread.currentThread().getId();
        // try to get lock
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(RedisConstants.LOCK_KEY_PREFIX + name,
                lockValue, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        // execute lua script to ensure atomicity of query and delete
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(RedisConstants.LOCK_KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId()
                );
    }

/*    @Override
    public void unlock() {
        // get current thread id and lock value
        String lockValue = ID_PREFIX + Thread.currentThread().getId();
        // compare with current lock value
        String v = stringRedisTemplate.opsForValue().get(RedisConstants.LOCK_KEY_PREFIX + name);
        if (lockValue.equals(v)) {
            stringRedisTemplate.delete(RedisConstants.LOCK_KEY_PREFIX + name);
        }
    }*/
}
