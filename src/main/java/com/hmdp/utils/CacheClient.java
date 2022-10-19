package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Cache Client
 * <p>
 * Implement redis cache and define some solutions for common high concurrent problem
 * </p>
 *
 * @author Zihao Shen
 *
 */
@Slf4j
@Component
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_RELOAD_EXECUTOR = Executors.newFixedThreadPool(10);

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpiration(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /*
     * Solve Cache Penetration (Pass Through):
     *   Client always make requests to a non-existent data
     */
    public <R, ID> R queryByID_passThroughSolution(String keyPrefix, ID id, Long time, TimeUnit unit,
                                                   Class<R> tClass, Function<ID, R> dbFallBack) {
        String key = keyPrefix + id;
        // 1. query cache from redis
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. check if exist
        if (StrUtil.isNotBlank(json)) {
            // 3. if exist, return
            return JSONUtil.toBean(json, tClass);
        }
        // 2.1 check if shop is empty string (create to avoid cache penetration)
        if (json != null){ // shopJson is "" or null
            return null;
        }

        // 4. else, query from database
        R r = dbFallBack.apply(id);

        // 5. if shop not exist, save null to redis and return error to avoid cache penetration
        if (r == null) {
//            stringRedisTemplate.opsForValue().set(key, "",
//                    RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            this.set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 6. else, save shop into redis, expire after 30 min
        this.set(key, r, time, unit);

        // 7. return
        return r;
    }

    /*
     * Solve Cache Avalanche:
     *   Many entries in cache expire at the same time or the cache is unavailable
     */
    public <R,ID> R queryByID_MutexSolution(String keyPredix, ID id, String lockKeyPrefix, Long time, TimeUnit unit,
                                        Class<R> rClass, Function<ID, R> dbFallBack) {
        String key = keyPredix + id;
        // 1. query cache from redis
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. check if exist
        if (StrUtil.isNotBlank(json)) {
            // 3. if exist, return
            R r = JSONUtil.toBean(json, rClass);
            return r;
        }
        // 2.1 check if it is empty string (create to avoid cache penetration)
        if (json != null){ // json is "" or null
            return null;
        }

        // 4 rebuild cache
        // 4.1 try to get mutex
        R r = null;
        String lockKey = lockKeyPrefix + id;
        try {
            boolean getLock = tryLock(lockKey);
            // 4.2 check if get mutex
            if (! getLock) {
                // 4.3 sleep if not get mutex and repeat
                Thread.sleep(50);
                return queryByID_MutexSolution(keyPredix, id, lockKeyPrefix, time, unit, rClass, dbFallBack);
            }
            // 4.4 double check cache to make sure the shop is not in cache
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                r = JSONUtil.toBean(json, rClass);
                return r;
            }
            if (json != null){
                return null;
            }

            // 4.4 query sql database if get mutex
            r = dbFallBack.apply(id);
            Thread.sleep(200);//simulate long task

            // 5. if not exist, save null to redis and return error to avoid cache penetration
            if (r == null) {
                stringRedisTemplate.opsForValue().set(key, "",
                        RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            // 6. else, save it into redis, expire after 30 min
            this.set(key, r, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7. unlock mutex and return
            unLock(lockKey);
        }
        return r;
    }


    /*
     * Solve Cache BreakDown:
     *   A hot topic / product is unavailable in cache, and many request go to database
     *
     *   The solution need to prepare the original data in cache first
     */
    public <R, ID> R queryByID_LogicExpirationSolution(String keyPredix, ID id, String lockKeyPrefix, Long time, TimeUnit unit,
                                                       Class<R> rClass, Function<ID, R> dbFallBack) {
        String key = keyPredix + id;
        // 1. query cache from redis
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. check if exist
        if (StrUtil.isBlank(json)) {
            // 3. if not exist, return null
            return null;
        }
        // 4. if exist, change Json to object
        RedisData redisData= JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), rClass);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5. check if expire or not
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 if not expire, return the information
            return r;
        }

        // 5.2 if expire, reload the cache
        // 6. reload the cache
        // 6.1 get mutex lock
        String lockKey = lockKeyPrefix + id;
        boolean getLock = tryLock(lockKey);

        // 6.2 check if get lock or not
        if (getLock) {
            // 6.3 if get the lock, new a thread to do the job of reloading the cache
            CACHE_RELOAD_EXECUTOR.submit(() -> {
                try {
                    R newR = dbFallBack.apply(id);
                    this.setWithLogicalExpiration(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }

        // 6.4 return the old information
        return r;
    }

    public <R, ID> void saveShopToRedis(String keyPrefix, ID id, Long ExpireSeconds, Function<ID, R> dbFallBack){
        // 1. query shop information
        R r = dbFallBack.apply(id);
        try {
            Thread.sleep(200);//simulate long task
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        /* // 2. set logical expiration time
        RedisData data = new RedisData();
        data.setData(r);
        data.setExpireTime(LocalDateTime.now().plusSeconds(ExpireSeconds));
        // 3. save to redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(data));*/
        this.setWithLogicalExpiration(keyPrefix + id, r, ExpireSeconds, TimeUnit.SECONDS);
    }
}
