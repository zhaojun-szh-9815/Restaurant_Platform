package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    private static final long INIT_TIME_STAMP = LocalDateTime.of(2022, 1, 1, 0,0,0)
            .toEpochSecond(ZoneOffset.UTC);

    private static final int MOVE_BITS = 32;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyPrefix) {
        // 1. generate timestamp
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowSecond - INIT_TIME_STAMP;

        // 2. generate series number (key = prefix + date)
        // 2.1 get now date
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Long number = stringRedisTemplate.opsForValue().increment(RedisConstants.INCR_KEY_PREFIX + keyPrefix + date);

        // 3. concat and return
        return timeStamp << MOVE_BITS | number;
    }

}
