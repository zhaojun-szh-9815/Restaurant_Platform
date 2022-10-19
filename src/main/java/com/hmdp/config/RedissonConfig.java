package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {
    @Bean
    public RedissonClient redissonClient(){
        // add config
        Config config = new Config();
        config.useSingleServer().setAddress("redis://10.0.0.26:6379").setPassword("123");

        // create redissonClient object
        return Redisson.create(config);
    }
}
