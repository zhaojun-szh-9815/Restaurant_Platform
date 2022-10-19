package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥, Zihao Shen
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;


    @Override
    public Result queryById(Long id) {
        // TODO: if change function between (1, 2) and (3), clear the information in cache. And function 3 need initial data in cache

        // 1. 缓存穿透解决方案
//        Shop shop = cacheClient.queryByID_passThroughSolution(RedisConstants.CACHE_SHOP_KEY, id, RedisConstants.CACHE_SHOP_TTL,
//                TimeUnit.MINUTES, Shop.class, shopID -> getById(shopID));

        // 2. 互斥锁解决缓存击穿
        Shop shop = cacheClient.queryByID_MutexSolution(RedisConstants.CACHE_SHOP_KEY, id, RedisConstants.LOCK_SHOP_KEY,
                RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES, Shop.class, shopID -> getById(shopID));

        // 3. 逻辑过期解决缓存击穿
//        Shop shop = cacheClient.queryByID_LogicExpirationSolution(RedisConstants.CACHE_SHOP_KEY, id, RedisConstants.LOCK_SHOP_KEY,
//                RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES, Shop.class, shopID -> getById(shopID));

        if (shop == null) {
            return Result.fail("Shop didn't find");
        }
        return Result.ok(shop);
    }


    @Override
    // ensure the update and delete operation success or fail together
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("Shop id can not be null");
        }
        // 1. update sql database
        updateById(shop);
        // 2. delete cache on redis
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1. check if it needs query by position
        if (x == null || y == null) {
            // do not need to query by position
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 2. calculate page parameter
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3. query redis: sort and page, result: shopId, distance
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();

        if (content.size() <= from) {
            // there is no more data, no next page, return empty list
            return Result.ok(Collections.emptyList());
        }

        // select result from start to end
        List<Long> ids = new ArrayList<>(content.size() - from);
        Map<String, Distance> distanceMap = new HashMap<>(content.size() - from);
        content.stream().skip(from).forEach(result -> {
            // 4. extract shopId
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        // 5. query shop by id
        String idsStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD( id, " + idsStr + ")").list();

        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
