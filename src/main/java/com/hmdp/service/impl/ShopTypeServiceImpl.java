package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryAll() {
        // 1. query shop types cache from redis
        String typesJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_TYPE_KEY);
        // 2. check if exist
        if (StrUtil.isNotBlank(typesJson)) {
            // 3. if shop exist, return
            List<ShopType> shopTypes = JSONUtil.toList(typesJson, ShopType.class);
            return Result.ok(shopTypes);
        }

        // 4. else, query from database
        QueryWrapper<ShopType> wrapper = new QueryWrapper<>();
        wrapper.orderByAsc("sort");
        List<ShopType> shopTypes = list(wrapper);

        // 5. if shop not exist, return error
        if (shopTypes == null) {
            return Result.fail("System error");
        }

        // 6. else, save shop into redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypes));
        stringRedisTemplate.expire(RedisConstants.CACHE_SHOP_TYPE_KEY, RedisConstants.CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);

        // 7. return
        return Result.ok(shopTypes);
    }
}
