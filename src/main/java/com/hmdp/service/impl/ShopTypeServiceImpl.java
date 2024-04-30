package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.MessageConstants;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    /**
     * 查询商铺类型列表
     *
     * @return
     */
    @Override
    public Result queryShopTypeList() {
        // 查询redis
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        List<String> typeJson = stringRedisTemplate.opsForList().range(key, 0, -1);    // -1 表示获取所有元素

        // 如果存在，直接返回
        if (typeJson != null && !typeJson.isEmpty()) {
            List<ShopType> shopTypes = typeJson.stream()
                    .map(str -> JSONUtil.toBean(str, ShopType.class))
                    .collect(Collectors.toList());
            return Result.ok(shopTypes);
        }

        // 如果不存在，到数据库中查
        List<ShopType> typeList = query().orderByAsc("sort").list();

        // 如果数据库中也不存在，报错
        if (typeList == null || typeList.isEmpty()) {
            return Result.fail(MessageConstants.SHOP_TYPE_NOT_EXIST);
        }

        // 如果存在，还需要写入redis缓存
        List<String> jsonTypeList = typeList.stream()
                .map(type -> JSONUtil.toJsonStr(type))
                .collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(key, jsonTypeList);
        stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);

        return Result.ok(typeList);
    }
}
