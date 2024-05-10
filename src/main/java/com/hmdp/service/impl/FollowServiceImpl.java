package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.MessageConstants;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.redisson.api.annotation.REntity;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    /**
     * 关注或取关某用户
     * @param followUserId
     * @param isFollow
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 1. 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.FOLLOWS_KEY + userId;

        // 2. 判断是要关注还是取关
        if (isFollow) {
            // 3.1 要关注，新增关注记录
            Follow follow = Follow.builder()
                    .followUserId(followUserId)
                    .userId(userId)
                    .build();
            boolean success = save(follow);

            if (!success) {
                return Result.fail(MessageConstants.DATABASE_ERROR);
            }

            // 3.2 将关注用户的id放入redis的set集合，方便求共同关注    sadd follows:userId followUserId
            stringRedisTemplate.opsForSet().add(key, followUserId.toString());
        } else {
            // 4.1 要取关，删除关注记录 delete from tb_follow where user_id = ? and follow_user_id = ?
            boolean success = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));

            if (!success) {
                return Result.fail(MessageConstants.DATABASE_ERROR);
            }

            // 4.2 将要取关的用户id从redis的set中移除
            stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
        }
        return Result.ok();
    }

    /**
     * 当前是否关注了某用户
     * @param followUserId
     * @return
     */
    @Override
    public Result followOrNot(Long followUserId) {
        // 1. 获取当前登录用户
        Long userId = UserHolder.getUser().getId();

        // 2. 查询tb_follow中是否有关注记录
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();

        return Result.ok(count > 0);    // 有记录就是关注了，返回true，否则返回false
    }


    /**
     * 查看当前用户与目标用户的共同关注
     * @param id 目标用户id
     * @return
     */
    @Override
    public Result followCommons(Long id) {
        // 1. 获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key1 = RedisConstants.FOLLOWS_KEY + userId;
        String key2 = RedisConstants.FOLLOWS_KEY + id;

        // 2. set求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);

        if (intersect == null || intersect.isEmpty()) {
            // 若无交集
            return Result.ok(Collections.emptyList());
        }

        // 3. 解析出共同关注的id
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());

        // 4. 根据id查询用户
        List<UserDTO> userDTOS = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(userDTOS);
    }
}
