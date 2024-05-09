package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.MessageConstants;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查看热门笔记
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {

        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }


    /**
     * 查看某篇笔记
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        // 1. 查询blog基本信息
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail(MessageConstants.BLOG_NOT_EXIST);
        }

        // 2. 查询发布该blog的用户，将昵称和头像存入blog
        queryBlogUser(blog);

        // 3. 查询该笔记是否被该用户点过赞了
        isBlogLiked(blog);

        return Result.ok(blog);
    }

    /**
     * 查询该笔记是否被该用户点过赞了（便于前端高亮显示点赞按钮）
     * @param blog
     */
    private void isBlogLiked(Blog blog) {
        // 1. 获取当前登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) { // 当前未登录，则不高亮显示点赞，isLike保持默认false即可
            return;
        }

        Long userId = user.getId();

        // 2. 判断当前用户是否已经点过赞了
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }


    /**
     * 点赞笔记
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {
        // 1. 获取登录用户
        Long userId = UserHolder.getUser().getId();

        // 2. 判断当前登录用户是否已经给这篇笔记点过赞
        String key = RedisConstants.BLOG_LIKED_KEY + id;    // key就是笔记id，value就是给这篇笔记点过赞的用户id
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {    // 查得到score就代表元素存在，查不到返回nil就代表元素不存在
            // 3. 如果未点赞，可以点赞
            // 3.1 数据库点赞数 + 1
            boolean success = update().setSql("liked = liked + 1").eq("id", id).update();

            if (!success) {
                return Result.fail(MessageConstants.DATABASE_ERROR);
            }

            // 3.2 保存用户到redis的sortedSet zadd key value score
            // score就用时间戳，越早点赞score越小，排在越前面
            stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());

        } else {
            // 4. 如果已点赞，取消点赞
            // 4.1 数据库点赞数 - 1
            boolean success = update().setSql("liked = liked - 1").eq("id", id).update();

            if (!success) {
                return Result.fail(MessageConstants.DATABASE_ERROR);
            }

            // 4.2 将用户从redis的set集合中移除
            stringRedisTemplate.opsForZSet().remove(key, userId.toString());
        }

        return Result.ok();
    }

    /**
     * 查询点赞排行榜
     * 最早点赞的5个人
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLikes(Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        // 1. 查询top5的点赞用户 zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);   // 返回的是score排行前五的元素

        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        // 2. 解析出用户id
        List<Long> ids = top5.stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());

        // 3. 根据用户id查询用户（不要将用户所有信息都返回，而应该只返回UserDTO）
        // userService.listByIds(ids)这样查出的是按照id大小顺序的5个用户
        // 要想保持原来在zset中的顺序，需要加order by， WHERE id IN (5,1) ORDER BY (id, 5, 1)
        String idStr = StrUtil.join(",", ids);  // 5,1
        List<UserDTO> userDTOList = userService.query().in("id", ids).last("ORDER BY FIELD(id,"+ idStr +")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        // 4. 返回
        return Result.ok(userDTOList);
    }

    /**
     * 查询发布该blog的用户，将昵称和头像存入blog
     * @param blog
     */
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
