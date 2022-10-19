package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥, Zihao Shen
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryBlogById(Long id) {
        // 1. query blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("No blog with id " + id);
        }
        // 2. query author
        queryBlogUser(blog);
        // query if the blog is liked
        isBlogLiked(blog);
        return Result.ok(blog);
    }


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
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("You have not login.");
        }
        Long userId = user.getId();
        // 1. check if the user likes the blog before
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 2. if not like before
            // 2.1 number of like + 1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 2.2 store the user to redis set
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 3. if like before
            // 3.1 number of like - 1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            // 3.2 remove the user from redis set
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        // query top5 likes user: zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // extract user id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());

        // query user by id: where id in () order by field (id, )
        String idString = StrUtil.join(",", ids);
        List<User> users = userService.query().in("id", ids).last("ORDER BY FIELD(id, " + idString + ")").list();
        // change user to userDto
        List<UserDTO> usersDTO = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(usersDTO);
    }

    @Override
    public Result saveBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // save blog to sql
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("Save blog fail.");
        }

        // query followers of the author
        List<Follow> followUserId = followService.query().eq("follow_user_id", user.getId()).list();
        // push blog to all followers
        for (Follow follow : followUserId) {
            Long userId = follow.getUserId();
            // push to mailbox
            String key = RedisConstants.FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }

        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1. get current user
        Long userId = UserHolder.getUser().getId();
        // 2. query mailbox
        String key = RedisConstants.FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, SystemConstants.DEFAULT_PAGE_SIZE);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 3. extract blogId, timeStamp
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        // 4. minTime, offset (count how many minTime in this query)
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            String blogId = typedTuple.getValue();
            blogIds.add(Long.valueOf(blogId));

            long timeStamp = typedTuple.getScore().longValue();
            if (timeStamp == minTime) {
                os ++;
            } else {
                minTime = timeStamp;
                os = 1;
            }
        }

        // 5. query blog by id
        String blogIdStr = StrUtil.join(",", blogIds);
        List<Blog> blogs = query().in("id", blogIds).last("ORDER BY FIELD(id," + blogIdStr + ")").list();

        for (Blog blog : blogs) {
            // query author
            queryBlogUser(blog);
            // query if the blog is liked
            isBlogLiked(blog);
        }

        // 6. package result
        ScrollResult result = new ScrollResult();
        result.setList(blogs);
        result.setOffset(os);
        result.setMinTime(minTime);

        return Result.ok(result);
    }

    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return;
        }
        Long userId = user.getId();
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
