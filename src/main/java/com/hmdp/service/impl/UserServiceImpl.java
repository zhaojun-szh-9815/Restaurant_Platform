package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥, Zihao Shen
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1. check phone number
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2. if the phone number is not match the rule, reject
            return Result.fail("Wrong format of phone number");
        }

        // 3. else, generate the code
        String code = RandomUtil.randomNumbers(6);

        // 4. save code to redis, limited in 2 min
        // session.setAttribute("code", code);
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 5. send code
        // simulate
        log.debug("send code success, code -> {} ", code);

        // 6. return ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1. check phone number
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("Wrong format of phone number");
        }
        // 2. check code
        // Object cacheCode = session.getAttribute("code");
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            // 3. if not match, reject
            return Result.fail("code not match");
        }

        // 4. else, search in database by phone number
        User user = query().eq("phone", phone).one();

        // 5. check if the user exist
        if (user == null) {
            // 6. if not exist, create a new user and store in database
            user = createUserByPhone(phone);
        }

        // 7. store the user in redis
        // session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        // 7.1 generate token as key
        String token = UUID.randomUUID().toString(true);

        // 7.2 store the user by hash
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));

        String token_key = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(token_key, userMap);
        // 7.3 set user expiration
        stringRedisTemplate.expire(token_key, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 8. return ok, and token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1. get current user, current date
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        // 2. concat key
        String dateStr = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = RedisConstants.USER_SIGN_KEY + userId + dateStr;
        // 3. get the number of days of this month
        int dayOfMonth = now.getDayOfMonth();
        // 4. store into redis: Setbit key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1. get record from 0 to today
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        String dateStr = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = RedisConstants.USER_SIGN_KEY + userId + dateStr;
        int dayOfMonth = now.getDayOfMonth();
        List<Long> list = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (list == null || list.isEmpty()) {
            // No sign result
            return Result.ok();
        }

        // 2. loop to get count
        Long num = list.get(0);
        if (num == null || num == 0) {
            return Result.ok();
        }
        int count = 0;
        while (true) {
            if ((num & 1) == 0) {
                // did no sign, break
                break;
            } else {
                count += 1;
            }
            // unsigned right move
            num = num >>> 1;
        }
        return Result.ok(count);
    }

    private User createUserByPhone(String phone) {
        // 1. create user object
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(8));
        // 2. save user to database
        save(user);
        return user;
    }


}
