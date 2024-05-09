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
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送验证码
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1. 校验手机号
        boolean phoneInvalid = RegexUtils.isPhoneInvalid(phone);

        // 2. 如果不符合，返回错误信息
        if (phoneInvalid) {
            return Result.fail("手机号格式错误！");
        }

        // 3. 如果符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 4. 保存验证码到redis
        // 注意要设置验证码的有效期 set key value ex 120
        // 手机号作为key
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 5. 发送验证码
        // 如果真实发送需要使用第三方服务，这里不是重点，就模拟发送了
        log.info("发送短信验证码成功，验证码：{}", code);

        // 返回ok
        // return Result.ok(code); // TODO 生成完token后改回下面
        return Result.ok();
    }


    /**
     * 短信验证码登录/注册
     * 如果验证码正确且该手机号尚未注册，则直接快捷注册并完成登录
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();

        // 1. 校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }

        // 2. 从redis获取验证码并校验
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            // 3. 如果验证码不一致则报错
            return Result.fail("验证码错误！");
        }

        // 4. 如果验证码一致，则根据手机号查询用户
        // 用mybatis-plus省去写sql语句
        User user = query().eq("phone", phone).one();

        // 5. 判断用户是否存在
        if (user == null) {
            // 6. 如果用户不存在，则创建新用户并保存
            user = createUserWithPhone(phone);
        }

        // 7. 保存用户信息到redis中
        // 7.1 生成随机token，作为登录令牌
        String token = UUID.randomUUID().toString(true);
        // 7.2 将User对象转为HashMap存储
        // 不要将所有用户信息放到session，这样并不安全
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));    // 这里得将HashMap中的字段值转成字符串
        // 7.3 存储到redis
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        // 7.4 设置token的有效期
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 返回token
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        // 1. 创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));

        // 2. 保存用户到数据库
        save(user);
        return user;
    }
}
