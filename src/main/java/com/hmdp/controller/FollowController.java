package com.hmdp.controller;


import cn.hutool.log.Log;
import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {

    @Resource
    private IFollowService followService;

    /**
     * 关注或取关
     * @param followUserId 要关注或取关的用户id
     * @param isFollow 是要关注还是取关
     * @return
     */
    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long followUserId, @PathVariable("isFollow") Boolean isFollow) {
        return followService.follow(followUserId, isFollow);
    }


    /**
     * 当前用户是否已关注某个用户
     * @param followUserId
     * @return
     */
    @GetMapping("/or/not/{id}")
    public Result followOrNot(@PathVariable("id") Long followUserId) {
        return followService.followOrNot(followUserId);
    }

    /**
     * 查看共同关注
     * @param id
     * @return
     */
    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long id) {
        return followService.followCommons(id);
    }

}
