package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";

    // 用户token过期时间（开发测试时设置成了10天）
    // public static final Long LOGIN_USER_TTL = 30L;
    public static final Long LOGIN_USER_TTL = 14400L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final Long CACHE_SHOP_TYPE_TTL = 30L;
    public static final String CACHE_SHOP_TYPE_KEY = "cache:shop:type";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final String LOCK_KEY_PREFIX = "lock:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
    public static final String INCREMENT_ID_KEY = "icr:";
    public static final String ORDER_PREFIX = "order:";
    public static final String FOLLOWS_KEY = "follows:";
    // 滚动分页的pageSize
    public static final Long SCORE_PAGE_SIZE = 2L;

}
