package com.asiainfo.ocdp.flume.sink.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.RedisConstants;

/**
 * Created by peng on 2016/11/14.
 */
public class RedisSinkConstants extends RedisConstants{

    public static final String HANDLER_CLASS = REDIS_PREFIX + "handlerClass";
    public static final String FOREIGN_KEYS = REDIS_PREFIX + "foreignKeys";
    public static final String HASH_FIELDS = REDIS_PREFIX + "hashFields";

    public static final String ZK_ADDRESS = REDIS_PREFIX + "zkAddress";
    public static final String ZK_SESSION_TIMEOUT_MS = REDIS_PREFIX + "zkSessionTimeout";
    public static final String ZK_PROXY_DIR = REDIS_PREFIX + "zkProxyDir";

    public static final String FILE_NAME = REDIS_PREFIX + "sourceFileName";
    public static final String KEY_SEPARATOR = REDIS_PREFIX + "keySeparator";
    public static final String FOREIGNKEYS_SEPARATOR = REDIS_PREFIX + "foreignKeysSeparator";

    public static final String DEFAULT_KEY_SEPARATOR = ":";
    public static final String DEFAULT_FOREIGNKEYS_SEPARATOR = "_";
}
