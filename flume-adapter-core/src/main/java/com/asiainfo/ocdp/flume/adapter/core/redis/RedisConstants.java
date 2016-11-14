package com.asiainfo.ocdp.flume.adapter.core.redis;

/**
 * Created by peng on 2016/11/14.
 */
public class RedisConstants {
    public static final String REDIS_PREFIX = "redis.";
    public static final String SCHEMA = REDIS_PREFIX + "schema";
    public static final String KEY_PREFIX = REDIS_PREFIX + "keyPrefix";
    public static final String BATCH_SIZE = REDIS_PREFIX + "flumeBatchSize";
    public static final String THREAD_POOL_SIZE = REDIS_PREFIX + "threadsPoolSize";

    public static final int DEFAULT_BATCH_SIZE = 1;
}
