package com.asiainfo.ocdp.flume.source.redis;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisSourceConstants {
    public static final String REDIS_PREFIX = "redis.";
    public static final String HOST = REDIS_PREFIX + "host";
    public static final String PORT = REDIS_PREFIX + "port";
    public static final String TIMEOUT = REDIS_PREFIX + "timeout";
    public static final String SCHEMA = REDIS_PREFIX + "schema";
    public static final String KEY_PREFIX = REDIS_PREFIX + "key_prefix";
    public static final String INTERVAL = REDIS_PREFIX + "interval";
    public static final String BATCH_SIZE = REDIS_PREFIX + "batch_size";
    public static final String REDIS_MAX_IDLE = REDIS_PREFIX + "max_idle";
    public static final String REDIS_MIN_IDLE = REDIS_PREFIX + "min_idle";
    public static final String REDIS_MAX_WAIT = REDIS_PREFIX + "redis_max_wait";
    public static final String REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS = REDIS_PREFIX + "minEvictableIdleTimeMillis";
    public static final String REDIS_NUM_TESTS_PER_EVICTION_RUN = REDIS_PREFIX + "numTestsPerEvictionRun";
    public static final String REDIS_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = REDIS_PREFIX + "softMinEvictableIdleTimeMillis";
    public static final String REDIS_TEST_ON_BORROW = REDIS_PREFIX + "test_on_borrow";
    public static final String REDIS_TEST_ON_RETURN = REDIS_PREFIX + "test_on_return";
    public static final String REDIS_TEST_WHILE_IDLE = REDIS_PREFIX + "test_while_idle";
    public static final String REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS = REDIS_PREFIX + "timeBetweenEvictionRunsMillis";
    public static final String SEPARATOR = REDIS_PREFIX + "separator";
    public static final String SCAN_COUNT = REDIS_PREFIX + "scan.count";

    public static final String DEFAULT_SEPARATOR = ",";
    public static final int DEFAULT_BATCH_SIZE = 1;
    public static final int DEFAULT_SCAN_COUNT = 100;

}
