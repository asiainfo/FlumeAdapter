package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.RedisConstants;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisSourceConstants extends RedisConstants{
    public static final String HOST = REDIS_PREFIX + "host";
    public static final String PORT = REDIS_PREFIX + "port";
    public static final String TIMEOUT = REDIS_PREFIX + "timeout";

    public static final String INTERVAL = REDIS_PREFIX + "interval";
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

    public static final String THREADS_MONITOR_INTERVAL = REDIS_PREFIX + "threadsMonitorInterval";


    public static final String ALGORITHM = REDIS_PREFIX + "algorithm";
    public final static String REDIS_BATCH_SIZE = REDIS_PREFIX + "redisBatchSize";

    public static final String DEFAULT_SEPARATOR = ",";
        public static final int DEFAULT_REDIS_BATCH_SIZE = 100;

    public static final String DEFAULT_ALGORITHM = "default";

    public static final int DEFAULT_THREAD_POOL_SIZE = 8;
    public static final long DEFAULT_THREADS_MONITOR_INTERVAL_MS = 10000L;


}
