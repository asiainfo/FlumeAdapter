package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.MessageQueue;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisSource extends AbstractPollableSource {
    private final static Logger logger = Logger.getLogger(RedisSource.class);

    /**
     * Configuration attributes
     */
    private String host = null;
    private Integer port = null;
    private int timeout;
    private String keyPrefix = null;
    private int batchSize;

    private Integer maxTotal = null;
    private Integer maxIdle = null;
    private Integer minIdle = null;
    private Integer numTestsPerEvictionRun = null;
    private Long maxWait = null;
    private Long minEvictableIdleTimeMillis = null;
    private Long softMinEvictableIdleTimeMillis = null;
    private Long timeBetweenEvictionRunsMillis = null;
    private Boolean testOnBorrow = null;
    private Boolean testOnReturn = null;
    private Boolean testWhileIdle = null;
    private long redisInterval_ms = 300000;

    private JedisPool jedisPool = null;
    private ExecutorService threadPool = null;
    private MessageQueue<String> msgQueue = null;
    private final List<Event> eventList = new ArrayList<Event>();

    private SourceCounter sourceCounter = null;
    private String schema = null;
    private String separator = null;
    private Integer redisBatchSize = null;
    private String algorithm = null;
    private Integer threadsPoolSize = null;
    private Long threadsMonitorInterval = null;

    @Override
    protected Status doProcess() throws EventDeliveryException {

        try
        {

            while (eventList.size() < batchSize){
                String msg = msgQueue.takeMessage();

                if (StringUtils.isNotEmpty(msg)) {
                    eventList.add(EventBuilder.withBody(msg, Charset.forName("UTF-8")));
                }

            }

            int size = eventList.size();
            if (size == 0) {
                return Status.BACKOFF;
            }

            sourceCounter.incrementAppendBatchReceivedCount();
            sourceCounter.addToEventReceivedCount(size);
            getChannelProcessor().processEventBatch(eventList);
            sourceCounter.addToEventAcceptedCount(size);
            sourceCounter.incrementAppendBatchAcceptedCount();

            eventList.clear();

            return Status.READY;
        } catch (Exception e) {
            logger.error("Error receiving events", e);
        }
        return Status.BACKOFF;
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        host = context.getString(RedisSourceConstants.HOST);

        assertNotEmpty(host, "host cannot be empty, please specify in configuration file");

        port = context.getInteger(RedisSourceConstants.PORT, Protocol.DEFAULT_PORT);
        timeout = context.getInteger(RedisSourceConstants.TIMEOUT, Protocol.DEFAULT_TIMEOUT);

        keyPrefix = context.getString(RedisSourceConstants.KEY_PREFIX);
        batchSize = context.getInteger(RedisSourceConstants.BATCH_SIZE, RedisSourceConstants.DEFAULT_BATCH_SIZE);


        maxIdle = context.getInteger(RedisSourceConstants.REDIS_MAX_IDLE);
        minIdle = context.getInteger(RedisSourceConstants.REDIS_MIN_IDLE);
        numTestsPerEvictionRun = context.getInteger(RedisSourceConstants.REDIS_NUM_TESTS_PER_EVICTION_RUN);
        maxWait = context.getLong(RedisSourceConstants.REDIS_MAX_WAIT);
        minEvictableIdleTimeMillis = context.getLong(RedisSourceConstants.REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        softMinEvictableIdleTimeMillis = context.getLong(RedisSourceConstants.REDIS_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        testOnBorrow = context.getBoolean(RedisSourceConstants.REDIS_TEST_ON_BORROW);
        testOnReturn = context.getBoolean(RedisSourceConstants.REDIS_TEST_ON_RETURN);
        testWhileIdle = context.getBoolean(RedisSourceConstants.REDIS_TEST_WHILE_IDLE);
        timeBetweenEvictionRunsMillis = context.getLong(RedisSourceConstants.REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        schema = context.getString(RedisSourceConstants.SCHEMA);
        redisInterval_ms = context.getLong(RedisSourceConstants.INTERVAL);
        separator = context.getString(RedisSourceConstants.SEPARATOR, RedisSourceConstants.DEFAULT_SEPARATOR);

        redisBatchSize = context.getInteger(RedisSourceConstants.REDIS_BATCH_SIZE, RedisSourceConstants.DEFAULT_REDIS_BATCH_SIZE);
        algorithm = context.getString(RedisSourceConstants.ALGORITHM);
        threadsPoolSize = context.getInteger(RedisSourceConstants.THREAD_POOL_SIZE, RedisSourceConstants.DEFAULT_THREAD_POOL_SIZE);
        threadsMonitorInterval = context.getLong(RedisSourceConstants.THREADS_MONITOR_INTERVAL, RedisSourceConstants.DEFAULT_THREADS_MONITOR_INTERVAL_MS);

        Preconditions.checkState(batchSize > 0, RedisSourceConstants.BATCH_SIZE + " parameter must be greater than 1");

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

    }

    @Override
    protected void doStart() throws FlumeException {
        if (jedisPool != null) {
            jedisPool.destroy();
        }

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

        sourceCounter.start();

        jedisPool = new JedisPool(initPoolConfig(), host, port, timeout);

        if (StringUtils.equalsIgnoreCase(StringUtils.trimToEmpty(algorithm), RedisMessageQueueFactory.Scan.name())){
            msgQueue = RedisMessageQueueFactory.Scan.create(jedisPool);
        }
        else if (StringUtils.equalsIgnoreCase(StringUtils.trimToEmpty(algorithm), RedisMessageQueueFactory.MultithreadDefault.name())){
            msgQueue = RedisMessageQueueFactory.MultithreadDefault.create(jedisPool);
        }
        else if (StringUtils.equalsIgnoreCase(StringUtils.trimToEmpty(algorithm), RedisMessageQueueFactory.MultithreadScan.name())){
            msgQueue = RedisMessageQueueFactory.MultithreadScan.create(jedisPool);
        }
        else {
            msgQueue = RedisMessageQueueFactory.Default.create(jedisPool);
        }

        msgQueue.getProperties().put(RedisSourceConstants.SCHEMA, schema);
        msgQueue.getProperties().put(RedisSourceConstants.KEY_PREFIX, keyPrefix);
        msgQueue.getProperties().put(RedisSourceConstants.SEPARATOR, separator);
        msgQueue.getProperties().put(RedisSourceConstants.REDIS_BATCH_SIZE, String.valueOf(redisBatchSize));
        msgQueue.getProperties().put(RedisSourceConstants.THREAD_POOL_SIZE, String.valueOf(threadsPoolSize));
        msgQueue.getProperties().put(RedisSourceConstants.THREADS_MONITOR_INTERVAL, String.valueOf(threadsMonitorInterval));

        threadPool = Executors.newSingleThreadExecutor();
        threadPool.execute(new RedisMessageFactory(msgQueue, redisInterval_ms));
    }

    @Override
    protected void doStop() throws FlumeException {
        if (threadPool != null){
            logger.info("Shutdown thread pool");
            threadPool.shutdown();
        }

        if (jedisPool != null){
            jedisPool.destroy();
        }

        sourceCounter.stop();
    }


    private void assertNotEmpty(String arg, String msg) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(arg), msg);
    }


    private JedisPoolConfig initPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        if (maxTotal != null) {
            jedisPoolConfig.setMaxTotal(maxTotal);
        }else {
            jedisPoolConfig.setMaxTotal(100);
        }


        if (maxIdle != null) {
            jedisPoolConfig.setMaxIdle(maxIdle);
        }else {
            jedisPoolConfig.setMaxIdle(100);
        }

        if (maxWait != null) {
            jedisPoolConfig.setMaxWaitMillis(maxWait);
        }else {
            jedisPoolConfig.setMaxWaitMillis(10000L);
        }

        if (minEvictableIdleTimeMillis != null) {
            jedisPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        }else {
            jedisPoolConfig.setMinEvictableIdleTimeMillis(1800000L);
        }


        if (minIdle != null) {
            jedisPoolConfig.setMinIdle(minIdle);
        }
        if (numTestsPerEvictionRun != null) {
            jedisPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        }

        if (softMinEvictableIdleTimeMillis != null) {
            jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
        }else {
            jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(1800000L);
        }

        if (testOnBorrow != null) {
            jedisPoolConfig.setTestOnBorrow(testOnBorrow);
        }else {
            jedisPoolConfig.setTestOnBorrow(true);
        }

        if (testOnReturn != null) {
            jedisPoolConfig.setTestOnReturn(testOnReturn);
        }else {
            jedisPoolConfig.setTestOnReturn(true);
        }

        if (testWhileIdle != null) {
            jedisPoolConfig.setTestWhileIdle(testWhileIdle);
        }else {
            jedisPoolConfig.setTestWhileIdle(true);
        }

        if (timeBetweenEvictionRunsMillis != null) {
            jedisPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        }

        return jedisPoolConfig;
    }
}
