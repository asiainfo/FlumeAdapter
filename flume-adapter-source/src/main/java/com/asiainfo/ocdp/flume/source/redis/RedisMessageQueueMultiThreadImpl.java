package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.MessageQueue;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisMessageQueueMultiThreadImpl extends MessageQueue<String> {
    private final static Logger logger = Logger.getLogger(RedisMessageQueueMultiThreadImpl.class);

    private JedisPool jedisPool;
    private Map<String, String> properties;

    public RedisMessageQueueMultiThreadImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        properties = new HashedMap();
    }

    @Override
    public String takeMessage() {
        String msg = "";
        if (messages.isEmpty()){
            return msg;
        }

        try {
            msg = messages.take();
        } catch (Exception e) {
            logger.error("Get messages failed.", e);
        }

        return msg;
    }

    @Override
    public boolean produceMessage() {
        Jedis jedis = null;
        try
        {
            if (jedisPool == null){
                logger.error("Cannot get jedis pool.");
                return false;
            }
            jedis = jedisPool.getResource();

            long startTime = System.currentTimeMillis();
            Set<String> keySet = jedis.keys(StringUtils.trimToEmpty(properties.get(RedisSourceConstants.KEY_PREFIX)) + "*");
            long getAllKeysTime = System.currentTimeMillis();

            logger.info("Take " + (getAllKeysTime - startTime) + " ms to get " + keySet.size() + " keys.");

            Object[] keys = keySet.toArray();

            int keyNum = keys.length;

            ForkJoinPool taskPool = new ForkJoinPool(NumberUtils.toInt(properties.get(RedisSourceConstants.THREAD_POOL_SIZE)));

            taskPool.submit(new RedisMessageTask(0, keyNum - 1, keys, jedisPool, properties, messages));

            taskPool.shutdown();


            while (!taskPool.awaitTermination(NumberUtils.toInt(properties.get(RedisSourceConstants.THREADS_MONITOR_INTERVAL)), TimeUnit.MILLISECONDS) ){
                logger.debug("There are <" + taskPool.getParallelism() + "> threads running at the same time.");
                logger.debug("Left <" + (taskPool.getRunningThreadCount()) + "> running thread.");
            }

            return true;
        }catch (Exception e){
            logger.error("Unexpected error producing messages.", e);
        }finally {
            if (jedis != null){
                jedis.close();
            }
        }

        return false;
    }


    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
