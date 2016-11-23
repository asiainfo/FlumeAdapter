package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.MessageQueue;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisMessageQueueMultiThreadScanImpl extends MessageQueue<String> {
    private final static Logger logger = Logger.getLogger(RedisMessageQueueMultiThreadScanImpl.class);

    private JedisPool jedisPool;
    private Map<String, String> properties;

    public RedisMessageQueueMultiThreadScanImpl(JedisPool jedisPool) {
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
        ExecutorService taskPool = null;
        try
        {
            if (jedisPool == null){
                logger.error("Cannot get jedis pool.");
                return false;
            }
            jedis = jedisPool.getResource();

            int scanCount = NumberUtils.toInt(properties.getOrDefault(RedisSourceConstants.REDIS_BATCH_SIZE, RedisSourceConstants.DEFAULT_REDIS_BATCH_SIZE));

            ScanParams scanParams = new ScanParams().count(scanCount);
            scanParams.match(StringUtils.trimToEmpty(properties.get(RedisSourceConstants.KEY_PREFIX)) + "*");
            String cur = ScanParams.SCAN_POINTER_START;
            boolean cycleIsFinished = false;

            long startTime = System.currentTimeMillis();

            taskPool = Executors.newFixedThreadPool(NumberUtils.toInt(properties.get(RedisSourceConstants.THREAD_POOL_SIZE)));

            while(!cycleIsFinished){
                ScanResult<String> scanResult = jedis.scan(cur, scanParams);
                List<String> keys = scanResult.getResult();

                cur = scanResult.getStringCursor();
                if (ScanParams.SCAN_POINTER_START.equals(cur)){
                    cycleIsFinished = true;
                }

                taskPool.execute(new RedisMessageScanTask(keys, jedis, properties, messages));
            }

            taskPool.shutdown();

            while (!taskPool.awaitTermination(NumberUtils.toInt(properties.get(RedisSourceConstants.THREADS_MONITOR_INTERVAL)), TimeUnit.MILLISECONDS) ){
                logger.debug("The task pool is running.");
            }

            long getAllKeysTime = System.currentTimeMillis();

            logger.info("Take " + (getAllKeysTime - startTime) + " ms to get all keys.");

            return true;
        }catch (Exception e){
            logger.error("Unexpected error producing messages.", e);
        }finally {
            if (jedis != null){
                jedis.close();
            }

            if (taskPool != null && !taskPool.isShutdown()){
                taskPool.shutdownNow();
            }
        }

        return false;
    }


    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
