package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.FlumeRedisUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RecursiveAction;

/**
 * Created by peng on 2016/11/11.
 */
public class RedisMessageTask extends RecursiveAction {
    private final static Logger logger = Logger.getLogger(RedisMessageTask.class);
    private int start;
    private int end;
    private Object[] keys;

    private JedisPool jedisPool;
    private Map<String, String> properties;
    private BlockingQueue<String> messages;

    public RedisMessageTask(int start, int end, Object[] keys, JedisPool jedisPool, Map<String, String> properties, BlockingQueue<String> messages) {
        this.start = start;
        this.end = end;
        this.keys = keys;
        this.jedisPool = jedisPool;
        this.properties = properties;
        this.messages = messages;
    }


    @Override
    protected void compute() {
        if (end - start > NumberUtils.toInt(properties.get(RedisSourceConstants.REDIS_BATCH_SIZE))){
            int mid = (start + end) / 2;

            RedisMessageTask leftTask = new RedisMessageTask(start, mid, keys, jedisPool, properties, messages);
            RedisMessageTask rightTask = new RedisMessageTask(mid, end, keys, jedisPool, properties, messages);

            this.invokeAll(leftTask, rightTask);
        }
        else {
            Jedis jedis = null;
            try{
                long startTime=System.currentTimeMillis();

                jedis = jedisPool.getResource();
                Pipeline pipeline = jedis.pipelined();

                for (int i = start; i <= end; i++) {
                    String index = (String) keys[i];
                    pipeline.hgetAll(index);
                }

                List<Object> kvList = pipeline.syncAndReturnAll();
                long endTime=System.currentTimeMillis();

                logger.debug("Take " + (endTime - startTime) + "ms to get " + kvList.size() + " values.");

                for (Object m : kvList) { // go through every row

                    Map<String, String> allColumnDataMap = (Map<String, String>) m; // a row in codis, the key is col name

                    if (StringUtils.isNotEmpty(properties.get(RedisSourceConstants.SCHEMA))){
                        String[] headers = properties.get(RedisSourceConstants.SCHEMA).split(",");//all table columns

                        StringBuilder bs = new StringBuilder();
                        for (String _header : headers) {
                            String value = StringUtils.trimToEmpty(allColumnDataMap.get(_header.trim()));
                            bs.append(value).append(properties.get(RedisSourceConstants.SEPARATOR));
                        }

                        messages.put(FlumeRedisUtils.removeLastSeparator(bs, properties.get(RedisSourceConstants.SEPARATOR)));
                    }
                }
            }catch (Exception e){
                logger.error("Unexpected error producing messages.", e);
            }finally {
                if (jedis != null){
                    jedis.close();
                }
            }
        }
    }
}
