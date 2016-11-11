package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.MessageQueue;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisMessageQueueImpl extends MessageQueue<String> {
    private final static Logger logger = Logger.getLogger(RedisMessageQueueImpl.class);

    private JedisPool jedisPool;
    private Map<String, String> properties;

    public RedisMessageQueueImpl(JedisPool jedisPool) {
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
            Pipeline pipeline = jedis.pipelined();

            long startTime = System.currentTimeMillis();
            Set<String> keys = jedis.keys(StringUtils.trimToEmpty(properties.get(RedisSourceConstants.KEY_PREFIX)) + "*");
            long getAllKeysTime = System.currentTimeMillis();

            logger.info("Take " + (getAllKeysTime - startTime) + " ms to get " + keys.size() + " keys.");

            for (String index : keys) {
                pipeline.hgetAll(index);
            }

            List<Object> kvList = pipeline.syncAndReturnAll();

            long getAllMessagesTime = System.currentTimeMillis();
            logger.info("Take " + (getAllMessagesTime - getAllKeysTime) + " ms to get all messages.");

            for (Object m : kvList) { // go through every row

                Map<String, String> allColumnDataMap = (Map<String, String>) m; // a row in codis, the key is col name

                if (StringUtils.isNotEmpty(properties.get(RedisSourceConstants.SCHEMA))){
                    String[] headers = properties.get(RedisSourceConstants.SCHEMA).split(",");//all table columns

                    StringBuilder bs = new StringBuilder();
                    for (String _header : headers) {
                        String value = StringUtils.trimToEmpty(allColumnDataMap.get(_header.trim()));
                        bs.append(value).append(properties.get(RedisSourceConstants.SEPARATOR));
                    }

                    int lastIndexSeparator = bs.lastIndexOf(properties.get(RedisSourceConstants.SEPARATOR));
                    String msg = bs.toString();
                    if (lastIndexSeparator > 0){
                        msg = bs.deleteCharAt(lastIndexSeparator).toString();
                    }

                    messages.put(msg);
                }
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
