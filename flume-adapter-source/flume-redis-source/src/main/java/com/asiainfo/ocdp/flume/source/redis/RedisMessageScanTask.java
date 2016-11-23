package com.asiainfo.ocdp.flume.source.redis;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by peng on 2016/11/11.
 */
public class RedisMessageScanTask implements Runnable {
    private final static Logger logger = Logger.getLogger(RedisMessageScanTask.class);
    private List<String> keys;

    private Jedis jedis;
    private Map<String, String> properties;
    private BlockingQueue<String> messages;

    public RedisMessageScanTask(List<String> keys, Jedis jedis, Map<String, String> properties, BlockingQueue<String> messages) {
        this.keys = keys;
        this.jedis = jedis;
        this.properties = properties;
        this.messages = messages;
    }


    @Override
    public void run() {
        try {
            Pipeline pipeline = jedis.pipelined();
            for (String index : keys) {
                pipeline.hgetAll(index);
            }

            List<Object> kvList = pipeline.syncAndReturnAll();

            for (Object m : kvList) { // go through every row

                Map<String, String> allColumnDataMap = (Map<String, String>) m; // a row in codis, the key is col name

                if (StringUtils.isNotEmpty(properties.get(RedisSourceConstants.SCHEMA))) {
                    String[] headers = properties.get(RedisSourceConstants.SCHEMA).split(",");//all table columns

                    StringBuilder bs = new StringBuilder();
                    for (String _header : headers) {
                        String value = StringUtils.trimToEmpty(allColumnDataMap.get(_header.trim()));
                        bs.append(value).append(properties.get(RedisSourceConstants.SEPARATOR));
                    }

                    int lastIndexSeparator = bs.lastIndexOf(properties.get(RedisSourceConstants.SEPARATOR));
                    String msg = bs.toString();
                    if (lastIndexSeparator > 0) {
                        msg = bs.deleteCharAt(lastIndexSeparator).toString();
                    }

                    messages.put(msg);
                }
            }
        } catch (Exception e) {
            logger.error("Unexpected error producing messages.", e);
        }
    }

}
