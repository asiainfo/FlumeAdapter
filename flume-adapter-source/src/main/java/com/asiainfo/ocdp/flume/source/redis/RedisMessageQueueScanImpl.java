package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.MessageQueue;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisMessageQueueScanImpl extends MessageQueue<String> {
    private final static Logger logger = Logger.getLogger(RedisMessageQueueScanImpl.class);

    private JedisPool jedisPool;
    private Map<String, String> properties;

    public RedisMessageQueueScanImpl(JedisPool jedisPool) {
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

            int scanCount = NumberUtils.toInt(properties.get(RedisSourceConstants.SCAN_COUNT));

            ScanParams scanParams = new ScanParams().count(scanCount);
            scanParams.match(StringUtils.trimToEmpty(properties.get(RedisSourceConstants.KEY_PREFIX)) + "*");
            String cur = redis.clients.jedis.ScanParams.SCAN_POINTER_START;
            boolean cycleIsFinished = false;
            Pipeline pipeline = jedis.pipelined();

            long startTime = System.currentTimeMillis();

            int sum = 0;

            while(!cycleIsFinished){
                ScanResult<String> scanResult = jedis.scan(cur, scanParams);
                List<String> keys = scanResult.getResult();
                cur = scanResult.getStringCursor();
                if (redis.clients.jedis.ScanParams.SCAN_POINTER_START.equals(cur)){
                    cycleIsFinished = true;
                }

                for (String index : keys) {
                    pipeline.hgetAll(index);
                }

                List<Object> kvList = pipeline.syncAndReturnAll();

                sum = sum + kvList.size();

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

            }


            long getAllKeysTime = System.currentTimeMillis();

            logger.info("Take " + (getAllKeysTime - startTime) + " ms to get " + sum + " keys.");

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
