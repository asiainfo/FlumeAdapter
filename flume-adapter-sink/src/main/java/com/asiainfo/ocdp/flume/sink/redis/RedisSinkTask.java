package com.asiainfo.ocdp.flume.sink.redis;

import io.codis.jodis.JedisResourcePool;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;

/**
 * Created by peng on 2016/11/14.
 */
public class RedisSinkTask implements Runnable {
    private final static Logger logger = Logger.getLogger(RedisSinkTask.class);

    private Assembly assembly = null;
    private JedisResourcePool jedisPool = null;
    private List<String> batchEvents = null;


    public RedisSinkTask(JedisResourcePool jedisPool, Assembly assembly, List<String> batchEvents) {
        this.jedisPool = jedisPool;
        this.assembly = assembly;
        this.batchEvents = batchEvents;
    }


    @Override
    public void run() {

        Jedis jedis;
        try {
            jedis = jedisPool.getResource();
        }catch (Exception e){
            logger.error("Can not get resource from JedisResourcePool.", e);
            return;
        }

        Pipeline pipeline = jedis.pipelined();

        try {
            for (String rowVale : batchEvents){
                if (assembly != null) {
                    assembly.setRowValue(rowVale).init();

                    if (assembly.execute()){

                        Map<String, Map<String, String>> hmset = assembly.getHmset();

                        for (Map.Entry<String, Map<String, String>> entry : hmset.entrySet()) {
                            String hmsetKey = entry.getKey();
                            Map<String, String> hmsetValue = entry.getValue();
                            jedis.hmset(hmsetKey, hmsetValue);
                        }

                        pipeline.syncAndReturnAll();
                    }

                } else {
                    logger.error("Unknown error, please check schema configuration.");
                }
            }



        } catch (Exception e) {
            logger.error(e);
        }finally {
            if (jedis != null){
                jedis.close();
            }
        }

    }
}
