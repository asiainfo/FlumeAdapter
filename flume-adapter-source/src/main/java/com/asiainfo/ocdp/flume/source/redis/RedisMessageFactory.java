package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.MessageQueue;
import org.apache.log4j.Logger;

/**
 * Created by peng on 2016/11/10.
 */
public class RedisMessageFactory implements Runnable{
    private final static Logger logger = Logger.getLogger(RedisMessageFactory.class);
    private MessageQueue<String> msgQueue;
    private long redisInterval_ms;

    public RedisMessageFactory(MessageQueue<String> msgQueue, long redisInterval_ms) {
        this.msgQueue = msgQueue;
        this.redisInterval_ms = redisInterval_ms;
    }

    public void run() {
        while (true) {

            try {
                msgQueue.produceMessage();
                Thread.sleep(redisInterval_ms);
            } catch (Exception e) {
                logger.error("Produce messages failed.", e);
            }

        }
    }
}
