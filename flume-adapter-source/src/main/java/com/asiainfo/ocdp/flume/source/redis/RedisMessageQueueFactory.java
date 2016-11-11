package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.MessageQueue;
import redis.clients.jedis.JedisPool;

/**
 * Created by peng on 2016/11/11.
 */
public enum RedisMessageQueueFactory {
    Scan {
        @Override
        public MessageQueue<String> create(JedisPool jedisPool) {
            return new RedisMessageQueueScanImpl(jedisPool);
        }
    },
    MultithreadScan {
        @Override
        public MessageQueue<String> create(JedisPool jedisPool) {
            return new RedisMessageQueueMultiThreadScanImpl(jedisPool);
        }
    },
    Default {
        @Override
        public MessageQueue<String> create(JedisPool jedisPool) {
            return new RedisMessageQueueImpl(jedisPool);
        }
    },
    MultithreadDefault {
        @Override
        public MessageQueue<String> create(JedisPool jedisPool) {
            return new RedisMessageQueueMultiThreadImpl(jedisPool);
        }
    };

    public abstract MessageQueue<String> create(JedisPool jedisPool);
}
