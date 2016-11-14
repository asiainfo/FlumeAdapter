package com.asiainfo.ocdp.flume.adapter.core.redis;

import io.codis.jodis.JedisResourcePool;
import io.codis.jodis.RoundRobinJedisPool;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolFactory {
	public JedisPool create(JedisPoolConfig jedisPoolConfig, String host, Integer port, Integer timeout) {
        return new JedisPool(jedisPoolConfig, host, port, timeout);
    }

    public JedisResourcePool create(JedisPoolConfig jedisPoolConfig, String zkAddress, String zkProxyDir, Integer timeout) {
        return RoundRobinJedisPool.create()
                .poolConfig(jedisPoolConfig)
                .curatorClient(zkAddress, timeout)
                .zkProxyDir(zkProxyDir).build();
    }
}
