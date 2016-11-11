package com.asiainfo.ocdp.flume.source.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.JedisPoolFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolFactoryImpl implements JedisPoolFactory {
	public JedisPool create(JedisPoolConfig jedisPoolConfig, String host, Integer port, Integer timeout) {
		JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout);
		return jedisPool;
	}

}
