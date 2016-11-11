package com.asiainfo.ocdp.flume.adapter.core.redis;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public interface JedisPoolFactory {

	JedisPool create(JedisPoolConfig jedisPoolConfig, String host, Integer port, Integer timeout);
	
}
