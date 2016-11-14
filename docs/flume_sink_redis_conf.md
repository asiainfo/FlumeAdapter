# Flume Adapter

Redis Sink
-------

For example, a redis sink for agent named a1:
```
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /tmp/spooldir
a1.sources.r1.basenameHeader = true
a1.sources.r1.basenameHeaderKey = redis.sourceFileName

# Describe the sink
a1.sinks.k1.type = com.asiainfo.ocdp.flume.sink.redis.RedisSink
a1.sinks.k1.redis.zkAddress = ochadoop02:2181
a1.sinks.k1.redis.zkProxyDir = /zk/codis/db_codis-ha-demo/proxy
a1.sinks.k1.redis.zkSessionTimeout = 30000
a1.sinks.k1.redis.schema = acyc_id,imsi,area_code,age_level,city_code
a1.sinks.k1.redis.keyPrefix = user_base_info
a1.sinks.k1.redis.foreignKeys = imsi
a1.sinks.k1.redis.hashFields = imsi,area_code
a1.sinks.k1.redis.handlerClass = com.asiainfo.ocdp.flume.sink.redis.SingleForeignKeysAssemblyImpl
a1.sinks.k1.redis.sourceFileName = a.txt
a1.sinks.k1.redis.threadsPoolSize = 3


# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 100000
a1.channels.c1.transactionCapacity = 10000

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```